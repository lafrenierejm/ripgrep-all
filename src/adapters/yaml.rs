use super::writing::{WritingFileAdapter, async_writeln};
use super::{AdapterMeta, FastFileMatcher, FileMatcher, GetMetadata};
use anyhow::{Context, Result};
use async_trait::async_trait;
use granit_parser::{Event, Options, Parser, ScalarStyle, ScanError, Span};
use lazy_static::lazy_static;
use log::warn;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

static EXTENSIONS: &[&str] = &["yaml", "yml"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "yaml".to_owned(),
        version: 1,
        description:
            "Converts YAML files into a gron-like format with dot-delimited paths for nested keys"
                .to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: Some(vec![
            FileMatcher::MimeType("application/yaml".to_owned()),
            FileMatcher::MimeType("application/x-yaml".to_owned()),
            FileMatcher::MimeType("text/yaml".to_owned()),
            FileMatcher::MimeType("text/x-yaml".to_owned()),
        ]),
        keep_fast_matchers_if_accurate: false,
        disabled_by_default: false
    };
}

#[derive(Default, Clone)]
pub struct YamlAdapter;

impl YamlAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for YamlAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl WritingFileAdapter for YamlAdapter {
    async fn adapt_write(
        mut a: super::AdaptInfo,
        _detection_reason: &FileMatcher,
        mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()> {
        // Read the entire YAML content
        let mut content = String::new();
        a.inp
            .read_to_string(&mut content)
            .await
            .context("Failed to read YAML content")?;

        // Parse YAML with span information
        match parse_documents(&content) {
            Ok(docs) => {
                if docs.is_empty() {
                    // Empty document, write nothing
                    return Ok(());
                }

                // Build line-to-output mapping
                let mut line_map: HashMap<usize, String> = HashMap::new();
                let source_lines: Vec<&str> = content.lines().collect();

                for doc in &docs {
                    flatten_yaml_value(doc, String::new(), &source_lines, &mut line_map)?;
                }

                // Output line by line, matching the original line count
                for line_num in 0..source_lines.len() {
                    if let Some(output) = line_map.get(&line_num) {
                        async_writeln!(oup, "{}", output)?;
                    } else {
                        // Empty line to maintain line count
                        async_writeln!(oup)?;
                    }
                }
            }
            Err(e) => {
                // Log warning and pass through original content
                warn!(
                    "Failed to parse YAML file '{}': {}. Passing through unmodified.",
                    a.filepath_hint.display(),
                    e
                );
                oup.write_all(content.as_bytes()).await?;
            }
        }

        Ok(())
    }
}

/// A scalar resolved according to the YAML 1.2 core schema.
enum Scalar {
    String(String),
    Integer(i64),
    FloatingPoint(f64),
    Boolean(bool),
    Null,
}

enum YamlData {
    Sequence(Vec<Node>),
    Mapping(Vec<(Node, Node)>),
    Value(Scalar),
    Alias,
}

/// A YAML node annotated with the (1-indexed) lines it spans.
struct Node {
    data: YamlData,
    start_line: usize,
    end_line: usize,
    /// 0-indexed column of the span's start.
    start_col: usize,
    /// 0-indexed column of the span's (exclusive) end.
    end_col: usize,
    /// Source style, for scalars only.
    style: Option<ScalarStyle>,
}

fn resolve_scalar(value: String, style: ScalarStyle, tagged: bool) -> Scalar {
    if style != ScalarStyle::Plain || tagged {
        return Scalar::String(value);
    }
    match value.as_str() {
        "" | "~" | "null" | "Null" | "NULL" => return Scalar::Null,
        "true" | "True" | "TRUE" => return Scalar::Boolean(true),
        "false" | "False" | "FALSE" => return Scalar::Boolean(false),
        ".inf" | ".Inf" | ".INF" | "+.inf" | "+.Inf" | "+.INF" => {
            return Scalar::FloatingPoint(f64::INFINITY);
        }
        "-.inf" | "-.Inf" | "-.INF" => return Scalar::FloatingPoint(f64::NEG_INFINITY),
        ".nan" | ".NaN" | ".NAN" => return Scalar::FloatingPoint(f64::NAN),
        _ => {}
    }
    let digits_only = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(&value);
    if digits_only(unsigned) {
        if let Ok(i) = value.parse::<i64>() {
            return Scalar::Integer(i);
        }
    } else if let Some(hex) = value.strip_prefix("0x") {
        if let Ok(i) = i64::from_str_radix(hex, 16) {
            return Scalar::Integer(i);
        }
    } else if let Some(oct) = value.strip_prefix("0o")
        && let Ok(i) = i64::from_str_radix(oct, 8)
    {
        return Scalar::Integer(i);
    }
    if value
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'e' | b'E'))
        && value.bytes().any(|b| b.is_ascii_digit())
        && let Ok(f) = value.parse::<f64>()
    {
        return Scalar::FloatingPoint(f);
    }
    Scalar::String(value)
}

/// Parse every document in `content` into span-annotated trees.
fn parse_documents(content: &str) -> std::result::Result<Vec<Node>, ScanError> {
    let mut options = Options::default();
    options.emit_comments = false;
    let mut parser = Parser::new_from_str_with_options(content, options);
    let mut docs = Vec::new();
    while let Some(item) = parser.next_event() {
        let (ev, span) = item?;
        match ev {
            Event::StreamStart
            | Event::StreamEnd
            | Event::DocumentStart(..)
            | Event::DocumentEnd => {}
            other => docs.push(build_node(&mut parser, other, span)?),
        }
    }
    Ok(docs)
}

fn build_node<'a>(
    parser: &mut Parser<'a, granit_parser::StrInput<'a>>,
    ev: Event<'a>,
    span: Span,
) -> std::result::Result<Node, ScanError> {
    let start_line = span.start.line();
    let start_col = span.start.col();
    match ev {
        Event::Scalar(value, style, _, tag) => Ok(Node {
            data: YamlData::Value(resolve_scalar(value.into_owned(), style, tag.is_some())),
            start_line,
            end_line: span.end.line(),
            start_col,
            end_col: span.end.col(),
            style: Some(style),
        }),
        Event::SequenceStart(..) => {
            let mut items = Vec::new();
            loop {
                let (ev, span) = next_event(parser)?;
                if matches!(ev, Event::SequenceEnd) {
                    return Ok(Node {
                        data: YamlData::Sequence(items),
                        start_line,
                        end_line: span.end.line(),
                        start_col,
                        end_col: span.end.col(),
                        style: None,
                    });
                }
                items.push(build_node(parser, ev, span)?);
            }
        }
        Event::MappingStart(..) => {
            let mut entries = Vec::new();
            loop {
                let (ev, span) = next_event(parser)?;
                if matches!(ev, Event::MappingEnd) {
                    return Ok(Node {
                        data: YamlData::Mapping(entries),
                        start_line,
                        end_line: span.end.line(),
                        start_col,
                        end_col: span.end.col(),
                        style: None,
                    });
                }
                let key = build_node(parser, ev, span)?;
                let (ev, span) = next_event(parser)?;
                let val = build_node(parser, ev, span)?;
                entries.push((key, val));
            }
        }
        // Aliases, unresolved aliases and anything else carry no data of their own.
        _ => Ok(Node {
            data: YamlData::Alias,
            start_line,
            end_line: span.end.line(),
            start_col,
            end_col: span.end.col(),
            style: None,
        }),
    }
}

fn next_event<'a>(
    parser: &mut Parser<'a, granit_parser::StrInput<'a>>,
) -> std::result::Result<(Event<'a>, Span), ScanError> {
    parser.next_event().unwrap_or_else(|| {
        Err(ScanError::new(
            Default::default(),
            "unexpected end of stream",
        ))
    })
}

fn flatten_yaml_value(
    value: &Node,
    path: String,
    source_lines: &[&str],
    line_map: &mut HashMap<usize, String>,
) -> Result<()> {
    flatten_yaml_value_with_key_line(value, path, source_lines, line_map, value.start_line)
}

fn flatten_yaml_value_with_key_line(
    value: &Node,
    path: String,
    source_lines: &[&str],
    line_map: &mut HashMap<usize, String>,
    key_line: usize,
) -> Result<()> {
    // Convert 1-indexed line to 0-indexed
    // Use key_line for values in mappings, otherwise use value's own line
    let line_num = key_line.saturating_sub(1);

    match &value.data {
        YamlData::Sequence(seq) => {
            if seq.is_empty() {
                // Empty array
                let output = format!("{}: []", path);
                line_map.insert(line_num, output);
            } else if value.start_line == value.end_line {
                // Single-line array (flow style): output entire array on one line
                let output = format_flow_array(&path, seq)?;
                line_map.insert(line_num, output);
            } else {
                // Multi-line array: output each element with index
                for (idx, elem) in seq.iter().enumerate() {
                    let indexed_path = format!("{}[{}]", path, idx);
                    // For array elements, use the element's own line
                    flatten_yaml_value_with_key_line(
                        elem,
                        indexed_path,
                        source_lines,
                        line_map,
                        elem.start_line,
                    )?;
                }
            }
        }
        YamlData::Mapping(map) => {
            if map.is_empty() {
                // Empty object
                let output = format!("{}: {{}}", path);
                line_map.insert(line_num, output);
            } else {
                for (key, val) in map {
                    let key_str = scalar_to_string(&key.data);
                    let new_path = if path.is_empty() {
                        key_str
                    } else {
                        format!("{}.{}", path, key_str)
                    };
                    // Use the key's line number for output placement
                    flatten_yaml_value_with_key_line(
                        val,
                        new_path,
                        source_lines,
                        line_map,
                        key.start_line,
                    )?;
                }
            }
        }
        YamlData::Value(scalar) => {
            // Strings covering several source lines (block scalars, multi-line
            // plain or quoted scalars) are emitted line by line instead.
            let is_block = matches!(
                value.style,
                Some(ScalarStyle::Literal | ScalarStyle::Folded)
            );
            let is_multiline = matches!(scalar, Scalar::String(_))
                && (is_block || value.start_line != value.end_line);
            if !(is_multiline && flatten_multiline_string(value, &path, source_lines, line_map)) {
                let value_str = format_scalar_value(scalar);
                let output = format!("{}: {}", path, value_str);
                line_map.insert(line_num, output);
            }
        }
        YamlData::Alias => {
            // YAML aliases are references to anchors
            // For now, just output a placeholder
            let output = format!("{}: <alias>", path);
            line_map.insert(line_num, output);
        }
    }

    Ok(())
}

fn scalar_to_string(data: &YamlData) -> String {
    match data {
        YamlData::Value(scalar) => match scalar {
            Scalar::String(s) => s.clone(),
            Scalar::Integer(i) => i.to_string(),
            Scalar::FloatingPoint(f) => f.to_string(),
            Scalar::Boolean(b) => b.to_string(),
            Scalar::Null => "null".to_string(),
        },
        _ => "unknown".to_string(),
    }
}

fn format_scalar_value(scalar: &Scalar) -> String {
    match scalar {
        Scalar::String(s) => quote_escape(s),
        Scalar::Integer(i) => i.to_string(),
        Scalar::FloatingPoint(f) => f.to_string(),
        Scalar::Boolean(b) => b.to_string(),
        Scalar::Null => "null".to_string(),
    }
}

fn quote_escape(s: &str) -> String {
    // Escape special characters in strings
    let escaped = s
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
        .replace('"', "\\\"");
    format!("\"{}\"", escaped)
}

/// Emits a string scalar that covers several source lines as a newline-delimited
/// array: each source line of the scalar becomes an indexed element placed on the
/// same line it came from. Uses the source text rather than the decoded value so
/// that folded (`>`) and multi-line flow scalars keep their original line layout.
///
/// Returns `false` if no lines could be attributed to the scalar, in which case
/// the caller falls back to single-line output.
fn flatten_multiline_string(
    value: &Node,
    path: &str,
    source_lines: &[&str],
    line_map: &mut HashMap<usize, String>,
) -> bool {
    let is_block = matches!(
        value.style,
        Some(ScalarStyle::Literal | ScalarStyle::Folded)
    );
    let leading_spaces = |line: &str| line.chars().take_while(|c| *c == ' ').count();

    // Convert 1-indexed span lines to 0-indexed. For every style the span starts at the
    // scalar's first character; for block scalars that is the first content line (not
    // the `|`/`>` header), and for flow scalars it is the opening quote.
    let first_line = value.start_line.saturating_sub(1);
    let mut last_line = value.end_line.saturating_sub(1);

    match source_lines.get(last_line) {
        Some(line) => {
            // For block scalars the end marker sits at the start of the token
            // that follows the scalar, not after the scalar's last character.
            // In that case the marker's line is not part of the scalar.
            let indent = line.chars().take_while(|c| c.is_whitespace()).count();
            if value.end_col <= indent {
                last_line = last_line.saturating_sub(1);
            }
        }
        // The marker is past the final line (input ended with a line break).
        None => last_line = source_lines.len().saturating_sub(1),
    }

    // Trailing blank lines carry no searchable text; drop them.
    while last_line > first_line && source_lines[last_line].trim().is_empty() {
        last_line -= 1;
    }

    if last_line < first_line || first_line >= source_lines.len() {
        return false;
    }

    // Block scalars are indented uniformly; take the indentation from the first
    // non-blank content line.
    let Some(block_indent) = source_lines[first_line..=last_line]
        .iter()
        .find(|l| !l.trim().is_empty())
        .map(|l| leading_spaces(l))
    else {
        // No text at all (e.g. an empty block scalar).
        return false;
    };

    for (idx, line_idx) in (first_line..=last_line).enumerate() {
        let line = source_lines[line_idx];
        let text: String = if is_block {
            // Strip the block's indentation while keeping any deeper indentation,
            // which is significant in `|` blocks.
            line.chars()
                .skip(leading_spaces(line).min(block_indent))
                .collect()
        } else if line_idx == first_line {
            // The first line may begin mid-line (e.g. `key: first part`).
            line.chars().skip(value.start_col).collect()
        } else {
            let strip = leading_spaces(line).min(value.start_col);
            line.chars().skip(strip).collect()
        };
        let output = format!("{}[{}]: {}", path, idx, quote_escape(&text));
        line_map.insert(line_idx, output);
    }

    true
}

fn format_flow_array(path: &str, seq: &[Node]) -> Result<String> {
    let elements: Vec<String> = seq
        .iter()
        .map(|elem| match &elem.data {
            YamlData::Value(scalar) => format_scalar_value(scalar),
            _ => "...".to_string(),
        })
        .collect();

    Ok(format!("{}: [{}]", path, elements.join(", ")))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::{assert_eq, assert_str_eq};
    use std::io::Cursor;

    #[tokio::test]
    async fn test_simple_yaml() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"name: John

age: 30
children:
  - Alpha
  - Bravo
siblings:
  - Zulu
  - Yankee
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), yaml_content.lines().count());

        let mut expected = HashMap::new();
        expected.insert(0, "name: \"John\"");
        expected.insert(2, "age: 30");
        expected.insert(4, "children[0]: \"Alpha\"");
        expected.insert(5, "children[1]: \"Bravo\"");
        expected.insert(7, "siblings[0]: \"Zulu\"");
        expected.insert(8, "siblings[1]: \"Yankee\"");

        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_nested_objects() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"person:
  name: Alice
  age: 25
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(1, "person.name: \"Alice\"");
        expected.insert(2, "person.age: 25");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_array_of_objects() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"users:
  - name: Bob
  - name: Carol
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(1, "users[0].name: \"Bob\"");
        expected.insert(2, "users[1].name: \"Carol\"");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_and_null() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"empty_string: ""
null_value: null
empty_array: []
empty_object: {}
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(0, "empty_string: \"\"");
        expected.insert(1, "null_value: null");
        expected.insert(2, "empty_array: []");
        expected.insert(3, "empty_object: {}");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_flow_style_array() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"name: John
tags: [developer, rust, python]
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(0, "name: \"John\"");
        expected.insert(1, "tags: [\"developer\", \"rust\", \"python\"]");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_yaml_with_comments() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"# This is a comment
name: John
# Another comment
age: 30
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(1, "name: \"John\"");
        expected.insert(3, "age: 30");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                // Comments and blank lines remain empty
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_boolean_values() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"enabled: true
disabled: false
yes_value: yes
no_value: no
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();

        // Debug output
        log::debug!(
            "Boolean test - Input lines: {}",
            yaml_content.lines().count()
        );
        log::debug!("Boolean test - Output lines: {}", lines.len());
        for (i, line) in lines.iter().enumerate() {
            log::debug!("Boolean test - Line {}: '{}'", i, line);
        }

        let mut expected = HashMap::new();
        expected.insert(0, "enabled: true");
        expected.insert(1, "disabled: false");
        // In YAML 1.2, "yes" and "no" are strings, not booleans
        expected.insert(2, "yes_value: \"yes\"");
        expected.insert(3, "no_value: \"no\"");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_deeply_nested_structure() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"database:
  host: localhost
  credentials:
    username: admin
    password: secret
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(1, "database.host: \"localhost\"");
        expected.insert(3, "database.credentials.username: \"admin\"");
        expected.insert(4, "database.credentials.password: \"secret\"");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_yaml_passthrough() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let invalid_yaml = r#"This is not valid YAML
{broken: yaml
missing quotes and proper structure
]
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("invalid.yaml"),
            Box::pin(Cursor::new(invalid_yaml.as_bytes())),
        );

        // Should not panic or return an error
        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        // Should pass through the original content unchanged
        assert_str_eq!(output, invalid_yaml);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiline_string() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let yaml_content = r#"description: |
  This is a multiline
  string in YAML
name: test
"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();

        // Debug output
        log::debug!("Input lines: {}", yaml_content.lines().count());
        log::debug!("Output lines: {}", lines.len());
        for (i, line) in lines.iter().enumerate() {
            log::debug!("Line {}: '{}'", i, line);
        }

        let mut expected = HashMap::new();
        expected.insert(1, "description[0]: \"This is a multiline\"");
        expected.insert(2, "description[1]: \"string in YAML\"");
        expected.insert(3, "name: \"test\"");

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        Ok(())
    }

    /// Runs the adapter over `yaml_content` and asserts that each output line
    /// matches `expected` (0-indexed line number to text), that every other
    /// line is empty, and that the line count is preserved.
    async fn assert_adapted_lines(
        yaml_content: &str,
        expected: &[(usize, &str)],
    ) -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(YamlAdapter::new());

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.yaml"),
            Box::pin(Cursor::new(yaml_content.as_bytes().to_vec())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let expected: HashMap<usize, &str> = expected.iter().copied().collect();

        assert_eq!(lines.len(), yaml_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            assert_str_eq!(*line, expected.get(&line_num).copied().unwrap_or(""));
        }

        Ok(())
    }

    /// Literal block scalar (`|`): each line is its own element and interior
    /// blank lines are kept so indexes stay aligned with source lines.
    #[tokio::test]
    async fn test_multiline_literal_block() -> anyhow::Result<()> {
        let yaml_content = r#"lit: |
  line one

  line three
after: done
"#;
        assert_adapted_lines(
            yaml_content,
            &[
                (1, "lit[0]: \"line one\""),
                (2, "lit[1]: \"\""),
                (3, "lit[2]: \"line three\""),
                (4, "after: \"done\""),
            ],
        )
        .await
    }

    /// Literal block scalar with the keep indicator (`|+`): trailing blank
    /// lines belong to the value but carry no text, so they are dropped.
    #[tokio::test]
    async fn test_multiline_literal_block_keep() -> anyhow::Result<()> {
        let yaml_content = r#"keep: |+
  kept

after: done
"#;
        assert_adapted_lines(
            yaml_content,
            &[(1, "keep[0]: \"kept\""), (3, "after: \"done\"")],
        )
        .await
    }

    /// Literal block scalar with the strip indicator (`|-`).
    #[tokio::test]
    async fn test_multiline_literal_block_strip() -> anyhow::Result<()> {
        let yaml_content = r#"strip: |-
  stripped
after: done
"#;
        assert_adapted_lines(
            yaml_content,
            &[(1, "strip[0]: \"stripped\""), (2, "after: \"done\"")],
        )
        .await
    }

    /// Folded block scalar (`>`): the source lines are emitted as written,
    /// not the folded value, so each line keeps its original position.
    #[tokio::test]
    async fn test_multiline_folded_block() -> anyhow::Result<()> {
        let yaml_content = r#"fold: >
  folded one
  folded two

  para two
after: done
"#;
        assert_adapted_lines(
            yaml_content,
            &[
                (1, "fold[0]: \"folded one\""),
                (2, "fold[1]: \"folded two\""),
                (3, "fold[2]: \"\""),
                (4, "fold[3]: \"para two\""),
                (5, "after: \"done\""),
            ],
        )
        .await
    }

    /// Multi-line plain scalar: the first element starts mid-line after the
    /// key, and continuation indentation is stripped.
    #[tokio::test]
    async fn test_multiline_plain_scalar() -> anyhow::Result<()> {
        let yaml_content = r#"plain: first part
  second part
after: done
"#;
        assert_adapted_lines(
            yaml_content,
            &[
                (0, "plain[0]: \"first part\""),
                (1, "plain[1]: \"second part\""),
                (2, "after: \"done\""),
            ],
        )
        .await
    }

    /// Multi-line double-quoted scalar: the source text is used verbatim, so
    /// the surrounding quote characters appear in the elements.
    #[tokio::test]
    async fn test_multiline_double_quoted_scalar() -> anyhow::Result<()> {
        let yaml_content = r#"dq: "double
  quoted"
after: done
"#;
        assert_adapted_lines(
            yaml_content,
            &[
                (0, "dq[0]: \"\\\"double\""),
                (1, "dq[1]: \"quoted\\\"\""),
                (2, "after: \"done\""),
            ],
        )
        .await
    }

    /// Block scalar nested inside a mapping: the block's own indentation is
    /// stripped and the following sibling key is not swallowed.
    #[tokio::test]
    async fn test_multiline_block_in_nested_mapping() -> anyhow::Result<()> {
        let yaml_content = r#"nested:
  inner: |
    deep one
    deep two
  next: 1
"#;
        assert_adapted_lines(
            yaml_content,
            &[
                (2, "nested.inner[0]: \"deep one\""),
                (3, "nested.inner[1]: \"deep two\""),
                (4, "nested.next: 1"),
            ],
        )
        .await
    }

    /// Block scalar as a sequence element: the line index nests under the
    /// element index.
    #[tokio::test]
    async fn test_multiline_block_in_sequence() -> anyhow::Result<()> {
        let yaml_content = r#"seq:
  - |
    in seq one
    in seq two
  - x
"#;
        assert_adapted_lines(
            yaml_content,
            &[
                (2, "seq[0][0]: \"in seq one\""),
                (3, "seq[0][1]: \"in seq two\""),
                (4, "seq[1]: \"x\""),
            ],
        )
        .await
    }
}
