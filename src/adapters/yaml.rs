use super::writing::{WritingFileAdapter, async_writeln};
use super::{AdapterMeta, FastFileMatcher, FileMatcher, GetMetadata};
use anyhow::{Context, Result};
use async_trait::async_trait;
use lazy_static::lazy_static;
use log::warn;
use saphyr::{LoadableYamlNode, MarkedYaml, Scalar, YamlData};
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
        match MarkedYaml::load_from_str(&content) {
            Ok(docs) => {
                if docs.is_empty() {
                    // Empty document, write nothing
                    return Ok(());
                }

                // Build line-to-output mapping
                let mut line_map: HashMap<usize, String> = HashMap::new();

                // Process the first document (most YAML files have a single document)
                for doc in &docs {
                    flatten_yaml_value(doc, String::new(), &mut line_map)?;
                }

                // Output line by line, matching the original line count
                let line_count = content.lines().count();
                for line_num in 0..line_count {
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

fn flatten_yaml_value(
    value: &MarkedYaml,
    path: String,
    line_map: &mut HashMap<usize, String>,
) -> Result<()> {
    flatten_yaml_value_with_key_line(value, path, line_map, value.span.start.line())
}

fn flatten_yaml_value_with_key_line(
    value: &MarkedYaml,
    path: String,
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
            } else {
                // Check if array is single-line by checking if all elements are on the same line
                let start_line = value.span.start.line();
                let end_line = value.span.end.line();

                if start_line == end_line {
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
                            line_map,
                            elem.span.start.line(),
                        )?;
                    }
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
                    let key_str = scalar_to_string(&key.data)?;
                    let new_path = if path.is_empty() {
                        key_str
                    } else {
                        format!("{}.{}", path, key_str)
                    };
                    // Use the key's line number for output placement
                    flatten_yaml_value_with_key_line(
                        val,
                        new_path,
                        line_map,
                        key.span.start.line(),
                    )?;
                }
            }
        }
        YamlData::Value(scalar) => {
            let value_str = format_scalar_value(scalar);
            let output = format!("{}: {}", path, value_str);
            line_map.insert(line_num, output);
        }
        YamlData::Tagged(_tag, node) => {
            // Recursively process the tagged node, preserving the key line
            flatten_yaml_value_with_key_line(node, path, line_map, key_line)?;
        }
        YamlData::Representation(_repr, _style, _tag) => {
            // Handle representation by converting to string
            let output = format!("{}: <representation>", path);
            line_map.insert(line_num, output);
        }
        YamlData::BadValue => {
            // Skip bad values
            warn!("Encountered bad YAML value at line {}", line_num + 1);
        }
        YamlData::Alias(_alias) => {
            // YAML aliases are references to anchors
            // For now, just output a placeholder
            let output = format!("{}: <alias>", path);
            line_map.insert(line_num, output);
        }
    }

    Ok(())
}

fn scalar_to_string<'a>(data: &YamlData<'a, MarkedYaml<'a>>) -> Result<String> {
    match data {
        YamlData::Value(scalar) => Ok(match scalar {
            Scalar::String(s) => s.to_string(),
            Scalar::Integer(i) => i.to_string(),
            Scalar::FloatingPoint(f) => f.to_string(),
            Scalar::Boolean(b) => b.to_string(),
            Scalar::Null => "null".to_string(),
        }),
        _ => Ok("unknown".to_string()),
    }
}

fn format_scalar_value(scalar: &Scalar) -> String {
    match scalar {
        Scalar::String(s) => {
            // Escape special characters in strings
            let escaped = s
                .replace('\\', "\\\\")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t")
                .replace('"', "\\\"");
            format!("\"{}\"", escaped)
        }
        Scalar::Integer(i) => i.to_string(),
        Scalar::FloatingPoint(f) => f.to_string(),
        Scalar::Boolean(b) => b.to_string(),
        Scalar::Null => "null".to_string(),
    }
}

fn format_flow_array(path: &str, seq: &[MarkedYaml]) -> Result<String> {
    let elements: Vec<String> = seq
        .iter()
        .map(|elem| match &elem.data {
            YamlData::Value(scalar) => Ok(format_scalar_value(scalar)),
            _ => Ok("...".to_string()),
        })
        .collect::<Result<Vec<_>>>()?;

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
        expected.insert(
            0,
            "description: \"This is a multiline\\nstring in YAML\\n\"",
        );
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
}
