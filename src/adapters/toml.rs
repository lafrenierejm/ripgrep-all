use super::writing::{WritingFileAdapter, async_writeln};
use super::{AdapterMeta, FastFileMatcher, FileMatcher, GetMetadata};
use anyhow::{Context, Result};
use async_trait::async_trait;
use lazy_static::lazy_static;
use log::warn;
use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Range;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use toml_edit::{Document, Item, Table, TomlError, Value};

static EXTENSIONS: &[&str] = &["toml"];

/// TOML files whose names carry no `.toml` extension.
static FILENAMES: &[&str] = &[
    "Cargo.lock",
    "Pipfile",
    "pdm.lock",
    "poetry.lock",
    "uv.lock",
];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "toml".to_owned(),
        version: 1,
        description:
            "Converts TOML files into a gron-like format with dot-delimited paths for nested keys"
                .to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .chain(
                FILENAMES
                    .iter()
                    .map(|s| FastFileMatcher::FileName(s.to_string()))
            )
            .collect(),
        slow_matchers: Some(vec![FileMatcher::MimeType("application/toml".to_owned())]),
        keep_fast_matchers_if_accurate: false,
        disabled_by_default: false
    };
}

#[derive(Default, Clone)]
pub struct TomlAdapter;

impl TomlAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for TomlAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl WritingFileAdapter for TomlAdapter {
    async fn adapt_write(
        mut a: super::AdaptInfo,
        _detection_reason: &FileMatcher,
        mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()> {
        // Read the entire TOML content
        let mut content = String::new();
        a.inp
            .read_to_string(&mut content)
            .await
            .context("Failed to read TOML content")?;

        match flatten_document(&content) {
            Ok(line_map) => {
                // Output line by line, matching the original line count. Several
                // entries can share a line (the keys of a single-line inline
                // table, for instance); they are joined with ", ".
                let line_count = content.lines().count();
                for line_num in 0..line_count {
                    match line_map.get(&line_num) {
                        Some(entries) => {
                            let joined = entries
                                .iter()
                                .map(|entry| format!("{}: {}", entry.path, entry.value))
                                .collect::<Vec<_>>()
                                .join(", ");
                            async_writeln!(oup, "{}", joined)?;
                        }
                        None => {
                            // Empty line to maintain line count
                            async_writeln!(oup)?;
                        }
                    }
                }
            }
            Err(e) => {
                // Log warning and pass through original content
                warn!(
                    "Failed to parse TOML file '{}': {}. Passing through unmodified.",
                    a.filepath_hint.display(),
                    e
                );
                oup.write_all(content.as_bytes()).await?;
            }
        }

        Ok(())
    }
}

/// A single gron-style entry: a dot-delimited path and its rendered value.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    path: String,
    value: String,
}

/// Entries keyed by 0-indexed source line. A line can carry several entries
/// when more than one leaf value sits on it.
type LineMap = HashMap<usize, Vec<Entry>>;

/// Parses `content` as TOML and flattens it into entries keyed by the source
/// line each one belongs on.
fn flatten_document(content: &str) -> std::result::Result<LineMap, TomlError> {
    let doc = Document::parse(content)?;
    let mut line_map = LineMap::new();
    flatten_table(doc.as_table(), String::new(), content, &mut line_map);
    Ok(line_map)
}

fn emit(line_map: &mut LineMap, line: usize, path: impl Into<String>, value: impl Display) {
    line_map.entry(line).or_default().push(Entry {
        path: path.into(),
        value: value.to_string(),
    });
}

/// Converts a byte offset span into a 0-indexed line number, using the span's start.
fn line_of(span: Option<Range<usize>>, source: &str) -> usize {
    match span {
        Some(range) => {
            let start = range.start.min(source.len());
            source[..start].matches('\n').count()
        }
        None => 0,
    }
}

fn join_path(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{}.{}", path, key)
    }
}

fn flatten_table(table: &Table, path: String, source: &str, line_map: &mut LineMap) {
    for (key_name, item) in table.iter() {
        let new_path = join_path(&path, key_name);
        let key_line = table
            .key(key_name)
            .map(|key| line_of(key.span(), source))
            .unwrap_or(0);

        match item {
            Item::None => {}
            Item::Value(value) => {
                flatten_value(value, new_path, source, line_map, key_line);
            }
            Item::Table(sub) => {
                // Implicit tables come from dotted keys or `[a.b]` headers with
                // no `[a]` of their own; they have no header line to report.
                if sub.is_empty() && !sub.is_implicit() {
                    let header_line = line_of(sub.span(), source);
                    emit(line_map, header_line, new_path, "{}");
                } else {
                    flatten_table(sub, new_path, source, line_map);
                }
            }
            Item::ArrayOfTables(tables) => {
                for (idx, sub) in tables.iter().enumerate() {
                    let indexed_path = format!("{}[{}]", new_path, idx);
                    if sub.is_empty() {
                        let header_line = line_of(sub.span(), source);
                        emit(line_map, header_line, indexed_path, "{}");
                    } else {
                        flatten_table(sub, indexed_path, source, line_map);
                    }
                }
            }
        }
    }
}

fn flatten_value(
    value: &Value,
    path: String,
    source: &str,
    line_map: &mut LineMap,
    key_line: usize,
) {
    match value {
        Value::String(s) => {
            let span = s.span();
            let is_multiline = span
                .as_ref()
                .map(|r| {
                    line_of(Some(r.start..r.start), source) != line_of(Some(r.end..r.end), source)
                })
                .unwrap_or(false);
            if !(is_multiline && flatten_multiline_string(span, &path, source, line_map)) {
                emit(line_map, key_line, path, quote_escape(s.value()));
            }
        }
        Value::Integer(n) => emit(line_map, key_line, path, n.value()),
        Value::Float(n) => emit(line_map, key_line, path, n.value()),
        Value::Boolean(b) => emit(line_map, key_line, path, b.value()),
        Value::Datetime(dt) => emit(line_map, key_line, path, dt.value()),
        Value::Array(arr) => {
            if arr.is_empty() {
                emit(line_map, key_line, path, "[]");
                return;
            }

            // Check if the array spans a single source line
            let span = arr.span();
            let (start_line, end_line) = match &span {
                Some(r) => (
                    line_of(Some(r.start..r.start), source),
                    line_of(Some(r.end..r.end), source),
                ),
                None => (key_line, key_line),
            };

            if start_line == end_line {
                // Single-line array: output the array's source text on one line
                let text = span
                    .map(|r| source[r].to_string())
                    .unwrap_or_else(|| arr.to_string());
                emit(line_map, key_line, path, text.trim());
            } else {
                // Multi-line array: output each element with index on its own line
                for (idx, elem) in arr.iter().enumerate() {
                    let indexed_path = format!("{}[{}]", path, idx);
                    let elem_line = line_of(elem.span(), source);
                    flatten_value(elem, indexed_path, source, line_map, elem_line);
                }
            }
        }
        Value::InlineTable(table) => {
            if table.is_empty() {
                emit(line_map, key_line, path, "{}");
                return;
            }
            for (key_name, val) in table.iter() {
                let new_path = join_path(&path, key_name);
                let val_line = table
                    .key(key_name)
                    .map(|key| line_of(key.span(), source))
                    .unwrap_or(key_line);
                flatten_value(val, new_path, source, line_map, val_line);
            }
        }
    }
}

/// Emits a multi-line string (`"""` or `'''`) as a newline-delimited array:
/// each source line of the string becomes an indexed element placed on the
/// same line it came from. The source text is used rather than the decoded
/// value so that line continuations keep their original line layout. The
/// opening and closing delimiter lines are dropped when they carry no text.
///
/// Returns `false` if no lines could be attributed to the string, in which
/// case the caller falls back to single-line output.
fn flatten_multiline_string(
    span: Option<Range<usize>>,
    path: &str,
    source: &str,
    line_map: &mut LineMap,
) -> bool {
    let Some(range) = span else {
        return false;
    };
    let raw = &source[range.clone()];
    let delimiter = if raw.starts_with("'''") {
        "'''"
    } else {
        "\"\"\""
    };
    let inner = raw
        .strip_prefix(delimiter)
        .and_then(|s| s.strip_suffix(delimiter))
        .unwrap_or(raw);

    let first_line = line_of(Some(range.start..range.start), source);
    let mut elements: Vec<(usize, &str)> = inner
        .split('\n')
        .enumerate()
        .map(|(offset, text)| (first_line + offset, text.strip_suffix('\r').unwrap_or(text)))
        .collect();

    // The opening delimiter line usually holds nothing but the delimiter,
    // or the delimiter plus a line-continuation backslash (`"""\`); the
    // closing delimiter line usually holds nothing but the delimiter. Drop
    // them so indexes start at the first line of content.
    if elements
        .first()
        .is_some_and(|(_, text)| text.trim().is_empty() || text.trim() == "\\")
    {
        elements.remove(0);
    }
    if elements.last().is_some_and(|(_, text)| text.is_empty()) {
        elements.pop();
    }

    if elements.is_empty() {
        return false;
    }

    for (idx, (line_idx, text)) in elements.into_iter().enumerate() {
        emit(
            line_map,
            line_idx,
            format!("{}[{}]", path, idx),
            quote_escape(text),
        );
    }

    true
}

fn quote_escape(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
        .replace('"', "\\\"");
    format!("\"{}\"", escaped)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::{assert_eq, assert_str_eq};

    /// Directory holding the TOML fixtures: `exampledir/toml/`.
    fn toml_fixture_dir() -> std::path::PathBuf {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("exampledir/toml/");
        d
    }

    /// Flattens a fixture in `exampledir/toml/` and returns its entries as
    /// `(line, path, value)` triples ordered by 0-indexed source line, before
    /// they are serialized into output text.
    fn flatten_fixture(name: &str) -> anyhow::Result<Vec<(usize, String, String)>> {
        let source = std::fs::read_to_string(toml_fixture_dir().join(name))?;
        let line_map = flatten_document(&source)?;

        let mut lines: Vec<(usize, Vec<Entry>)> = line_map.into_iter().collect();
        lines.sort_by_key(|(line, _)| *line);
        Ok(lines
            .into_iter()
            .flat_map(|(line, entries)| {
                entries
                    .into_iter()
                    .map(move |entry| (line, entry.path, entry.value))
            })
            .collect())
    }

    /// Converts a literal table of expected entries into the owned form
    /// returned by [`flatten_fixture`].
    fn entries(expected: &[(usize, &str, &str)]) -> Vec<(usize, String, String)> {
        expected
            .iter()
            .map(|(line, path, value)| (*line, path.to_string(), value.to_string()))
            .collect()
    }

    /// Generic TOML: every scalar type, flow and multi-line arrays, empty
    /// collections, inline tables, dotted keys, multi-line strings, tables,
    /// subtables, and arrays of tables.
    #[test]
    fn test_simple_toml() -> anyhow::Result<()> {
        assert_eq!(
            flatten_fixture("simple.toml")?,
            entries(&[
                (1, "title", r#""TOML""#),
                (2, "count", "42"),
                (3, "ratio", "1.5"),
                (4, "enabled", "true"),
                (5, "created", "1979-05-27T07:32:00Z"),
                (8, "tags", r#"["developer", "rust", "python"]"#),
                (10, "ports[0]", "80"),
                (11, "ports[1]", "443"),
                (13, "empty_array", "[]"),
                (14, "empty_table", "{}"),
                (15, "point.x", "1"),
                (15, "point.y", r#""two""#),
                (16, "owner.name.first", r#""dotted""#),
                (19, "description[0]", r#""line one""#),
                (20, "description[1]", r#""""#),
                (21, "description[2]", r#""line three""#),
                (24, "literal[0]", r#""raw \\one""#),
                (27, "joined[0]", r#""  first part \\""#),
                (28, "joined[1]", r#""  second part""#),
                (31, "server.host", r#""localhost""#),
                (34, "server.tls.enabled", "false"),
                (36, "empty", "{}"),
                (39, "bin[0].name", r#""first""#),
                (42, "bin[1].name", r#""second""#),
                (44, "nothing[0]", "{}"),
            ])
        );

        Ok(())
    }

    /// A Cargo manifest: package metadata, dependencies as strings and inline
    /// tables, feature lists, a `[[bin]]` target, and a profile subtable.
    #[test]
    fn test_cargo_manifest() -> anyhow::Result<()> {
        assert_eq!(
            flatten_fixture("manifest.toml")?,
            entries(&[
                (1, "package.name", r#""example""#),
                (2, "package.version", r#""0.1.0""#),
                (3, "package.edition", r#""2024""#),
                (4, "package.authors", r#"["Jane Doe <jane@example.com>"]"#),
                (5, "package.description", r#""An example crate""#),
                (8, "dependencies.anyhow", r#""1.0""#),
                (9, "dependencies.serde.version", r#""1.0""#),
                (9, "dependencies.serde.features", r#"["derive"]"#),
                (10, "dependencies.tokio.version", r#""1""#),
                (11, "dependencies.tokio.features[0]", r#""rt-multi-thread""#),
                (12, "dependencies.tokio.features[1]", r#""macros""#),
                (16, "dev-dependencies.pretty_assertions", r#""1.4""#),
                (19, "features.default", r#"["cli"]"#),
                (20, "features.cli", r#"["dep:clap"]"#),
                (23, "bin[0].name", r#""example""#),
                (24, "bin[0].path", r#""src/main.rs""#),
                (27, "profile.release.lto", r#""thin""#),
                (28, "profile.release.codegen-units", "1"),
            ])
        );

        Ok(())
    }

    /// A pyproject file: multi-line arrays of strings and inline tables,
    /// nested tool tables, and quoted keys.
    #[test]
    fn test_pyproject() -> anyhow::Result<()> {
        assert_eq!(
            flatten_fixture("pyproject.toml")?,
            entries(&[
                (1, "build-system.requires", r#"["hatchling"]"#),
                (2, "build-system.build-backend", r#""hatchling.build""#),
                (5, "project.name", r#""example""#),
                (6, "project.version", r#""0.1.0""#),
                (7, "project.requires-python", r#"">=3.12""#),
                (9, "project.dependencies[0]", r#""httpx>=0.27""#),
                (10, "project.dependencies[1]", r#""pydantic>=2""#),
                (
                    14,
                    "project.optional-dependencies.dev",
                    r#"["pytest", "ruff"]"#
                ),
                (
                    17,
                    "project.urls.Bug Tracker",
                    r#""https://example.com/issues""#
                ),
                (20, "tool.ruff.line-length", "100"),
                (21, "tool.ruff.select", r#"["E", "F"]"#),
                (24, "tool.ruff.per-file-ignores.tests/*", r#"["E501"]"#),
            ])
        );

        Ok(())
    }

    /// The writer renders each entry as `path: value`, joins entries that
    /// share a line with ", ", and pads every other line so the output line
    /// count matches the source.
    #[tokio::test]
    async fn test_serialization_preserves_lines() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(TomlAdapter::new());
        let path = toml_fixture_dir().join("manifest.toml");
        let source = std::fs::read_to_string(&path)?;

        let (a, d) = simple_fs_adapt_info(&path).await?;
        let res = adapter.adapt(a, &d).await?;
        let output = String::from_utf8(adapted_to_vec(res).await?)?;

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), source.lines().count());
        assert_str_eq!(lines[0], "");
        assert_str_eq!(lines[1], r#"package.name: "example""#);
        assert_str_eq!(
            lines[9],
            r#"dependencies.serde.version: "1.0", dependencies.serde.features: ["derive"]"#
        );
        assert_str_eq!(lines[28], "profile.release.codegen-units: 1");

        Ok(())
    }

    /// Unparsable input is passed through unchanged rather than failing.
    #[tokio::test]
    async fn test_invalid_toml_passthrough() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(TomlAdapter::new());
        let path = toml_fixture_dir().join("invalid.toml");
        let source = std::fs::read_to_string(&path)?;

        let (a, d) = simple_fs_adapt_info(&path).await?;
        let res = adapter.adapt(a, &d).await?;
        let output = String::from_utf8(adapted_to_vec(res).await?)?;

        assert_str_eq!(output, source);

        Ok(())
    }
}
