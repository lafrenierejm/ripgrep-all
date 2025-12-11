use super::writing::{WritingFileAdapter, async_writeln};
use super::{AdapterMeta, FastFileMatcher, FileMatcher, GetMetadata};
use anyhow::{Context, Result};
use async_trait::async_trait;
use json_spanned_value::{Value, spanned};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWrite};

static EXTENSIONS: &[&str] = &["json"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "json".to_owned(),
        version: 1,
        description:
            "Converts JSON files into a gron-like format with dot-delimited paths for nested keys"
                .to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: Some(vec![FileMatcher::MimeType("application/json".to_owned())]),
        keep_fast_matchers_if_accurate: false,
        disabled_by_default: false
    };
}

#[derive(Default, Clone)]
pub struct JsonAdapter;

impl JsonAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for JsonAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl WritingFileAdapter for JsonAdapter {
    async fn adapt_write(
        mut a: super::AdaptInfo,
        _detection_reason: &FileMatcher,
        mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()> {
        // Read the entire JSON content
        let mut content = String::new();
        a.inp
            .read_to_string(&mut content)
            .await
            .context("Failed to read JSON content")?;

        // Parse JSON with span information
        let parsed: spanned::Value =
            json_spanned_value::from_str(&content).context("Failed to parse JSON")?;

        // Build line-to-output mapping
        let mut line_map: HashMap<usize, String> = HashMap::new();

        // Flatten JSON into line mappings
        flatten_value(&parsed, String::new(), &content, &mut line_map)?;

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

        Ok(())
    }
}

fn flatten_value(
    value: &spanned::Value,
    path: String,
    source: &str,
    line_map: &mut HashMap<usize, String>,
) -> Result<()> {
    match value.get_ref() {
        Value::Object(obj) => {
            if obj.is_empty() {
                // Empty object
                let line_num = get_line_number(value.start(), source);
                let output = format!("{}: {{}}", path);
                line_map.insert(line_num, output);
            } else {
                for (key, val) in obj {
                    let new_path = if path.is_empty() {
                        key.get_ref().to_string()
                    } else {
                        format!("{}.{}", path, key.get_ref())
                    };
                    flatten_value(val, new_path, source, line_map)?;
                }
            }
        }
        Value::Array(arr) => {
            // Check if array is single-line or multi-line
            if arr.is_empty() {
                // Empty array
                let line_num = get_line_number(value.start(), source);
                let output = format!("{}: []", path);
                line_map.insert(line_num, output);
            } else {
                let start_line = get_line_number(arr[0].start(), source);
                let end_line = get_line_number(arr.last().unwrap().end(), source);

                if start_line == end_line {
                    // Single-line array: output the entire array on one line
                    let array_str = &source[value.start()..value.end()];
                    let output = format!("{}: {}", path, array_str);
                    line_map.insert(start_line, output);
                } else {
                    // Multi-line array: output each element with index
                    for (idx, elem) in arr.iter().enumerate() {
                        let indexed_path = format!("{}[{}]", path, idx);
                        flatten_value(elem, indexed_path, source, line_map)?;
                    }
                }
            }
        }
        Value::String(s) => {
            let line_num = get_line_number(value.start(), source);
            let output = format!("{}: \"{}\"", path, s);
            line_map.insert(line_num, output);
        }
        Value::Number(n) => {
            let line_num = get_line_number(value.start(), source);
            let output = format!("{}: {}", path, n);
            line_map.insert(line_num, output);
        }
        Value::Bool(b) => {
            let line_num = get_line_number(value.start(), source);
            let output = format!("{}: {}", path, b);
            line_map.insert(line_num, output);
        }
        Value::Null => {
            let line_num = get_line_number(value.start(), source);
            let output = format!("{}: null", path);
            line_map.insert(line_num, output);
        }
    }

    Ok(())
}

fn get_line_number(byte_offset: usize, source: &str) -> usize {
    source[..byte_offset].lines().count() - 1
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::{assert_eq, assert_str_eq};
    use std::io::Cursor;

    #[tokio::test]
    async fn test_simple_json() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(JsonAdapter::new());

        let json_content = r#"{
  "name": "John",

  "age": 30,
  "children": ["Alpha", "Bravo"],
  "siblings": [
    "Zulu",
    "Yankee"
  ]
}"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.json"),
            Box::pin(Cursor::new(json_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), json_content.lines().count());
        let mut expected = HashMap::new();
        expected.insert(1, "name: \"John\"");
        expected.insert(3, "age: 30");
        expected.insert(4, "children: [\"Alpha\", \"Bravo\"]");
        expected.insert(6, "siblings[0]: \"Zulu\"");
        expected.insert(7, "siblings[1]: \"Yankee\"");
        assert_eq!(lines.len(), json_content.lines().count());
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
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(JsonAdapter::new());

        let json_content = r#"{
  "person": {
    "name": "Alice",
    "age": 25
  }
}"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.json"),
            Box::pin(Cursor::new(json_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(2, "person.name: \"Alice\"");
        expected.insert(3, "person.age: 25");
        assert_eq!(lines.len(), json_content.lines().count());
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
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(JsonAdapter::new());

        let json_content = r#"{
  "users": [
    {
      "name": "Bob"
    },
    {
      "name": "Carol"
    }
  ]
}"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.json"),
            Box::pin(Cursor::new(json_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(3, "users[0].name: \"Bob\"");
        expected.insert(6, "users[1].name: \"Carol\"");
        assert_eq!(lines.len(), json_content.lines().count());
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
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(JsonAdapter::new());

        let json_content = r#"{
  "empty_string": "",
  "null_value": null,
  "empty_array": [],
  "empty_object": {}
}"#;

        let (a, d) = simple_adapt_info(
            std::path::Path::new("test.json"),
            Box::pin(Cursor::new(json_content.as_bytes())),
        );

        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        let lines: Vec<&str> = output.lines().collect();
        let mut expected = HashMap::new();
        expected.insert(1, "empty_string: \"\"");
        expected.insert(2, "null_value: null");
        expected.insert(3, "empty_array: []");
        expected.insert(4, "empty_object: {}");
        assert_eq!(lines.len(), json_content.lines().count());
        for (line_num, line) in lines.iter().enumerate() {
            if expected.contains_key(&line_num) {
                assert_str_eq!(*line, expected[&line_num]);
            } else {
                assert_str_eq!(*line, "");
            }
        }

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), json_content.lines().count());

        Ok(())
    }
}
