use super::writing::{WritingFileAdapter, async_writeln};
use super::{AdapterMeta, FastFileMatcher, FileMatcher, GetMetadata};
use anyhow::{Context, Result};
use async_trait::async_trait;
use hcl_edit::Span;
use hcl_edit::expr::{Expression, ObjectKey};
use hcl_edit::structure::{Body, Structure};
use lazy_static::lazy_static;
use log::warn;
use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

static EXTENSIONS: &[&str] = &["hcl", "tf", "tfvars"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "hcl".to_owned(),
        version: 1,
        description:
            "Converts HCL (Hashicorp Configuration Language) files into a gron-like format with dot-delimited paths for nested keys"
                .to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: Some(vec![
            FileMatcher::MimeType("application/hcl".to_owned()),
            FileMatcher::MimeType("text/x-hcl".to_owned()),
        ]),
        keep_fast_matchers_if_accurate: false,
        disabled_by_default: false
    };
}

#[derive(Default, Clone)]
pub struct HclAdapter;

impl HclAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for HclAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl WritingFileAdapter for HclAdapter {
    async fn adapt_write(
        mut a: super::AdaptInfo,
        _detection_reason: &FileMatcher,
        mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()> {
        // Read the entire HCL content
        let mut content = String::new();
        a.inp
            .read_to_string(&mut content)
            .await
            .context("Failed to read HCL content")?;

        // Parse HCL with span information
        match hcl_edit::parser::parse_body(&content) {
            Ok(body) => {
                // Build line-to-output mapping
                let mut line_map: HashMap<usize, String> = HashMap::new();

                flatten_body(&body, String::new(), &content, &mut line_map)?;

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
                    "Failed to parse HCL file '{}': {}. Passing through unmodified.",
                    a.filepath_hint.display(),
                    e
                );
                oup.write_all(content.as_bytes()).await?;
            }
        }

        Ok(())
    }
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

fn flatten_body(
    body: &Body,
    path: String,
    source: &str,
    line_map: &mut HashMap<usize, String>,
) -> Result<()> {
    for structure in body.iter() {
        match structure {
            Structure::Attribute(attr) => {
                let new_path = join_path(&path, attr.key.as_str());
                let key_line = line_of(attr.key.span(), source);
                flatten_expr(&attr.value, new_path, source, line_map, key_line)?;
            }
            Structure::Block(block) => {
                let mut new_path = join_path(&path, block.ident.as_str());
                for label in &block.labels {
                    new_path = format!("{}.{}", new_path, label.as_str());
                }
                if block.body.is_empty() {
                    let line = line_of(block.span(), source);
                    line_map.insert(line, format!("{}: {{}}", new_path));
                } else {
                    flatten_body(&block.body, new_path, source, line_map)?;
                }
            }
        }
    }

    Ok(())
}

fn flatten_expr(
    value: &Expression,
    path: String,
    source: &str,
    line_map: &mut HashMap<usize, String>,
    key_line: usize,
) -> Result<()> {
    match value {
        Expression::Array(arr) => {
            if arr.is_empty() {
                let output = format!("{}: []", path);
                line_map.insert(key_line, output);
            } else {
                // Check if the array spans a single source line (flow style)
                let span = value.span();
                let start_line = line_of(span.clone(), source);
                let end_line = span
                    .map(|r| line_of(Some(r.end..r.end), source))
                    .unwrap_or(start_line);

                if start_line == end_line {
                    // Single-line array: output the entire array on one line.
                    // `value.to_string()` includes the source decor (e.g. the
                    // whitespace between `=` and the array), so trim it off.
                    let output = format!("{}: {}", path, value.to_string().trim());
                    line_map.insert(key_line, output);
                } else {
                    // Multi-line array: output each element with index
                    for (idx, elem) in arr.iter().enumerate() {
                        let indexed_path = format!("{}[{}]", path, idx);
                        let elem_line = line_of(elem.span(), source);
                        flatten_expr(elem, indexed_path, source, line_map, elem_line)?;
                    }
                }
            }
        }
        Expression::Object(obj) => {
            if obj.is_empty() {
                let output = format!("{}: {{}}", path);
                line_map.insert(key_line, output);
            } else {
                for (key, val) in obj.iter() {
                    let key_str = match key {
                        ObjectKey::Ident(ident) => ident.as_str().to_string(),
                        ObjectKey::Expression(expr) => expr.to_string().trim().to_string(),
                    };
                    let new_path = join_path(&path, &key_str);
                    let val_line = line_of(key.span(), source);
                    flatten_expr(val.expr(), new_path, source, line_map, val_line)?;
                }
            }
        }
        Expression::Null(_) => {
            line_map.insert(key_line, format!("{}: null", path));
        }
        Expression::Bool(b) => {
            line_map.insert(key_line, format!("{}: {}", path, b.value()));
        }
        Expression::Number(n) => {
            line_map.insert(key_line, format!("{}: {}", path, n));
        }
        Expression::String(s) => {
            line_map.insert(key_line, format!("{}: {}", path, quote_escape(s.as_str())));
        }
        Expression::Variable(ident) => {
            line_map.insert(key_line, format!("{}: {}", path, ident.as_str()));
        }
        Expression::HeredocTemplate(heredoc) => {
            // Treat the heredoc body as a newline-delimited array: each line is
            // emitted as an indexed element on the same source line it came from.
            // The template text is already dedented for `<<-` heredocs.
            let body = heredoc.template.to_string();
            let lines: Vec<&str> = body.lines().collect();
            if lines.is_empty() {
                line_map.insert(key_line, format!("{}: []", path));
            } else {
                // The heredoc's span starts at `<<`; the body begins on the next line.
                let first_line = line_of(value.span(), source) + 1;
                for (idx, text) in lines.iter().enumerate() {
                    let output = format!("{}[{}]: {}", path, idx, quote_escape(text));
                    line_map.insert(first_line + idx, output);
                }
            }
        }
        // StringTemplate, Traversal, FuncCall, Conditional, UnaryOp, BinaryOp, ForExpr,
        // Parenthesis: render the expression's own HCL syntax verbatim. StringTemplate's
        // rendering is already a properly quoted and escaped string.
        _ => {
            let rendered = value.to_string().replace('\n', " ");
            line_map.insert(key_line, format!("{}: {}", path, rendered.trim()));
        }
    }

    Ok(())
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

    /// Directory holding the HCL fixtures: `exampledir/hcl/`.
    fn hcl_fixture_dir() -> std::path::PathBuf {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("exampledir/hcl/");
        d
    }

    /// Runs the adapter over a fixture in `exampledir/hcl/` and returns
    /// `(source, output)`, asserting that the output preserves the line count.
    async fn adapt_fixture(name: &str) -> anyhow::Result<(String, String)> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(HclAdapter::new());
        let path = hcl_fixture_dir().join(name);
        let source = std::fs::read_to_string(&path)?;

        let (a, d) = simple_fs_adapt_info(&path).await?;
        let res = adapter.adapt(a, &d).await?;
        let buf = adapted_to_vec(res).await?;
        let output = String::from_utf8(buf)?;

        assert_eq!(output.lines().count(), source.lines().count());
        Ok((source, output))
    }

    /// Generic HCL: scalars, flow and multi-line arrays, empty collections,
    /// object expressions, nested blocks, empty blocks, heredocs, and escapes.
    #[tokio::test]
    async fn test_simple_hcl() -> anyhow::Result<()> {
        let (_, output) = adapt_fixture("simple.hcl").await?;

        let expected = r##"
name: "John"
age: 30
ratio: 1.5
enabled: true
nothing: null


tags: ["developer", "rust", "python"]

ports[0]: 80
ports[1]: 443

empty_array: []
empty_object: {}


tags_map.Name: "foo"
tags_map.Env: "prod"



person.name: "Alice"
person.age: 25


person.address.city: "Springfield"



locals: {}


description[0]: "line one"
description[1]: ""
description[2]: "line three"


escaped: "tab\there \"quoted\""
"##;
        assert_str_eq!(output, expected);

        Ok(())
    }

    /// Terraform: labeled blocks, variable references, traversals, string
    /// interpolation, indented heredocs, function calls, conditionals, and
    /// for-expressions.
    #[tokio::test]
    async fn test_terraform_tf() -> anyhow::Result<()> {
        let (_, output) = adapt_fixture("main.tf").await?;

        let expected = r##"


terraform.required_providers.aws.source: "hashicorp/aws"
terraform.required_providers.aws.version: "~> 5.0"





provider.aws.region: var.region



variable.instance_count.type: number
variable.instance_count.default: 2



resource.aws_instance.web.count: var.instance_count
resource.aws_instance.web.ami: data.aws_ami.ubuntu.id
resource.aws_instance.web.instance_type: "t3.micro"


resource.aws_instance.web.tags.Name: "web-${count.index}"



resource.aws_instance.web.user_data[0]: "#!/bin/bash"
resource.aws_instance.web.user_data[1]: "echo hello"


resource.aws_instance.web.security_groups: [aws_security_group.web.id, "default"]
resource.aws_instance.web.name: join("-", ["web", var.region])
resource.aws_instance.web.monitoring: var.production ? true : false



output.instance_ids.value: [for i in aws_instance.web : i.id]

"##;
        assert_str_eq!(output, expected);

        Ok(())
    }

    /// Terraform variable definitions: flat attributes, a flow-style list,
    /// and a map.
    #[tokio::test]
    async fn test_terraform_tfvars() -> anyhow::Result<()> {
        let (_, output) = adapt_fixture("terraform.tfvars").await?;

        let expected = r##"region: "us-east-1"
instance_count: 3
production: false

allowed_cidrs: ["10.0.0.0/8", "192.168.0.0/16"]


tags.Owner: "platform"
tags.Team: "infra"

"##;
        assert_str_eq!(output, expected);

        Ok(())
    }

    /// Unparsable input is passed through unchanged rather than failing.
    #[tokio::test]
    async fn test_invalid_hcl_passthrough() -> anyhow::Result<()> {
        let (source, output) = adapt_fixture("invalid.hcl").await?;

        assert_str_eq!(output, source);

        Ok(())
    }
}
