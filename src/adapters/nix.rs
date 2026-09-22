use super::writing::{WritingFileAdapter, async_writeln};
use super::{AdapterMeta, FastFileMatcher, FileMatcher, GetMetadata};
use anyhow::{Context, Result};
use async_trait::async_trait;
use lazy_static::lazy_static;
use log::warn;
use rnix::TextRange;
use rnix::ast::{self, HasEntry};
use rowan::ast::AstNode;
use std::collections::HashMap;
use std::pin::Pin;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

static EXTENSIONS: &[&str] = &["nix"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "nix".to_owned(),
        version: 1,
        description:
            "Converts Nix files into a gron-like format with dot-delimited paths for attrset keys; everything outside an attrset (e.g. function arguments) is passed through unchanged"
                .to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: None,
        keep_fast_matchers_if_accurate: true,
        disabled_by_default: false
    };
}

#[derive(Default, Clone)]
pub struct NixAdapter;

impl NixAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for NixAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl WritingFileAdapter for NixAdapter {
    async fn adapt_write(
        mut a: super::AdaptInfo,
        _detection_reason: &FileMatcher,
        mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()> {
        // Read the entire Nix content
        let mut content = String::new();
        a.inp
            .read_to_string(&mut content)
            .await
            .context("Failed to read Nix content")?;

        let parse = rnix::Root::parse(&content);
        if parse.errors().is_empty() {
            let line_map = flatten_document(&parse, &content);
            write_output(oup, &content, &line_map).await?;
        } else {
            let messages: Vec<String> = parse.errors().iter().map(|e| e.to_string()).collect();
            warn!(
                "Failed to parse Nix file '{}': {}. Passing through unmodified.",
                a.filepath_hint.display(),
                messages.join("; ")
            );
            oup.write_all(content.as_bytes()).await?;
        }

        Ok(())
    }
}

async fn write_output(
    mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    content: &str,
    line_map: &LineMap,
) -> Result<()> {
    // Output line by line, matching the original line count. Lines that
    // belong to an attrset entry are rendered as gron paths (several entries
    // sharing a line are joined with ", "); every other line - anything not
    // part of an attrset, such as function arguments or `let` bindings - is
    // passed through unchanged.
    for (line_num, source_line) in content.lines().enumerate() {
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
                async_writeln!(oup, "{}", source_line)?;
            }
        }
    }
    Ok(())
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

fn flatten_document(parse: &rnix::Parse<ast::Root>, source: &str) -> LineMap {
    let mut line_map = LineMap::new();
    let root = parse.tree();
    let index = LineIndex::new(source);
    if let Some(expr) = root.expr() {
        flatten_expr(&expr, String::new(), &index, &mut line_map);
    }
    line_map
}

fn emit(line_map: &mut LineMap, line: usize, path: impl Into<String>, value: impl Into<String>) {
    line_map.entry(line).or_default().push(Entry {
        path: path.into(),
        value: value.into(),
    });
}

/// The source text plus a precomputed table of newline byte offsets, so that
/// [`LineIndex::line_of`] can binary-search for a byte offset's line number
/// instead of rescanning the file prefix on every lookup.
struct LineIndex<'a> {
    text: &'a str,
    newlines: Vec<usize>,
}

impl<'a> LineIndex<'a> {
    fn new(text: &'a str) -> Self {
        let newlines = text.match_indices('\n').map(|(i, _)| i).collect();
        Self { text, newlines }
    }

    /// Converts a byte offset range into a 0-indexed line number, using the range's start.
    fn line_of(&self, range: TextRange) -> usize {
        let start = usize::from(range.start()).min(self.text.len());
        self.newlines.partition_point(|&nl| nl < start)
    }
}

fn join_path(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{}.{}", path, key)
    }
}

/// The static name of an attrpath segment or an `inherit` target. Computed
/// keys (`${expr}`) and interpolated strings have no static name, so their
/// own source syntax is used instead.
fn attr_name(attr: &ast::Attr) -> String {
    match attr {
        ast::Attr::Ident(ident) => ident
            .ident_token()
            .map(|t| t.text().to_string())
            .unwrap_or_else(|| ident.syntax().text().to_string()),
        ast::Attr::Dynamic(dynamic) => dynamic.syntax().text().to_string(),
        ast::Attr::Str(str_node) => {
            let mut parts = str_node.normalized_parts();
            if parts.len() == 1 && matches!(parts[0], ast::InterpolPart::Literal(_)) {
                let ast::InterpolPart::Literal(s) = parts.remove(0) else {
                    unreachable!()
                };
                s
            } else {
                str_node.syntax().text().to_string()
            }
        }
    }
}

/// Only real attrsets (`NODE_ATTR_SET`) are converted to gron notation.
/// A handful of wrapper expressions are "transparent": their own syntax
/// (a lambda's parameter pattern, `let` bindings, a `with` namespace, an
/// `assert` condition) is left as source text, but the meaningful
/// expression they wrap - their body, or an applied function's argument -
/// is still searched for attrsets to convert. Everything else (function
/// calls whose argument isn't an attrset, operators, comparisons, literals
/// outside of an attrset, etc.) is left as source text via the catch-all
/// case, matched by [`flatten_opaque`].
fn flatten_expr(expr: &ast::Expr, path: String, source: &LineIndex<'_>, line_map: &mut LineMap) {
    match expr {
        ast::Expr::AttrSet(attrset) => flatten_attrset(attrset, path, source, line_map),
        ast::Expr::List(list) => flatten_list(list, path, source, line_map),
        ast::Expr::Str(str_node) => flatten_str(str_node, path, source, line_map),
        ast::Expr::Literal(lit) => {
            if !path.is_empty() {
                let line = source.line_of(lit.syntax().text_range());
                emit(line_map, line, path, lit.syntax().text().to_string());
            }
        }
        ast::Expr::Ident(ident) => {
            if !path.is_empty() {
                let line = source.line_of(ident.syntax().text_range());
                let text = ident
                    .ident_token()
                    .map(|t| t.text().to_string())
                    .unwrap_or_else(|| ident.syntax().text().to_string());
                emit(line_map, line, path, text);
            }
        }
        ast::Expr::Paren(paren) => {
            if let Some(inner) = paren.expr() {
                flatten_expr(&inner, path, source, line_map);
            }
        }
        ast::Expr::Lambda(lambda) => {
            if let Some(body) = lambda.body() {
                flatten_expr(&body, path, source, line_map);
            }
        }
        ast::Expr::LetIn(let_in) => {
            if let Some(body) = let_in.body() {
                flatten_expr(&body, path, source, line_map);
            }
        }
        ast::Expr::With(with_expr) => {
            if let Some(body) = with_expr.body() {
                flatten_expr(&body, path, source, line_map);
            }
        }
        ast::Expr::Assert(assert_expr) => {
            if let Some(body) = assert_expr.body() {
                flatten_expr(&body, path, source, line_map);
            }
        }
        ast::Expr::Apply(apply) => {
            // A function call whose argument is (possibly parenthesized)
            // an attrset literal, e.g. `pkgs.mkShell { ... }`, is common
            // enough to be worth converting; the function itself is left as
            // source text since its line carries no gron entry.
            match apply.argument().map(unwrap_parens) {
                Some(ast::Expr::AttrSet(attrset)) => {
                    flatten_attrset(&attrset, path, source, line_map)
                }
                _ => flatten_opaque(expr, path, source, line_map),
            }
        }
        _ => flatten_opaque(expr, path, source, line_map),
    }
}

fn unwrap_parens(mut expr: ast::Expr) -> ast::Expr {
    while let ast::Expr::Paren(paren) = &expr {
        match paren.expr() {
            Some(inner) => expr = inner,
            None => break,
        }
    }
    expr
}

fn flatten_attrset(
    attrset: &ast::AttrSet,
    path: String,
    source: &LineIndex<'_>,
    line_map: &mut LineMap,
) {
    let mut entries = attrset.entries().peekable();
    if entries.peek().is_none() {
        if !path.is_empty() {
            let line = source.line_of(attrset.syntax().text_range());
            emit(line_map, line, path, "{}");
        }
        return;
    }

    for entry in entries {
        match entry {
            ast::Entry::AttrpathValue(apv) => {
                let (Some(attrpath), Some(value)) = (apv.attrpath(), apv.value()) else {
                    continue;
                };
                let mut new_path = path.clone();
                for attr in attrpath.attrs() {
                    new_path = join_path(&new_path, &attr_name(&attr));
                }
                flatten_expr(&value, new_path, source, line_map);
            }
            ast::Entry::Inherit(inherit) => flatten_inherit(&inherit, &path, source, line_map),
        }
    }
}

/// `inherit a b;` re-exports the enclosing scope's bindings; `inherit (expr)
/// a b;` re-exports fields of `expr`. Both forms are treated as attrset
/// entries whose value is the variable (or field access) they resolve to.
fn flatten_inherit(
    inherit: &ast::Inherit,
    path: &str,
    source: &LineIndex<'_>,
    line_map: &mut LineMap,
) {
    let from_text = inherit
        .from()
        .and_then(|from| from.expr())
        .map(|e| e.syntax().text().to_string());

    for attr in inherit.attrs() {
        let name = attr_name(&attr);
        let line = source.line_of(attr.syntax().text_range());
        let new_path = join_path(path, &name);
        let value = match &from_text {
            Some(expr_text) => format!("{}.{}", expr_text, name),
            None => name.clone(),
        };
        emit(line_map, line, new_path, value);
    }
}

fn flatten_list(list: &ast::List, path: String, source: &LineIndex<'_>, line_map: &mut LineMap) {
    let items: Vec<ast::Expr> = list.items().collect();
    if items.is_empty() {
        if !path.is_empty() {
            let line = source.line_of(list.syntax().text_range());
            emit(line_map, line, path, "[]");
        }
        return;
    }

    let range = list.syntax().text_range();
    let start_line = source.line_of(range);
    let end_line = source.line_of(TextRange::new(range.end(), range.end()));

    if start_line == end_line {
        if !path.is_empty() {
            let start = usize::from(range.start());
            let end = usize::from(range.end());
            emit(line_map, start_line, path, source.text[start..end].trim());
        }
        return;
    }

    // Multi-line list: output each element with index on its own line(s).
    for (idx, item) in items.into_iter().enumerate() {
        let indexed_path = format!("{}[{}]", path, idx);
        flatten_expr(&item, indexed_path, source, line_map);
    }
}

fn flatten_str(str_node: &ast::Str, path: String, source: &LineIndex<'_>, line_map: &mut LineMap) {
    let parts = str_node.normalized_parts();
    let has_interpolation = parts
        .iter()
        .any(|p| matches!(p, ast::InterpolPart::Interpolation(_)));

    if has_interpolation {
        // Interpolated strings mix data and code; render the source as-is
        // rather than trying to gron the interpolated expressions.
        if !path.is_empty() {
            let line = source.line_of(str_node.syntax().text_range());
            let text = str_node.to_string().replace('\n', " ");
            emit(line_map, line, path, text.trim());
        }
        return;
    }

    let range = str_node.syntax().text_range();
    let start_line = source.line_of(range);
    let end_line = source.line_of(TextRange::new(range.end(), range.end()));

    if start_line == end_line {
        if !path.is_empty() {
            let text: String = parts
                .into_iter()
                .map(|p| match p {
                    ast::InterpolPart::Literal(s) => s,
                    ast::InterpolPart::Interpolation(_) => unreachable!(),
                })
                .collect();
            emit(line_map, start_line, path, quote_escape(&text));
        }
        return;
    }

    if !path.is_empty() {
        flatten_multiline_str(range, &path, source, line_map);
    }
}

/// Emits a multi-line string (`''...''` or a `"..."` string containing a
/// literal newline) as a newline-delimited array: each source line of the
/// string becomes an indexed element placed on the same line it came from.
/// The source text is used rather than the decoded value so that line
/// continuations keep their original line layout, matching the TOML and HCL
/// adapters' handling of multi-line strings.
fn flatten_multiline_str(
    range: TextRange,
    path: &str,
    source: &LineIndex<'_>,
    line_map: &mut LineMap,
) {
    let start = usize::from(range.start());
    let end = usize::from(range.end());
    let raw = &source.text[start..end];
    let delimiter = if raw.starts_with("''") { "''" } else { "\"" };
    let inner = raw
        .strip_prefix(delimiter)
        .and_then(|s| s.strip_suffix(delimiter))
        .unwrap_or(raw);

    let first_line = source.line_of(range);
    let mut elements: Vec<(usize, &str)> = inner
        .split('\n')
        .enumerate()
        .map(|(offset, text)| (first_line + offset, text.strip_suffix('\r').unwrap_or(text)))
        .collect();

    // The opening delimiter line usually holds nothing but the delimiter;
    // the closing delimiter line usually holds nothing but leading
    // whitespace before the delimiter. Drop them so indexes start at the
    // first line of content.
    if elements
        .first()
        .is_some_and(|(_, text)| text.trim().is_empty())
    {
        elements.remove(0);
    }
    if elements
        .last()
        .is_some_and(|(_, text)| text.trim().is_empty())
    {
        elements.pop();
    }

    for (idx, (line_idx, text)) in elements.into_iter().enumerate() {
        emit(
            line_map,
            line_idx,
            format!("{}[{}]", path, idx),
            quote_escape(text),
        );
    }
}

/// An expression with no special handling: rendered as a single leaf on its
/// own starting line, using its own Nix source syntax rather than trying to
/// reformat it (an operator, a comparison, a function call whose argument
/// isn't an attrset, etc.). Emitted only when `path` is non-empty, i.e. when
/// this expression is actually the value of some attrset key; otherwise (at
/// the top level, or inside an unwrapped `let`/`with`/lambda that never
/// reaches an attrset) there is nothing to anchor a gron entry to, and the
/// line is left as source text.
fn flatten_opaque(expr: &ast::Expr, path: String, source: &LineIndex<'_>, line_map: &mut LineMap) {
    if !path.is_empty() {
        let line = source.line_of(expr.syntax().text_range());
        let text = expr.to_string().replace('\n', " ");
        emit(line_map, line, path, text.trim());
    }
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

    /// Directory holding the Nix fixtures: `exampledir/nix/`.
    fn nix_fixture_dir() -> std::path::PathBuf {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("exampledir/nix/");
        d
    }

    /// Flattens a fixture in `exampledir/nix/` and returns its entries as
    /// `(line, path, value)` triples ordered by 0-indexed source line, before
    /// they are serialized into output text.
    fn flatten_fixture(name: &str) -> anyhow::Result<Vec<(usize, String, String)>> {
        let source = std::fs::read_to_string(nix_fixture_dir().join(name))?;
        let parse = rnix::Root::parse(&source);
        assert!(
            parse.errors().is_empty(),
            "fixture {name} failed to parse: {:?}",
            parse.errors()
        );
        let line_map = flatten_document(&parse, &source);

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

    /// Generic Nix attrset: every scalar type, nested attrsets, flow and
    /// multi-line lists, empty collections, `inherit` (both forms), a
    /// multi-line string, and a computed (`${...}`) key.
    #[test]
    fn test_simple_nix() -> anyhow::Result<()> {
        assert_eq!(
            flatten_fixture("simple.nix")?,
            entries(&[
                (1, "name", r#""John""#),
                (2, "age", "30"),
                (3, "ratio", "1.5"),
                (4, "enabled", "true"),
                (5, "nothing", "null"),
                (6, "tags", r#"["developer" "rust" "python"]"#),
                (9, "ports[0]", "80"),
                (10, "ports[1]", "443"),
                (13, "empty_list", "[]"),
                (14, "empty_set", "{}"),
                (15, "point.x", "1"),
                (15, "point.y", r#""two""#),
                (18, "person.name", r#""Alice""#),
                (19, "person.age", "25"),
                (22, "computed.${dynamicKey}", "1"),
                (25, "description[0]", r#""    line one""#),
                (26, "description[1]", r#""    line two""#),
                (29, "base.value", "1"),
                (30, "derived.a", "base.value"),
                (31, "derived.b", "extra.thing"),
            ])
        );

        Ok(())
    }

    /// A NixOS-module-style file: a lambda whose parameter pattern must be
    /// left untouched, wrapping an attrset body.
    #[test]
    fn test_module() -> anyhow::Result<()> {
        assert_eq!(
            flatten_fixture("module.nix")?,
            entries(&[
                (
                    4,
                    "options.services.example.enable",
                    "mkEnableOption \"example\""
                ),
                (
                    7,
                    "config.systemd.services.example.description",
                    r#""Example service""#
                ),
                (
                    8,
                    "config.systemd.services.example.wantedBy",
                    r#"["multi-user.target"]"#
                ),
            ])
        );

        Ok(())
    }

    /// A flake.nix-style file: a lambda (`outputs = { self, nixpkgs }: ...`)
    /// and `let ... in` bindings (both left untouched, but still unwrapped to
    /// reach the attrset they return) feeding a returned attrset, and a
    /// function call (`mkShell { ... }`) whose attrset argument is still
    /// converted.
    #[test]
    fn test_flake() -> anyhow::Result<()> {
        assert_eq!(
            flatten_fixture("flake.nix")?,
            entries(&[
                (1, "inputs.nixpkgs.url", r#""github:NixOS/nixpkgs""#),
                (9, "outputs.devShells.default.buildInputs[0]", "pkgs.hello"),
                (9, "outputs.devShells.default.buildInputs[1]", "pkgs.cowsay"),
            ])
        );

        Ok(())
    }

    /// The writer renders each entry as `path: value`, joins entries that
    /// share a line with ", ", passes through everything outside an
    /// attrset unchanged, and preserves the source line count.
    #[tokio::test]
    async fn test_serialization_preserves_lines() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(NixAdapter::new());
        let path = nix_fixture_dir().join("module.nix");
        let source = std::fs::read_to_string(&path)?;

        let (a, d) = simple_fs_adapt_info(&path).await?;
        let res = adapter.adapt(a, &d).await?;
        let output = String::from_utf8(adapted_to_vec(res).await?)?;

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), source.lines().count());

        assert_str_eq!(lines[0], "{ config, lib, pkgs, ... }:");
        assert_str_eq!(lines[1], "");
        assert_str_eq!(lines[2], "{");
        assert_str_eq!(lines[3], "  options.services.example = {");
        assert_str_eq!(
            lines[4],
            r#"options.services.example.enable: mkEnableOption "example""#
        );
        assert_str_eq!(lines[5], "  };");
        assert_str_eq!(lines[6], "");
        assert_str_eq!(
            lines[7],
            r#"config.systemd.services.example.description: "Example service""#
        );
        assert_str_eq!(
            lines[8],
            r#"config.systemd.services.example.wantedBy: ["multi-user.target"]"#
        );

        Ok(())
    }

    /// Unparsable input is passed through unchanged rather than failing.
    #[tokio::test]
    async fn test_invalid_nix_passthrough() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(NixAdapter::new());
        let path = nix_fixture_dir().join("invalid.nix");
        let source = std::fs::read_to_string(&path)?;

        let (a, d) = simple_fs_adapt_info(&path).await?;
        let res = adapter.adapt(a, &d).await?;
        let output = String::from_utf8(adapted_to_vec(res).await?)?;

        assert_str_eq!(output, source);

        Ok(())
    }
}
