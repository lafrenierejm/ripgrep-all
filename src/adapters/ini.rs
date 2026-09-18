use super::writing::{WritingFileAdapter, async_writeln};
use super::{AdapterMeta, FastFileMatcher, FileMatcher, GetMetadata};
use anyhow::Result;
use async_trait::async_trait;
use lazy_static::lazy_static;
use std::pin::Pin;
use tokio::io::{AsyncBufReadExt, AsyncWrite, BufReader};

/// INI-family formats that share the `[section]` plus `key = value` shape:
/// classic INI, Python config files, MySQL/PostgreSQL style `.cnf`/`.conf`,
/// systemd units, and freedesktop `.desktop` entries. Java properties files
/// use the same key/value lines without sections.
static EXTENSIONS: &[&str] = &[
    "ini",
    "cfg",
    "conf",
    "cnf",
    "properties",
    "desktop",
    "service",
    "socket",
    "timer",
    "mount",
    "target",
];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "ini".to_owned(),
        version: 1,
        description: "Converts INI-family files into a gron-like format with section-prefixed keys"
            .to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: None,
        keep_fast_matchers_if_accurate: false,
        disabled_by_default: false
    };
}

#[derive(Default, Clone)]
pub struct IniAdapter;

impl IniAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl GetMetadata for IniAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}

#[async_trait]
impl WritingFileAdapter for IniAdapter {
    async fn adapt_write(
        a: super::AdaptInfo,
        _detection_reason: &FileMatcher,
        mut oup: Pin<Box<dyn AsyncWrite + Send>>,
    ) -> Result<()> {
        // INI is line-oriented, so the file is streamed: each input line is
        // scanned as it arrives and its output written straight away. The
        // scanner only ever holds back one key line, while it waits to see
        // whether continuation lines follow it.
        let mut segments = BufReader::new(a.inp).split(b'\n');
        let mut scanner = Scanner::default();
        let mut out = Vec::new();
        let mut first = true;

        while let Some(segment) = segments.next_segment().await? {
            let mut line = String::from_utf8_lossy(&segment).into_owned();
            if line.ends_with('\r') {
                line.pop();
            }
            if first {
                first = false;
                if let Some(stripped) = line.strip_prefix('\u{feff}') {
                    line = stripped.to_owned();
                }
            }

            scanner.push(&line, &mut out);
            write_lines(&mut oup, &mut out).await?;
        }

        scanner.finish(&mut out);
        write_lines(&mut oup, &mut out).await?;

        Ok(())
    }
}

async fn write_lines(
    mut oup: &mut Pin<Box<dyn AsyncWrite + Send>>,
    out: &mut Vec<Line>,
) -> Result<()> {
    for line in out.drain(..) {
        match line {
            Line::Blank => async_writeln!(oup)?,
            Line::Entry { path, value } => {
                async_writeln!(oup, "{}: {}", path, quote_escape(&value))?
            }
            Line::Verbatim(text) => async_writeln!(oup, "{}", text)?,
        }
    }
    Ok(())
}

/// One line of output. The scanner produces exactly one `Line` per input
/// line, though it may hold a key line back until the following line is seen.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Line {
    /// A comment, section header, blank line, or the opening line of a
    /// multi-line value whose first line carries no text.
    Blank,
    /// A gron-style entry: a section-prefixed key and its raw value. Values
    /// are untyped strings; they are quoted when serialized.
    Entry { path: String, value: String },
    /// A line the scanner does not recognize, kept exactly as written so that
    /// non-INI files matched by extension (many `.conf` files) lose nothing.
    Verbatim(String),
}

/// A key line whose output is deferred until the next line shows whether it
/// is a scalar (`key: "value"`) or the start of a multi-line value
/// (`key[0]: ...`, one element per continuation line).
struct Pending {
    path: String,
    value: String,
    /// Leading whitespace of the key line; continuation lines are indented
    /// deeper than this. Git config indents every key by the same amount, so
    /// equal indentation is not a continuation.
    indent: usize,
    /// Set once the key line has been emitted, in either form.
    resolved: bool,
    /// Number of array elements emitted so far.
    count: usize,
    /// Blank output lines (blank or comment input lines) seen after an
    /// unresolved key line; emitted right after it once it resolves.
    deferred_blanks: usize,
}

#[derive(Default)]
struct Scanner {
    section: String,
    pending: Option<Pending>,
}

impl Scanner {
    fn push(&mut self, line: &str, out: &mut Vec<Line>) {
        let trimmed = line.trim();
        let indent = line.chars().take_while(|c| c.is_whitespace()).count();
        let is_blank = trimmed.is_empty() || is_comment(trimmed);

        if let Some(pending) = &mut self.pending {
            // A deeper-indented, non-blank, non-header line continues the value.
            if !is_blank && !is_section(trimmed) && indent > pending.indent {
                if !pending.resolved {
                    if pending.value.is_empty() {
                        out.push(Line::Blank);
                    } else {
                        out.push(Line::Entry {
                            path: format!("{}[0]", pending.path),
                            value: pending.value.clone(),
                        });
                        pending.count = 1;
                    }
                    pending.resolved = true;
                    out.extend(std::iter::repeat_n(Line::Blank, pending.deferred_blanks));
                    pending.deferred_blanks = 0;
                }
                out.push(Line::Entry {
                    path: format!("{}[{}]", pending.path, pending.count),
                    value: trimmed.to_owned(),
                });
                pending.count += 1;
                return;
            }

            // Blank and comment lines do not end a value; hold them back
            // until the key line is resolved so output stays in order.
            if is_blank {
                if pending.resolved {
                    out.push(Line::Blank);
                } else {
                    pending.deferred_blanks += 1;
                }
                return;
            }
        }

        // Anything else ends the pending value.
        self.flush_pending(out);

        if is_blank {
            out.push(Line::Blank);
        } else if let Some(name) = parse_section(trimmed) {
            self.section = name;
            out.push(Line::Blank);
        } else if let Some((key, value)) = split_key_value(trimmed) {
            let path = if self.section.is_empty() {
                key.to_owned()
            } else {
                format!("{}.{}", self.section, key)
            };
            self.pending = Some(Pending {
                path,
                value: value.to_owned(),
                indent,
                resolved: false,
                count: 0,
                deferred_blanks: 0,
            });
        } else {
            out.push(Line::Verbatim(line.to_owned()));
        }
    }

    fn finish(&mut self, out: &mut Vec<Line>) {
        self.flush_pending(out);
    }

    /// Emits a pending key line as a scalar entry if nothing resolved it.
    fn flush_pending(&mut self, out: &mut Vec<Line>) {
        if let Some(pending) = self.pending.take()
            && !pending.resolved
        {
            out.push(Line::Entry {
                path: pending.path,
                value: pending.value,
            });
            out.extend(std::iter::repeat_n(Line::Blank, pending.deferred_blanks));
        }
    }
}

fn is_comment(trimmed: &str) -> bool {
    trimmed.starts_with(';') || trimmed.starts_with('#')
}

fn is_section(trimmed: &str) -> bool {
    trimmed.starts_with('[') && trimmed.ends_with(']')
}

/// Parses `[name]` into `name`. Git-style subsections, `[remote "origin"]`,
/// become `remote.origin`.
fn parse_section(trimmed: &str) -> Option<String> {
    if !is_section(trimmed) {
        return None;
    }
    let inner = trimmed[1..trimmed.len() - 1].trim();
    match inner.split_once(char::is_whitespace) {
        Some((name, rest)) if rest.trim_start().starts_with('"') => {
            let sub = rest.trim().trim_matches('"');
            Some(format!("{}.{}", name, sub))
        }
        _ => Some(inner.to_owned()),
    }
}

/// Splits `key = value` or `key: value` at the first separator. A colon only
/// counts when the key is a single token, so lines like `listen 127.0.0.1:80;`
/// from non-INI files are left alone. One pair of matching surrounding quotes
/// is removed from the value, as git config and systemd do.
fn split_key_value(trimmed: &str) -> Option<(&str, &str)> {
    let (idx, sep) = trimmed
        .char_indices()
        .find(|(_, c)| *c == '=' || *c == ':')?;
    let key = trimmed[..idx].trim();
    if key.is_empty() || (sep == ':' && key.contains(char::is_whitespace)) {
        return None;
    }
    let value = trimmed[idx + 1..].trim();
    let value = strip_quotes(value);
    Some((key, value))
}

fn strip_quotes(value: &str) -> &str {
    for quote in ['"', '\''] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return &value[1..value.len() - 1];
        }
    }
    value
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

    /// Directory holding the INI fixtures: `exampledir/ini/`.
    fn ini_fixture_dir() -> std::path::PathBuf {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("exampledir/ini/");
        d
    }

    /// Scans a fixture in `exampledir/ini/` and returns its non-blank output
    /// lines as `(line, Line)` pairs, 0-indexed, before serialization. Also
    /// asserts that the scanner produced exactly one output per input line.
    fn scan_fixture(name: &str) -> anyhow::Result<Vec<(usize, Line)>> {
        let source = std::fs::read_to_string(ini_fixture_dir().join(name))?;

        let mut scanner = Scanner::default();
        let mut out = Vec::new();
        for line in source.lines() {
            scanner.push(line, &mut out);
        }
        scanner.finish(&mut out);

        assert_eq!(out.len(), source.lines().count());
        Ok(out
            .into_iter()
            .enumerate()
            .filter(|(_, line)| *line != Line::Blank)
            .collect())
    }

    fn entry(line: usize, path: &str, value: &str) -> (usize, Line) {
        (
            line,
            Line::Entry {
                path: path.to_owned(),
                value: value.to_owned(),
            },
        )
    }

    fn verbatim(line: usize, text: &str) -> (usize, Line) {
        (line, Line::Verbatim(text.to_owned()))
    }

    /// Generic INI: keys outside any section, both separators, an empty
    /// value, comments, inline text after a value, quoted values, a dotted
    /// section name, an indented multi-line value, and a bare word.
    #[test]
    fn test_simple_ini() -> anyhow::Result<()> {
        assert_eq!(
            scan_fixture("simple.ini")?,
            vec![
                entry(1, "name", "John"),
                entry(2, "age", "30"),
                entry(3, "empty", ""),
                entry(6, "server.host", "localhost"),
                entry(7, "server.port", "8080"),
                entry(9, "server.motd", "Hello, world! ; not a comment"),
                entry(10, "server.path", r"C:\Program Files\App"),
                entry(13, "server.tls.enabled", "yes"),
                entry(17, "options.description[0]", "first line"),
                entry(18, "options.description[1]", "second line"),
                verbatim(19, "Color"),
            ]
        );

        Ok(())
    }

    /// A Python `setup.cfg`: values containing the other separator, indented
    /// multi-line values with a comment inside, and continuation lines that
    /// themselves contain `=` and `:`.
    #[test]
    fn test_setup_cfg() -> anyhow::Result<()> {
        assert_eq!(
            scan_fixture("setup.cfg")?,
            vec![
                entry(1, "metadata.name", "example"),
                entry(2, "metadata.version", "0.1.0"),
                entry(3, "metadata.long_description", "file: README.md"),
                entry(6, "options.packages", "find:"),
                entry(8, "options.install_requires[0]", "requests>=2.0"),
                entry(10, "options.install_requires[1]", "click"),
                entry(
                    14,
                    "options.entry_points.console_scripts[0]",
                    "example = example.cli:main",
                ),
            ]
        );

        Ok(())
    }

    /// A git config: every key indented by a tab (not a continuation),
    /// `[remote "origin"]` subsections, repeated keys, and values containing
    /// colons after the `=`.
    #[test]
    fn test_git_config() -> anyhow::Result<()> {
        assert_eq!(
            scan_fixture("gitconfig.ini")?,
            vec![
                entry(1, "user.name", "Jane Doe"),
                entry(2, "user.email", "jane@example.com"),
                entry(4, "remote.origin.url", "https://example.com/repo.git"),
                entry(
                    5,
                    "remote.origin.fetch",
                    "+refs/heads/*:refs/remotes/origin/*"
                ),
                entry(6, "remote.origin.fetch", "+refs/tags/*:refs/tags/*"),
                entry(8, "alias.lg", "log --oneline --graph"),
            ]
        );

        Ok(())
    }

    /// A systemd unit: a backslash-continued value whose first line carries
    /// text, and a quoted value containing `=`.
    #[test]
    fn test_systemd_unit() -> anyhow::Result<()> {
        assert_eq!(
            scan_fixture("app.service")?,
            vec![
                entry(1, "Unit.Description", "Example service"),
                entry(2, "Unit.After", "network.target"),
                entry(5, "Service.Type", "simple"),
                entry(6, "Service.ExecStart[0]", r"/usr/bin/example \"),
                entry(7, "Service.ExecStart[1]", r"--config /etc/example.conf \"),
                entry(8, "Service.ExecStart[2]", "--verbose"),
                entry(9, "Service.Environment", "KEY=value"),
                entry(12, "Install.WantedBy", "multi-user.target"),
            ]
        );

        Ok(())
    }

    /// A desktop entry: a section name with a space and a localized key.
    #[test]
    fn test_desktop_entry() -> anyhow::Result<()> {
        assert_eq!(
            scan_fixture("app.desktop")?,
            vec![
                entry(1, "Desktop Entry.Type", "Application"),
                entry(2, "Desktop Entry.Name", "Example"),
                entry(3, "Desktop Entry.Name[de]", "Beispiel"),
                entry(4, "Desktop Entry.Exec", "example %U"),
                entry(5, "Desktop Entry.Categories", "Utility;Development;"),
            ]
        );

        Ok(())
    }

    /// A `.conf` file that is not INI at all: every unrecognized line is kept
    /// verbatim, including one with a colon inside a multi-word directive.
    #[test]
    fn test_non_ini_conf_passthrough() -> anyhow::Result<()> {
        assert_eq!(
            scan_fixture("nginx.conf")?,
            vec![
                verbatim(1, "worker_processes auto;"),
                verbatim(3, "events {"),
                verbatim(4, "    worker_connections 1024;"),
                verbatim(5, "}"),
                verbatim(7, "http {"),
                verbatim(8, "    server {"),
                verbatim(9, "        listen 127.0.0.1:80;"),
                verbatim(10, "        server_name example.com;"),
                verbatim(11, "    }"),
                verbatim(12, "}"),
            ]
        );

        Ok(())
    }

    /// The streaming writer quotes values, keeps verbatim lines as-is, and
    /// preserves the line count end to end.
    #[tokio::test]
    async fn test_serialization_preserves_lines() -> anyhow::Result<()> {
        let adapter: Box<dyn crate::adapters::FileAdapter> = Box::new(IniAdapter::new());
        let path = ini_fixture_dir().join("simple.ini");
        let source = std::fs::read_to_string(&path)?;

        let (a, d) = simple_fs_adapt_info(&path).await?;
        let res = adapter.adapt(a, &d).await?;
        let output = String::from_utf8(adapted_to_vec(res).await?)?;

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), source.lines().count());
        assert_str_eq!(lines[0], "");
        assert_str_eq!(lines[1], r#"name: "John""#);
        assert_str_eq!(lines[10], r#"server.path: "C:\\Program Files\\App""#);
        assert_str_eq!(lines[16], "");
        assert_str_eq!(lines[17], r#"options.description[0]: "first line""#);
        assert_str_eq!(lines[19], "Color");

        Ok(())
    }
}
