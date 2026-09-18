/**
 * Module for matching adapters to files based on file name or mime type
 */
use crate::adapters::*;

use anyhow::*;

use regex::Regex;

use std::iter::Iterator;

use std::sync::Arc;

// match only based on file path
#[derive(Clone, Debug)]
pub enum FastFileMatcher {
    // MimeType(Regex),
    /**
     * without the leading dot, e.g. "jpg" or "tar.gz". Matched as /.*\.ext$/
     *
     */
    FileExtension(String),
    /// The complete file name, e.g. ".gitconfig" or "Cargo.lock". Matched
    /// case-insensitively against the last path component, so files with no
    /// useful extension (dotfiles, lock files) can still be routed to an adapter.
    FileName(String),
    // todo: maybe add others, e.g. regex on whole filename or even paths
    // todo: maybe allow matching a directory (e.g. /var/lib/postgres)
}

#[derive(Clone, Debug)]
pub enum FileMatcher {
    /// any type of fast matcher
    Fast(FastFileMatcher),
    ///
    /// match by exact mime type extracted using tree_magic
    /// TODO: allow match ignoring suffix etc?
    MimeType(String),
}

impl From<FastFileMatcher> for FileMatcher {
    fn from(t: FastFileMatcher) -> Self {
        Self::Fast(t)
    }
}

pub struct FileMeta {
    // filename is not actually a utf8 string, but since we can't do regex on OsStr and can't get a &[u8] from OsStr either,
    // and since we probably only want to do only matching on ascii stuff anyways, this is the filename as a string with non-valid bytes removed
    pub lossy_filename: String,
    // only given when slow matching is enabled
    pub mimetype: Option<&'static str>,
}

pub fn extension_to_regex(extension: &str) -> Regex {
    Regex::new(&format!("(?i)\\.{}$", regex::escape(extension)))
        .expect("we know this regex compiles")
}

#[allow(clippy::type_complexity)]
pub fn adapter_matcher(
    adapters: &[Arc<dyn FileAdapter>],
    slow: bool,
) -> Result<Box<dyn Fn(FileMeta) -> Option<(Arc<dyn FileAdapter>, FileMatcher)> + Send + Sync>> {
    let adapter_names: Vec<String> = adapters.iter().map(|e| e.metadata().name.clone()).collect();
    let mut ext_map: std::collections::HashMap<String, Vec<(Arc<dyn FileAdapter>, FileMatcher)>> =
        std::collections::HashMap::new();
    let mut mime_map: std::collections::HashMap<String, Vec<(Arc<dyn FileAdapter>, FileMatcher)>> =
        std::collections::HashMap::new();
    let mut name_map: std::collections::HashMap<String, Vec<(Arc<dyn FileAdapter>, FileMatcher)>> =
        std::collections::HashMap::new();
    for adapter in adapters.iter() {
        let metadata = adapter.metadata();
        for matcher in metadata.get_matchers(slow) {
            match matcher.as_ref() {
                FileMatcher::MimeType(m) => {
                    let k = m.to_string();
                    mime_map
                        .entry(k)
                        .or_default()
                        .push((adapter.clone(), FileMatcher::MimeType(m.clone())));
                }
                FileMatcher::Fast(FastFileMatcher::FileExtension(ext)) => {
                    let k = ext.to_ascii_lowercase();
                    ext_map.entry(k).or_default().push((
                        adapter.clone(),
                        FileMatcher::Fast(FastFileMatcher::FileExtension(ext.clone())),
                    ));
                }
                FileMatcher::Fast(FastFileMatcher::FileName(name)) => {
                    let k = name.to_ascii_lowercase();
                    name_map.entry(k).or_default().push((
                        adapter.clone(),
                        FileMatcher::Fast(FastFileMatcher::FileName(name.clone())),
                    ));
                }
            }
        }
    }
    let func = move |meta: FileMeta| {
        let path = std::path::Path::new(&meta.lossy_filename);
        let name = path
            .file_name()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase());
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase());
        let mut candidates: Vec<(Arc<dyn FileAdapter>, FileMatcher)> = vec![];
        if let Some(name) = name
            && let Some(v) = name_map.get(&name)
        {
            candidates.extend(v.iter().cloned());
        }
        if let Some(ext) = ext
            && let Some(v) = ext_map.get(&ext)
        {
            candidates.extend(v.iter().cloned());
        }
        if slow
            && let Some(mt) = meta.mimetype
            && let Some(v) = mime_map.get(mt)
        {
            candidates.extend(v.iter().cloned());
        }
        if candidates.is_empty() {
            return None;
        }
        if candidates.len() > 1 {
            candidates.sort_by_key(|e| {
                adapter_names
                    .iter()
                    .position(|r| r == &e.0.metadata().name)
                    .unwrap_or(usize::MAX)
            });
            eprintln!(
                "Warning: found multiple adapters for {}:",
                meta.lossy_filename
            );
            for mmatch in candidates.iter() {
                eprintln!(" - {}", mmatch.0.metadata().name);
            }
        }
        Some(candidates.remove(0))
    };
    Ok(Box::new(func))
}

#[cfg(test)]
mod test {
    use super::*;

    /// Name of the adapter the built-in set selects for `filename`, if any.
    fn adapter_for(filename: &str) -> Option<String> {
        let (adapters, _) = get_all_adapters(None);
        let matcher = adapter_matcher(&adapters, false).unwrap();
        matcher(FileMeta {
            lossy_filename: filename.to_owned(),
            mimetype: None,
        })
        .map(|(adapter, _)| adapter.metadata().name.clone())
    }

    #[test]
    fn matches_by_extension_case_insensitively() {
        assert_eq!(adapter_for("dir/settings.ini").as_deref(), Some("ini"));
        assert_eq!(adapter_for("UPPER.TOML").as_deref(), Some("toml"));
    }

    #[test]
    fn matches_by_file_name_case_insensitively() {
        assert_eq!(adapter_for("/home/user/.gitconfig").as_deref(), Some("ini"));
        assert_eq!(adapter_for("project/Cargo.lock").as_deref(), Some("toml"));
        assert_eq!(adapter_for("project/CARGO.LOCK").as_deref(), Some("toml"));
    }

    #[test]
    fn unknown_files_have_no_adapter() {
        assert_eq!(adapter_for("notes.unknownext"), None);
        assert_eq!(adapter_for(".bashrc"), None);
        // A file name matcher must not match a longer name that merely ends
        // with the same text.
        assert_eq!(adapter_for("not-a-Cargo.lock"), None);
    }
}
