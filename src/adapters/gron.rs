use super::*;
use lazy_static::lazy_static;
use spawning::{SpawningFileAdapter, SpawningFileAdapterTrait};
use std::process::Command;

static EXTENSIONS: &[&str] = &["json"];

lazy_static! {
    static ref METADATA: AdapterMeta = AdapterMeta {
        name: "gron".to_owned(),
        version: 1,
        description: "Uses gron to flatten JSON files to make their structure searchable by line.".to_owned(),
        recurses: false,
        fast_matchers: EXTENSIONS
            .iter()
            .map(|s| FastFileMatcher::FileExtension(s.to_string()))
            .collect(),
        slow_matchers: Some(vec![FileMatcher::MimeType(
            "application/json".to_owned()
        )]),
        keep_fast_matchers_if_accurate: false,
        disabled_by_default: true
    };
}
#[derive(Default)]
pub struct GronAdapter {}

impl GronAdapter {
    pub fn new() -> GronAdapter {
        GronAdapter {}
    }
}

impl GetMetadata for GronAdapter {
    fn metadata(&self) -> &AdapterMeta {
        &METADATA
    }
}
impl SpawningFileAdapterTrait for GronAdapter {
    fn get_exe(&self) -> &str {
        "gron"
    }
    fn command(&self, _filepath_hint: &Path, mut cmd: Command) -> Command {
        cmd.arg("-").arg("-");
        Some(cmd)
    }
}
