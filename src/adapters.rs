pub mod custom;
pub mod decompress;
pub mod ffmpeg;
pub mod mbox;
pub mod postproc;
use std::sync::Arc;
pub mod sqlite;
pub mod tar;
pub mod writing;
pub mod zip;
use crate::{adapted_iter::AdaptedFilesIterBox, config::RgaConfig, matching::*};
use anyhow::{format_err, Context, Result};
use async_trait::async_trait;
use custom::strs;
use custom::Builtin;
use custom::CustomAdapterConfig;
use custom::BUILTIN_SPAWNING_ADAPTERS;
use log::*;
use tokio::io::AsyncRead;

use core::fmt::Debug;
use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::Iterator;
use std::path::PathBuf;
use std::pin::Pin;

use self::postproc::PostprocPageBreaks;

pub type ReadBox = Pin<Box<dyn AsyncRead + Send>>;
pub struct AdapterMeta {
    /// unique short name of this adapter (a-z0-9 only)
    pub name: String,
    /// version identifier. used to key cache entries, change if your output format changes
    pub version: i32,
    pub description: String,
    /// indicates whether this adapter can descend (=call rga_preproc again). if true, the cache key needs to include the list of active adapters
    pub recurses: bool,
    /// list of matchers (interpreted as a OR b OR ...)
    pub fast_matchers: Vec<FastFileMatcher>,
    /// list of matchers when we have mime type detection active (interpreted as ORed)
    /// warning: this *overrides* the fast matchers
    pub slow_matchers: Vec<FileMatcher>,
    /// if true, slow_matchers is merged with fast matchers if accurate is enabled
    /// for example, in sqlite you want this disabled since the db extension can mean other things and the mime type matching is very accurate for sqlite.
    /// but for tar you want it enabled, since the tar extension is very accurate but the tar mime matcher can have false negatives
    pub keep_fast_matchers_if_accurate: bool,
    // if true, adapter is only used when user lists it in `--rga-adapters`
    pub disabled_by_default: bool,
}
impl AdapterMeta {
    // todo: this is pretty ugly
    pub fn get_matchers(&self, slow: bool) -> Box<dyn Iterator<Item = Cow<FileMatcher>> + '_> {
        match (
            slow,
            self.keep_fast_matchers_if_accurate,
            &self.slow_matchers,
            &self.fast_matchers,
        ) {
            (true, false, sm, _) => Box::new(sm.iter().map(Cow::Borrowed)),
            (true, true, sm, fm) => Box::new(
                sm.iter().map(Cow::Borrowed).chain(
                    fm.iter()
                        .map(|e| Cow::Owned(FileMatcher::Fast(e.clone())))
                        .collect::<Vec<_>>(),
                ),
            ),
            // slow matching disabled
            (false, _, _, fm) => {
                Box::new(fm.iter().map(|e| Cow::Owned(FileMatcher::Fast(e.clone()))))
            }
        }
    }
}

pub trait Adapter {
    fn name(&self) -> String;
    fn version(&self) -> i32;
    fn description(&self) -> String;
    fn recurses(&self) -> bool;
    fn disabled_by_default(&self) -> bool;
    fn keep_fast_matchers_if_accurate(&self) -> bool;
    fn extensions(&self) -> Vec<String>;
    fn mimetypes(&self) -> Vec<String>;

    fn metadata(&self) -> AdapterMeta {
        return AdapterMeta {
            name: self.name(),
            version: self.version(),
            description: self.description(),
            recurses: true,
            fast_matchers: self
                .extensions()
                .iter()
                .map(|s| FastFileMatcher::FileExtension(s.to_string()))
                .collect(),
            slow_matchers: self
                .mimetypes()
                .iter()
                .map(|mimetype| FileMatcher::MimeType(mimetype.to_string()))
                .collect(),
            disabled_by_default: self.disabled_by_default(),
            keep_fast_matchers_if_accurate: self.keep_fast_matchers_if_accurate(),
        };
    }
}

#[async_trait]
pub trait FileAdapter: Adapter + Send + Sync {
    /// adapt a file.
    ///
    /// detection_reason is the Matcher that was used to identify this file. Unless --rga-accurate was given, it is always a FastMatcher
    async fn adapt(
        &self,
        a: AdaptInfo,
        detection_reason: &FileMatcher,
    ) -> Result<AdaptedFilesIterBox>;
}

pub struct AdaptInfo {
    /// file path. May not be an actual file on the file system (e.g. in an archive). Used for matching file extensions.
    pub filepath_hint: PathBuf,
    /// true if filepath_hint is an actual file on the file system
    pub is_real_file: bool,
    /// depth at which this file is in archives. 0 for real filesystem
    pub archive_recursion_depth: i32,
    /// stream to read the file from. can be from a file or from some decoder
    pub inp: ReadBox,
    /// prefix every output line with this string to better indicate the file's location if it is in some archive
    pub line_prefix: String,
    pub postprocess: bool,
    pub config: RgaConfig,
}

/// (enabledAdapters, disabledAdapters)
type AdaptersTuple = (Vec<Arc<dyn FileAdapter>>, Vec<Arc<dyn FileAdapter>>);

static EMPTY_SLICE: &'static [&'static str] = &[];

pub fn get_all_adapters(
    additional_extensions: &HashMap<&[&str], Builtin>,
    custom_adapters: Option<Vec<CustomAdapterConfig>>,
) -> AdaptersTuple {
    let mut extensions = additional_extensions.clone();

    for extension in decompress::EXTENSIONS_BZ2 {
        extensions.try_insert(extension, Builtin::Bz2)
    }

    let mut bz2_extensions: [&str] = [
        decompress::EXTENSIONS_BZ2[..],
        (additional_extensions
            .get(&Builtin::Bz2)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // .as_ref()
    // .and_then(|ids| ids.bz2.as_ref())
    // .and_then(|bz2| bz2.extensions.clone())
    // .unwrap_or_else(|| strs(decompress::EXTENSIONS_BZ2));
    // let bz2_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.bz2.as_ref())
    //     .and_then(|bz2| bz2.mimetypes.clone())
    //     .unwrap_or_else(|| strs(decompress::MIMETYPES_BZ2));

    let gz_extensions = [
        decompress::EXTENSIONS_GZ[..],
        (additional_extensions
            .get(&Builtin::Gz)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let gz_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.gz.as_ref())
    //     .and_then(|gz| gz.mimetypes.clone())
    //     .unwrap_or_else(|| strs(decompress::MIMETYPES_GZ));

    let xz_extensions = [
        decompress::EXTENSIONS_XZ[..],
        (additional_extensions
            .get(&Builtin::Xz)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let xz_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.xz.as_ref())
    //     .and_then(|xz| xz.mimetypes.clone())
    //     .unwrap_or_else(|| strs(decompress::MIMETYPES_XZ));

    let zst_extensions = [
        decompress::EXTENSIONS_ZST[..],
        (additional_extensions
            .get(&Builtin::Zst)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let zst_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.zst.as_ref())
    //     .and_then(|zst| zst.mimetypes.clone())
    //     .unwrap_or_else(|| strs(decompress::MIMETYPES_ZST));

    let ffmpeg_extensions = [
        ffmpeg::EXTENSIONS[..],
        (additional_extensions
            .get(&Builtin::Ffmpeg)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let ffmpeg_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.ffmpeg.as_ref())
    //     .and_then(|ffmpeg| ffmpeg.mimetypes.clone())
    //     .unwrap_or_else(|| strs(ffmpeg::MIMETYPES));

    let mbox_extensions = [
        mbox::EXTENSIONS[..],
        (additional_extensions
            .get(&Builtin::Mbox)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let mbox_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.mbox.as_ref())
    //     .and_then(|mbox| mbox.mimetypes.clone())
    //     .unwrap_or_else(|| strs(mbox::MIMETYPES));

    let sqlite_extensions = [
        sqlite::EXTENSIONS[..],
        (additional_extensions
            .get(&Builtin::Sqlite)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let sqlite_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.sqlite.as_ref())
    //     .and_then(|sqlite| sqlite.mimetypes.clone())
    //     .unwrap_or_else(|| strs(sqlite::MIMETYPES));

    let tar_extensions = [
        tar::EXTENSIONS[..],
        (additional_extensions
            .get(&Builtin::Tar)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let tar_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.tar.as_ref())
    //     .and_then(|tar| tar.mimetypes.clone())
    //     .unwrap_or_else(|| strs(tar::MIMETYPES));

    let zip_extensions = [
        zip::EXTENSIONS[..],
        (additional_extensions
            .get(&Builtin::Zip)
            .unwrap_or(&EMPTY_SLICE))[..],
    ];
    // let zip_mimetypes = additional_extensions
    //     .as_ref()
    //     .and_then(|ids| ids.zip.as_ref())
    //     .and_then(|zip| zip.mimetypes.clone())
    //     .unwrap_or_else(|| strs(zip::MIMETYPES));

    // order in descending priority
    let mut adapters: Vec<Arc<dyn FileAdapter>> = vec![];
    if let Some(custom_adapters) = custom_adapters {
        for adapter_config in custom_adapters {
            adapters.push(Arc::new(adapter_config.to_adapter()));
        }
    }

    let internal_adapters: Vec<Arc<dyn FileAdapter>> = vec![
        Arc::new(PostprocPageBreaks::default()),
        Arc::new(ffmpeg::FFmpegAdapter {
            extensions: ffmpeg_extensions,
            mimetypes: ffmpeg_mimetypes,
        }),
        Arc::new(zip::ZipAdapter {
            extensions: zip_extensions,
            mimetypes: zip_mimetypes,
        }),
        Arc::new(decompress::DecompressAdapter {
            extensions_gz: gz_extensions,
            extensions_bz2: bz2_extensions,
            extensions_xz: xz_extensions,
            extensions_zst: zst_extensions,
            mimetypes_gz: gz_mimetypes,
            mimetypes_bz2: bz2_mimetypes,
            mimetypes_xz: xz_mimetypes,
            mimetypes_zst: zst_mimetypes,
        }),
        Arc::new(mbox::MboxAdapter {
            extensions: mbox_extensions,
            mimetypes: mbox_mimetypes,
        }),
        Arc::new(sqlite::SqliteAdapter {
            extensions: sqlite_extensions,
            mimetypes: sqlite_mimetypes,
        }),
        Arc::new(tar::TarAdapter {
            extensions: tar_extensions,
            mimetypes: tar_mimetypes,
        }),
    ];
    adapters.extend(
        BUILTIN_SPAWNING_ADAPTERS
            .iter()
            .map(|e| -> Arc<dyn FileAdapter> { Arc::new(e.to_adapter()) }),
    );
    adapters.extend(internal_adapters);

    adapters
        .into_iter()
        .partition(|e| !e.metadata().disabled_by_default)
}

/**
 * filter adapters by given names:
 *
 *  - "" means use default enabled adapter list
 *  - "a,b" means use adapters a,b
 *  - "-a,b" means use default list except for a and b
 *  - "+a,b" means use default list but also a and b (a,b will be prepended to the list so given higher priority)
 */
pub fn get_adapters_filtered<T: AsRef<str>>(
    additional_extensions: &HashMap<&Builtin, &[&str]>,
    custom_adapters: Option<Vec<CustomAdapterConfig>>,
    adapter_names: &[T],
) -> Result<Vec<Arc<dyn FileAdapter>>> {
    let (def_enabled_adapters, def_disabled_adapters) =
        get_all_adapters(additional_extensions, custom_adapters);
    let adapters = if !adapter_names.is_empty() {
        let adapters_map: HashMap<_, _> = def_enabled_adapters
            .iter()
            .chain(def_disabled_adapters.iter())
            .map(|e| (e.metadata().name.clone(), e.clone()))
            .collect();
        let mut adapters = vec![];
        let mut subtractive = false;
        let mut additive = false;
        for (i, name) in adapter_names.iter().enumerate() {
            let mut name = name.as_ref();
            if i == 0 && (name.starts_with('-')) {
                subtractive = true;
                name = &name[1..];
                adapters = def_enabled_adapters.clone();
            } else if i == 0 && (name.starts_with('+')) {
                name = &name[1..];
                adapters = def_enabled_adapters.clone();
                additive = true;
            }
            if subtractive {
                let inx = adapters
                    .iter()
                    .position(|a| a.metadata().name == name)
                    .ok_or_else(|| format_err!("Could not remove adapter {}: Not in list", name))?;
                adapters.remove(inx);
            } else {
                let adapter = adapters_map
                    .get(name)
                    .ok_or_else(|| {
                        format_err!(
                            "Unknown adapter: \"{}\". Known adapters: {}",
                            name,
                            adapters_map
                                .keys()
                                .map(|e| e.as_ref())
                                .collect::<Vec<&str>>()
                                .join(", ")
                        )
                    })?
                    .clone();
                if additive {
                    adapters.insert(0, adapter);
                } else {
                    adapters.push(adapter);
                }
            }
        }
        adapters
    } else {
        def_enabled_adapters
    };
    debug!(
        "Chosen available adapters: {}",
        adapters
            .iter()
            .map(|a| a.metadata().name.clone())
            .collect::<Vec<String>>()
            .join(",")
    );
    Ok(adapters)
}
