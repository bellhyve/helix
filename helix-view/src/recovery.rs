//! Synchronous, full-snapshot recovery storage. Callers serialize access to a
//! `Swap`; dropping it deliberately leaves its files available for recovery.
//!
//! Files contain a magic string, a big-endian u32 JSON-header length, a bounded
//! versioned header, and an exact-length UTF-8 payload. Native path units are
//! stored without loss; sidefiles are not portable between operating systems.

mod state;
pub(crate) use state::State;

use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, ensure, Context, Result};
use helix_core::Rope;
use same_file::Handle;
use serde::{Deserialize, Deserializer, Serialize};
use tempfile::{Builder, NamedTempFile};

const MAGIC: &[u8] = b"HELIX-RECOVERY\n";
const VERSION: u32 = 1;
const MAX_HEADER: usize = 64 * 1024;
const PREFIX: &str = ".helix-recovery-";
const TEMP_PREFIX: &str = ".helix-pending-";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
pub struct Config {
    pub enable: bool,
    pub directories: Vec<PathBuf>,
    /// A filename suffix, not a path. ASCII letters, digits, '.', '_' and '-'.
    #[serde(deserialize_with = "deserialize_suffix")]
    pub suffix: String,
    /// Maximum snapshot UTF-8 bytes; zero means unlimited.
    pub size_threshold: usize,
    /// Positive edit count between automatic snapshots (enforced by callers).
    #[serde(deserialize_with = "deserialize_update_count")]
    pub update_count: usize,
    /// Positive, representable maximum pending-change interval in seconds (enforced by callers).
    #[serde(deserialize_with = "deserialize_update_time")]
    pub update_time: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enable: false,
            directories: [".", "~/tmp", "/var/tmp", "/tmp"]
                .into_iter()
                .map(PathBuf::from)
                .collect(),
            suffix: ".swp".into(),
            size_threshold: 0,
            update_count: 200,
            update_time: 4,
        }
    }
}

impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(valid_suffix(&self.suffix), "unsafe recovery suffix");
        ensure!(
            self.update_count > 0,
            "recovery update-count must be positive"
        );
        ensure!(
            valid_update_time(self.update_time),
            "recovery update-time must be positive and representable as an Instant deadline"
        );
        Ok(())
    }
}

fn valid_suffix(suffix: &str) -> bool {
    !suffix.is_empty()
        && suffix.len() <= 64
        && !suffix.ends_with('.')
        && suffix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn deserialize_suffix<'de, D: Deserializer<'de>>(de: D) -> Result<String, D::Error> {
    let suffix = String::deserialize(de)?;
    if !valid_suffix(&suffix) {
        return Err(serde::de::Error::custom("unsafe recovery suffix"));
    }
    Ok(suffix)
}

fn deserialize_update_count<'de, D: Deserializer<'de>>(de: D) -> Result<usize, D::Error> {
    let value = usize::deserialize(de)?;
    if value == 0 {
        return Err(serde::de::Error::custom("update-count must be positive"));
    }
    Ok(value)
}

fn deserialize_update_time<'de, D: Deserializer<'de>>(de: D) -> Result<u64, D::Error> {
    let value = u64::deserialize(de)?;
    if !valid_update_time(value) {
        return Err(serde::de::Error::custom(
            "update-time must be positive and representable as an Instant deadline",
        ));
    }
    Ok(value)
}

fn valid_update_time(value: u64) -> bool {
    value > 0
        && Instant::now()
            .checked_add(Duration::from_secs(value))
            .is_some()
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    /// Relative originals are interpreted against `cwd`; writes resolve symlinks
    /// and store the absolute target. Reads retain that saved identity unchanged.
    pub path: Option<PathBuf>,
    /// Must be absolute, including for unnamed buffers.
    pub cwd: PathBuf,
    pub text: Rope,
    pub encoding: String,
    pub has_bom: bool,
    pub line_ending: String,
    /// Anchor/head character offsets, not byte offsets.
    pub selections: Vec<(usize, usize)>,
    pub primary: usize,
}

impl Snapshot {
    fn validate(&self) -> Result<()> {
        ensure!(self.cwd.is_absolute(), "recovery cwd must be absolute");
        ensure!(
            !self.cwd.as_os_str().as_encoded_bytes().contains(&0)
                && self
                    .path
                    .as_ref()
                    .is_none_or(|p| !p.as_os_str().as_encoded_bytes().contains(&0)),
            "NUL in recovery path"
        );
        ensure!(
            self.primary < self.selections.len(),
            "invalid recovery primary selection"
        );
        ensure!(
            self.selections
                .iter()
                .all(|&(anchor, head)| anchor <= self.text.len_chars()
                    && head <= self.text.len_chars()),
            "recovery selection outside text"
        );
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "platform", content = "units", rename_all = "lowercase")]
enum StoredPath {
    #[cfg(unix)]
    Unix(Vec<u8>),
    #[cfg(windows)]
    Windows(Vec<u16>),
    #[cfg(not(any(unix, windows)))]
    Utf8(String),
}

impl StoredPath {
    fn encode(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Ok(Self::Unix(path.as_os_str().as_bytes().to_vec()))
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            Ok(Self::Windows(path.as_os_str().encode_wide().collect()))
        }
        #[cfg(not(any(unix, windows)))]
        {
            Ok(Self::Utf8(path.to_str().context("non-UTF-8 path")?.into()))
        }
    }

    fn decode(self) -> PathBuf {
        match self {
            #[cfg(unix)]
            Self::Unix(units) => {
                use std::os::unix::ffi::OsStringExt;
                OsString::from_vec(units).into()
            }
            #[cfg(windows)]
            Self::Windows(units) => {
                use std::os::windows::ffi::OsStringExt;
                OsString::from_wide(&units).into()
            }
            #[cfg(not(any(unix, windows)))]
            Self::Utf8(text) => text.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    version: u32,
    timestamp: u64,
    original_size: Option<u64>,
    original_modified: Option<u64>,
    path: Option<StoredPath>,
    cwd: StoredPath,
    payload_len: u64,
    encoding: String,
    has_bom: bool,
    line_ending: String,
    selections: Vec<(usize, usize)>,
    primary: usize,
}

impl Header {
    fn new(snapshot: &Snapshot) -> Result<Self> {
        let original = snapshot
            .path
            .as_ref()
            .and_then(|path| fs::metadata(snapshot.cwd.join(path)).ok());
        Ok(Self {
            version: VERSION,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            original_size: original.as_ref().map(|m| m.len()),
            original_modified: original
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs()),
            path: snapshot
                .path
                .as_deref()
                .map(StoredPath::encode)
                .transpose()?,
            cwd: StoredPath::encode(&snapshot.cwd)?,
            payload_len: snapshot.text.len_bytes() as u64,
            encoding: snapshot.encoding.clone(),
            has_bom: snapshot.has_bom,
            line_ending: snapshot.line_ending.clone(),
            selections: snapshot.selections.clone(),
            primary: snapshot.primary,
        })
    }
}

// O_NOFOLLOW / OPEN_REPARSE_POINT protect the leaf even if replaced between
// lstat and open. O_NONBLOCK prevents a substituted FIFO from hanging a worker.
fn open_regular(path: &Path) -> Result<File> {
    ensure!(
        fs::symlink_metadata(path)?.file_type().is_file(),
        "not a regular recovery file: {}",
        path.display()
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "not a regular recovery file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no arguments or preconditions.
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "recovery file belongs to another user"
        );
        ensure!(
            metadata.mode() & 0o022 == 0,
            "recovery file is writable by other users"
        );
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        ensure!(
            metadata.file_attributes() & 0x400 == 0, // FILE_ATTRIBUTE_REPARSE_POINT
            "recovery file is a reparse point"
        );
    }
    Ok(file)
}

fn read_header(reader: &mut BufReader<File>) -> Result<Header> {
    let mut magic = [0; MAGIC.len()];
    reader.read_exact(&mut magic)?;
    ensure!(&magic[..] == MAGIC, "not a Helix recovery file");
    let mut length = [0; 4];
    reader.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    ensure!(
        length > 0 && length <= MAX_HEADER,
        "invalid recovery header length"
    );
    let mut json = vec![0; length];
    reader.read_exact(&mut json)?;
    let header: Header = serde_json::from_slice(&json)?;
    ensure!(
        header.version == VERSION,
        "unsupported recovery version {}",
        header.version
    );
    let expected = (MAGIC.len() as u64 + 4 + length as u64)
        .checked_add(header.payload_len)
        .context("recovery payload length overflow")?;
    ensure!(
        reader.get_ref().metadata()?.len() == expected,
        "recovery payload length does not match file"
    );
    Ok(header)
}

/// Read an explicit sidefile, rejecting symlinks, invalid UTF-8, truncated or
/// trailing data, and invalid selection offsets. Allocation follows actual
/// streamed text, never an untrusted declared payload length.
pub fn read(path: &Path) -> Result<Snapshot> {
    (|| -> Result<Snapshot> {
        let mut reader = BufReader::new(open_regular(path)?);
        let header = read_header(&mut reader)?;
        let text = Rope::from_reader((&mut reader).take(header.payload_len))?;
        ensure!(
            text.len_bytes() as u64 == header.payload_len,
            "truncated recovery text"
        );
        ensure!(reader.read(&mut [0; 1])? == 0, "trailing recovery data");
        let snapshot = Snapshot {
            path: header.path.map(StoredPath::decode),
            cwd: header.cwd.decode(),
            text,
            encoding: header.encoding,
            has_bom: header.has_bom,
            line_ending: header.line_ending,
            selections: header.selections,
            primary: header.primary,
        };
        snapshot.validate()?;
        Ok(snapshot)
    })()
    .with_context(|| format!("reading recovery file {}", path.display()))
}

// A new file may not exist yet, and some filesystems cannot represent all native
// path units. Resolve its parent where possible without losing those units.
fn original_path(path: &Path, cwd: &Path) -> Result<PathBuf> {
    let mut path = cwd.join(path);
    for _ in 0..40 {
        if let Ok(resolved) = fs::canonicalize(&path) {
            return Ok(resolved);
        }
        // Resolve even a dangling leaf symlink so its saved identity cannot
        // change when that alias is retargeted before explicit recovery.
        if let Ok(target) = fs::read_link(&path) {
            path = path
                .parent()
                .context("original path has no parent")?
                .join(target);
            continue;
        }
        for parent in path.ancestors().skip(1) {
            if let Ok(resolved) = fs::canonicalize(parent) {
                return Ok(resolved.join(path.strip_prefix(parent)?));
            }
        }
        return Ok(helix_stdx::path::normalize(&path));
    }
    bail!(
        "too many symlinks in recovery original path: {}",
        path.display()
    )
}

// Named originals passed here are already resolved by write/discover.
fn directories(config: &Config, path: Option<&Path>, cwd: &Path) -> Result<Vec<PathBuf>> {
    config.validate()?;
    ensure!(cwd.is_absolute(), "recovery cwd must be absolute");
    let original = path.map(|path| cwd.join(path));
    let mut directories = Vec::new();
    for directory in &config.directories {
        let directory = if directory == Path::new(".") {
            original
                .as_deref()
                .and_then(Path::parent)
                .unwrap_or(cwd)
                .to_owned()
        } else {
            cwd.join(helix_stdx::path::expand_tilde(directory.as_path()))
        };
        if !directories.contains(&directory) {
            directories.push(directory);
        }
    }
    Ok(directories)
}

/// Discovery does not read payloads or remove anything. Callers should surface
/// warnings, which include explicit paths to unreadable/corrupt candidates.
#[derive(Debug, Default)]
pub struct Discovery {
    pub candidates: Vec<PathBuf>,
    /// Native candidate/directory path and diagnostic, kept separate to avoid
    /// losing non-UTF-8 paths when reporting an unreadable recovery file.
    pub warnings: Vec<(PathBuf, String)>,
}

/// Scan every configured directory, even when automatic recovery is disabled.
/// Matching resolves the requested original, not the saved header path: an alias
/// retargeted since the snapshot must not identify the previous target's swap.
/// Unnamed buffers match `cwd` without requiring it to exist.
pub fn discover(config: &Config, path: Option<&Path>, cwd: &Path) -> Result<Discovery> {
    ensure!(cwd.is_absolute(), "recovery cwd must be absolute");
    let path = path.map(|path| original_path(path, cwd)).transpose()?;
    let mut found = Discovery::default();
    for directory in directories(config, path.as_deref(), cwd)? {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                found.warnings.push((directory.clone(), error.to_string()));
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    found.warnings.push((directory.clone(), error.to_string()));
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with(PREFIX) || !name.ends_with(&config.suffix) {
                continue;
            }
            let candidate = entry.path();
            let header =
                open_regular(&candidate).and_then(|file| read_header(&mut BufReader::new(file)));
            match header {
                Ok(header) => {
                    let original = header.path.map(StoredPath::decode);
                    let original_cwd = header.cwd.decode();
                    let matches = match (original.as_deref(), path.as_deref()) {
                        (Some(original), Some(path)) => {
                            original_cwd.join(original) == cwd.join(path)
                        }
                        (None, None) => original_cwd == cwd,
                        _ => false,
                    };
                    if matches {
                        found.candidates.push(candidate);
                    }
                }
                Err(error) => found.warnings.push((candidate, format!("{error:#}"))),
            }
        }
    }
    found.candidates.sort();
    found.candidates.dedup();
    Ok(found)
}

fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path.parent().context("recovery file has no parent")?)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

struct OwnedFile {
    path: PathBuf,
    // Keeping the handle open also prevents inode/file-ID reuse after unlink.
    identity: Handle,
}

impl OwnedFile {
    fn check(&self) -> Result<()> {
        ensure!(
            Handle::from_file(open_regular(&self.path)?)? == self.identity,
            "recovery file was replaced: {}",
            self.path.display()
        );
        Ok(())
    }

    fn remove(&self) -> Result<()> {
        match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            result => {
                result?;
            }
        }
        self.check()?;
        fs::remove_file(&self.path)?;
        sync_parent(&self.path)
    }
}

/// Only files published by this instance are owned. There is no Drop cleanup.
/// Serialize every write, migration and close using the same external mutex.
#[derive(Default)]
pub struct Swap {
    files: Vec<OwnedFile>,
    source_path: Option<PathBuf>,
    source_cwd: PathBuf,
    directories: Vec<PathBuf>,
    suffix: String,
    last_generation: Option<u64>,
    closed: bool,
}

impl Swap {
    pub fn path(&self) -> Option<&Path> {
        self.files.last().map(|file| file.path.as_path())
    }

    /// Write a full snapshot. Returns None when closed, older than the newest
    /// attempted generation, disabled, or over the configured byte threshold.
    /// Equal generations are allowed for explicit preserve/retry operations.
    /// Failed writes still fence off older queued generations.
    ///
    /// The path stays stable until the original path (or unnamed cwd), configured
    /// directories, or suffix changes.
    /// Migration publishes and syncs the new file before removing old files.
    /// A post-publication directory-sync/cleanup error retains ownership of all
    /// remaining files; `path()` exposes the newly published complete snapshot.
    pub fn write(
        &mut self,
        snapshot: &Snapshot,
        config: &Config,
        generation: u64,
    ) -> Result<Option<PathBuf>> {
        if self.closed || self.last_generation.is_some_and(|last| generation < last) {
            return Ok(None);
        }
        self.last_generation = Some(generation);
        config.validate()?;
        if !config.enable
            || (config.size_threshold != 0 && snapshot.text.len_bytes() > config.size_threshold)
        {
            return Ok(None);
        }
        snapshot.validate()?;
        // Resolve once in the storage worker, not on the document edit path.
        // Naming, header metadata, migration and directory choice share it.
        let resolved = Snapshot {
            path: snapshot
                .path
                .as_deref()
                .map(|path| original_path(path, &snapshot.cwd))
                .transpose()?,
            ..snapshot.clone()
        };
        let snapshot = &resolved;
        let header = serde_json::to_vec(&Header::new(snapshot)?)?;
        ensure!(header.len() <= MAX_HEADER, "recovery header exceeds 64 KiB");
        let directories = directories(config, snapshot.path.as_deref(), &snapshot.cwd)?;
        let migrate = self.files.is_empty()
            || self.source_path != snapshot.path
            || (snapshot.path.is_none() && self.source_cwd != snapshot.cwd)
            || self.directories != directories
            || self.suffix != config.suffix;
        let published = if migrate {
            let mut errors = Vec::new();
            let mut published = None;
            for directory in &directories {
                let attempt = (|| {
                    let temporary = stage(directory, snapshot, config, &header)?;
                    let name = temporary.path().file_name().unwrap().to_str().unwrap();
                    let destination =
                        directory.join(format!("{PREFIX}{}", &name[TEMP_PREFIX.len()..]));
                    publish(temporary, destination, None)
                })();
                match attempt {
                    Ok(file) => {
                        published = Some(file);
                        break;
                    }
                    Err(error) => errors.push(format!("{}: {error:#}", directory.display())),
                }
            }
            published
                .with_context(|| format!("no usable recovery directory: {}", errors.join("; ")))?
        } else {
            let current = self.files.last().unwrap();
            current.check()?;
            let temporary = stage(current.path.parent().unwrap(), snapshot, config, &header)?;
            publish(temporary, current.path.clone(), Some(current))?
        };
        if !migrate {
            self.files.pop();
        }
        self.source_path.clone_from(&snapshot.path);
        self.source_cwd.clone_from(&snapshot.cwd);
        self.directories = directories;
        self.suffix.clone_from(&config.suffix);
        self.files.push(published);
        let path = self.path().unwrap().to_owned();
        sync_parent(&path)
            .with_context(|| format!("syncing recovery directory for {}", path.display()))?;
        self.remove_files(true)?;
        Ok(Some(path))
    }

    /// Permanently fence writes before cleanup, even when removal fails. Calling
    /// close again retries cleanup; it never unlinks a replaced file or symlink.
    pub fn close(&mut self) -> Result<()> {
        self.closed = true;
        self.remove_files(false)
    }

    fn remove_files(&mut self, keep_last: bool) -> Result<()> {
        let end = self.files.len().saturating_sub(usize::from(keep_last));
        let mut errors = Vec::new();
        for index in (0..end).rev() {
            match self.files[index].remove() {
                Ok(()) => {
                    self.files.remove(index);
                }
                Err(error) => {
                    errors.push(format!("{}: {error:#}", self.files[index].path.display()))
                }
            }
        }
        ensure!(
            errors.is_empty(),
            "recovery cleanup failed: {}",
            errors.join("; ")
        );
        Ok(())
    }
}

fn stage(
    directory: &Path,
    snapshot: &Snapshot,
    config: &Config,
    header: &[u8],
) -> Result<NamedTempFile> {
    let description: String = snapshot
        .path
        .as_deref()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .unwrap_or("unnamed")
        .chars()
        .take(40)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut builder = Builder::new();
    let prefix = format!("{TEMP_PREFIX}{description}-");
    builder
        .prefix(&prefix)
        .suffix(&config.suffix)
        .rand_bytes(16);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o600));
    }
    let mut temporary = builder.tempfile_in(directory)?;
    {
        let mut writer = BufWriter::new(temporary.as_file_mut());
        writer.write_all(MAGIC)?;
        writer.write_all(&(header.len() as u32).to_be_bytes())?;
        writer.write_all(header)?;
        snapshot.text.write_to(&mut writer)?;
        writer.flush()?;
    }
    temporary.as_file().sync_all()?;
    Ok(temporary)
}

fn publish(
    temporary: NamedTempFile,
    path: PathBuf,
    previous: Option<&OwnedFile>,
) -> Result<OwnedFile> {
    let identity = Handle::from_file(temporary.as_file().try_clone()?)?;
    ensure!(
        Handle::from_file(open_regular(temporary.path())?)? == identity,
        "recovery temporary file was replaced"
    );
    if let Some(previous) = previous {
        previous.check()?;
        temporary.persist(&path).map_err(|error| error.error)?;
    } else {
        temporary
            .persist_noclobber(&path)
            .map_err(|error| error.error)?;
    }
    Ok(OwnedFile { path, identity })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn config(root: &Path) -> Config {
        Config {
            enable: true,
            directories: vec![root.to_owned()],
            ..Config::default()
        }
    }

    fn snapshot(root: &Path) -> Snapshot {
        Snapshot {
            path: Some(root.join("example.txt")),
            cwd: root.to_owned(),
            text: Rope::from_str("hello \u{1f30d}\r\n\u{65e5}\u{672c}\u{8a9e}\n"),
            encoding: "utf-16le".into(),
            has_bom: true,
            line_ending: "crlf".into(),
            selections: vec![(0, 5), (10, 9)],
            primary: 1,
        }
    }

    fn assert_snapshot(actual: &Snapshot, expected: &Snapshot) {
        assert_eq!(
            actual.path,
            expected
                .path
                .as_deref()
                .map(|path| original_path(path, &expected.cwd).unwrap())
        );
        assert_eq!(actual.cwd, expected.cwd);
        assert_eq!(actual.text, expected.text);
        assert_eq!(actual.encoding, expected.encoding);
        assert_eq!(actual.has_bom, expected.has_bom);
        assert_eq!(actual.line_ending, expected.line_ending);
        assert_eq!(actual.selections, expected.selections);
        assert_eq!(actual.primary, expected.primary);
    }

    fn raw_file(path: &Path, header: &serde_json::Value, payload: &[u8]) {
        let header = serde_json::to_vec(header).unwrap();
        let mut file = File::create(path).unwrap();
        file.write_all(MAGIC).unwrap();
        file.write_all(&(header.len() as u32).to_be_bytes())
            .unwrap();
        file.write_all(&header).unwrap();
        file.write_all(payload).unwrap();
    }

    #[test]
    fn config_defaults_and_validation() {
        let defaults: Config = serde_json::from_str("{}").unwrap();
        assert!(!defaults.enable);
        assert_eq!(defaults.directories, Config::default().directories);
        assert_eq!(
            (
                defaults.update_count,
                defaults.update_time,
                defaults.size_threshold
            ),
            (200, 4, 0)
        );
        assert_eq!(defaults.suffix, ".swp");
        for suffix in [
            "",
            ".",
            "..",
            "../swap",
            "/swap",
            "\\swap",
            ":swap",
            "\0",
            "with space",
            "trailing.",
        ] {
            assert!(
                serde_json::from_value::<Config>(serde_json::json!({"suffix": suffix})).is_err(),
                "{suffix:?}"
            );
        }
        for json in [
            r#"{"update-count":0}"#,
            r#"{"update-time":0}"#,
            r#"{"unknown":1}"#,
        ] {
            assert!(serde_json::from_str::<Config>(json).is_err());
        }
        assert!(
            serde_json::from_value::<Config>(serde_json::json!({"update-time": u64::MAX})).is_err()
        );
        assert!(Config {
            update_time: u64::MAX,
            ..defaults
        }
        .validate()
        .is_err());
    }

    #[test]
    fn roundtrip_state_and_streamed_utf8() {
        let root = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        state.text = Rope::from_str(&state.text.to_string().repeat(20_000));
        let mut swap = Swap::default();
        let path = swap
            .write(&state, &config(root.path()), 0)
            .unwrap()
            .unwrap();
        assert_snapshot(&read(&path).unwrap(), &state);
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(PREFIX));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn unnamed_and_all_directories() {
        let root = TempDir::new().unwrap();
        let second = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        state.path = None;
        let mut first_swap = Swap::default();
        let first = first_swap
            .write(&state, &config(root.path()), 1)
            .unwrap()
            .unwrap();
        let mut second_swap = Swap::default();
        let other = second_swap
            .write(&state, &config(second.path()), 1)
            .unwrap()
            .unwrap();
        let mut conf = config(root.path());
        conf.directories = vec![PathBuf::from("."), second.path().to_owned()];
        let found = discover(&conf, None, root.path()).unwrap();
        assert_eq!(found.candidates.len(), 2);
        assert!(found.candidates.contains(&first) && found.candidates.contains(&other));
        assert!(found.warnings.is_empty());
        assert_snapshot(&read(&first).unwrap(), &state);
        assert!(discover(&conf, None, second.path())
            .unwrap()
            .candidates
            .is_empty());
        assert!(discover(&conf, Some(Path::new("example.txt")), root.path())
            .unwrap()
            .candidates
            .is_empty());
    }

    #[test]
    fn directory_resolution_and_fallback() {
        let root = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        let mut conf = config(root.path());
        conf.directories = vec![".".into(), "relative".into(), "~/tmp".into()];
        state.path = Some("subdir/file.txt".into());
        let dirs = directories(&conf, state.path.as_deref(), &state.cwd).unwrap();
        assert_eq!(dirs[0], root.path().join("subdir"));
        assert_eq!(dirs[1], root.path().join("relative"));
        assert_eq!(
            dirs[2],
            root.path()
                .join(helix_stdx::path::expand_tilde(Path::new("~/tmp")))
        );
        let blocker = root.path().join("not-a-directory");
        fs::write(&blocker, "untouched").unwrap();
        conf.directories = vec![
            root.path().join("missing"),
            blocker.clone(),
            root.path().to_owned(),
        ];
        let path = Swap::default().write(&state, &conf, 1).unwrap().unwrap();
        assert_eq!(path.parent(), Some(root.path()));
        assert!(!root.path().join("missing").exists());
        assert_eq!(fs::read_to_string(blocker).unwrap(), "untouched");
        assert_eq!(
            discover(&conf, state.path.as_deref(), root.path())
                .unwrap()
                .candidates,
            vec![path]
        );
    }

    #[test]
    fn unique_names_and_no_clobber() {
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let conf = config(root.path());
        let mut first = Swap::default();
        let a = first.write(&state, &conf, 1).unwrap().unwrap();
        let b = Swap::default().write(&state, &conf, 1).unwrap().unwrap();
        assert_ne!(a, b);
        let original = fs::read(&a).unwrap();
        let header = serde_json::to_vec(&Header::new(&state).unwrap()).unwrap();
        let temporary = stage(root.path(), &state, &conf, &header).unwrap();
        assert!(publish(temporary, a.clone(), None).is_err());
        assert_eq!(fs::read(&a).unwrap(), original);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
    }

    #[test]
    fn atomic_replacement_and_failed_write_preserve_last_snapshot() {
        let root = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        let conf = config(root.path());
        let mut swap = Swap::default();
        let path = swap.write(&state, &conf, 1).unwrap().unwrap();
        let old = fs::read(&path).unwrap();
        state.encoding = "x".repeat(MAX_HEADER);
        assert!(swap.write(&state, &conf, 2).is_err());
        assert_eq!(fs::read(&path).unwrap(), old);
        state.encoding = "utf-8".into();
        state.text.append(Rope::from_str("new text"));
        // A failed newer attempt also fences off stale jobs.
        assert!(swap.write(&state, &conf, 1).unwrap().is_none());
        let previous_identity = Handle::from_file(open_regular(&path).unwrap()).unwrap();
        assert_eq!(swap.write(&state, &conf, 2).unwrap(), Some(path.clone()));
        assert_ne!(
            Handle::from_file(open_regular(&path).unwrap()).unwrap(),
            previous_identity
        );
        assert_snapshot(&read(&path).unwrap(), &state);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn unpublished_or_failed_publication_leaves_old_file_intact() {
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let conf = config(root.path());
        let mut swap = Swap::default();
        let path = swap.write(&state, &conf, 1).unwrap().unwrap();
        let old = fs::read(&path).unwrap();
        let header = serde_json::to_vec(&Header::new(&state).unwrap()).unwrap();
        let temporary = stage(root.path(), &state, &conf, &header).unwrap();
        assert_eq!(fs::read(&path).unwrap(), old);
        let staged_path = temporary.path().to_owned();
        // A lost staging file must fail publication, not truncate the target.
        fs::remove_file(&staged_path).unwrap();
        assert!(publish(temporary, path.clone(), swap.files.last()).is_err());
        assert_eq!(fs::read(&path).unwrap(), old);
        assert!(!staged_path.exists());
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }

    #[test]
    fn migration_and_failed_migration() {
        let root = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let mut conf = config(root.path());
        conf.directories = vec![".".into()];
        let mut state = snapshot(root.path());
        let mut swap = Swap::default();
        let old = swap.write(&state, &conf, 1).unwrap().unwrap();
        state.path = Some(root.path().join("missing/new.txt"));
        assert!(swap.write(&state, &conf, 2).is_err());
        assert!(read(&old).is_ok());
        assert_eq!(swap.path(), Some(old.as_path()));
        state.path = Some(destination.path().join("renamed.txt"));
        let new = swap.write(&state, &conf, 3).unwrap().unwrap();
        assert_ne!(old, new);
        assert!(!old.exists());
        assert_eq!(
            new.parent(),
            Some(fs::canonicalize(destination.path()).unwrap().as_path())
        );
        assert_snapshot(&read(&new).unwrap(), &state);
        conf.directories = vec![root.path().join("unavailable"), root.path().to_owned()];
        let moved = swap.write(&state, &conf, 4).unwrap().unwrap();
        assert!(!new.exists());
        assert_eq!(moved.parent(), Some(root.path()));
        assert_eq!(
            discover(&conf, state.path.as_deref(), &state.cwd)
                .unwrap()
                .candidates,
            vec![moved.clone()]
        );
        conf.suffix = ".recover".into();
        let renamed = swap.write(&state, &conf, 5).unwrap().unwrap();
        assert!(!moved.exists());
        assert!(renamed
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".recover"));
        assert_eq!(
            discover(&conf, state.path.as_deref(), &state.cwd)
                .unwrap()
                .candidates,
            vec![renamed.clone()]
        );
        assert_snapshot(&read(&renamed).unwrap(), &state);
        conf.directories = vec![root.path().join("unavailable")];
        assert!(swap.write(&state, &conf, 6).is_err());
        assert!(read(&renamed).is_ok());
        swap.close().unwrap();
    }

    #[test]
    fn migration_publishes_before_cleanup_and_retains_cleanup_failures() {
        let root = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        let conf = config(root.path());
        let mut swap = Swap::default();
        let old = swap.write(&state, &conf, 1).unwrap().unwrap();
        fs::rename(&old, root.path().join("saved-original")).unwrap();
        fs::write(&old, "stranger").unwrap();
        state.path = Some(root.path().join("renamed.txt"));
        assert!(swap.write(&state, &conf, 2).is_err());
        let new = swap.path().unwrap().to_owned();
        assert_ne!(old, new);
        assert_snapshot(&read(&new).unwrap(), &state);
        assert_eq!(swap.files.len(), 2);
        assert!(swap.close().is_err());
        assert!(!new.exists());
        assert_eq!(fs::read_to_string(old).unwrap(), "stranger");
        assert!(swap.write(&state, &conf, 3).unwrap().is_none());
    }

    #[test]
    fn relative_original_paths_and_empty_text() {
        let root = TempDir::new().unwrap();
        let other = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        state.path = Some("relative.txt".into());
        state.text = Rope::new();
        state.selections = vec![(0, 0)];
        state.primary = 0;
        let conf = config(root.path());
        let mut swap = Swap::default();
        let first = swap.write(&state, &conf, 1).unwrap().unwrap();
        assert_snapshot(&read(&first).unwrap(), &state);
        assert_eq!(
            discover(&conf, Some(&root.path().join("relative.txt")), root.path())
                .unwrap()
                .candidates,
            vec![first.clone()]
        );
        assert!(
            discover(&conf, Some(Path::new("different.txt")), root.path())
                .unwrap()
                .candidates
                .is_empty()
        );
        state.cwd = other.path().to_owned();
        let second = swap.write(&state, &conf, 2).unwrap().unwrap();
        assert_ne!(first, second);
        assert!(!first.exists());
        assert_snapshot(&read(&second).unwrap(), &state);
    }

    #[test]
    fn close_drop_and_stale_jobs() {
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let conf = config(root.path());
        let mut swap = Swap::default();
        let path = swap.write(&state, &conf, 10).unwrap().unwrap();
        assert!(swap.write(&state, &conf, 9).unwrap().is_none());
        assert!(swap.write(&state, &conf, 10).unwrap().is_some());
        drop(swap);
        assert!(path.exists());
        let mut swap = Swap::default();
        let owned = swap.write(&state, &conf, 1).unwrap().unwrap();
        swap.close().unwrap();
        swap.close().unwrap();
        assert!(!owned.exists());
        assert!(path.exists());
        assert!(swap.path().is_none());
        assert!(swap.write(&state, &conf, u64::MAX).unwrap().is_none());
        let mut closed = Swap::default();
        closed.close().unwrap();
        assert!(closed.write(&state, &conf, 1).unwrap().is_none());
    }

    #[test]
    fn disabled_and_byte_threshold() {
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let mut conf = config(root.path());
        let mut swap = Swap::default();
        conf.enable = false;
        assert!(swap.write(&state, &conf, 1).unwrap().is_none());
        conf.enable = true;
        conf.size_threshold = state.text.len_bytes() - 1;
        assert!(swap.write(&state, &conf, 2).unwrap().is_none());
        conf.size_threshold += 1;
        assert!(swap.write(&state, &conf, 3).unwrap().is_some());
    }

    #[test]
    fn malicious_and_truncated_files() {
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let path = root.path().join("malformed");
        let valid = serde_json::to_value(Header::new(&state).unwrap()).unwrap();
        let text = state.text.to_string();
        for (field, value) in [
            ("version", serde_json::json!(2)),
            ("payload_len", serde_json::json!(u64::MAX)),
            ("payload_len", serde_json::json!(0)),
            ("selections", serde_json::json!([])),
            ("selections", serde_json::json!([[0, usize::MAX]])),
            ("primary", serde_json::json!(2)),
        ] {
            let mut header = valid.clone();
            header[field] = value;
            raw_file(&path, &header, text.as_bytes());
            assert!(read(&path).is_err(), "{field}");
        }
        raw_file(&path, &valid, &vec![0xff; text.len()]);
        assert!(read(&path).is_err());
        raw_file(&path, &valid, text.as_bytes());
        let full = fs::read(&path).unwrap();
        for length in [
            0,
            MAGIC.len() - 1,
            MAGIC.len() + 2,
            MAGIC.len() + 5,
            full.len() - 1,
        ] {
            fs::write(&path, &full[..length]).unwrap();
            assert!(read(&path).is_err());
        }
        let mut oversized = MAGIC.to_vec();
        oversized.extend_from_slice(&u32::MAX.to_be_bytes());
        fs::write(&path, oversized).unwrap();
        assert!(read(&path).is_err());
        let mut trailing = full;
        trailing.push(0);
        fs::write(&path, trailing).unwrap();
        assert!(read(&path).is_err());
        raw_file(&path, &serde_json::json!({}), b"");
        assert!(read(&path).is_err());
        let mut header = valid.clone();
        header["cwd"] =
            serde_json::to_value(StoredPath::encode(Path::new("relative")).unwrap()).unwrap();
        raw_file(&path, &header, text.as_bytes());
        assert!(read(&path).is_err());
        header = valid;
        header["path"] =
            serde_json::to_value(StoredPath::encode(Path::new("bad\0path")).unwrap()).unwrap();
        raw_file(&path, &header, text.as_bytes());
        assert!(read(&path).is_err());
    }

    #[test]
    fn discovery_reports_corruption_without_hiding_valid_files() {
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let conf = config(root.path());
        let valid = Swap::default().write(&state, &conf, 1).unwrap().unwrap();
        let corrupt = root.path().join(format!("{PREFIX}broken.swp"));
        fs::write(&corrupt, MAGIC).unwrap();
        fs::write(root.path().join("unrelated.swp"), "ignored").unwrap();
        fs::write(
            root.path().join(format!("{TEMP_PREFIX}in-progress.swp")),
            "ignored",
        )
        .unwrap();
        let found = discover(&conf, state.path.as_deref(), root.path()).unwrap();
        assert_eq!(found.candidates, vec![valid]);
        assert_eq!(found.warnings.len(), 1);
        assert_eq!(found.warnings[0].0, corrupt);
        assert!(corrupt.exists());
    }

    #[test]
    fn replaced_regular_file_is_not_owned() {
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let conf = config(root.path());
        let mut swap = Swap::default();
        let path = swap.write(&state, &conf, 1).unwrap().unwrap();
        fs::rename(&path, root.path().join("saved-original")).unwrap();
        fs::write(&path, "stranger").unwrap();
        assert!(swap.write(&state, &conf, 2).is_err());
        assert!(swap.close().is_err());
        assert!(swap.write(&state, &conf, 3).unwrap().is_none());
        assert_eq!(fs::read_to_string(path).unwrap(), "stranger");
    }

    #[cfg(unix)]
    #[test]
    fn original_symlinks_share_identity_and_survive_alias_retargeting() {
        use std::os::unix::fs::symlink;
        let root = TempDir::new().unwrap();
        let real = root.path().join("real");
        let aliases = root.path().join("aliases");
        fs::create_dir(&real).unwrap();
        fs::create_dir(&aliases).unwrap();
        let target = real.join("original.txt");
        fs::write(&target, "disk").unwrap();
        let alias = aliases.join("alias.txt");
        let parent_alias = aliases.join("parent");
        symlink(&target, &alias).unwrap();
        symlink(&real, &parent_alias).unwrap();
        let conf = Config {
            directories: vec![".".into()],
            ..config(root.path())
        };
        let mut state = snapshot(root.path());
        state.path = Some(alias.clone());
        let mut swap = Swap::default();
        let path = swap.write(&state, &conf, 1).unwrap().unwrap();
        let saved_target = fs::canonicalize(&target).unwrap();
        assert_eq!(path.parent(), saved_target.parent());
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(".helix-recovery-original.txt-"));
        assert_eq!(read(&path).unwrap().path, Some(saved_target.clone()));
        assert_eq!(
            read_header(&mut BufReader::new(open_regular(&path).unwrap()))
                .unwrap()
                .original_size,
            Some(4)
        );
        for original in [&alias, &target, &parent_alias.join("original.txt")] {
            assert_eq!(
                discover(&conf, Some(original), root.path())
                    .unwrap()
                    .candidates,
                vec![path.clone()]
            );
        }
        state.path = Some(target.clone());
        assert_eq!(swap.write(&state, &conf, 2).unwrap(), Some(path.clone()));
        let other = real.join("other.txt");
        fs::write(&other, "other").unwrap();
        fs::remove_file(&alias).unwrap();
        symlink(&other, &alias).unwrap();
        assert_eq!(read(&path).unwrap().path, Some(saved_target));
        assert!(discover(&conf, Some(&alias), root.path())
            .unwrap()
            .candidates
            .is_empty());
        assert_eq!(
            discover(&conf, Some(&target), root.path())
                .unwrap()
                .candidates,
            vec![path.clone()]
        );
        state.path = Some(alias.clone());
        let migrated = swap.write(&state, &conf, 3).unwrap().unwrap();
        assert!(!path.exists());
        assert_eq!(
            read(&migrated).unwrap().path,
            Some(fs::canonicalize(&other).unwrap())
        );
        // Canonicalize a parent alias for a not-yet-created original, and a
        // dangling leaf alias to that same original.
        let future = parent_alias.join("future.txt");
        let dangling = aliases.join("dangling.txt");
        symlink(&future, &dangling).unwrap();
        state.path = Some(dangling);
        let pending = swap.write(&state, &conf, 4).unwrap().unwrap();
        assert_eq!(
            read(&pending).unwrap().path,
            Some(fs::canonicalize(&real).unwrap().join("future.txt"))
        );
        assert_eq!(
            discover(&conf, Some(&future), root.path())
                .unwrap()
                .candidates,
            vec![pending.clone()]
        );
        let cycle = aliases.join("cycle");
        symlink(&cycle, &cycle).unwrap();
        state.path = Some(cycle);
        assert!(swap.write(&state, &conf, 5).is_err());
        assert!(read(&pending).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn insecure_writable_recovery_files_are_rejected() {
        use std::os::unix::fs::PermissionsExt;
        let root = TempDir::new().unwrap();
        let state = snapshot(root.path());
        let conf = config(root.path());
        let mut swap = Swap::default();
        let path = swap.write(&state, &conf, 1).unwrap().unwrap();
        for mode in [0o620, 0o602, 0o666] {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            assert!(read(&path).is_err());
            let found = discover(&conf, state.path.as_deref(), &state.cwd).unwrap();
            assert!(found.candidates.is_empty());
            assert_eq!(found.warnings[0].0, path);
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_snapshot(&read(&path).unwrap(), &state);
        swap.close().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unix_paths_permissions_and_symlinks() {
        use std::os::unix::{
            ffi::OsStringExt,
            fs::{symlink, PermissionsExt},
        };
        let root = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        state.path = Some(
            root.path()
                .join(OsString::from_vec(b"non-utf8-\xff".to_vec())),
        );
        let conf = config(root.path());
        let mut swap = Swap::default();
        let path = swap.write(&state, &conf, 1).unwrap().unwrap();
        assert_snapshot(&read(&path).unwrap(), &state);
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let header = serde_json::to_vec(&Header::new(&state).unwrap()).unwrap();
        let temporary = stage(root.path(), &state, &conf, &header).unwrap();
        assert_eq!(
            temporary.as_file().metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
        let link = root.path().join(format!("{PREFIX}link.swp"));
        symlink(&path, &link).unwrap();
        assert!(read(&link).is_err());
        assert!(publish(temporary, link.clone(), None).is_err());
        assert!(fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink());
        fs::remove_file(&path).unwrap();
        let stranger = root.path().join("stranger");
        fs::write(&stranger, "untouched").unwrap();
        symlink(&stranger, &path).unwrap();
        assert!(swap.write(&state, &conf, 2).is_err());
        assert!(swap.close().is_err());
        assert!(swap.write(&state, &conf, 3).unwrap().is_none());
        assert!(fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read_to_string(stranger).unwrap(), "untouched");
    }

    #[cfg(unix)]
    #[test]
    fn unnamed_non_utf8_cwd_and_discovery() {
        use std::os::unix::ffi::OsStringExt;
        let root = TempDir::new().unwrap();
        let cwd = root.path().join(OsString::from_vec(b"cwd-\xff".to_vec()));
        let mut state = snapshot(&cwd);
        state.path = None;
        let conf = Config {
            directories: vec![".".into(), root.path().to_owned()],
            ..config(root.path())
        };
        let path = Swap::default().write(&state, &conf, 1).unwrap().unwrap();
        assert_snapshot(&read(&path).unwrap(), &state);
        assert_eq!(discover(&conf, None, &cwd).unwrap().candidates, vec![path]);
    }

    #[cfg(windows)]
    #[test]
    fn windows_unpaired_surrogate_paths() {
        use std::os::windows::ffi::OsStringExt;
        let root = TempDir::new().unwrap();
        let mut state = snapshot(root.path());
        state.path = Some(
            root.path()
                .join(OsString::from_wide(&[0xd800, b'x' as u16])),
        );
        let path = Swap::default()
            .write(&state, &config(root.path()), 1)
            .unwrap()
            .unwrap();
        assert_snapshot(&read(&path).unwrap(), &state);
    }
}
