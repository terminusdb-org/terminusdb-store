//! Directory-based implementation of storage traits.

use locking::*;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

use async_trait::async_trait;

pub use tdb_succinct::storage::file::*;

use super::*;

const PREFIX_DIR_SIZE: usize = 3;

#[derive(Clone)]
pub struct DirectoryLayerStore {
    path: PathBuf,
}

impl DirectoryLayerStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> DirectoryLayerStore {
        DirectoryLayerStore { path: path.into() }
    }
}

#[async_trait]
impl PersistentLayerStore for DirectoryLayerStore {
    type File = FileBackedStore;
    async fn directories(&self) -> io::Result<Vec<[u32; 5]>> {
        let mut stream = fs::read_dir(&self.path).await?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next_entry().await? {
            if direntry.file_type().await?.is_dir() {
                let os_name = direntry.file_name();
                let name = os_name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected non-utf8 directory name",
                    )
                })?;
                // skip non-layer dirs like the label store's `.tmp`
                if let Ok(name) = string_to_name(name) {
                    result.push(name);
                }
            }
        }

        Ok(result)
    }

    async fn create_named_directory(&self, name: [u32; 5]) -> io::Result<[u32; 5]> {
        let mut p = self.path.clone();
        let name_str = name_to_string(name);
        p.push(&name_str[0..PREFIX_DIR_SIZE]);
        p.push(name_str);

        fs::create_dir_all(p).await?;

        Ok(name)
    }

    async fn directory_exists(&self, name: [u32; 5]) -> io::Result<bool> {
        let mut p = self.path.clone();
        let name = name_to_string(name);
        p.push(&name[0..PREFIX_DIR_SIZE]);
        p.push(name);

        match fs::metadata(p).await {
            Ok(m) => Ok(m.is_dir()),
            Err(_) => Ok(false),
        }
    }

    async fn get_file(&self, directory: [u32; 5], name: &str) -> io::Result<Self::File> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(name);
        Ok(FileBackedStore::new(p))
    }

    async fn file_exists(&self, directory: [u32; 5], file: &str) -> io::Result<bool> {
        let mut p = self.path.clone();
        let dir_name = name_to_string(directory);
        p.push(&dir_name[0..PREFIX_DIR_SIZE]);
        p.push(dir_name);
        p.push(file);

        match fs::metadata(p).await {
            Ok(m) => Ok(m.is_file()),
            Err(_) => Ok(false),
        }
    }
    async fn finalize(&self, directory: [u32; 5]) -> io::Result<()> {
        if cfg!(unix) {
            // ensure the underlying directory record is properly synchronized
            let mut directory_path = self.path.clone();
            let dir_name = name_to_string(directory);
            directory_path.push(&dir_name[0..PREFIX_DIR_SIZE]);
            directory_path.push(dir_name);

            let mut options = tokio::fs::OpenOptions::new();
            options.create(false);
            options.read(true);
            options.write(false);
            let dir_fd = options.open(directory_path).await?;
            dir_fd.sync_all().await?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct DirectoryLabelStore {
    path: PathBuf,
}

/// `*.label` files in `.tmp/` older than this are crash residue — a live
/// writer holds the label lock while its tmp exists, so a fresh tmp is never
/// swept.
const LABEL_TMP_TTL: Duration = Duration::from_secs(60);

impl DirectoryLabelStore {
    pub fn new<P: Into<PathBuf>>(path: P) -> DirectoryLabelStore {
        let path = path.into();
        sweep_stale_label_tmps(&path);
        DirectoryLabelStore { path }
    }
}

/// Tmp files live in `<dir>/.tmp/` with the label's own name — a filename
/// suffix would overflow NAME_MAX at the 255-byte db-name boundary.
fn label_tmp_path(path: &Path, name: &str) -> PathBuf {
    path.join(".tmp").join(format!("{name}.label"))
}

fn sweep_stale_label_tmps(path: &Path) {
    let tmp_dir = path.join(".tmp");
    let _ = std::fs::create_dir_all(&tmp_dir);
    let entries = match std::fs::read_dir(tmp_dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name.to_str().is_none() {
            continue;
        }
        let stale = entry
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .map(|t| {
                now.duration_since(t)
                    .map(|d| d > LABEL_TMP_TTL)
                    .unwrap_or(false) // mtime in the future: live file, keep
            })
            .unwrap_or(false);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// fsync that retries once then panics — durability is unknowable after a
/// failed flush (postgres data_sync_retry semantics: crash, don't mask).
#[allow(unused_variables)] // `first` is consumed by chrono_log!, a feature-gated macro
async fn fsync_fatal(file: &fs::File) -> io::Result<()> {
    match file.sync_all().await {
        Err(first) => match file.sync_all().await {
            Ok(()) => {
                crate::chrono_log!("label fsync retried after transient failure: {first}");
                Ok(())
            }
            Err(second) => {
                panic!("fsync failed twice; label durability unknowable: {}", second)
            }
        },
        ok => ok,
    }
}

/// fsync the parent directory so the rename result is durable. Discrete
/// joinable step — a future fsync queue batches commits onto this barrier.
async fn fsync_parent_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let file = fs::File::open(dir).await?;
        fsync_fatal(&file).await
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Create `p` as a new `0\n\n` label via tmp + hard link — atomic
/// create-if-absent without the write-after-create crash window.
/// Caller must have already established `p` does not exist.
async fn create_label_file(p: &Path, tmp: &Path) -> io::Result<()> {
    let mut file = fs::File::create(tmp).await?;
    file.write_all(b"0\n\n").await?;
    file.flush().await?;
    fsync_fatal(&file).await?;
    drop(file);

    match fs::hard_link(tmp, p).await {
        Ok(()) => {}
        Err(e) => {
            let _ = fs::remove_file(tmp).await;
            return match e.kind() {
                io::ErrorKind::AlreadyExists => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "database already exists",
                )),
                _ => Err(e),
            };
        }
    }
    fs::remove_file(tmp).await
}

/// Write `contents` to `tmp`, fsync it, atomically replace `dest`, fsync the
/// parent directory. Callers must hold the label file lock; the rename is the
/// atomic commit point.
async fn replace_file_durable(tmp: &Path, dest: &Path, dir: &Path, contents: &[u8]) -> io::Result<()> {
    let mut file = fs::File::create(tmp).await?;
    file.write_all(contents).await?;
    file.flush().await?;
    fsync_fatal(&file).await?;
    drop(file);

    #[cfg(unix)]
    fs::rename(tmp, dest).await?;
    #[cfg(not(unix))]
    atomicwrites::replace_atomic(tmp, dest)?;

    fsync_parent_dir(dir).await
}

/// Whether two metadata objects describe the same filesystem object —
/// `dev`+`ino` on unix, `file_index`+`volume_serial_number` on Windows.
/// Used to detect that the path was renamed onto a different inode while we
/// waited for a lock. On other platforms identity can't be checked — the
/// lock recheck degrades to a no-op, same as the old behaviour.
#[cfg(unix)]
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}

#[cfg(windows)]
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    a.file_index() == b.file_index() && a.volume_serial_number() == b.volume_serial_number()
}

#[cfg(not(any(unix, windows)))]
fn same_file(_a: &std::fs::Metadata, _b: &std::fs::Metadata) -> bool {
    true
}

/// Read and parse a label file by path — plain read, no flock. A shared lock
/// here would self-deadlock when the caller already holds LOCK_EX.
async fn get_label_from_path(path: &Path) -> io::Result<Label> {
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();
    let mut data = Vec::new();
    fs::File::open(path).await?.read_to_end(&mut data).await?;

    get_label_from_data(label, &data)
}

fn get_label_from_data(name: String, data: &[u8]) -> io::Result<Label> {
    let s = String::from_utf8_lossy(data);
    let lines: Vec<&str> = s.lines().collect();
    if lines.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected label file to have two lines. contents were ({:?})",
                lines
            ),
        ));
    }

    let version_str = &lines[0];
    let layer_str = &lines[1];

    let version = version_str.parse::<u64>();
    if version.is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected first line of label file to be a number but it was {}",
                version_str
            ),
        ));
    }

    if layer_str.is_empty() {
        Ok(Label {
            name,
            layer: None,
            version: version.unwrap(),
        })
    } else {
        let layer = layer::string_to_name(layer_str)?;
        Ok(Label {
            name,
            layer: Some(layer),
            version: version.unwrap(),
        })
    }
}

async fn get_label_from_file<P: Into<PathBuf>>(path: P) -> io::Result<Label> {
    let path: PathBuf = path.into();
    let label = path.file_stem().unwrap().to_str().unwrap().to_owned();

    let mut file = LockedFile::open(path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    get_label_from_data(label, &data)
}

#[async_trait]
impl LabelStore for DirectoryLabelStore {
    async fn labels(&self) -> io::Result<Vec<Label>> {
        let mut stream = fs::read_dir(self.path.clone()).await?;
        let mut result = Vec::new();
        while let Some(direntry) = stream.next_entry().await? {
            if direntry.file_type().await?.is_file() {
                let os_name = direntry.file_name();
                let name = os_name.to_str().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected non-utf8 directory name",
                    )
                })?;
                if name.ends_with(".label") {
                    let label = get_label_from_file(direntry.path()).await?;
                    result.push(label);
                }
            }
        }

        Ok(result)
    }

    async fn create_label(&self, label: &str) -> io::Result<Label> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", label));
        let tmp = label_tmp_path(&self.path, label);
        match fs::metadata(&p).await {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "database already exists",
            )),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    create_label_file(&p, &tmp).await?;
                    fsync_parent_dir(&self.path).await?;

                    Ok(Label::new_empty(label))
                }
                _ => Err(e),
            },
        }
    }

    async fn get_label(&self, label: &str) -> io::Result<Option<Label>> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", label));

        match get_label_from_file(p).await {
            Ok(label) => Ok(Some(label)),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(None),
                _ => Err(e),
            },
        }
    }

    async fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let mut p = self.path.clone();
        p.push(format!("{}.label", label.name));
        let tmp = label_tmp_path(&self.path, &label.name);

        // A rename moves the path to a new inode, so a lock acquired on a
        // pre-rename inode serializes nothing: another contender can hold the
        // new inode's lock at the same time. After the (blocking) acquire,
        // verify the locked fd is still the path's current inode and retry
        // until it is — then nobody can rename past us while we hold it.
        let _guard = loop {
            let guard = ExclusiveLockedFile::open(p.clone()).await?;
            let fd_meta = guard.metadata().await?;
            let path_meta = fs::metadata(&p).await?;
            if same_file(&fd_meta, &path_meta) {
                break guard;
            }
        };
        // re-read the current label by path — a contender that renamed before
        // our lock acquisition is found here and loses the compare
        let retrieved_label = get_label_from_path(&p).await?;
        if retrieved_label == *label {
            // all good, let's a go
            replace_file_durable(&tmp, &p, &self.path, &contents).await?;
            Ok(Some(new_label))
        } else {
            Ok(None)
        }
    }

    async fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut p = self.path.clone();
        p.push(format!("{}.label", name));

        // We're not locking here to remove the file. The assumption
        // is that any concurrent operation that is done on the label
        // file will not matter. If it is a label read, a concurrent
        // operation will simply get the label contents, which
        // immediately afterwards become invalid. Similarly if it is
        // for a write, the write will appear to be succesful even
        // though the file will be gone afterwards. This is
        // indistinguishable from the case where the read/write and
        // the remove happened in reverse order.
        // crash-residue tmp of any writer is removed with the label
        let _ = tokio::fs::remove_file(label_tmp_path(&self.path, name)).await;
        match tokio::fs::remove_file(p).await {
            Ok(()) => Ok(true),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(false),
                _ => Err(e),
            },
        }
    }
}

/// A version of the directory label store that keeps all labels in
/// memory and doesn't lock.
///
/// This is useful for situations where we can be sure that only one
/// process is working with a set of label files. In that case, we can
/// keep all label files cached in memory in order to process reads as
/// quickly as possible, and we can perform writes without any sort of
/// file system locking.
pub struct CachedDirectoryLabelStore {
    path: PathBuf,
    labels: Arc<RwLock<HashMap<String, Label>>>,
}

impl CachedDirectoryLabelStore {
    /// Open a new label store.
    ///
    /// This will read in all label files on startup, which is why
    /// this is an async operation.
    pub async fn open<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
        let path: PathBuf = path.into();
        sweep_stale_label_tmps(&path);
        let labels = get_all_labels_from_dir(&path).await?;

        Ok(Self {
            path,
            labels: Arc::new(RwLock::new(labels)),
        })
    }
}

async fn get_all_labels_from_dir(p: &PathBuf) -> io::Result<HashMap<String, Label>> {
    let mut result = HashMap::new();
    let mut entries = tokio::fs::read_dir(p).await?;

    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        if let Ok(file_name) = entry.file_name().into_string() {
            if !file_name.ends_with(".label") {
                continue;
            }

            let label_name = file_name[..file_name.len() - 6].to_string();
            let label = get_label_from_file(entry.path()).await?;

            result.insert(label_name, label);
        }
    }

    Ok(result)
}

#[async_trait]
impl LabelStore for CachedDirectoryLabelStore {
    async fn labels(&self) -> io::Result<Vec<Label>> {
        let labels = self.labels.read().await;
        Ok(labels.values().cloned().collect())
    }

    async fn create_label(&self, label: &str) -> io::Result<Label> {
        let mut labels = self.labels.write().await;
        if labels.contains_key(label) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "database already exists",
            ));
        }

        let mut p = self.path.clone();
        p.push(format!("{}.label", label));
        match fs::metadata(&p).await {
            Ok(_) => Err(io::Error::other(
                "label was not in cached map but was found on disk",
            )),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => {
                    create_label_file(&p, &label_tmp_path(&self.path, label)).await?;
                    fsync_parent_dir(&self.path).await?;

                    let l = Label::new_empty(label);
                    labels.insert(label.to_string(), l.clone());

                    Ok(l)
                }
                _ => Err(e),
            },
        }
    }
    async fn get_label(&self, label: &str) -> io::Result<Option<Label>> {
        let labels = self.labels.read().await;
        Ok(labels.get(label).cloned())
    }
    async fn set_label_option(
        &self,
        label: &Label,
        layer: Option<[u32; 5]>,
    ) -> io::Result<Option<Label>> {
        let new_label = label.with_updated_layer(layer);
        let contents = match new_label.layer {
            None => format!("{}\n\n", new_label.version).into_bytes(),
            Some(layer) => {
                format!("{}\n{}\n", new_label.version, layer::name_to_string(layer)).into_bytes()
            }
        };

        let mut labels = self.labels.write().await;
        if let Some(retrieved_label) = labels.get(&label.name) {
            if retrieved_label == label {
                // all good, let's a go
                let mut p = self.path.clone();
                p.push(format!("{}.label", label.name));
                replace_file_durable(
                    &label_tmp_path(&self.path, &label.name),
                    &p,
                    &self.path,
                    &contents,
                )
                .await?;

                labels.insert(label.name.clone(), new_label.clone());
                Ok(Some(new_label))
            } else {
                Ok(None)
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "label not found"))
        }
    }

    async fn delete_label(&self, name: &str) -> io::Result<bool> {
        let mut labels = self.labels.write().await;
        if labels.remove(name).is_some() {
            let mut p = self.path.clone();
            p.push(format!("{}.label", name));
            tokio::fs::remove_file(p).await?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn write_and_read_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().await.unwrap();
        w.write_all(&[1, 2, 3]).await.unwrap();
        w.flush().await.unwrap();
        let mut buf = Vec::new();
        file.open_read()
            .await
            .unwrap()
            .read_to_end(&mut buf)
            .await
            .unwrap();

        assert_eq!(vec![1, 2, 3], buf);
    }

    #[tokio::test]
    async fn layer_directories_ignore_non_layer_dirs() {
        let dir = tempdir().unwrap();
        // the label store keeps its rename tmp files in `<store>/.tmp/` —
        // layer enumeration must skip it, not error on the non-hex name
        std::fs::create_dir(dir.path().join(".tmp")).unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        assert_eq!(Vec::<[u32; 5]>::new(), store.directories().await.unwrap());
    }

    #[tokio::test]
    async fn write_and_map_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().await.unwrap();
        w.write_all(&[1, 2, 3]).await.unwrap();
        w.flush().await.unwrap();

        let map = file.map().await.unwrap();

        assert_eq!(&vec![1, 2, 3][..], map.as_ref());
    }

    #[tokio::test]
    async fn write_and_map_large_file_backed() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("foo");
        let file = FileBackedStore::new(file_path);

        let mut w = file.open_write().await.unwrap();
        let mut contents = vec![0u8; 4096 << 4];
        for i in 0..contents.capacity() {
            contents[i] = (i % 256) as u8;
        }

        w.write_all(&contents).await.unwrap();
        w.flush().await.unwrap();

        let map = file.map().await.unwrap();

        assert_eq!(contents, map.as_ref());
    }

    #[tokio::test]
    async fn create_layers_from_directory_store() {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());

        let layer = async {
            let mut builder = store.create_base_layer().await?;
            let base_name = builder.name();

            builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
            builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
            builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

            builder.commit_boxed().await?;

            let mut builder = store.create_child_layer(base_name).await?;
            let child_name = builder.name();

            builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
            builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

            builder.commit_boxed().await?;

            store.get_layer(child_name).await
        }
        .await
        .unwrap()
        .unwrap();

        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo")));
        assert!(layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink")));
        assert!(layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!layer.value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn directory_create_and_retrieve_equal_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let (stored, retrieved) = async {
            let stored = store.create_label("foo").await?;
            let retrieved = store.get_label("foo").await?;

            Ok::<_, io::Error>((stored, retrieved))
        }
        .await
        .unwrap();

        assert_eq!(None, stored.layer);
        assert_eq!(stored, retrieved.unwrap());
    }

    #[tokio::test]
    async fn directory_update_label_succeeds() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let retrieved = async {
            let stored = store.create_label("foo").await?;
            store.set_label(&stored, [6, 7, 8, 9, 10]).await?;

            store.get_label("foo").await
        }
        .await
        .unwrap()
        .unwrap();

        assert_eq!(Some([6, 7, 8, 9, 10]), retrieved.layer);
    }

    #[tokio::test]
    async fn directory_update_label_twice_from_same_label_object_fails() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        let (stored2, stored3) = async {
            let stored1 = store.create_label("foo").await?;

            let stored2 = store.set_label(&stored1, [6, 7, 8, 9, 10]).await?;
            let stored3 = store.set_label(&stored1, [10, 9, 8, 7, 6]).await?;

            Ok::<_, io::Error>((stored2, stored3))
        }
        .await
        .unwrap();

        assert!(stored2.is_some());
        assert!(stored3.is_none());
    }

    #[tokio::test]
    async fn directory_create_label_twice_errors() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").await.unwrap();
        let result = store.create_label("foo").await;

        assert!(result.is_err());

        let error = result.err().unwrap();
        assert_eq!(io::ErrorKind::InvalidInput, error.kind());
    }

    #[tokio::test]
    async fn nonexistent_file_is_nonexistent() {
        let file = FileBackedStore::new("asdfasfopivbuzxcvopiuvpoawehkafpouzvxv");
        assert!(!file.exists().await.unwrap());
    }

    #[tokio::test]
    async fn rollup_and_retrieve_base() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        let _rolled_id = store.clone().rollup(unrolled_layer).await.unwrap();
        let rolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn rollup_and_retrieve_child() {
        let dir = tempdir().unwrap();
        let store = Arc::new(DirectoryLayerStore::new(dir.path()));

        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"));
        builder.add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"));
        builder.add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("duck", "says", "quack"));
        builder.add_value_triple(ValueTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let mut builder = store.create_child_layer(child_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_value_triple(ValueTriple::new_string_value("cow", "likes", "pig"));
        builder.add_value_triple(ValueTriple::new_node("cow", "hates", "pig"));

        builder.commit_boxed().await.unwrap();

        let unrolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        let _rolled_id = store
            .clone()
            .rollup_upto(unrolled_layer, base_name)
            .await
            .unwrap();
        let rolled_layer = store.get_layer(child_name).await.unwrap().unwrap();

        match *rolled_layer {
            InternalLayer::Rollup(_) => {}
            _ => panic!("not a rollup"),
        }

        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("cow", "says", "moo"))
        );
        assert!(
            rolled_layer.value_triple_exists(&ValueTriple::new_string_value("pig", "says", "oink"))
        );
        assert!(rolled_layer.value_triple_exists(&ValueTriple::new_node("cow", "hates", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("cow", "likes", "pig")));
        assert!(!rolled_layer
            .value_triple_exists(&ValueTriple::new_string_value("duck", "says", "quack")));
    }

    #[tokio::test]
    async fn create_and_delete_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").await.unwrap();
        assert!(store.get_label("foo").await.unwrap().is_some());
        assert!(store.delete_label("foo").await.unwrap());
        assert!(store.get_label("foo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        assert!(!store.delete_label("foo").await.unwrap());
    }

    #[tokio::test]
    async fn delete_shared_locked_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").await.unwrap();
        let label_path = dir.path().join("foo.label");
        let _f = LockedFile::open(label_path).await.unwrap();

        assert!(store.delete_label("foo").await.unwrap());
    }

    #[tokio::test]
    async fn delete_exclusive_locked_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());

        store.create_label("foo").await.unwrap();
        let label_path = dir.path().join("foo.label");
        let _f = ExclusiveLockedFile::open(label_path).await.unwrap();

        assert!(store.delete_label("foo").await.unwrap());
    }

    #[tokio::test]
    async fn concurrent_label_updates_one_winner() {
        let dir = tempdir().unwrap();
        let stored = DirectoryLabelStore::new(dir.path())
            .create_label("foo")
            .await
            .unwrap();

        // two separate stores = two "processes" competing on the same file
        let store_a = DirectoryLabelStore::new(dir.path());
        let store_b = DirectoryLabelStore::new(dir.path());
        let (a, b) = (
            stored.clone(),
            stored,
        );

        let (ra, rb) = tokio::join!(
            store_a.set_label(&a, [1, 2, 3, 4, 5]),
            store_b.set_label(&b, [6, 7, 8, 9, 10])
        );

        let landed = [ra.unwrap(), rb.unwrap()]
            .iter()
            .filter(|o| o.is_some())
            .count();
        assert_eq!(1, landed);

        let final_label = store_a.get_label("foo").await.unwrap().unwrap();
        assert_eq!(1, final_label.version);
    }

    #[tokio::test]
    async fn concurrent_readers_never_see_torn_label() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());
        let mut label = store.create_label("foo").await.unwrap();

        let writer_store = store.clone();
        let writer = tokio::spawn(async move {
            for i in 0..20 {
                label = writer_store
                    .set_label(&label, [i, i, i, i, i])
                    .await
                    .unwrap()
                    .unwrap();
            }
        });

        let reader_store = DirectoryLabelStore::new(dir.path());
        let reader = tokio::spawn(async move {
            // unlocked reads must always see a complete label
            for _ in 0..20 {
                reader_store.get_label("foo").await.unwrap();
            }
        });

        writer.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test]
    async fn no_tmp_residue_after_cas_failure() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());
        let stored = store.create_label("foo").await.unwrap();

        let updated = store.set_label(&stored, [1, 2, 3, 4, 5]).await.unwrap();
        assert!(updated.is_some());
        // losing CAS must not leave a .label.tmp
        assert!(store.set_label(&stored, [9, 9, 9, 9, 9]).await.unwrap().is_none());
        assert!(!dir.path().join(".tmp").join("foo.label").exists());
    }

    #[tokio::test]
    async fn label_version_increments_monotonically() {
        let dir = tempdir().unwrap();
        let store = DirectoryLabelStore::new(dir.path());
        let mut label = store.create_label("foo").await.unwrap();
        assert_eq!(0, label.version);

        for expected in 1..5 {
            label = store
                .set_label(&label, [expected as u32; 5])
                .await
                .unwrap()
                .unwrap();
            assert_eq!(expected, label.version);
        }
    }
}
