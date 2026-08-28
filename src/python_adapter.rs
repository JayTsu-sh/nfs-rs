use crate::client_core::{ClientCore, ClientDriver, CoreOperation, ResourceKey};
use crate::{
    Attr, Mount, MountHealth, NFSVersion, NfsError, OpenFile, Result, parse_url_and_mount,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use pyo3::exceptions::{
    PyFileNotFoundError, PyPermissionError, PyRuntimeError, PyStopAsyncIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule, PyType};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::sync::mpsc;

type DirectoryItem = std::result::Result<Py<PyDict>, NfsError>;

async fn send_directory_item(
    sender: &mpsc::Sender<DirectoryItem>,
    item: DirectoryItem,
    core: &ClientCore,
) -> bool {
    tokio::select! {
        result = sender.send(item) => result.is_ok(),
        () = core.wait_for_lifecycle(crate::client_core::ClientLifecycle::Closing) => false,
    }
}

#[derive(Debug)]
struct MountDriver {
    mount: tokio::sync::Mutex<Option<Arc<dyn Mount>>>,
    resources: Arc<AdapterResources>,
}

struct ConnectedParts {
    core: Arc<ClientCore>,
    version: NFSVersion,
    health: MountHealth,
    health_source: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
}

#[derive(Debug, Default)]
struct AdapterResources {
    files: Mutex<HashMap<ResourceKey, Arc<FileResource>>>,
}

impl AdapterResources {
    fn insert(&self, key: ResourceKey, resource: Arc<FileResource>) -> Result<()> {
        self.files
            .lock()
            .map_err(|_| NfsError::Rpc("file registry lock poisoned".to_string()))?
            .insert(key, resource);
        Ok(())
    }

    fn remove(&self, key: ResourceKey) -> Result<Option<Arc<FileResource>>> {
        Ok(self
            .files
            .lock()
            .map_err(|_| NfsError::Rpc("file registry lock poisoned".to_string()))?
            .remove(&key))
    }
}

#[derive(Debug)]
enum ReadBackend {
    Mount {
        mount: Arc<dyn Mount>,
        file_handle: Bytes,
        max_read: u32,
    },
    #[cfg(feature = "python-test-support")]
    Test { data: Arc<[u8]>, max_read: u32 },
}

#[derive(Debug)]
struct FileResource {
    backend: ReadBackend,
    size: u64,
    relative_gate: tokio::sync::Mutex<()>,
    position: AtomicU64,
    close_state: Mutex<FileCloseState>,
    close_notify: Notify,
}

#[derive(Debug, Default)]
struct FileCloseState {
    started: bool,
    file: Option<OpenFile>,
    result: Option<std::result::Result<(), String>>,
}

impl FileResource {
    fn mount(mount: Arc<dyn Mount>, file: OpenFile, size: u64) -> Arc<Self> {
        let max_read = mount.get_max_read_size().max(1);
        let file_handle = file.file_handle();
        Arc::new(Self {
            backend: ReadBackend::Mount {
                mount,
                file_handle,
                max_read,
            },
            size,
            relative_gate: tokio::sync::Mutex::new(()),
            position: AtomicU64::new(0),
            close_state: Mutex::new(FileCloseState {
                started: false,
                file: Some(file),
                result: None,
            }),
            close_notify: Notify::new(),
        })
    }

    #[cfg(feature = "python-test-support")]
    fn test() -> Arc<Self> {
        let data: Arc<[u8]> = Arc::from(&b"abcdefghijklmnopqrstuvwxyz"[..]);
        Arc::new(Self {
            size: data.len() as u64,
            backend: ReadBackend::Test { data, max_read: 4 },
            relative_gate: tokio::sync::Mutex::new(()),
            position: AtomicU64::new(0),
            close_state: Mutex::new(FileCloseState::default()),
            close_notify: Notify::new(),
        })
    }

    fn closed(&self) -> bool {
        self.close_state
            .lock()
            .map(|state| state.result.is_some())
            .unwrap_or(true)
    }

    async fn close(self: &Arc<Self>) -> std::result::Result<(), String> {
        let file = self
            .close_state
            .lock()
            .map(|mut state| {
                if state.started {
                    None
                } else {
                    state.started = true;
                    Some(state.file.take())
                }
            })
            .map_err(|_| "file close state lock poisoned".to_string())?;
        if let Some(file) = file {
            let resource = self.clone();
            tokio::spawn(async move {
                let result = match (&resource.backend, file) {
                    (ReadBackend::Mount { mount, .. }, Some(file)) => mount
                        .close_stateful(file)
                        .await
                        .map_err(|error| error.to_string()),
                    _ => Ok(()),
                };
                if let Ok(mut state) = resource.close_state.lock() {
                    state.result = Some(result);
                }
                resource.close_notify.notify_waiters();
            });
        }
        loop {
            let notified = self.close_notify.notified();
            if let Some(result) = self
                .close_state
                .lock()
                .ok()
                .and_then(|state| state.result.clone())
            {
                return result;
            }
            notified.await;
        }
    }

    async fn read_chunk(&self, offset: u64, count: u32) -> Result<Bytes> {
        match &self.backend {
            ReadBackend::Mount {
                mount, file_handle, ..
            } => mount.read(file_handle.clone(), offset, count).await,
            #[cfg(feature = "python-test-support")]
            ReadBackend::Test { data, .. } => {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(data.len());
                let end = start.saturating_add(count as usize).min(data.len());
                Ok(Bytes::copy_from_slice(&data[start..end]))
            }
        }
    }

    fn max_read(&self) -> u32 {
        match &self.backend {
            ReadBackend::Mount { max_read, .. } => *max_read,
            #[cfg(feature = "python-test-support")]
            ReadBackend::Test { max_read, .. } => *max_read,
        }
    }

    async fn read_at(&self, offset: u64, size: i64) -> Result<Vec<u8>> {
        if size < -1 {
            return Err(NfsError::InvalidInput(
                "read size must be -1 or non-negative".to_string(),
            ));
        }
        let requested = if size == -1 {
            self.size.saturating_sub(offset)
        } else {
            size as u64
        };
        let mut result = Vec::new();
        let mut current = offset;
        let mut remaining = requested;
        while remaining != 0 {
            let count = remaining.min(u64::from(self.max_read())) as u32;
            let chunk = self.read_chunk(current, count).await?;
            if chunk.is_empty() {
                break;
            }
            current = current.saturating_add(chunk.len() as u64);
            remaining = remaining.saturating_sub(chunk.len() as u64);
            result.extend_from_slice(&chunk);
            if chunk.len() < count as usize {
                break;
            }
        }
        Ok(result)
    }

    async fn read(&self, size: i64) -> Result<Vec<u8>> {
        let _guard = self.relative_gate.lock().await;
        let position = self.position.load(Ordering::Acquire);
        let data = self.read_at(position, size).await?;
        self.position.store(
            position.saturating_add(data.len() as u64),
            Ordering::Release,
        );
        Ok(data)
    }

    async fn seek(&self, offset: i64, whence: i32) -> Result<u64> {
        let _guard = self.relative_gate.lock().await;
        let position = self.position.load(Ordering::Acquire);
        let base = match whence {
            0 => 0_i128,
            1 => i128::from(position),
            2 => i128::from(self.size),
            _ => {
                return Err(NfsError::InvalidInput(
                    "whence must be SEEK_SET, SEEK_CUR, or SEEK_END".to_string(),
                ));
            }
        };
        let next = base + i128::from(offset);
        if !(0..=i128::from(u64::MAX)).contains(&next) {
            return Err(NfsError::InvalidInput("negative seek position".to_string()));
        }
        let next = next as u64;
        self.position.store(next, Ordering::Release);
        Ok(next)
    }

    fn tell(&self) -> u64 {
        self.position.load(Ordering::Acquire)
    }
}

#[cfg(feature = "python-test-support")]
#[derive(Debug)]
struct TestDriver {
    resources: Arc<AdapterResources>,
}

#[cfg(feature = "python-test-support")]
#[async_trait]
impl ClientDriver for TestDriver {
    async fn execute(&self, _operation: CoreOperation) -> Result<()> {
        Ok(())
    }

    async fn close_resource(&self, key: ResourceKey) -> Result<()> {
        if let Some(resource) = self.resources.remove(key)? {
            resource.close().await.map_err(NfsError::Rpc)?;
        }
        Ok(())
    }

    async fn umount(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ClientDriver for MountDriver {
    async fn execute(&self, _operation: CoreOperation) -> Result<()> {
        Err(NfsError::Unsupported(
            "operation is not implemented by the minimal Python client".to_string(),
        ))
    }

    async fn close_resource(&self, key: ResourceKey) -> Result<()> {
        if let Some(resource) = self.resources.remove(key)? {
            resource.close().await.map_err(NfsError::Rpc)?;
        }
        Ok(())
    }

    async fn umount(&self) -> Result<()> {
        let mount = self.mount.lock().await.take();
        if let Some(mount) = mount {
            mount.umount().await
        } else {
            Ok(())
        }
    }
}

fn python_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn nfs_error(error: NfsError) -> PyErr {
    let permission_denied = matches!(
        &error,
        NfsError::Nfs3(
            crate::nfs3::ErrorCode::NFS3ERR_ACCES | crate::nfs3::ErrorCode::NFS3ERR_PERM
        ) | NfsError::Nfs4(
            crate::nfs4::Nfs4ErrorCode::NFS4ERR_ACCESS | crate::nfs4::Nfs4ErrorCode::NFS4ERR_PERM
        )
    );
    if error.is_not_found() {
        PyFileNotFoundError::new_err(error.to_string())
    } else if permission_denied || error.kind() == std::io::ErrorKind::PermissionDenied {
        PyPermissionError::new_err(error.to_string())
    } else {
        python_error(error)
    }
}

fn file_type(type_: u32) -> &'static str {
    match type_ {
        1 => "file",
        2 => "directory",
        3 => "block_device",
        4 => "character_device",
        5 => "symlink",
        6 => "socket",
        7 => "fifo",
        _ => "unknown",
    }
}

fn nanoseconds(seconds: u32, nanoseconds: u32) -> u64 {
    u64::from(seconds)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(nanoseconds))
}

fn attr_dict<'py>(py: Python<'py>, attr: &Attr) -> PyResult<Bound<'py, PyDict>> {
    let values = PyDict::new(py);
    values.set_item("type", file_type(attr.type_))?;
    values.set_item("mode", attr.file_mode)?;
    values.set_item("nlink", attr.nlink)?;
    values.set_item("uid", attr.uid)?;
    values.set_item("gid", attr.gid)?;
    values.set_item("size", attr.filesize)?;
    values.set_item("used", attr.used)?;
    values.set_item("fsid", attr.fsid)?;
    values.set_item("fileid", attr.fileid)?;
    values.set_item(
        "atime_ns",
        nanoseconds(attr.atime.seconds, attr.atime.nseconds),
    )?;
    values.set_item(
        "mtime_ns",
        nanoseconds(attr.mtime.seconds, attr.mtime.nseconds),
    )?;
    values.set_item(
        "ctime_ns",
        nanoseconds(attr.ctime.seconds, attr.ctime.nseconds),
    )?;
    values.set_item(
        "owner",
        (!attr.owner.is_empty()).then_some(attr.owner.as_str()),
    )?;
    values.set_item(
        "group",
        (!attr.owner_group.is_empty()).then_some(attr.owner_group.as_str()),
    )?;
    Ok(values)
}

fn entry_dict(name: String, attr: Attr) -> PyResult<Py<PyDict>> {
    Python::attach(|py| {
        let values = PyDict::new(py);
        values.set_item("name", name)?;
        values.set_item("info", attr_dict(py, &attr)?)?;
        Ok(values.unbind())
    })
}

#[cfg(feature = "python-test-support")]
fn test_attr(path: &str) -> Option<Result<Attr>> {
    let fileid = match path {
        "missing" => return Some(Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT))),
        "denied" => return Some(Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_ACCES))),
        "forbidden" => return Some(Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_PERM))),
        _ => 9,
    };
    Some(Ok(Attr {
        type_: 1,
        file_mode: 0o644,
        nlink: 1,
        uid: 1000,
        gid: 1000,
        filesize: 12,
        used: 512,
        fsid: 7,
        fileid,
        atime: crate::Time {
            seconds: 1,
            nseconds: 2,
        },
        mtime: crate::Time {
            seconds: 3,
            nseconds: 4,
        },
        ctime: crate::Time {
            seconds: 5,
            nseconds: 6,
        },
        ..Attr::default()
    }))
}

#[cfg(not(feature = "python-test-support"))]
fn test_attr(_path: &str) -> Option<Result<Attr>> {
    None
}

async fn stat_attr(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    path: String,
) -> Result<Attr> {
    let _operation = core.begin_operation()?;
    if let Some(result) = test_attr(&path) {
        return result;
    }
    let mount = mount.ok_or_else(|| {
        NfsError::Unsupported("stat requires a connected protocol engine".to_string())
    })?;
    mount.getattr_path(&path).await
}

#[cfg(feature = "python-test-support")]
fn test_directory_entries(path: &str) -> Option<Vec<(String, Attr)>> {
    (path == "." || path == "folder" || path == "large").then(|| {
        let first = test_attr("first")
            .and_then(std::result::Result::ok)
            .unwrap_or_default();
        let mut second = first.clone();
        second.fileid = 10;
        let mut entries = vec![
            ("first".to_string(), first),
            ("second".to_string(), second.clone()),
        ];
        if path == "large" {
            let mut third = second;
            third.fileid = 11;
            entries.push(("third".to_string(), third));
        }
        entries
    })
}

#[cfg(not(feature = "python-test-support"))]
fn test_directory_entries(_path: &str) -> Option<Vec<(String, Attr)>> {
    None
}

#[cfg(feature = "python-test-support")]
fn test_directory_blocks(path: &str) -> bool {
    path == "blocked"
}

#[cfg(not(feature = "python-test-support"))]
fn test_directory_blocks(_path: &str) -> bool {
    false
}

#[cfg(feature = "python-test-support")]
fn test_directory_fails(path: &str) -> bool {
    path == "denied-directory"
}

#[cfg(not(feature = "python-test-support"))]
fn test_directory_fails(_path: &str) -> bool {
    false
}

async fn directory_receiver(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    path: String,
) -> Result<mpsc::Receiver<DirectoryItem>> {
    let operation = core.begin_operation()?;
    let (sender, receiver) = mpsc::channel(1);
    if test_directory_blocks(&path) {
        tokio::spawn(async move {
            let _operation = operation;
            core.wait_for_lifecycle(crate::client_core::ClientLifecycle::Closing)
                .await;
        });
        return Ok(receiver);
    }
    if test_directory_fails(&path) {
        let closing = core.clone();
        tokio::spawn(async move {
            let _operation = operation;
            let first = test_attr("first")
                .and_then(std::result::Result::ok)
                .unwrap_or_default();
            let first = entry_dict("first".to_string(), first)
                .map_err(|error| NfsError::Rpc(error.to_string()));
            if send_directory_item(&sender, first, &closing).await {
                let error = NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_PERM);
                let _ = send_directory_item(&sender, Err(error), &closing).await;
            }
        });
        return Ok(receiver);
    }
    if let Some(entries) = test_directory_entries(&path) {
        let closing = core.clone();
        tokio::spawn(async move {
            let _operation = operation;
            for (name, attr) in entries {
                let item = entry_dict(name, attr).map_err(|error| NfsError::Rpc(error.to_string()));
                if !send_directory_item(&sender, item, &closing).await {
                    break;
                }
            }
        });
        return Ok(receiver);
    }
    let mount = mount.ok_or_else(|| {
        NfsError::Unsupported("scandir requires a connected protocol engine".to_string())
    })?;
    let closing = core.clone();
    tokio::spawn(async move {
        let _operation = operation;
        let stream = tokio::select! {
            result = mount.readdirplus_path(&path) => Some(result),
            () = closing.wait_for_lifecycle(crate::client_core::ClientLifecycle::Closing) => None,
        };
        let Some(stream) = stream else {
            return;
        };
        match stream {
            Ok(mut entries) => loop {
                let next = tokio::select! {
                    result = entries.try_next() => Some(result),
                    () = closing.wait_for_lifecycle(crate::client_core::ClientLifecycle::Closing) => None,
                };
                let Some(next) = next else {
                    break;
                };
                let item = match next {
                    Ok(Some(entry)) => match entry.attr {
                        Some(attr) => entry_dict(entry.file_name, attr)
                            .map_err(|error| NfsError::Rpc(error.to_string())),
                        None => Err(NfsError::Rpc(
                            "directory entry did not include attributes".to_string(),
                        )),
                    },
                    Ok(None) => break,
                    Err(error) => Err(error),
                };
                let stop = item.is_err();
                if !send_directory_item(&sender, item, &closing).await || stop {
                    break;
                }
            },
            Err(error) => {
                let _ = send_directory_item(&sender, Err(error), &closing).await;
            }
        }
    });
    Ok(receiver)
}

#[pyclass(module = "nfs_rs._internal")]
struct SyncDirectoryCursor {
    runtime: Arc<Runtime>,
    receiver: Mutex<mpsc::Receiver<DirectoryItem>>,
}

#[pymethods]
impl SyncDirectoryCursor {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        py.detach(|| {
            let mut receiver = self
                .receiver
                .lock()
                .map_err(|_| PyRuntimeError::new_err("directory cursor lock poisoned"))?;
            match self.runtime.block_on(receiver.recv()) {
                Some(Ok(values)) => Ok(Some(values)),
                Some(Err(error)) => Err(nfs_error(error)),
                None => Ok(None),
            }
        })
    }
}

#[pyclass(module = "nfs_rs._internal")]
struct AsyncDirectoryCursor {
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<DirectoryItem>>>,
}

#[pymethods]
impl AsyncDirectoryCursor {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = self.receiver.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match receiver.lock().await.recv().await {
                Some(Ok(values)) => Ok(values),
                Some(Err(error)) => Err(nfs_error(error)),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

fn version_tuple(version: NFSVersion) -> (u8, Option<u8>) {
    match version {
        NFSVersion::NFSv3 => (3, None),
        NFSVersion::NFSv4p0 => (4, Some(0)),
        NFSVersion::NFSv4p1 => (4, Some(1)),
        _ => (0, None),
    }
}

fn health_dict<'py>(py: Python<'py>, health: MountHealth) -> PyResult<Bound<'py, PyDict>> {
    let result = PyDict::new(py);
    result.set_item(
        "lifecycle",
        format!("{:?}", health.lifecycle).to_lowercase(),
    )?;
    result.set_item("generation", health.generation)?;
    result.set_item("lease_healthy", health.lease_healthy)?;
    Ok(result)
}

fn current_health(
    initial: MountHealth,
    source: Option<&Arc<dyn Mount>>,
    core: &ClientCore,
) -> MountHealth {
    let mut health = source.map_or(initial, |mount| mount.health());
    health.lifecycle = match core.lifecycle() {
        crate::client_core::ClientLifecycle::Ready => crate::MountLifecycleState::Ready,
        crate::client_core::ClientLifecycle::Closing => crate::MountLifecycleState::Closing,
        crate::client_core::ClientLifecycle::Closed => crate::MountLifecycleState::Closed,
    };
    health
}

fn connected_parts(mount: Box<dyn Mount>, capacity: usize) -> PyResult<ConnectedParts> {
    let mount: Arc<dyn Mount> = Arc::from(mount);
    let version = mount.version();
    let health = mount.health();
    let resources = Arc::new(AdapterResources::default());
    let driver = Arc::new(MountDriver {
        mount: tokio::sync::Mutex::new(Some(mount.clone())),
        resources: resources.clone(),
    });
    let core = ClientCore::with_recovery_event_capacity(driver, capacity).map_err(python_error)?;
    Ok(ConnectedParts {
        core,
        version,
        health,
        health_source: Some(mount),
        resources,
    })
}

#[cfg(feature = "python-test-support")]
fn test_connected_parts(url: &str, capacity: usize) -> Option<PyResult<ConnectedParts>> {
    matches!(
        url,
        "nfs-test://fixture/export" | "nfs-test://fixture/delay"
    )
    .then(|| {
        let resources = Arc::new(AdapterResources::default());
        let core = ClientCore::with_recovery_event_capacity(
            Arc::new(TestDriver {
                resources: resources.clone(),
            }),
            capacity,
        )
        .map_err(python_error)?;
        Ok(ConnectedParts {
            core,
            version: NFSVersion::NFSv4p1,
            health: MountHealth::default(),
            health_source: None,
            resources,
        })
    })
}

#[cfg(not(feature = "python-test-support"))]
fn test_connected_parts(_url: &str, _capacity: usize) -> Option<PyResult<ConnectedParts>> {
    None
}

fn recovery_capacity(options: Option<&Bound<'_, PyDict>>) -> PyResult<usize> {
    match options.and_then(|values| values.get_item("recovery_event_capacity").ok().flatten()) {
        Some(value) => value.extract(),
        None => Ok(256),
    }
}

fn timeout_option(options: Option<&Bound<'_, PyDict>>, name: &str) -> PyResult<Option<Duration>> {
    options
        .and_then(|values| values.get_item(name).ok().flatten())
        .map(|value| value.extract::<f64>())
        .transpose()?
        .map(|seconds| {
            if seconds.is_finite() && seconds > 0.0 {
                Ok(Duration::from_secs_f64(seconds))
            } else {
                Err(PyValueError::new_err(format!("{name} must be positive")))
            }
        })
        .transpose()
}

async fn connect_mount(url: &str, timeout: Option<Duration>) -> Result<Box<dyn Mount>> {
    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, parse_url_and_mount(url))
            .await
            .map_err(|_| NfsError::Rpc("connection deadline exceeded".to_string()))?
    } else {
        parse_url_and_mount(url).await
    }
}

async fn open_file(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
    path: String,
) -> Result<(ResourceKey, Arc<FileResource>)> {
    let _operation = core.begin_operation()?;
    let resource = if let Some(mount) = mount {
        // Resolve size before acquiring protocol-owned open state. Once open
        // succeeds, registration is synchronous so cancellation cannot orphan it.
        let size = mount.getattr_path(&path).await?.filesize;
        let file = mount.open_path_stateful(&path, crate::OPEN_READ).await?;
        FileResource::mount(mount, file, size)
    } else {
        #[cfg(feature = "python-test-support")]
        {
            FileResource::test()
        }
        #[cfg(not(feature = "python-test-support"))]
        {
            return Err(NfsError::Unsupported("mount is unavailable".to_string()));
        }
    };
    let key = core.allocate_resource_key()?;
    resources.insert(key, resource.clone())?;
    if let Err(error) = core.publish_resource(key) {
        let _ = resources.remove(key);
        let _ = resource.close().await;
        return Err(error);
    }
    Ok((key, resource))
}

async fn close_file(
    core: Arc<ClientCore>,
    resources: Arc<AdapterResources>,
    key: ResourceKey,
    resource: Arc<FileResource>,
) -> Result<()> {
    core.unregister_resource(key)?;
    let _ = resources.remove(key)?;
    resource.close().await.map_err(NfsError::Rpc)
}

fn validate_read_mode(mode: &str) -> PyResult<()> {
    if mode == "rb" {
        Ok(())
    } else {
        Err(PyValueError::new_err(
            "Ticket 05 supports only binary read mode 'rb'",
        ))
    }
}

#[pyclass(name = "SyncFile", module = "nfs_rs._internal")]
struct SyncFile {
    key: ResourceKey,
    resource: Arc<FileResource>,
    resources: Arc<AdapterResources>,
    core: Arc<ClientCore>,
    runtime: Arc<Runtime>,
}

#[pymethods]
impl SyncFile {
    #[getter]
    fn closed(&self) -> bool {
        self.resource.closed()
    }

    #[pyo3(signature = (size = -1))]
    fn read(&self, py: Python<'_>, size: i64) -> PyResult<Py<PyBytes>> {
        let data = py
            .detach(|| self.runtime.block_on(self.resource.read(size)))
            .map_err(nfs_error)?;
        Ok(PyBytes::new(py, &data).unbind())
    }

    #[pyo3(signature = (offset, size = -1))]
    fn read_at(&self, py: Python<'_>, offset: u64, size: i64) -> PyResult<Py<PyBytes>> {
        let data = py
            .detach(|| self.runtime.block_on(self.resource.read_at(offset, size)))
            .map_err(nfs_error)?;
        Ok(PyBytes::new(py, &data).unbind())
    }

    #[pyo3(signature = (offset, whence = 0))]
    fn seek(&self, py: Python<'_>, offset: i64, whence: i32) -> PyResult<u64> {
        py.detach(|| self.runtime.block_on(self.resource.seek(offset, whence)))
            .map_err(nfs_error)
    }

    fn tell(&self) -> u64 {
        self.resource.tell()
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(close_file(
                self.core.clone(),
                self.resources.clone(),
                self.key,
                self.resource.clone(),
            ))
        })
        .map_err(nfs_error)
    }
}

#[pyclass(name = "AsyncFile", module = "nfs_rs._internal")]
struct AsyncFile {
    key: ResourceKey,
    resource: Arc<FileResource>,
    resources: Arc<AdapterResources>,
    core: Arc<ClientCore>,
}

#[pymethods]
impl AsyncFile {
    #[getter]
    fn closed(&self) -> bool {
        self.resource.closed()
    }

    #[pyo3(signature = (size = -1))]
    fn read<'py>(&self, py: Python<'py>, size: i64) -> PyResult<Bound<'py, PyAny>> {
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let data = resource.read(size).await.map_err(nfs_error)?;
            Python::attach(|py| Ok(PyBytes::new(py, &data).unbind()))
        })
    }

    #[pyo3(signature = (offset, size = -1))]
    fn read_at<'py>(&self, py: Python<'py>, offset: u64, size: i64) -> PyResult<Bound<'py, PyAny>> {
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let data = resource.read_at(offset, size).await.map_err(nfs_error)?;
            Python::attach(|py| Ok(PyBytes::new(py, &data).unbind()))
        })
    }

    #[pyo3(signature = (offset, whence = 0))]
    fn seek<'py>(&self, py: Python<'py>, offset: i64, whence: i32) -> PyResult<Bound<'py, PyAny>> {
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            resource.seek(offset, whence).await.map_err(nfs_error)
        })
    }

    fn tell(&self) -> u64 {
        self.resource.tell()
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let resources = self.resources.clone();
        let key = self.key;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            close_file(core, resources, key, resource)
                .await
                .map_err(nfs_error)
        })
    }
}

async fn export_values(
    host: &str,
    timeout: Option<Duration>,
) -> Result<Vec<(String, Vec<String>)>> {
    let exports = if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, crate::list_exports(host))
            .await
            .map_err(|_| NfsError::Rpc("export discovery deadline exceeded".to_string()))??
    } else {
        crate::list_exports(host).await?
    };
    Ok(exports
        .into_iter()
        .map(|entry| (entry.path, entry.groups))
        .collect())
}

#[cfg(feature = "python-test-support")]
fn test_export_values(host: &str) -> Option<Vec<(String, Vec<String>)>> {
    host.starts_with("nfs-test://")
        .then(|| vec![("/data".to_string(), vec!["team".to_string()])])
}

#[cfg(not(feature = "python-test-support"))]
fn test_export_values(_host: &str) -> Option<Vec<(String, Vec<String>)>> {
    None
}

#[pyfunction(name = "list_exports", signature = (host, **options))]
fn python_list_exports(
    py: Python<'_>,
    host: String,
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<(String, Vec<String>)>> {
    if let Some(values) = test_export_values(&host) {
        return Ok(values);
    }
    let timeout = timeout_option(options, "connect_timeout")?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("nfs-rs-python-exports")
        .build()
        .map_err(python_error)?;
    py.detach(|| runtime.block_on(export_values(&host, timeout)))
        .map_err(nfs_error)
}

#[pyfunction(name = "async_list_exports", signature = (host, **options))]
fn python_async_list_exports<'py>(
    py: Python<'py>,
    host: String,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<Bound<'py, PyAny>> {
    if let Some(values) = test_export_values(&host) {
        return pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(values) });
    }
    let timeout = timeout_option(options, "connect_timeout")?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        export_values(&host, timeout).await.map_err(nfs_error)
    })
}

#[pyclass(name = "SyncClient", module = "nfs_rs._internal")]
struct SyncClient {
    core: Arc<ClientCore>,
    resources: Arc<AdapterResources>,
    runtime: Arc<Runtime>,
    version: NFSVersion,
    health: MountHealth,
    health_source: Option<Arc<dyn Mount>>,
    _operation_timeout: Option<Duration>,
}

#[pymethods]
impl SyncClient {
    #[classmethod]
    #[pyo3(signature = (url, **options))]
    fn connect(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        url: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let capacity = recovery_capacity(options)?;
        let connect_timeout = timeout_option(options, "connect_timeout")?;
        let operation_timeout = timeout_option(options, "operation_timeout")?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("nfs-rs-python-sync")
            .build()
            .map_err(python_error)?;
        if let Some(parts) = test_connected_parts(&url, capacity) {
            if url.ends_with("/delay") {
                py.detach(|| {
                    runtime.block_on(async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    })
                });
            }
            let ConnectedParts {
                core,
                version,
                health,
                health_source,
                resources,
            } = parts?;
            return Ok(Self {
                core,
                resources,
                runtime: Arc::new(runtime),
                version,
                health,
                health_source,
                _operation_timeout: operation_timeout,
            });
        }
        let mount = py
            .detach(|| runtime.block_on(connect_mount(&url, connect_timeout)))
            .map_err(python_error)?;
        let ConnectedParts {
            core,
            version,
            health,
            health_source,
            resources,
        } = connected_parts(mount, capacity)?;
        Ok(Self {
            core,
            resources,
            runtime: Arc::new(runtime),
            version,
            health,
            health_source,
            _operation_timeout: operation_timeout,
        })
    }

    #[getter]
    fn version(&self) -> (u8, Option<u8>) {
        version_tuple(self.version)
    }

    #[getter]
    fn health<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        health_dict(
            py,
            current_health(self.health, self.health_source.as_ref(), &self.core),
        )
    }

    #[getter]
    fn closed(&self) -> bool {
        self.core.lifecycle() == crate::client_core::ClientLifecycle::Closed
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let report = py.detach(|| self.runtime.block_on(self.core.close()));
        if let Some(error) = report.errors().first() {
            Err(python_error(error))
        } else {
            Ok(())
        }
    }

    fn stat<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyDict>> {
        let attr = py
            .detach(|| {
                self.runtime.block_on(stat_attr(
                    self.core.clone(),
                    self.health_source.clone(),
                    path,
                ))
            })
            .map_err(nfs_error)?;
        attr_dict(py, &attr)
    }

    fn scandir(&self, py: Python<'_>, path: String) -> PyResult<SyncDirectoryCursor> {
        let receiver = py
            .detach(|| {
                self.runtime.block_on(directory_receiver(
                    self.core.clone(),
                    self.health_source.clone(),
                    path,
                ))
            })
            .map_err(nfs_error)?;
        Ok(SyncDirectoryCursor {
            runtime: self.runtime.clone(),
            receiver: Mutex::new(receiver),
        })
    }

    #[pyo3(signature = (path, mode = "rb"))]
    fn open(&self, py: Python<'_>, path: String, mode: &str) -> PyResult<SyncFile> {
        validate_read_mode(mode)?;
        let (key, resource) = py
            .detach(|| {
                self.runtime.block_on(open_file(
                    self.core.clone(),
                    self.health_source.clone(),
                    self.resources.clone(),
                    path,
                ))
            })
            .map_err(nfs_error)?;
        Ok(SyncFile {
            key,
            resource,
            resources: self.resources.clone(),
            core: self.core.clone(),
            runtime: self.runtime.clone(),
        })
    }
}

#[pyclass(name = "AsyncClient", module = "nfs_rs._internal")]
struct AsyncClient {
    core: Arc<ClientCore>,
    resources: Arc<AdapterResources>,
    version: NFSVersion,
    health: MountHealth,
    health_source: Option<Arc<dyn Mount>>,
    _operation_timeout: Option<Duration>,
}

#[pymethods]
impl AsyncClient {
    #[classmethod]
    #[pyo3(signature = (url, **options))]
    fn connect<'py>(
        _class: &Bound<'py, PyType>,
        py: Python<'py>,
        url: String,
        options: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let capacity = recovery_capacity(options)?;
        let connect_timeout = timeout_option(options, "connect_timeout")?;
        let operation_timeout = timeout_option(options, "operation_timeout")?;
        if let Some(parts) = test_connected_parts(&url, capacity) {
            let ConnectedParts {
                core,
                version,
                health,
                health_source,
                resources,
            } = parts?;
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                if url.ends_with("/delay") {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(AsyncClient {
                    core,
                    resources,
                    version,
                    health,
                    health_source,
                    _operation_timeout: operation_timeout,
                })
            });
        }
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mount = connect_mount(&url, connect_timeout)
                .await
                .map_err(python_error)?;
            let ConnectedParts {
                core,
                version,
                health,
                health_source,
                resources,
            } = connected_parts(mount, capacity)?;
            Ok(AsyncClient {
                core,
                resources,
                version,
                health,
                health_source,
                _operation_timeout: operation_timeout,
            })
        })
    }

    #[getter]
    fn version(&self) -> (u8, Option<u8>) {
        version_tuple(self.version)
    }

    #[getter]
    fn health<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        health_dict(
            py,
            current_health(self.health, self.health_source.as_ref(), &self.core),
        )
    }

    #[getter]
    fn closed(&self) -> bool {
        self.core.lifecycle() == crate::client_core::ClientLifecycle::Closed
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let report = core.close().await;
            if let Some(error) = report.errors().first() {
                Err(python_error(error))
            } else {
                Ok(())
            }
        })
    }

    fn stat<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let attr = stat_attr(core, mount, path).await.map_err(nfs_error)?;
            Python::attach(|py| attr_dict(py, &attr).map(Bound::unbind))
        })
    }

    fn scandir<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let receiver = directory_receiver(core, mount, path)
                .await
                .map_err(nfs_error)?;
            Ok(AsyncDirectoryCursor {
                receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
            })
        })
    }

    #[pyo3(signature = (path, mode = "rb"))]
    fn open<'py>(&self, py: Python<'py>, path: String, mode: &str) -> PyResult<Bound<'py, PyAny>> {
        validate_read_mode(mode)?;
        let core = self.core.clone();
        let mount = self.health_source.clone();
        let resources = self.resources.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (key, resource) = open_file(core.clone(), mount, resources.clone(), path)
                .await
                .map_err(nfs_error)?;
            Ok(AsyncFile {
                key,
                resource,
                resources,
                core,
            })
        })
    }
}

#[pymodule(gil_used = true)]
fn _internal(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SyncClient>()?;
    module.add_class::<AsyncClient>()?;
    module.add_class::<SyncDirectoryCursor>()?;
    module.add_class::<AsyncDirectoryCursor>()?;
    module.add_class::<SyncFile>()?;
    module.add_class::<AsyncFile>()?;
    module.add_function(wrap_pyfunction!(python_list_exports, module)?)?;
    module.add_function(wrap_pyfunction!(python_async_list_exports, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(all(test, feature = "python-test-support"))]
mod read_only_file_tests {
    use super::FileResource;

    #[tokio::test]
    async fn negotiated_chunks_are_reassembled() {
        let file = FileResource::test();
        assert_eq!(file.max_read(), 4);
        let Ok(data) = file.read_at(2, 11).await else {
            panic!("fixture read should succeed");
        };
        assert_eq!(data, b"cdefghijklm");
    }

    #[tokio::test]
    async fn positional_reads_do_not_change_relative_position() {
        let file = FileResource::test();
        let Ok(initial) = file.read(3).await else {
            panic!("fixture read should succeed");
        };
        assert_eq!(initial, b"abc");
        let (left, right) = tokio::join!(file.read_at(4, 4), file.read_at(8, 4));
        let (Ok(left), Ok(right)) = (left, right) else {
            panic!("positional fixture reads should succeed");
        };
        assert_eq!(left, b"efgh");
        assert_eq!(right, b"ijkl");
        assert_eq!(file.tell(), 3);
    }
}
