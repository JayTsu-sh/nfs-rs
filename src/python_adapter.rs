use crate::client_core::{ClientCore, ClientDriver, CoreOperation, ResourceKey};
use crate::{Attr, Mount, MountHealth, NFSVersion, NfsError, Result, parse_url_and_mount};
use async_trait::async_trait;
use futures::TryStreamExt;
use pyo3::exceptions::{
    PyFileNotFoundError, PyPermissionError, PyRuntimeError, PyStopAsyncIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyType};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
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
}

struct ConnectedParts {
    core: Arc<ClientCore>,
    version: NFSVersion,
    health: MountHealth,
    health_source: Option<Arc<dyn Mount>>,
}

#[cfg(feature = "python-test-support")]
#[derive(Debug)]
struct TestDriver;

#[cfg(feature = "python-test-support")]
#[async_trait]
impl ClientDriver for TestDriver {
    async fn execute(&self, _operation: CoreOperation) -> Result<()> {
        Ok(())
    }

    async fn close_resource(&self, _key: ResourceKey) -> Result<()> {
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

    async fn close_resource(&self, _key: ResourceKey) -> Result<()> {
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
        NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_ACCES)
            | NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_ACCESS)
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

async fn directory_receiver(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    path: String,
) -> Result<mpsc::Receiver<DirectoryItem>> {
    let operation = core.begin_operation()?;
    let (sender, receiver) = mpsc::channel(1);
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
        match mount.readdirplus_path(&path).await {
            Ok(mut entries) => loop {
                let item = match entries.try_next().await {
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
    let driver = Arc::new(MountDriver {
        mount: tokio::sync::Mutex::new(Some(mount.clone())),
    });
    let core = ClientCore::with_recovery_event_capacity(driver, capacity).map_err(python_error)?;
    Ok(ConnectedParts {
        core,
        version,
        health,
        health_source: Some(mount),
    })
}

#[cfg(feature = "python-test-support")]
fn test_connected_parts(url: &str, capacity: usize) -> Option<PyResult<ConnectedParts>> {
    matches!(
        url,
        "nfs-test://fixture/export" | "nfs-test://fixture/delay"
    )
    .then(|| {
        let core = ClientCore::with_recovery_event_capacity(Arc::new(TestDriver), capacity)
            .map_err(python_error)?;
        Ok(ConnectedParts {
            core,
            version: NFSVersion::NFSv4p1,
            health: MountHealth::default(),
            health_source: None,
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
            } = parts?;
            return Ok(Self {
                core,
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
        } = connected_parts(mount, capacity)?;
        Ok(Self {
            core,
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
}

#[pyclass(name = "AsyncClient", module = "nfs_rs._internal")]
struct AsyncClient {
    core: Arc<ClientCore>,
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
            } = parts?;
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                if url.ends_with("/delay") {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(AsyncClient {
                    core,
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
            } = connected_parts(mount, capacity)?;
            Ok(AsyncClient {
                core,
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
}

#[pymodule(gil_used = true)]
fn _internal(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SyncClient>()?;
    module.add_class::<AsyncClient>()?;
    module.add_class::<SyncDirectoryCursor>()?;
    module.add_class::<AsyncDirectoryCursor>()?;
    module.add_function(wrap_pyfunction!(python_list_exports, module)?)?;
    module.add_function(wrap_pyfunction!(python_async_list_exports, module)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
