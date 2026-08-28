use crate::client_core::{ClientCore, ClientDriver, CoreOperation, ResourceKey};
use crate::{Mount, MountHealth, NFSVersion, NfsError, Result, parse_url_and_mount};
use async_trait::async_trait;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyType};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Debug)]
struct MountDriver {
    mount: tokio::sync::Mutex<Option<Box<dyn Mount>>>,
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

fn current_health(mut health: MountHealth, core: &ClientCore) -> MountHealth {
    health.lifecycle = match core.lifecycle() {
        crate::client_core::ClientLifecycle::Ready => crate::MountLifecycleState::Ready,
        crate::client_core::ClientLifecycle::Closing => crate::MountLifecycleState::Closing,
        crate::client_core::ClientLifecycle::Closed => crate::MountLifecycleState::Closed,
    };
    health
}

fn connected_parts(
    mount: Box<dyn Mount>,
    capacity: usize,
) -> PyResult<(Arc<ClientCore>, NFSVersion, MountHealth)> {
    let version = mount.version();
    let health = mount.health();
    let driver = Arc::new(MountDriver {
        mount: tokio::sync::Mutex::new(Some(mount)),
    });
    let core = ClientCore::with_recovery_event_capacity(driver, capacity).map_err(python_error)?;
    Ok((core, version, health))
}

#[cfg(feature = "python-test-support")]
fn test_connected_parts(
    url: &str,
    capacity: usize,
) -> Option<PyResult<(Arc<ClientCore>, NFSVersion, MountHealth)>> {
    matches!(
        url,
        "nfs-test://fixture/export" | "nfs-test://fixture/delay"
    )
    .then(|| {
        let core = ClientCore::with_recovery_event_capacity(Arc::new(TestDriver), capacity)
            .map_err(python_error)?;
        Ok((core, NFSVersion::NFSv4p1, MountHealth::default()))
    })
}

#[cfg(not(feature = "python-test-support"))]
fn test_connected_parts(
    _url: &str,
    _capacity: usize,
) -> Option<PyResult<(Arc<ClientCore>, NFSVersion, MountHealth)>> {
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

#[pyclass(name = "SyncClient", module = "nfs_rs._internal")]
struct SyncClient {
    core: Arc<ClientCore>,
    runtime: Mutex<Runtime>,
    version: NFSVersion,
    health: MountHealth,
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
            let (core, version, health) = parts?;
            return Ok(Self {
                core,
                runtime: Mutex::new(runtime),
                version,
                health,
                _operation_timeout: operation_timeout,
            });
        }
        let mount = py
            .detach(|| runtime.block_on(connect_mount(&url, connect_timeout)))
            .map_err(python_error)?;
        let (core, version, health) = connected_parts(mount, capacity)?;
        Ok(Self {
            core,
            runtime: Mutex::new(runtime),
            version,
            health,
            _operation_timeout: operation_timeout,
        })
    }

    #[getter]
    fn version(&self) -> (u8, Option<u8>) {
        version_tuple(self.version)
    }

    #[getter]
    fn health<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        health_dict(py, current_health(self.health, &self.core))
    }

    #[getter]
    fn closed(&self) -> bool {
        self.core.lifecycle() == crate::client_core::ClientLifecycle::Closed
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let report = py.detach(|| {
            let runtime = self
                .runtime
                .lock()
                .map_err(|_| PyRuntimeError::new_err("synchronous client runtime lock poisoned"))?;
            Ok::<_, PyErr>(runtime.block_on(self.core.close()))
        })?;
        if let Some(error) = report.errors().first() {
            Err(python_error(error))
        } else {
            Ok(())
        }
    }
}

#[pyclass(name = "AsyncClient", module = "nfs_rs._internal")]
struct AsyncClient {
    core: Arc<ClientCore>,
    version: NFSVersion,
    health: MountHealth,
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
            let (core, version, health) = parts?;
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                if url.ends_with("/delay") {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(AsyncClient {
                    core,
                    version,
                    health,
                    _operation_timeout: operation_timeout,
                })
            });
        }
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mount = connect_mount(&url, connect_timeout)
                .await
                .map_err(python_error)?;
            let (core, version, health) = connected_parts(mount, capacity)?;
            Ok(AsyncClient {
                core,
                version,
                health,
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
        health_dict(py, current_health(self.health, &self.core))
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
}

#[pymodule(gil_used = true)]
fn _internal(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SyncClient>()?;
    module.add_class::<AsyncClient>()?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
