//! A Python object as the mount's [`Dialer`].
//!
//! The object's `dial(host, port)` returns a connected stream socket. The
//! adapter takes ownership of its descriptor, so RPC records never pass
//! through Python. `dial` runs on its own thread, attached to the interpreter
//! only for the call: a tokio worker never waits for the GIL, and a runtime
//! being dropped by a thread that holds the GIL never waits for a dial.
//!
//! A synchronous `dial` cannot be cut: past the caller's deadline the call
//! goes on until it returns. At most [`MAX_DIALS_IN_FLIGHT`] calls run at
//! once in a process; waiting for a slot counts against the same deadline,
//! and a call keeps its slot until `dial` returns.
use crate::dialer::{BoxedRpcStream, Dialer, Endpoint};
use async_trait::async_trait;
use pyo3::exceptions::{
    PyConnectionAbortedError, PyConnectionRefusedError, PyConnectionResetError,
    PyFileNotFoundError, PyOSError, PyPermissionError, PyTimeoutError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Python `dial` calls running at once in this process.
pub(super) const MAX_DIALS_IN_FLIGHT: usize = 16;

static DIAL_SLOTS: Semaphore = Semaphore::const_new(MAX_DIALS_IN_FLIGHT);

pub(super) struct PythonDialer {
    target: Arc<Py<PyAny>>,
}

/// The `dialer` option, or `None` when it is absent or `None`.
pub(super) fn dialer_option(
    options: Option<&Bound<'_, PyDict>>,
) -> PyResult<Option<Arc<dyn Dialer>>> {
    let Some(value) = options
        .map(|values| values.get_item("dialer"))
        .transpose()?
        .flatten()
        .filter(|value| !value.is_none())
    else {
        return Ok(None);
    };
    let callable = value
        .getattr_opt("dial")?
        .is_some_and(|dial| dial.is_callable());
    if !callable {
        return Err(PyTypeError::new_err(
            "dialer must have a callable dial(host, port) method",
        ));
    }
    Ok(Some(Arc::new(PythonDialer {
        target: Arc::new(value.unbind()),
    })))
}

#[async_trait]
impl Dialer for PythonDialer {
    async fn dial(&self, endpoint: &Endpoint) -> std::io::Result<BoxedRpcStream> {
        let slot = DIAL_SLOTS
            .acquire()
            .await
            .map_err(|_| Error::other("Python dial slots are closed"))?;
        let target = Arc::clone(&self.target);
        let host = endpoint.host().to_owned();
        let port = endpoint.port();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("nfs-rs-python-dial".to_owned())
            .spawn(move || {
                let _slot = slot;
                // A socket that arrives after the client gave up is closed
                // when the unsent result is dropped.
                let _ = sender.send(dial_socket(&target, &host, port));
            })?;
        let socket = receiver
            .await
            .map_err(|_| Error::other("Python dialer ended without a result"))??;
        socket.set_nonblocking(true)?;
        let stream = tokio::net::TcpStream::from_std(socket.into())?;
        Ok(Box::new(stream))
    }
}

fn dial_socket(target: &Py<PyAny>, host: &str, port: u16) -> std::io::Result<socket2::Socket> {
    Python::try_attach(|py| {
        let socket = target
            .bind(py)
            .call_method1("dial", (host, port))
            .map_err(|error| io_error(py, error))?;
        owned_socket(py, &socket).map_err(|error| io_error(py, error))
    })
    .unwrap_or_else(|| {
        Err(Error::new(
            ErrorKind::NotConnected,
            "the Python interpreter is not available to dial",
        ))
    })
}

/// Takes the descriptor of a connected `socket.socket` of type `SOCK_STREAM`.
/// The Python object is left detached (closed); the descriptor is owned here.
///
/// Only the exact class is taken, never a subclass: a subclass can keep its
/// descriptor while returning it, and a wrapper such as `ssl.SSLSocket` holds
/// transport state the raw descriptor does not carry. The descriptor is taken
/// with the native `_socket.socket.detach`, never a method the object provides.
fn owned_socket(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<socket2::Socket> {
    let module = py.import("socket")?;
    if !value.get_type().is(&module.getattr("socket")?) {
        return Err(PyTypeError::new_err(
            "dialer.dial() must return a plain socket.socket, not a subclass or wrapper",
        ));
    }
    if !value.getattr("type")?.eq(module.getattr("SOCK_STREAM")?)? {
        return Err(PyValueError::new_err(
            "dialer.dial() must return a SOCK_STREAM socket",
        ));
    }
    let detach = py.import("_socket")?.getattr("socket")?.getattr("detach")?;
    let descriptor: i64 = detach.call1((value,))?.extract()?;
    if descriptor < 0 {
        return Err(PyValueError::new_err(
            "dialer.dial() returned a closed socket",
        ));
    }
    Ok(from_descriptor(descriptor)?)
}

#[cfg(unix)]
fn from_descriptor(descriptor: i64) -> std::io::Result<socket2::Socket> {
    use std::os::fd::{FromRawFd, OwnedFd, RawFd};
    let raw = RawFd::try_from(descriptor)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "socket descriptor out of range"))?;
    // SAFETY: the native `_socket.socket.detach` of an exact `socket.socket`
    // hands over an open descriptor that nothing else owns any more.
    Ok(socket2::Socket::from(unsafe { OwnedFd::from_raw_fd(raw) }))
}

#[cfg(windows)]
fn from_descriptor(descriptor: i64) -> std::io::Result<socket2::Socket> {
    use std::os::windows::io::{FromRawSocket, OwnedSocket, RawSocket};
    let raw = RawSocket::try_from(descriptor)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "socket handle out of range"))?;
    // SAFETY: the native `_socket.socket.detach` of an exact `socket.socket`
    // hands over an open socket that nothing else owns any more.
    Ok(socket2::Socket::from(unsafe {
        OwnedSocket::from_raw_socket(raw)
    }))
}

/// Keeps the kind of a Python exception (its errno when set, else its class)
/// and the exception itself as the source.
fn io_error(py: Python<'_>, error: PyErr) -> Error {
    let errno = error
        .is_instance_of::<PyOSError>(py)
        .then(|| error.value(py).getattr("errno").ok())
        .flatten()
        .and_then(|errno| errno.extract::<i32>().ok());
    let kind = match errno {
        Some(errno) => Error::from_raw_os_error(errno).kind(),
        None => class_kind(py, &error),
    };
    Error::new(kind, error)
}

fn class_kind(py: Python<'_>, error: &PyErr) -> ErrorKind {
    if error.is_instance_of::<PyTimeoutError>(py) {
        ErrorKind::TimedOut
    } else if error.is_instance_of::<PyConnectionRefusedError>(py) {
        ErrorKind::ConnectionRefused
    } else if error.is_instance_of::<PyConnectionResetError>(py) {
        ErrorKind::ConnectionReset
    } else if error.is_instance_of::<PyConnectionAbortedError>(py) {
        ErrorKind::ConnectionAborted
    } else if error.is_instance_of::<PyPermissionError>(py) {
        ErrorKind::PermissionDenied
    } else if error.is_instance_of::<PyFileNotFoundError>(py) {
        ErrorKind::NotFound
    } else if error.is_instance_of::<PyTypeError>(py) || error.is_instance_of::<PyValueError>(py) {
        ErrorKind::InvalidInput
    } else {
        ErrorKind::Other
    }
}
