use crate::{NfsError, Result};
use bytes::{Bytes, BytesMut};
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyOverflowError, PyTypeError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::{PyBytes, PyMemoryView, PyString};
use std::ffi::{c_int, c_void};
use std::io;

const NAME_BLOCK_BYTES: usize = 64 * 1024;
const MAX_LISTDIR_BYTES: usize = 64 * 1024 * 1024;

/// Materialized listings own compact name blocks independently of RPC pages.
#[derive(Default)]
pub(super) struct PackedNames {
    names: Vec<Bytes>,
    block: BytesMut,
    ends: Vec<usize>,
    used: usize,
}

impl PackedNames {
    pub(super) fn push(&mut self, name: &[u8]) -> Result<()> {
        self.used += name.len() + size_of::<Bytes>();
        if self.used > MAX_LISTDIR_BYTES {
            return Err(NfsError::Io(io::Error::from_raw_os_error(
                nix::libc::EOVERFLOW,
            )));
        }
        if self.block.len() + name.len() > self.block.capacity() {
            self.flush();
            self.block = BytesMut::with_capacity(NAME_BLOCK_BYTES);
        }
        self.block.extend_from_slice(name);
        self.ends.push(self.block.len());
        Ok(())
    }

    fn flush(&mut self) {
        let block = std::mem::take(&mut self.block).freeze();
        let mut start = 0;
        for end in self.ends.drain(..) {
            self.names.push(block.slice(start..end));
            start = end;
        }
    }

    pub(super) fn finish(mut self) -> Vec<Bytes> {
        self.flush();
        self.names
    }
}

#[pyclass(frozen, module = "nfs_rs._internal")]
struct ByteView {
    bytes: Bytes,
}

#[pymethods]
impl ByteView {
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = &slf.get().bytes;
        let len = isize::try_from(bytes.len())
            .map_err(|_| PyOverflowError::new_err("byte view length overflow"))?;
        // SAFETY: CPython supplied view and flags. FillInfo takes a reference
        // to the frozen exporter, which owns the immutable Bytes until release.
        let status = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                bytes.as_ptr().cast_mut().cast::<c_void>(),
                len,
                1,
                flags,
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(PyErr::fetch(slf.py()))
        }
    }
}

pub(super) fn byte_view(py: Python<'_>, bytes: Bytes) -> PyResult<Bound<'_, PyMemoryView>> {
    let owner = Bound::new(py, ByteView { bytes })?;
    PyMemoryView::from(owner.as_any())
}

pub(super) fn path_bytes(path: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(bytes) = path.cast::<PyBytes>() {
        return Ok(Bytes::from_owner(bytes.extract::<PyBackedBytes>()?));
    }
    if let Ok(text) = path.cast::<PyString>() {
        return Ok(Bytes::copy_from_slice(text.to_str()?.as_bytes()));
    }
    if let Ok(view) = path.cast::<PyMemoryView>() {
        let owner = view.getattr("obj")?;
        let owner_bytes = if let Ok(name) = owner.cast::<ByteView>() {
            name.get().bytes.clone()
        } else if let Ok(bytes) = owner.cast::<PyBytes>() {
            Bytes::from_owner(bytes.extract::<PyBackedBytes>()?)
        } else {
            return Err(PyTypeError::new_err("path view must own immutable bytes"));
        };
        let buffer = PyBuffer::<u8>::get(view.as_any())?;
        if buffer.is_c_contiguous() && buffer.readonly() {
            let offset = buffer
                .buf_ptr()
                .addr()
                .checked_sub(owner_bytes.as_ptr().addr());
            if let Some(start) = offset
                && let Some(end) = start.checked_add(buffer.len_bytes())
                && end <= owner_bytes.len()
            {
                return Ok(owner_bytes.slice(start..end));
            }
        }
    }
    Err(PyTypeError::new_err(
        "expected bytes or a contiguous read-only byte view",
    ))
}
