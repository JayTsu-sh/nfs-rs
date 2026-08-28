use crate::client_core::{ClientCore, ClientDriver, CoreOperation, OperationGuard, ResourceKey};
use crate::{
    Attr, Mount, MountHealth, NFSVersion, NfsError, OpenFile, Result, parse_url_and_mount,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use pyo3::exceptions::{
    PyFileExistsError, PyFileNotFoundError, PyIsADirectoryError, PyNotADirectoryError,
    PyNotImplementedError, PyOSError, PyPermissionError, PyRuntimeError, PyStopAsyncIteration,
    PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule, PyType};
use std::collections::HashMap;
#[cfg(feature = "python-test-support")]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

type DirectoryItem = std::result::Result<Py<PyDict>, NfsError>;

#[cfg(feature = "python-test-support")]
#[derive(Debug, Default)]
struct OpenTestBarrier {
    entered: Notify,
    release: Notify,
    registered: Notify,
}

#[cfg(feature = "python-test-support")]
static OPEN_TEST_BARRIER: OnceLock<Mutex<Option<Arc<OpenTestBarrier>>>> = OnceLock::new();

#[cfg(feature = "python-test-support")]
fn open_test_barrier() -> &'static Mutex<Option<Arc<OpenTestBarrier>>> {
    OPEN_TEST_BARRIER.get_or_init(|| Mutex::new(None))
}

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
    #[cfg(feature = "python-test-support")]
    test_files: Mutex<HashMap<String, Arc<tokio::sync::Mutex<Vec<u8>>>>>,
    #[cfg(feature = "python-test-support")]
    test_xattrs: Mutex<HashMap<(String, String), Vec<u8>>>,
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

    #[cfg(feature = "python-test-support")]
    fn test_file(&self, path: &str) -> Result<Arc<tokio::sync::Mutex<Vec<u8>>>> {
        let mut files = self
            .test_files
            .lock()
            .map_err(|_| NfsError::Rpc("test file registry lock poisoned".to_string()))?;
        Ok(files
            .entry(path.to_string())
            .or_insert_with(|| {
                Arc::new(tokio::sync::Mutex::new(
                    b"abcdefghijklmnopqrstuvwxyz".to_vec(),
                ))
            })
            .clone())
    }
}

#[derive(Debug)]
enum FileBackend {
    Mount {
        mount: Arc<dyn Mount>,
        file_handle: Bytes,
        max_read: u32,
        max_write: u32,
    },
    #[cfg(feature = "python-test-support")]
    Test {
        data: Arc<tokio::sync::Mutex<Vec<u8>>>,
        max_read: u32,
        max_write: u32,
        write_fault: Option<TestWriteFault>,
    },
}

#[cfg(feature = "python-test-support")]
#[derive(Clone, Copy, Debug)]
enum TestWriteFault {
    DefiniteAt(u64),
    ZeroAt(u64),
}

#[derive(Clone, Copy, Debug)]
struct FileMode {
    readable: bool,
    writable: bool,
    append: bool,
    create: bool,
    truncate: bool,
}

impl FileMode {
    fn parse(mode: &str) -> Result<Self> {
        let base = mode.as_bytes().first().copied();
        let update = mode.contains('+');
        let valid = matches!(mode, "rb" | "wb" | "ab" | "r+b" | "w+b" | "a+b");
        if !valid {
            return Err(NfsError::InvalidInput(
                "unsupported binary file mode".to_string(),
            ));
        }
        Ok(Self {
            readable: base == Some(b'r') || update,
            writable: base != Some(b'r') || update,
            append: base == Some(b'a'),
            create: matches!(base, Some(b'w' | b'a')),
            truncate: base == Some(b'w'),
        })
    }

    fn access(self) -> u32 {
        match (self.readable, self.writable) {
            (true, true) => crate::OPEN_BOTH,
            (false, true) => crate::OPEN_WRITE,
            _ => crate::OPEN_READ,
        }
    }
}

#[derive(Debug)]
struct FileResource {
    backend: FileBackend,
    mode: FileMode,
    operation_gate: Arc<RwLock<()>>,
    lifecycle: AtomicU64,
    relative_gate: tokio::sync::Mutex<()>,
    position: AtomicU64,
    position_uncertain: AtomicBool,
    dirty_state: tokio::sync::Mutex<DirtyState>,
    close_state: Mutex<FileCloseState>,
    close_started: Notify,
    close_notify: Notify,
    #[cfg(feature = "python-test-support")]
    test_fail_commit: bool,
    #[cfg(feature = "python-test-support")]
    test_commit_calls: AtomicU64,
    #[cfg(feature = "python-test-support")]
    test_verifier_change: bool,
}

#[derive(Debug, Default)]
struct DirtyState {
    ranges: Vec<(u64, u64)>,
    verifier: Option<[u8; 8]>,
}

#[derive(Debug, Default)]
struct FileCloseState {
    started: bool,
    file: Option<OpenFile>,
    result: Option<std::result::Result<(), Arc<NfsError>>>,
}

#[derive(Debug)]
struct SharedNfsFailure(Arc<NfsError>);

impl std::fmt::Display for SharedNfsFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for SharedNfsFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn shared_nfs_error(error: Arc<NfsError>) -> NfsError {
    NfsError::Io(std::io::Error::new(error.kind(), SharedNfsFailure(error)))
}

fn with_confirmed_bytes(mut error: NfsError, confirmed: u64, protocol: NFSVersion) -> NfsError {
    if let NfsError::OperationOutcome(outcome) = &mut error {
        outcome.completed_bytes = Some(confirmed);
        return error;
    }
    if confirmed == 0 {
        return error;
    }
    let definite = matches!(
        error,
        NfsError::Nfs3(_)
            | NfsError::Nfs4(_)
            | NfsError::LockDenied { .. }
            | NfsError::Mount(_)
            | NfsError::Unsupported(_)
            | NfsError::InvalidInput(_)
            | NfsError::RdattrError(_)
    );
    let outcome = if definite {
        crate::OperationOutcome::DefiniteFailure
    } else {
        crate::OperationOutcome::Uncertain
    };
    let recovery = if definite {
        crate::RecoveryAction::DoNotRetry
    } else {
        crate::RecoveryAction::VerifyThenResume
    };
    NfsError::OperationOutcome(Box::new(
        crate::OperationOutcomeError::new(
            outcome,
            crate::OperationClass::ReplaySensitive,
            recovery,
            crate::RequestContext {
                operation: "write".to_string(),
                protocol,
                request_id: None,
            },
            error,
        )
        .with_completed_bytes(confirmed),
    ))
}

fn write_uncertain_error(message: &str, protocol: NFSVersion, confirmed: u64) -> NfsError {
    with_confirmed_bytes(
        NfsError::OperationOutcome(Box::new(crate::OperationOutcomeError::new(
            crate::OperationOutcome::Uncertain,
            crate::OperationClass::ReplaySensitive,
            crate::RecoveryAction::VerifyThenResume,
            crate::RequestContext {
                operation: "write".to_string(),
                protocol,
                request_id: None,
            },
            NfsError::Rpc(message.to_string()),
        ))),
        confirmed,
        protocol,
    )
}

fn commit_uncertain_error(message: &str, protocol: NFSVersion) -> NfsError {
    NfsError::OperationOutcome(Box::new(crate::OperationOutcomeError::new(
        crate::OperationOutcome::Uncertain,
        crate::OperationClass::ReplaySensitive,
        crate::RecoveryAction::VerifyThenResume,
        crate::RequestContext {
            operation: "commit".to_string(),
            protocol,
            request_id: None,
        },
        NfsError::Rpc(message.to_string()),
    )))
}

impl FileResource {
    fn mount(mount: Arc<dyn Mount>, file: OpenFile, mode: FileMode, position: u64) -> Arc<Self> {
        let max_read = mount.get_max_read_size().max(1);
        let max_write = mount.get_max_write_size().max(1);
        let file_handle = file.file_handle();
        Arc::new(Self {
            backend: FileBackend::Mount {
                mount,
                file_handle,
                max_read,
                max_write,
            },
            mode,
            operation_gate: Arc::new(RwLock::new(())),
            lifecycle: AtomicU64::new(0),
            relative_gate: tokio::sync::Mutex::new(()),
            position: AtomicU64::new(position),
            position_uncertain: AtomicBool::new(false),
            dirty_state: tokio::sync::Mutex::new(DirtyState::default()),
            close_state: Mutex::new(FileCloseState {
                started: false,
                file: Some(file),
                result: None,
            }),
            close_started: Notify::new(),
            close_notify: Notify::new(),
            #[cfg(feature = "python-test-support")]
            test_fail_commit: false,
            #[cfg(feature = "python-test-support")]
            test_commit_calls: AtomicU64::new(0),
            #[cfg(feature = "python-test-support")]
            test_verifier_change: false,
        })
    }

    #[cfg(feature = "python-test-support")]
    async fn test(
        mode: FileMode,
        data: Arc<tokio::sync::Mutex<Vec<u8>>>,
        fail_commit: bool,
        write_fault: Option<TestWriteFault>,
        verifier_change: bool,
    ) -> Arc<Self> {
        if mode.truncate {
            data.lock().await.clear();
        }
        let position = if mode.append {
            data.lock().await.len() as u64
        } else {
            0
        };
        Arc::new(Self {
            operation_gate: Arc::new(RwLock::new(())),
            lifecycle: AtomicU64::new(0),
            backend: FileBackend::Test {
                data,
                max_read: 4,
                max_write: 4,
                write_fault,
            },
            mode,
            relative_gate: tokio::sync::Mutex::new(()),
            position: AtomicU64::new(position),
            position_uncertain: AtomicBool::new(false),
            dirty_state: tokio::sync::Mutex::new(DirtyState::default()),
            close_state: Mutex::new(FileCloseState::default()),
            close_started: Notify::new(),
            close_notify: Notify::new(),
            test_fail_commit: fail_commit,
            test_commit_calls: AtomicU64::new(0),
            test_verifier_change: verifier_change,
        })
    }

    fn closed(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) != 0
    }

    async fn begin_operation(&self) -> Result<OwnedRwLockReadGuard<()>> {
        if self.lifecycle.load(Ordering::Acquire) != 0 {
            return Err(NfsError::InvalidInput(
                "I/O operation on closed file".to_string(),
            ));
        }
        let guard = self.operation_gate.clone().read_owned().await;
        if self.lifecycle.load(Ordering::Acquire) == 0 {
            Ok(guard)
        } else {
            Err(NfsError::InvalidInput(
                "I/O operation on closed file".to_string(),
            ))
        }
    }

    async fn close(self: &Arc<Self>) -> std::result::Result<(), Arc<NfsError>> {
        self.lifecycle.store(1, Ordering::Release);
        self.close_started.notify_one();
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
            .map_err(|_| Arc::new(NfsError::Rpc("file close state lock poisoned".to_string())))?;
        if let Some(file) = file {
            let resource = self.clone();
            tokio::spawn(async move {
                let _operation_guard = resource.operation_gate.clone().write_owned().await;
                let mut result = resource.flush_inner().await.map_err(Arc::new);
                let close_result = match (&resource.backend, file) {
                    (FileBackend::Mount { mount, .. }, Some(file)) => {
                        mount.close_stateful(file).await.map_err(Arc::new)
                    }
                    _ => Ok(()),
                };
                if result.is_ok() {
                    result = close_result;
                }
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
            FileBackend::Mount {
                mount, file_handle, ..
            } => mount.read(file_handle.clone(), offset, count).await,
            #[cfg(feature = "python-test-support")]
            FileBackend::Test { data, .. } => {
                let data = data.lock().await;
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
            FileBackend::Mount { max_read, .. } => *max_read,
            #[cfg(feature = "python-test-support")]
            FileBackend::Test { max_read, .. } => *max_read,
        }
    }

    async fn read_at(&self, offset: u64, size: i64) -> Result<Vec<u8>> {
        if !self.mode.readable {
            return Err(NfsError::InvalidInput("file is not readable".to_string()));
        }
        if size < -1 {
            return Err(NfsError::InvalidInput(
                "read size must be -1 or non-negative".to_string(),
            ));
        }
        let requested = if size == -1 { u64::MAX } else { size as u64 };
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
        if self.position_uncertain.load(Ordering::Acquire) {
            return Err(NfsError::InvalidInput(
                "file position is uncertain; seek to an absolute offset".to_string(),
            ));
        }
        let position = self.position.load(Ordering::Acquire);
        let data = self.read_at(position, size).await?;
        self.position.store(
            position.saturating_add(data.len() as u64),
            Ordering::Release,
        );
        Ok(data)
    }

    fn max_write(&self) -> u32 {
        match &self.backend {
            FileBackend::Mount { max_write, .. } => *max_write,
            #[cfg(feature = "python-test-support")]
            FileBackend::Test { max_write, .. } => *max_write,
        }
    }

    fn protocol_version(&self) -> NFSVersion {
        match &self.backend {
            FileBackend::Mount { mount, .. } => mount.version(),
            #[cfg(feature = "python-test-support")]
            FileBackend::Test { .. } => NFSVersion::NFSv4p1,
        }
    }

    async fn write_chunk(&self, offset: u64, data: Bytes) -> Result<crate::WriteOutcome> {
        match &self.backend {
            FileBackend::Mount {
                mount, file_handle, ..
            } => {
                mount
                    .write_with_outcome(file_handle.clone(), offset, data)
                    .await
            }
            #[cfg(feature = "python-test-support")]
            FileBackend::Test {
                data: target,
                write_fault,
                ..
            } => {
                if matches!(write_fault, Some(TestWriteFault::DefiniteAt(at)) if offset >= *at) {
                    return Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOSPC));
                }
                if matches!(write_fault, Some(TestWriteFault::ZeroAt(at)) if offset >= *at) {
                    return Ok(crate::WriteOutcome {
                        count: 0,
                        stable: false,
                        verifier: None,
                    });
                }
                let written = data.len().min(2);
                let start = usize::try_from(offset).map_err(|_| {
                    NfsError::InvalidInput("write offset exceeds platform size".to_string())
                })?;
                let end = start.checked_add(written).ok_or_else(|| {
                    NfsError::InvalidInput("write range exceeds platform size".to_string())
                })?;
                let mut target = target.lock().await;
                let current_len = target.len();
                if current_len < start {
                    target.try_reserve(start - current_len).map_err(|_| {
                        NfsError::InvalidInput("write range is too large".to_string())
                    })?;
                    target.resize(start, 0);
                }
                let current_len = target.len();
                if current_len < end {
                    target.try_reserve(end - current_len).map_err(|_| {
                        NfsError::InvalidInput("write range is too large".to_string())
                    })?;
                    target.resize(end, 0);
                }
                target[start..end].copy_from_slice(&data[..written]);
                Ok(crate::WriteOutcome {
                    count: written as u32,
                    stable: false,
                    verifier: self.test_verifier_change.then_some([1; 8]),
                })
            }
        }
    }

    fn merge_dirty_range(ranges: &mut Vec<(u64, u64)>, start: u64, end: u64) {
        ranges.push((start, end));
        ranges.sort_unstable_by_key(|range| range.0);
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for range in ranges.drain(..) {
            if let Some(last) = merged.last_mut()
                && range.0 <= last.1
            {
                last.1 = last.1.max(range.1);
            } else {
                merged.push(range);
            }
        }
        *ranges = merged;
    }

    async fn record_write(&self, start: u64, end: u64, outcome: crate::WriteOutcome) -> bool {
        let mut dirty = self.dirty_state.lock().await;
        let verifier_changed = outcome
            .verifier
            .is_some_and(|verifier| dirty.verifier.is_some_and(|expected| expected != verifier));
        if !outcome.stable || verifier_changed {
            Self::merge_dirty_range(&mut dirty.ranges, start, end);
        }
        if !outcome.stable
            && !verifier_changed
            && let Some(verifier) = outcome.verifier
        {
            dirty.verifier = Some(verifier);
        }
        verifier_changed
    }

    async fn write_complete_at(&self, offset: u64, data: Bytes) -> Result<u64> {
        if !self.mode.writable {
            return Err(NfsError::InvalidInput("file is not writable".to_string()));
        }
        let mut current = offset;
        let mut confirmed = 0_u64;
        while confirmed < data.len() as u64 {
            let remaining = &data[confirmed as usize..];
            let count = remaining.len().min(self.max_write() as usize);
            let chunk = Bytes::copy_from_slice(&remaining[..count]);
            let outcome = match self.write_chunk(current, chunk).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Err(with_confirmed_bytes(
                        error,
                        confirmed,
                        self.protocol_version(),
                    ));
                }
            };
            let written = u64::from(outcome.count);
            if written == 0 || written > count as u64 {
                return Err(write_uncertain_error(
                    "server returned an invalid write count",
                    self.protocol_version(),
                    confirmed,
                ));
            }
            if self
                .record_write(current, current.saturating_add(written), outcome)
                .await
            {
                return Err(write_uncertain_error(
                    "write verifier changed before commit",
                    self.protocol_version(),
                    confirmed.saturating_add(written),
                ));
            }
            current = current.saturating_add(written);
            confirmed = confirmed.saturating_add(written);
        }
        Ok(confirmed)
    }

    async fn write_at(&self, offset: u64, data: Bytes) -> Result<u64> {
        if self.mode.append {
            return Err(NfsError::InvalidInput(
                "positional writes are unavailable in append mode".to_string(),
            ));
        }
        self.write_complete_at(offset, data).await
    }

    async fn write(&self, data: Bytes) -> Result<u64> {
        let _guard = self.relative_gate.lock().await;
        if self.position_uncertain.load(Ordering::Acquire) {
            return Err(NfsError::InvalidInput(
                "file position is uncertain; seek to an absolute offset".to_string(),
            ));
        }
        let position = if self.mode.append {
            self.current_size().await?
        } else {
            self.position.load(Ordering::Acquire)
        };
        match self.write_complete_at(position, data).await {
            Ok(written) => {
                self.position
                    .store(position.saturating_add(written), Ordering::Release);
                Ok(written)
            }
            Err(error) => {
                if let Some(outcome) = error.operation_outcome() {
                    if outcome.outcome == crate::OperationOutcome::Uncertain {
                        self.position_uncertain.store(true, Ordering::Release);
                    } else if let Some(confirmed) = outcome.completed_bytes {
                        self.position
                            .store(position.saturating_add(confirmed), Ordering::Release);
                    }
                }
                Err(error)
            }
        }
    }

    async fn truncate(&self, size: Option<u64>) -> Result<u64> {
        if !self.mode.writable {
            return Err(NfsError::InvalidInput("file is not writable".to_string()));
        }
        let size = size.unwrap_or_else(|| self.position.load(Ordering::Acquire));
        match &self.backend {
            FileBackend::Mount {
                mount, file_handle, ..
            } => {
                mount
                    .setattr(
                        file_handle.clone(),
                        None,
                        None,
                        None,
                        None,
                        Some(size),
                        None,
                        None,
                    )
                    .await?;
            }
            #[cfg(feature = "python-test-support")]
            FileBackend::Test { data, .. } => {
                let size = usize::try_from(size).map_err(|_| {
                    NfsError::InvalidInput("truncate size exceeds platform size".to_string())
                })?;
                data.lock().await.resize(size, 0);
            }
        }
        let mut dirty = self.dirty_state.lock().await;
        for range in dirty.ranges.iter_mut() {
            range.1 = range.1.min(size);
        }
        dirty.ranges.retain(|range| range.0 < range.1);
        if dirty.ranges.is_empty() {
            dirty.verifier = None;
        }
        Ok(size)
    }

    async fn flush_inner(&self) -> Result<()> {
        if !self.mode.writable {
            return Ok(());
        }
        let mut dirty = self.dirty_state.lock().await;
        let expected_verifier = dirty.verifier;
        for &(start, end) in dirty.ranges.iter() {
            let mut offset = start;
            while offset < end {
                let count = (end - offset).min(u64::from(u32::MAX)) as u32;
                match &self.backend {
                    FileBackend::Mount {
                        mount, file_handle, ..
                    } => {
                        let verifier = mount
                            .commit_with_verifier(file_handle.clone(), offset, count)
                            .await?;
                        if let (Some(expected), Some(actual)) = (expected_verifier, verifier)
                            && expected != actual
                        {
                            return Err(commit_uncertain_error(
                                "commit verifier changed; dirty data may have been lost",
                                mount.version(),
                            ));
                        }
                    }
                    #[cfg(feature = "python-test-support")]
                    FileBackend::Test { .. } => {
                        self.test_commit_calls.fetch_add(1, Ordering::Relaxed);
                        if self.test_fail_commit {
                            return Err(NfsError::Rpc("scripted commit failure".to_string()));
                        }
                        if self.test_verifier_change
                            && expected_verifier.is_some_and(|expected| expected != [2; 8])
                        {
                            return Err(commit_uncertain_error(
                                "commit verifier changed; dirty data may have been lost",
                                NFSVersion::NFSv3,
                            ));
                        }
                    }
                }
                offset = offset.saturating_add(u64::from(count));
            }
        }
        dirty.ranges.clear();
        dirty.verifier = None;
        Ok(())
    }

    async fn seek(&self, offset: i64, whence: i32) -> Result<u64> {
        let _guard = self.relative_gate.lock().await;
        if whence != 0 && self.position_uncertain.load(Ordering::Acquire) {
            return Err(NfsError::InvalidInput(
                "file position is uncertain; only absolute seek is allowed".to_string(),
            ));
        }
        let position = self.position.load(Ordering::Acquire);
        let base = match whence {
            0 => 0_i128,
            1 => i128::from(position),
            2 => i128::from(self.current_size().await?),
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
        if whence == 0 {
            self.position_uncertain.store(false, Ordering::Release);
        }
        Ok(next)
    }

    async fn current_size(&self) -> Result<u64> {
        match &self.backend {
            FileBackend::Mount {
                mount, file_handle, ..
            } => Ok(mount.getattr(file_handle.clone()).await?.filesize),
            #[cfg(feature = "python-test-support")]
            FileBackend::Test { data, .. } => Ok(data.lock().await.len() as u64),
        }
    }

    fn tell(&self) -> Result<u64> {
        if self.lifecycle.load(Ordering::Acquire) == 0 {
            Ok(self.position.load(Ordering::Acquire))
        } else {
            Err(NfsError::InvalidInput(
                "I/O operation on closed file".to_string(),
            ))
        }
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
            resource.close().await.map_err(shared_nfs_error)?;
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
            resource.close().await.map_err(shared_nfs_error)?;
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
    nfs_error_ref(&error)
}

fn nfs_error_ref(error: &NfsError) -> PyErr {
    let permission_denied = matches!(
        error,
        NfsError::Nfs3(
            crate::nfs3::ErrorCode::NFS3ERR_ACCES | crate::nfs3::ErrorCode::NFS3ERR_PERM
        ) | NfsError::Nfs4(
            crate::nfs4::Nfs4ErrorCode::NFS4ERR_ACCESS | crate::nfs4::Nfs4ErrorCode::NFS4ERR_PERM
        )
    );
    let not_directory = matches!(
        error,
        NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOTDIR)
            | NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_NOTDIR)
    );
    let is_directory = matches!(
        error,
        NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_ISDIR)
            | NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_ISDIR)
    );
    let not_empty = matches!(
        error,
        NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOTEMPTY)
            | NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_NOTEMPTY)
    );
    if error.is_not_found() {
        PyFileNotFoundError::new_err(error.to_string())
    } else if error.is_exist() {
        PyFileExistsError::new_err(error.to_string())
    } else if not_directory {
        PyNotADirectoryError::new_err(error.to_string())
    } else if is_directory {
        PyIsADirectoryError::new_err(error.to_string())
    } else if not_empty {
        PyOSError::new_err((nix::errno::Errno::ENOTEMPTY as i32, error.to_string()))
    } else if matches!(error, NfsError::Unsupported(_)) {
        PyNotImplementedError::new_err(error.to_string())
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

fn capabilities_dict<'py>(
    py: Python<'py>,
    capabilities: crate::MountCapabilities,
) -> PyResult<Bound<'py, PyDict>> {
    let values = PyDict::new(py);
    values.set_item("acl", capabilities.acl)?;
    values.set_item("named_attributes", capabilities.named_attributes)?;
    values.set_item("locks", capabilities.locks)?;
    values.set_item("callbacks", capabilities.callbacks)?;
    values.set_item("delegation_retention", capabilities.delegation_retention)?;
    values.set_item("pnfs", capabilities.pnfs)?;
    values.set_item("session_diagnostics", capabilities.session_diagnostics)?;
    Ok(values)
}

fn io_limits_dict<'py>(
    py: Python<'py>,
    mount: Option<&Arc<dyn Mount>>,
) -> PyResult<Bound<'py, PyDict>> {
    let values = PyDict::new(py);
    let max_read = mount.map_or(4, |mount| mount.get_max_read_size());
    let max_write = mount.map_or(4, |mount| mount.get_max_write_size());
    values.set_item("max_read", max_read)?;
    values.set_item("max_write", max_write)?;
    values.set_item("preferred_read", max_read)?;
    values.set_item("preferred_write", max_write)?;
    values.set_item("read_multiple", 1)?;
    values.set_item("write_multiple", 1)?;
    values.set_item("preferred_directory", max_read)?;
    Ok(values)
}

async fn fs_info_values(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
) -> Result<crate::FSInfo> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        return Ok(crate::FSInfo {
            rtmax: 4,
            rtpref: 4,
            rtmult: 1,
            wtmax: 4,
            wtpref: 4,
            wtmult: 1,
            dtpref: 4,
            maxfilesize: 1 << 40,
            time_delta: crate::Time {
                seconds: 0,
                nseconds: 1,
            },
            properties: 0,
            ..Default::default()
        });
    }
    mount
        .ok_or_else(|| {
            NfsError::Unsupported("fs_info requires a connected protocol engine".to_string())
        })?
        .fsinfo()
        .await
}

async fn fs_stat_values(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
) -> Result<crate::FSStat> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        return Ok(crate::FSStat {
            tbytes: 1000,
            fbytes: 600,
            abytes: 500,
            tfiles: 100,
            ffiles: 60,
            afiles: 50,
            invarsec: 1,
            ..Default::default()
        });
    }
    mount
        .ok_or_else(|| {
            NfsError::Unsupported("fs_stat requires a connected protocol engine".to_string())
        })?
        .fsstat()
        .await
}

fn fs_info_dict<'py>(py: Python<'py>, info: crate::FSInfo) -> PyResult<Bound<'py, PyDict>> {
    let values = PyDict::new(py);
    values.set_item("max_file_size", info.maxfilesize)?;
    values.set_item(
        "time_delta_ns",
        nanoseconds(info.time_delta.seconds, info.time_delta.nseconds),
    )?;
    values.set_item("supports_links", info.properties & 0x01 != 0)?;
    values.set_item("supports_symlinks", info.properties & 0x02 != 0)?;
    values.set_item("homogeneous", info.properties & 0x08 != 0)?;
    values.set_item("can_set_time", info.properties & 0x10 != 0)?;
    Ok(values)
}

fn fs_stat_dict<'py>(py: Python<'py>, info: crate::FSStat) -> PyResult<Bound<'py, PyDict>> {
    let values = PyDict::new(py);
    values.set_item("total_bytes", info.tbytes)?;
    values.set_item("free_bytes", info.fbytes)?;
    values.set_item("available_bytes", info.abytes)?;
    values.set_item("total_files", info.tfiles)?;
    values.set_item("free_files", info.ffiles)?;
    values.set_item("available_files", info.afiles)?;
    values.set_item("invariant_seconds", info.invarsec)?;
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

#[derive(Clone, Copy)]
enum NamespaceOperation {
    Mkdir(u32),
    Remove,
    Rmdir,
    Rename,
    Link,
    Symlink,
}

async fn namespace_operation(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    operation: NamespaceOperation,
    first: String,
    second: Option<String>,
) -> Result<()> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        if first.contains("__notdir__") {
            return Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOTDIR));
        }
        if first.contains("__isdir__") {
            return Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_ISDIR));
        }
        if first.contains("__notempty__") {
            return Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOTEMPTY));
        }
        if first.contains("__before_send__") {
            return Err(NfsError::before_send_failure(
                crate::OperationClass::ReplaySensitive,
                crate::RequestContext {
                    operation: "namespace mutation".to_string(),
                    protocol: NFSVersion::NFSv3,
                    request_id: None,
                },
                None,
                NfsError::Rpc("injected before-send failure".to_string()),
            ));
        }
        if first.contains("__after_send__") {
            return Err(NfsError::OperationOutcome(Box::new(
                crate::OperationOutcomeError::new(
                    crate::OperationOutcome::Uncertain,
                    crate::OperationClass::ReplaySensitive,
                    crate::RecoveryAction::VerifyThenResume,
                    crate::RequestContext {
                        operation: "namespace mutation".to_string(),
                        protocol: NFSVersion::NFSv3,
                        request_id: None,
                    },
                    NfsError::Rpc("injected after-send failure".to_string()),
                ),
            )));
        }
        return Ok(());
    }
    let mount = mount.ok_or_else(|| {
        NfsError::Unsupported("namespace mutation requires a connected protocol engine".to_string())
    })?;
    match operation {
        NamespaceOperation::Mkdir(mode) => mount.mkdir_path(&first, mode).await.map(|_| ()),
        NamespaceOperation::Remove => mount.remove_path(&first).await,
        NamespaceOperation::Rmdir => mount.rmdir_path(&first).await,
        NamespaceOperation::Rename => {
            let destination = second.as_deref().ok_or_else(|| {
                NfsError::InvalidInput("rename requires a destination".to_string())
            })?;
            mount.rename_path(&first, destination).await
        }
        NamespaceOperation::Link => mount
            .link_path(
                &first,
                second.as_deref().ok_or_else(|| {
                    NfsError::InvalidInput("link requires a destination".to_string())
                })?,
            )
            .await
            .map(|_| ()),
        NamespaceOperation::Symlink => mount
            .symlink_path(
                &first,
                second.as_deref().ok_or_else(|| {
                    NfsError::InvalidInput("symlink requires a link path".to_string())
                })?,
            )
            .await
            .map(|_| ()),
    }
}

async fn readlink_path(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    path: String,
) -> Result<String> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        return Ok("../target".to_string());
    }
    mount
        .ok_or_else(|| {
            NfsError::Unsupported("readlink requires a connected protocol engine".to_string())
        })?
        .readlink_path(&path)
        .await
}

fn time_from_ns(value: u64) -> Result<crate::Time> {
    let seconds = value / 1_000_000_000;
    Ok(crate::Time {
        seconds: u32::try_from(seconds)
            .map_err(|_| NfsError::InvalidInput("timestamp is out of range".to_string()))?,
        nseconds: (value % 1_000_000_000) as u32,
    })
}

fn optional_identity(value: i64) -> PyResult<Option<u32>> {
    if value == -1 {
        Ok(None)
    } else {
        u32::try_from(value).map(Some).map_err(|_| {
            PyValueError::new_err("uid and gid must be -1 or unsigned 32-bit integers")
        })
    }
}

#[derive(Clone, Copy)]
enum MetadataOperation {
    Chmod(u32),
    Chown(Option<u32>, Option<u32>),
    Utime(u64, u64),
    Truncate(u64),
}

async fn metadata_operation(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    operation: MetadataOperation,
    path: String,
) -> Result<()> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        if path == "denied" {
            return Err(NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_ACCES));
        }
        return Ok(());
    }
    let mount = mount.ok_or_else(|| {
        NfsError::Unsupported("metadata mutation requires a connected protocol engine".to_string())
    })?;
    match operation {
        MetadataOperation::Chmod(mode) => {
            mount
                .setattr_path(&path, false, Some(mode), None, None, None, None, None)
                .await
        }
        MetadataOperation::Chown(uid, gid) => {
            mount
                .setattr_path(&path, false, None, uid, gid, None, None, None)
                .await
        }
        MetadataOperation::Utime(atime, mtime) => {
            mount
                .setattr_path(
                    &path,
                    false,
                    None,
                    None,
                    None,
                    None,
                    Some(time_from_ns(atime)?),
                    Some(time_from_ns(mtime)?),
                )
                .await
        }
        MetadataOperation::Truncate(size) => {
            mount
                .setattr_path(&path, false, None, None, None, Some(size), None, None)
                .await
        }
    }
}

async fn access_path(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    path: String,
    mode: u32,
) -> Result<bool> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        return Ok(path != "denied" && mode & !0o7 == 0);
    }
    let granted = mount
        .ok_or_else(|| {
            NfsError::Unsupported("access requires a connected protocol engine".to_string())
        })?
        .access_path(&path, mode)
        .await?;
    Ok(granted & mode == mode)
}

async fn xattr_get(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
    path: String,
    name: String,
) -> Result<Vec<u8>> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        if path == "unsupported" {
            return Err(NfsError::Unsupported(
                "named attributes are not supported".to_string(),
            ));
        }
        return resources
            .test_xattrs
            .lock()
            .map_err(|_| NfsError::Rpc("xattr registry lock poisoned".to_string()))?
            .get(&(path, name))
            .cloned()
            .ok_or_else(|| NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT));
    }
    Ok(mount
        .ok_or_else(|| {
            NfsError::Unsupported("xattr requires a connected protocol engine".to_string())
        })?
        .getxattr_path(&path, &name)
        .await?
        .to_vec())
}

async fn xattr_set(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
    path: String,
    name: String,
    value: Vec<u8>,
) -> Result<()> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        if path == "unsupported" {
            return Err(NfsError::Unsupported(
                "named attributes are not supported".to_string(),
            ));
        }
        resources
            .test_xattrs
            .lock()
            .map_err(|_| NfsError::Rpc("xattr registry lock poisoned".to_string()))?
            .insert((path, name), value);
        return Ok(());
    }
    mount
        .ok_or_else(|| {
            NfsError::Unsupported("xattr requires a connected protocol engine".to_string())
        })?
        .setxattr_path(&path, &name, Bytes::from(value))
        .await
}

async fn xattr_list(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
    path: String,
) -> Result<Vec<String>> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        if path == "unsupported" {
            return Err(NfsError::Unsupported(
                "named attributes are not supported".to_string(),
            ));
        }
        let mut names: Vec<_> = resources
            .test_xattrs
            .lock()
            .map_err(|_| NfsError::Rpc("xattr registry lock poisoned".to_string()))?
            .keys()
            .filter(|(candidate, _)| candidate == &path)
            .map(|(_, name)| name.clone())
            .collect();
        names.sort();
        return Ok(names);
    }
    mount
        .ok_or_else(|| {
            NfsError::Unsupported("xattr requires a connected protocol engine".to_string())
        })?
        .listxattr_path(&path)
        .await
}

async fn xattr_remove(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
    path: String,
    name: String,
) -> Result<()> {
    let _operation_guard = core.begin_operation()?;
    #[cfg(feature = "python-test-support")]
    if mount.is_none() {
        if path == "unsupported" {
            return Err(NfsError::Unsupported(
                "named attributes are not supported".to_string(),
            ));
        }
        resources
            .test_xattrs
            .lock()
            .map_err(|_| NfsError::Rpc("xattr registry lock poisoned".to_string()))?
            .remove(&(path, name))
            .ok_or_else(|| NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOENT))?;
        return Ok(());
    }
    mount
        .ok_or_else(|| {
            NfsError::Unsupported("xattr requires a connected protocol engine".to_string())
        })?
        .removexattr_path(&path, &name)
        .await
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
    _operation: OperationGuard,
    mount: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
    path: String,
    mode: String,
) -> Result<(ResourceKey, Arc<FileResource>)> {
    let mode = FileMode::parse(&mode)?;
    let resource = if let Some(mount) = mount {
        let file = match mount.open_path_stateful(&path, mode.access()).await {
            Ok(file) => file,
            Err(error) if mode.create && error.is_not_found() => {
                match mount.create_path_stateful(&path, None).await {
                    Ok(file) => file,
                    Err(error) if error.is_exist() => {
                        mount.open_path_stateful(&path, mode.access()).await?
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        if mode.truncate
            && let Err(error) = mount
                .setattr(
                    file.file_handle(),
                    None,
                    None,
                    None,
                    None,
                    Some(0),
                    None,
                    None,
                )
                .await
        {
            let _ = mount.close_stateful(file).await;
            return Err(error);
        }
        let position = if mode.append {
            match mount.getattr(file.file_handle()).await {
                Ok(attr) => attr.filesize,
                Err(error) => {
                    let _ = mount.close_stateful(file).await;
                    return Err(error);
                }
            }
        } else {
            0
        };
        FileResource::mount(mount, file, mode, position)
    } else {
        #[cfg(feature = "python-test-support")]
        {
            FileResource::test(
                mode,
                resources.test_file(&path)?,
                path == "__commit_error__",
                match path.as_str() {
                    "__partial_write_error__" => Some(TestWriteFault::DefiniteAt(2)),
                    "__zero_write__" => Some(TestWriteFault::ZeroAt(0)),
                    _ => None,
                },
                path == "__verifier_change__",
            )
            .await
        }
        #[cfg(not(feature = "python-test-support"))]
        {
            return Err(NfsError::Unsupported("mount is unavailable".to_string()));
        }
    };
    #[cfg(feature = "python-test-support")]
    let barrier = if path == "__blocked_open__" {
        open_test_barrier()
            .lock()
            .map_err(|_| NfsError::Rpc("open test barrier lock poisoned".to_string()))?
            .clone()
    } else {
        None
    };
    #[cfg(feature = "python-test-support")]
    if let Some(barrier) = &barrier {
        barrier.entered.notify_one();
        barrier.release.notified().await;
    }
    let key = core.allocate_resource_key()?;
    resources.insert(key, resource.clone())?;
    if let Err(error) = core.publish_resource(key) {
        let _ = resources.remove(key);
        let _ = resource.close().await;
        return Err(error);
    }
    #[cfg(feature = "python-test-support")]
    if let Some(barrier) = barrier {
        barrier.registered.notify_one();
    }
    Ok((key, resource))
}

#[cfg(feature = "python-test-support")]
#[pyfunction]
fn _arm_open_test_barrier() -> PyResult<()> {
    *open_test_barrier()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("open test barrier lock poisoned"))? =
        Some(Arc::new(OpenTestBarrier::default()));
    Ok(())
}

#[cfg(feature = "python-test-support")]
fn current_open_test_barrier() -> PyResult<Arc<OpenTestBarrier>> {
    open_test_barrier()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("open test barrier lock poisoned"))?
        .clone()
        .ok_or_else(|| PyRuntimeError::new_err("open test barrier is not armed"))
}

#[cfg(feature = "python-test-support")]
#[pyfunction]
fn _wait_open_test_entered(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let barrier = current_open_test_barrier()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        barrier.entered.notified().await;
        Ok(())
    })
}

#[cfg(feature = "python-test-support")]
#[pyfunction]
fn _release_open_test_barrier() -> PyResult<()> {
    current_open_test_barrier()?.release.notify_one();
    Ok(())
}

#[cfg(feature = "python-test-support")]
#[pyfunction]
fn _wait_open_test_registered(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let barrier = current_open_test_barrier()?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        barrier.registered.notified().await;
        Ok(())
    })
}

async fn cancellation_safe_open_file(
    core: Arc<ClientCore>,
    mount: Option<Arc<dyn Mount>>,
    resources: Arc<AdapterResources>,
    path: String,
    mode: String,
) -> Result<(ResourceKey, Arc<FileResource>)> {
    let operation = core.begin_operation()?;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let task_core = core.clone();
    core.spawn_owned(async move {
        let result = open_file(task_core, operation, mount, resources, path, mode).await;
        let _ = sender.send(result);
    })?;
    receiver
        .await
        .map_err(|_| NfsError::Rpc("open task ended without a result".to_string()))?
}

async fn close_file(
    core: Arc<ClientCore>,
    resources: Arc<AdapterResources>,
    key: ResourceKey,
    resource: Arc<FileResource>,
) -> std::result::Result<(), Arc<NfsError>> {
    let result = resource.close().await;
    core.unregister_resource(key).map_err(Arc::new)?;
    let _ = resources.remove(key).map_err(Arc::new)?;
    result
}

fn validate_file_mode(mode: &str) -> PyResult<()> {
    FileMode::parse(mode).map(|_| ()).map_err(nfs_error)
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

    #[getter]
    fn max_read_size(&self) -> u32 {
        self.resource.max_read()
    }

    #[pyo3(signature = (size = -1))]
    fn read(&self, py: Python<'_>, size: i64) -> PyResult<Py<PyBytes>> {
        let _core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let data = py
            .detach(|| {
                self.runtime.block_on(async {
                    let _file_operation = self.resource.begin_operation().await?;
                    self.resource.read(size).await
                })
            })
            .map_err(nfs_error)?;
        Ok(PyBytes::new(py, &data).unbind())
    }

    #[pyo3(signature = (offset, size = -1))]
    fn read_at(&self, py: Python<'_>, offset: u64, size: i64) -> PyResult<Py<PyBytes>> {
        let _core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let data = py
            .detach(|| {
                self.runtime.block_on(async {
                    let _file_operation = self.resource.begin_operation().await?;
                    self.resource.read_at(offset, size).await
                })
            })
            .map_err(nfs_error)?;
        Ok(PyBytes::new(py, &data).unbind())
    }

    #[pyo3(signature = (offset, whence = 0))]
    fn seek(&self, py: Python<'_>, offset: i64, whence: i32) -> PyResult<u64> {
        let _core_operation = self.core.begin_operation().map_err(nfs_error)?;
        py.detach(|| {
            self.runtime.block_on(async {
                let _file_operation = self.resource.begin_operation().await?;
                self.resource.seek(offset, whence).await
            })
        })
        .map_err(|error| nfs_error_ref(&error))
    }

    fn tell(&self) -> PyResult<u64> {
        self.resource.tell().map_err(nfs_error)
    }

    fn write(&self, py: Python<'_>, data: Vec<u8>) -> PyResult<u64> {
        let _core_operation = self.core.begin_operation().map_err(nfs_error)?;
        py.detach(|| {
            self.runtime.block_on(async {
                let _file_operation = self.resource.begin_operation().await?;
                self.resource.write(Bytes::from(data)).await
            })
        })
        .map_err(nfs_error)
    }

    fn write_at(&self, py: Python<'_>, data: Vec<u8>, offset: u64) -> PyResult<u64> {
        let _core_operation = self.core.begin_operation().map_err(nfs_error)?;
        py.detach(|| {
            self.runtime.block_on(async {
                let _file_operation = self.resource.begin_operation().await?;
                self.resource.write_at(offset, Bytes::from(data)).await
            })
        })
        .map_err(nfs_error)
    }

    #[pyo3(signature = (size = None))]
    fn truncate(&self, py: Python<'_>, size: Option<u64>) -> PyResult<u64> {
        let _core_operation = self.core.begin_operation().map_err(nfs_error)?;
        py.detach(|| {
            self.runtime.block_on(async {
                let _file_operation = self.resource.begin_operation().await?;
                self.resource.truncate(size).await
            })
        })
        .map_err(nfs_error)
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let _core_operation = self.core.begin_operation().map_err(nfs_error)?;
        py.detach(|| {
            self.runtime.block_on(async {
                let _file_operation = self.resource.begin_operation().await?;
                self.resource.flush_inner().await
            })
        })
        .map_err(nfs_error)
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
        .map_err(|error| nfs_error_ref(&error))
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

    #[getter]
    fn max_read_size(&self) -> u32 {
        self.resource.max_read()
    }

    #[pyo3(signature = (size = -1))]
    fn read<'py>(&self, py: Python<'py>, size: i64) -> PyResult<Bound<'py, PyAny>> {
        let core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _core_operation = core_operation;
            let _file_operation = resource.begin_operation().await.map_err(nfs_error)?;
            let data = resource.read(size).await.map_err(nfs_error)?;
            Python::attach(|py| Ok(PyBytes::new(py, &data).unbind()))
        })
    }

    #[pyo3(signature = (offset, size = -1))]
    fn read_at<'py>(&self, py: Python<'py>, offset: u64, size: i64) -> PyResult<Bound<'py, PyAny>> {
        let core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _core_operation = core_operation;
            let _file_operation = resource.begin_operation().await.map_err(nfs_error)?;
            let data = resource.read_at(offset, size).await.map_err(nfs_error)?;
            Python::attach(|py| Ok(PyBytes::new(py, &data).unbind()))
        })
    }

    #[pyo3(signature = (offset, whence = 0))]
    fn seek<'py>(&self, py: Python<'py>, offset: i64, whence: i32) -> PyResult<Bound<'py, PyAny>> {
        let core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _core_operation = core_operation;
            let _file_operation = resource.begin_operation().await.map_err(nfs_error)?;
            resource.seek(offset, whence).await.map_err(nfs_error)
        })
    }

    fn tell(&self) -> PyResult<u64> {
        self.resource.tell().map_err(nfs_error)
    }

    fn write<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _core_operation = core_operation;
            let _file_operation = resource.begin_operation().await.map_err(nfs_error)?;
            resource.write(Bytes::from(data)).await.map_err(nfs_error)
        })
    }

    fn write_at<'py>(
        &self,
        py: Python<'py>,
        data: Vec<u8>,
        offset: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _core_operation = core_operation;
            let _file_operation = resource.begin_operation().await.map_err(nfs_error)?;
            resource
                .write_at(offset, Bytes::from(data))
                .await
                .map_err(nfs_error)
        })
    }

    #[pyo3(signature = (size = None))]
    fn truncate<'py>(&self, py: Python<'py>, size: Option<u64>) -> PyResult<Bound<'py, PyAny>> {
        let core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _core_operation = core_operation;
            let _file_operation = resource.begin_operation().await.map_err(nfs_error)?;
            resource.truncate(size).await.map_err(nfs_error)
        })
    }

    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core_operation = self.core.begin_operation().map_err(nfs_error)?;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _core_operation = core_operation;
            let _file_operation = resource.begin_operation().await.map_err(nfs_error)?;
            resource.flush_inner().await.map_err(nfs_error)
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let resources = self.resources.clone();
        let key = self.key;
        let resource = self.resource.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            close_file(core, resources, key, resource)
                .await
                .map_err(|error| nfs_error_ref(&error))
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
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        capabilities_dict(
            py,
            self.health_source
                .as_ref()
                .map_or_else(crate::MountCapabilities::default, |mount| {
                    mount.capabilities()
                }),
        )
    }

    #[getter]
    fn io_limits<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        io_limits_dict(py, self.health_source.as_ref())
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

    #[pyo3(signature = (path, mode = 0o777))]
    fn mkdir(&self, py: Python<'_>, path: String, mode: u32) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(namespace_operation(
                self.core.clone(),
                self.health_source.clone(),
                NamespaceOperation::Mkdir(mode),
                path,
                None,
            ))
        })
        .map_err(nfs_error)
    }

    fn remove(&self, py: Python<'_>, path: String) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(namespace_operation(
                self.core.clone(),
                self.health_source.clone(),
                NamespaceOperation::Remove,
                path,
                None,
            ))
        })
        .map_err(nfs_error)
    }

    fn rmdir(&self, py: Python<'_>, path: String) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(namespace_operation(
                self.core.clone(),
                self.health_source.clone(),
                NamespaceOperation::Rmdir,
                path,
                None,
            ))
        })
        .map_err(nfs_error)
    }

    fn rename(&self, py: Python<'_>, source: String, destination: String) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(namespace_operation(
                self.core.clone(),
                self.health_source.clone(),
                NamespaceOperation::Rename,
                source,
                Some(destination),
            ))
        })
        .map_err(nfs_error)
    }

    fn link(&self, py: Python<'_>, source: String, destination: String) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(namespace_operation(
                self.core.clone(),
                self.health_source.clone(),
                NamespaceOperation::Link,
                source,
                Some(destination),
            ))
        })
        .map_err(nfs_error)
    }

    fn symlink(&self, py: Python<'_>, target: String, link_path: String) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(namespace_operation(
                self.core.clone(),
                self.health_source.clone(),
                NamespaceOperation::Symlink,
                target,
                Some(link_path),
            ))
        })
        .map_err(nfs_error)
    }

    fn readlink(&self, py: Python<'_>, path: String) -> PyResult<String> {
        py.detach(|| {
            self.runtime.block_on(readlink_path(
                self.core.clone(),
                self.health_source.clone(),
                path,
            ))
        })
        .map_err(nfs_error)
    }

    fn chmod(&self, py: Python<'_>, path: String, mode: u32) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(metadata_operation(
                self.core.clone(),
                self.health_source.clone(),
                MetadataOperation::Chmod(mode),
                path,
            ))
        })
        .map_err(nfs_error)
    }

    fn chown(&self, py: Python<'_>, path: String, uid: i64, gid: i64) -> PyResult<()> {
        let uid = optional_identity(uid)?;
        let gid = optional_identity(gid)?;
        py.detach(|| {
            self.runtime.block_on(metadata_operation(
                self.core.clone(),
                self.health_source.clone(),
                MetadataOperation::Chown(uid, gid),
                path,
            ))
        })
        .map_err(nfs_error)
    }

    fn utime(&self, py: Python<'_>, path: String, atime_ns: u64, mtime_ns: u64) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(metadata_operation(
                self.core.clone(),
                self.health_source.clone(),
                MetadataOperation::Utime(atime_ns, mtime_ns),
                path,
            ))
        })
        .map_err(nfs_error)
    }

    fn truncate_path(&self, py: Python<'_>, path: String, size: u64) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(metadata_operation(
                self.core.clone(),
                self.health_source.clone(),
                MetadataOperation::Truncate(size),
                path,
            ))
        })
        .map_err(nfs_error)
    }

    fn access(&self, py: Python<'_>, path: String, mode: u32) -> PyResult<bool> {
        py.detach(|| {
            self.runtime.block_on(access_path(
                self.core.clone(),
                self.health_source.clone(),
                path,
                mode,
            ))
        })
        .map_err(nfs_error)
    }

    fn getxattr(&self, py: Python<'_>, path: String, name: String) -> PyResult<Vec<u8>> {
        py.detach(|| {
            self.runtime.block_on(xattr_get(
                self.core.clone(),
                self.health_source.clone(),
                self.resources.clone(),
                path,
                name,
            ))
        })
        .map_err(nfs_error)
    }

    fn setxattr(&self, py: Python<'_>, path: String, name: String, value: Vec<u8>) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(xattr_set(
                self.core.clone(),
                self.health_source.clone(),
                self.resources.clone(),
                path,
                name,
                value,
            ))
        })
        .map_err(nfs_error)
    }

    fn listxattr(&self, py: Python<'_>, path: String) -> PyResult<Vec<String>> {
        py.detach(|| {
            self.runtime.block_on(xattr_list(
                self.core.clone(),
                self.health_source.clone(),
                self.resources.clone(),
                path,
            ))
        })
        .map_err(nfs_error)
    }

    fn removexattr(&self, py: Python<'_>, path: String, name: String) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(xattr_remove(
                self.core.clone(),
                self.health_source.clone(),
                self.resources.clone(),
                path,
                name,
            ))
        })
        .map_err(nfs_error)
    }

    fn fs_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let info = py
            .detach(|| {
                self.runtime.block_on(fs_info_values(
                    self.core.clone(),
                    self.health_source.clone(),
                ))
            })
            .map_err(nfs_error)?;
        fs_info_dict(py, info)
    }

    fn fs_stat<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let info = py
            .detach(|| {
                self.runtime.block_on(fs_stat_values(
                    self.core.clone(),
                    self.health_source.clone(),
                ))
            })
            .map_err(nfs_error)?;
        fs_stat_dict(py, info)
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
        validate_file_mode(mode)?;
        let (key, resource) = py
            .detach(|| {
                self.runtime.block_on(cancellation_safe_open_file(
                    self.core.clone(),
                    self.health_source.clone(),
                    self.resources.clone(),
                    path,
                    mode.to_string(),
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

impl AsyncClient {
    fn namespace_future<'py>(
        &self,
        py: Python<'py>,
        operation: NamespaceOperation,
        first: String,
        second: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            namespace_operation(core, mount, operation, first, second)
                .await
                .map_err(nfs_error)
        })
    }

    fn metadata_future<'py>(
        &self,
        py: Python<'py>,
        operation: MetadataOperation,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            metadata_operation(core, mount, operation, path)
                .await
                .map_err(nfs_error)
        })
    }
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

    #[getter]
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        capabilities_dict(
            py,
            self.health_source
                .as_ref()
                .map_or_else(crate::MountCapabilities::default, |mount| {
                    mount.capabilities()
                }),
        )
    }

    #[getter]
    fn io_limits<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        io_limits_dict(py, self.health_source.as_ref())
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

    #[pyo3(signature = (path, mode = 0o777))]
    fn mkdir<'py>(&self, py: Python<'py>, path: String, mode: u32) -> PyResult<Bound<'py, PyAny>> {
        self.namespace_future(py, NamespaceOperation::Mkdir(mode), path, None)
    }

    fn remove<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        self.namespace_future(py, NamespaceOperation::Remove, path, None)
    }

    fn rmdir<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        self.namespace_future(py, NamespaceOperation::Rmdir, path, None)
    }

    fn rename<'py>(
        &self,
        py: Python<'py>,
        source: String,
        destination: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.namespace_future(py, NamespaceOperation::Rename, source, Some(destination))
    }

    fn link<'py>(
        &self,
        py: Python<'py>,
        source: String,
        destination: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.namespace_future(py, NamespaceOperation::Link, source, Some(destination))
    }

    fn symlink<'py>(
        &self,
        py: Python<'py>,
        target: String,
        link_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.namespace_future(py, NamespaceOperation::Symlink, target, Some(link_path))
    }

    fn readlink<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            readlink_path(core, mount, path).await.map_err(nfs_error)
        })
    }

    fn chmod<'py>(&self, py: Python<'py>, path: String, mode: u32) -> PyResult<Bound<'py, PyAny>> {
        self.metadata_future(py, MetadataOperation::Chmod(mode), path)
    }

    fn chown<'py>(
        &self,
        py: Python<'py>,
        path: String,
        uid: i64,
        gid: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.metadata_future(
            py,
            MetadataOperation::Chown(optional_identity(uid)?, optional_identity(gid)?),
            path,
        )
    }

    fn utime<'py>(
        &self,
        py: Python<'py>,
        path: String,
        atime_ns: u64,
        mtime_ns: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.metadata_future(py, MetadataOperation::Utime(atime_ns, mtime_ns), path)
    }

    fn truncate_path<'py>(
        &self,
        py: Python<'py>,
        path: String,
        size: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.metadata_future(py, MetadataOperation::Truncate(size), path)
    }

    fn access<'py>(&self, py: Python<'py>, path: String, mode: u32) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            access_path(core, mount, path, mode)
                .await
                .map_err(nfs_error)
        })
    }

    fn getxattr<'py>(
        &self,
        py: Python<'py>,
        path: String,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        let resources = self.resources.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            xattr_get(core, mount, resources, path, name)
                .await
                .map_err(nfs_error)
        })
    }

    fn setxattr<'py>(
        &self,
        py: Python<'py>,
        path: String,
        name: String,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        let resources = self.resources.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            xattr_set(core, mount, resources, path, name, value)
                .await
                .map_err(nfs_error)
        })
    }

    fn listxattr<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        let resources = self.resources.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            xattr_list(core, mount, resources, path)
                .await
                .map_err(nfs_error)
        })
    }

    fn removexattr<'py>(
        &self,
        py: Python<'py>,
        path: String,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        let resources = self.resources.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            xattr_remove(core, mount, resources, path, name)
                .await
                .map_err(nfs_error)
        })
    }

    fn fs_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = fs_info_values(core, mount).await.map_err(nfs_error)?;
            Python::attach(|py| fs_info_dict(py, info).map(Bound::unbind))
        })
    }

    fn fs_stat<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let mount = self.health_source.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = fs_stat_values(core, mount).await.map_err(nfs_error)?;
            Python::attach(|py| fs_stat_dict(py, info).map(Bound::unbind))
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
        validate_file_mode(mode)?;
        let core = self.core.clone();
        let mount = self.health_source.clone();
        let resources = self.resources.clone();
        let mode = mode.to_string();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (key, resource) =
                cancellation_safe_open_file(core.clone(), mount, resources.clone(), path, mode)
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
    #[cfg(feature = "python-test-support")]
    {
        module.add_function(wrap_pyfunction!(_arm_open_test_barrier, module)?)?;
        module.add_function(wrap_pyfunction!(_wait_open_test_entered, module)?)?;
        module.add_function(wrap_pyfunction!(_release_open_test_barrier, module)?)?;
        module.add_function(wrap_pyfunction!(_wait_open_test_registered, module)?)?;
    }
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(all(test, feature = "python-test-support"))]
mod read_only_file_tests {
    use super::{FileMode, FileResource, TestWriteFault, with_confirmed_bytes};
    use crate::error::{
        OperationClass, OperationOutcome, OperationOutcomeError, RecoveryAction, RequestContext,
    };
    use crate::{NFSVersion, NfsError};
    use std::sync::Arc;

    fn read_mode() -> FileMode {
        FileMode::parse("rb").unwrap_or_else(|_| panic!("rb mode must be valid"))
    }

    async fn test_file(mode: FileMode) -> Arc<FileResource> {
        FileResource::test(
            mode,
            Arc::new(tokio::sync::Mutex::new(
                b"abcdefghijklmnopqrstuvwxyz".to_vec(),
            )),
            false,
            None,
            false,
        )
        .await
    }

    #[tokio::test]
    async fn negotiated_chunks_are_reassembled() {
        let file = test_file(read_mode()).await;
        assert_eq!(file.max_read(), 4);
        let Ok(data) = file.read_at(2, 11).await else {
            panic!("fixture read should succeed");
        };
        assert_eq!(data, b"cdefghijklm");
    }

    #[tokio::test]
    async fn positional_reads_do_not_change_relative_position() {
        let file = test_file(read_mode()).await;
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
        assert_eq!(file.tell().ok(), Some(3));
    }

    #[tokio::test]
    async fn close_rejects_new_work_and_drains_an_active_operation() {
        let file = test_file(read_mode()).await;
        let active = file.begin_operation().await;
        let Ok(active) = active else {
            panic!("fixture operation should start");
        };
        let close_started = file.close_started.notified();
        let closing_file = file.clone();
        let close = tokio::spawn(async move { closing_file.close().await });
        close_started.await;

        assert!(file.begin_operation().await.is_err());
        assert!(!close.is_finished());
        drop(active);
        let result = close.await;
        assert!(matches!(result, Ok(Ok(()))));
        assert!(file.tell().is_err());
    }

    #[tokio::test]
    async fn complete_write_hides_partial_backend_writes_and_flushes_dirty_ranges() {
        let mode = FileMode::parse("w+b").unwrap_or_else(|_| panic!("w+b mode must be valid"));
        let file = test_file(mode).await;
        let data = bytes::Bytes::from_static(b"abcdefghij");
        assert_eq!(file.write(data).await.ok(), Some(10));
        assert_eq!(file.tell().ok(), Some(10));
        assert_eq!(
            file.read_at(0, -1).await.ok().as_deref(),
            Some(&b"abcdefghij"[..])
        );
        assert!(!file.dirty_state.lock().await.ranges.is_empty());
        assert!(file.flush_inner().await.is_ok());
        assert!(file.dirty_state.lock().await.ranges.is_empty());
    }

    #[tokio::test]
    async fn append_refreshes_eof_and_positional_write_preserves_position() {
        let mode = FileMode::parse("a+b").unwrap_or_else(|_| panic!("a+b mode must be valid"));
        let file = test_file(mode).await;
        assert_eq!(file.tell().ok(), Some(26));
        assert!(
            file.write_at(0, bytes::Bytes::from_static(b"XY"))
                .await
                .is_err()
        );
        assert_eq!(file.tell().ok(), Some(26));
        assert_eq!(
            file.write(bytes::Bytes::from_static(b"!")).await.ok(),
            Some(1)
        );
        assert_eq!(file.tell().ok(), Some(27));
        assert_eq!(
            file.read_at(24, -1).await.ok().as_deref(),
            Some(&b"yz!"[..])
        );
    }

    #[tokio::test]
    async fn failed_flush_preserves_dirty_state_and_close_reuses_terminal_result() {
        let mode = FileMode::parse("w+b").unwrap_or_else(|_| panic!("w+b mode must be valid"));
        let file = FileResource::test(
            mode,
            Arc::new(tokio::sync::Mutex::new(Vec::new())),
            true,
            None,
            false,
        )
        .await;
        assert_eq!(
            file.write(bytes::Bytes::from_static(b"dirty")).await.ok(),
            Some(5)
        );
        let first = file.close().await;
        let second = file.close().await;
        assert!(first.is_err());
        assert!(second.is_err());
        assert_eq!(
            file.test_commit_calls
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert!(!file.dirty_state.lock().await.ranges.is_empty());
    }

    #[test]
    fn chunk_failure_records_only_preceding_confirmed_bytes() {
        let error = NfsError::OperationOutcome(Box::new(OperationOutcomeError::new(
            OperationOutcome::Uncertain,
            OperationClass::ReplaySensitive,
            RecoveryAction::VerifyThenResume,
            RequestContext {
                operation: "write".to_string(),
                protocol: NFSVersion::NFSv4p1,
                request_id: None,
            },
            NfsError::Rpc("current chunk reply lost".to_string()),
        )));
        let error = with_confirmed_bytes(error, 8, NFSVersion::NFSv4p1);
        assert_eq!(
            error
                .operation_outcome()
                .and_then(|outcome| outcome.completed_bytes),
            Some(8)
        );
    }

    #[tokio::test]
    async fn definite_partial_write_advances_position_to_confirmed_boundary() {
        let mode = FileMode::parse("w+b").unwrap_or_else(|_| panic!("w+b mode must be valid"));
        let file = FileResource::test(
            mode,
            Arc::new(tokio::sync::Mutex::new(Vec::new())),
            false,
            Some(TestWriteFault::DefiniteAt(2)),
            false,
        )
        .await;
        let error = file.write(bytes::Bytes::from_static(b"abcde")).await;
        let Err(error) = error else {
            panic!("scripted partial write must fail");
        };
        assert_eq!(
            error
                .operation_outcome()
                .and_then(|outcome| outcome.completed_bytes),
            Some(2)
        );
        assert_eq!(file.tell().ok(), Some(2));
    }

    #[tokio::test]
    async fn zero_write_reply_poisons_relative_position() {
        let mode = FileMode::parse("w+b").unwrap_or_else(|_| panic!("w+b mode must be valid"));
        let file = FileResource::test(
            mode,
            Arc::new(tokio::sync::Mutex::new(Vec::new())),
            false,
            Some(TestWriteFault::ZeroAt(0)),
            false,
        )
        .await;
        assert!(file.write(bytes::Bytes::from_static(b"x")).await.is_err());
        assert!(file.write(bytes::Bytes::from_static(b"y")).await.is_err());
        assert!(file.seek(0, 1).await.is_err());
        assert_eq!(file.seek(3, 0).await.ok(), Some(3));
    }

    #[tokio::test]
    async fn verifier_change_preserves_dirty_ranges() {
        let mode = FileMode::parse("w+b").unwrap_or_else(|_| panic!("w+b mode must be valid"));
        let file = test_file(mode).await;
        assert!(
            !file
                .record_write(
                    0,
                    2,
                    crate::WriteOutcome {
                        count: 2,
                        stable: false,
                        verifier: Some([1; 8]),
                    },
                )
                .await
        );
        assert!(
            file.record_write(
                2,
                4,
                crate::WriteOutcome {
                    count: 2,
                    stable: false,
                    verifier: Some([2; 8]),
                },
            )
            .await
        );
        assert_eq!(file.dirty_state.lock().await.ranges, vec![(0, 4)]);
    }

    #[tokio::test]
    async fn commit_verifier_change_fails_flush_without_clearing_dirty_state() {
        let mode = FileMode::parse("w+b").unwrap_or_else(|_| panic!("w+b mode must be valid"));
        let file = FileResource::test(
            mode,
            Arc::new(tokio::sync::Mutex::new(Vec::new())),
            false,
            None,
            true,
        )
        .await;
        assert_eq!(
            file.write(bytes::Bytes::from_static(b"data")).await.ok(),
            Some(4)
        );
        let error = file.flush_inner().await;
        assert!(
            error
                .as_ref()
                .err()
                .and_then(NfsError::operation_outcome)
                .is_some_and(|outcome| outcome.outcome == OperationOutcome::Uncertain)
        );
        assert!(!file.dirty_state.lock().await.ranges.is_empty());
    }
}
