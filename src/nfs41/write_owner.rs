//! Ownership of admitted WRITE tasks and abandoned results, independent of layouts.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::error::{
    NfsError, OperationClass, OperationOutcome, OperationOutcomeError, RecoveryAction,
    RequestContext,
};

#[derive(Default)]
pub(super) struct WriteOwner {
    gates: Mutex<HashMap<Bytes, Weak<RwLock<()>>>>,
    // An abandoned error cannot be acknowledged by evicting a layout or OPEN.
    // Recovery uses a new mount after file-level verification (RFC 8881 §13.7).
    errors: Mutex<HashMap<Bytes, Arc<NfsError>>>,
    abandoned: Mutex<HashMap<Bytes, usize>>,
}

impl std::fmt::Debug for WriteOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteOwner")
            .field(
                "uncertain_files",
                &self
                    .errors
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish()
    }
}

impl WriteOwner {
    fn gate(&self, fh: &Bytes) -> Arc<RwLock<()>> {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(gate) = gates.get(fh).and_then(Weak::upgrade) {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() != 0);
        let gate = Arc::new(RwLock::new(()));
        gates.insert(fh.clone(), Arc::downgrade(&gate));
        gate
    }

    pub(super) async fn admit(&self, fh: &Bytes) -> OwnedRwLockReadGuard<()> {
        let gate = self.gate(fh);
        loop {
            let guard = gate.clone().read_owned().await;
            let abandoned = self
                .abandoned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(fh);
            if !abandoned
                || self
                    .errors
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains_key(fh)
            {
                return guard;
            }
            // Stop later writes until previously cancelled callers' tasks settle.
            // Healthy concurrent writes still share the read side of this gate.
            drop(guard);
            drop(gate.clone().write_owned().await);
        }
    }

    pub(super) async fn barrier(&self, fh: &Bytes) -> OwnedRwLockWriteGuard<()> {
        self.gate(fh).write_owned().await
    }

    fn abandon(&self, fh: Bytes) {
        *self
            .abandoned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(fh)
            .or_default() += 1;
    }

    fn finish_abandoned(&self, fh: &Bytes) {
        let mut abandoned = self
            .abandoned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = abandoned.get_mut(fh) {
            *count -= 1;
            if *count == 0 {
                abandoned.remove(fh);
            }
        }
    }

    pub(super) fn record(&self, fh: Bytes, error: NfsError) {
        self.errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(fh)
            .or_insert_with(|| Arc::new(error));
    }

    pub(super) fn error(&self, fh: &Bytes) -> Option<NfsError> {
        self.errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(fh)
            .map(shared_uncertainty)
    }

    pub(super) fn any_error(&self) -> Option<NfsError> {
        self.errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .next()
            .map(shared_uncertainty)
    }
}

fn shared_uncertainty(error: &Arc<NfsError>) -> NfsError {
    let context = error
        .operation_outcome()
        .map(|o| o.context().clone())
        .unwrap_or_else(|| RequestContext {
            operation: "abandoned_write".into(),
            protocol: crate::NFSVersion::NFSv4p1,
            request_id: None,
        });
    NfsError::OperationOutcome(Box::new(OperationOutcomeError::new(
        OperationOutcome::Uncertain,
        OperationClass::ReplaySensitive,
        RecoveryAction::VerifyThenResume,
        context,
        NfsError::Io(std::io::Error::other(error.clone())),
    )))
}

/// Task failure must publish uncertainty before releasing the file's barrier.
pub(super) struct WriteTaskGuard {
    owner: Arc<WriteOwner>,
    fh: Bytes,
    settled: bool,
    abandoned: Arc<AtomicBool>,
}
impl WriteTaskGuard {
    pub(super) fn new(owner: Arc<WriteOwner>, fh: Bytes, abandoned: Arc<AtomicBool>) -> Self {
        Self {
            owner,
            fh,
            settled: false,
            abandoned,
        }
    }
    pub(super) fn settled(&mut self) {
        if self.abandoned.swap(false, Ordering::AcqRel) {
            self.owner.finish_abandoned(&self.fh);
        }
        self.settled = true;
    }
}
impl Drop for WriteTaskGuard {
    fn drop(&mut self) {
        if !self.settled {
            self.owner.record(
                self.fh.clone(),
                NfsError::Rpc("admitted WRITE task ended without settlement".into()),
            );
            if self.abandoned.swap(false, Ordering::AcqRel) {
                self.owner.finish_abandoned(&self.fh);
            }
        }
    }
}

/// A dropped waiter announces abandonment before releasing its acknowledgement.
/// This also covers cancellation while a completed result is queued but unread.
pub(super) struct WriteWaiter {
    owner: Arc<WriteOwner>,
    fh: Bytes,
    abandoned: Arc<AtomicBool>,
    ack: Option<tokio::sync::oneshot::Sender<()>>,
}
impl WriteWaiter {
    pub(super) fn new(
        owner: Arc<WriteOwner>,
        fh: Bytes,
        abandoned: Arc<AtomicBool>,
        ack: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            owner,
            fh,
            abandoned,
            ack: Some(ack),
        }
    }
    pub(super) fn acknowledge(&mut self) {
        if let Some(ack) = self.ack.take() {
            let _ = ack.send(());
        }
    }
}
impl Drop for WriteWaiter {
    fn drop(&mut self) {
        if self.ack.is_some() {
            self.owner.abandon(self.fh.clone());
            self.abandoned.store(true, Ordering::Release);
        }
    }
}
