use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{RwLock, RwLockReadGuard, watch};
use tokio::task::JoinHandle;

use super::compound::{CompoundBuilder, decode_renew_response};
use crate::error::{NfsError, OperationClass, RequestContext, classify_sent_nfs40_error};
use crate::error::{OperationOutcome, OperationOutcomeError, RecoveryAction};
use crate::mount::{MountHealth, MountLifecycleState, NFSVersion};
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};

const RENEW_TIMEOUT: Duration = Duration::from_secs(5);
const RENEW_STOP_TIMEOUT: Duration = Duration::from_secs(15);
const RENEW_FORCE_SETTLE_TIMEOUT: Duration = Duration::from_secs(1);

fn renewal_delay(interval: Duration, sample: u16) -> Duration {
    let per_mille = 900 + u32::from(sample) % 201;
    interval.mul_f64(f64::from(per_mille) / 1000.0)
}

pub(crate) struct LeaseState {
    lifecycle: AtomicU8,
    generation: AtomicU64,
    healthy: AtomicBool,
    lease_seconds: u32,
    renewals: AtomicU64,
    deadline: watch::Sender<tokio::time::Instant>,
    publication: RwLock<()>,
    /// Serializes crash recovery (SETCLIENTID + reclaim) so the renewal task
    /// and any other caller never establish competing client identities.
    recovery: tokio::sync::Mutex<()>,
    /// While set, recovery hit a busy server and callers fail fast instead of
    /// re-running SETCLIENTID + reclaim on every attempt. The stored status is
    /// the busy reply (GRACE or DELAY) that started the backoff.
    recovery_backoff: Mutex<Option<(tokio::time::Instant, crate::Nfs4ErrorCode)>>,
    /// True while a retry task owns the backoff loop; prevents one task per
    /// failed I/O call during a long GRACE or DELAY window.
    retry_task_active: AtomicBool,
    /// Open/lock owners reclaimed since the current recovery session started.
    reclaimed_opens: Mutex<std::collections::HashSet<u64>>,
    reclaimed_locks: Mutex<std::collections::HashSet<u64>>,
}

impl LeaseState {
    pub(crate) fn ready(generation: u64, lease_seconds: u32) -> Arc<Self> {
        let lease_duration = Duration::from_secs(u64::from(lease_seconds));
        let (deadline, _) = watch::channel(tokio::time::Instant::now() + lease_duration);
        Arc::new(Self {
            lifecycle: AtomicU8::new(MountLifecycleState::Ready as u8),
            generation: AtomicU64::new(generation),
            healthy: AtomicBool::new(true),
            lease_seconds,
            renewals: AtomicU64::new(0),
            deadline,
            publication: RwLock::new(()),
            recovery: tokio::sync::Mutex::new(()),
            recovery_backoff: Mutex::new(None),
            retry_task_active: AtomicBool::new(false),
            reclaimed_opens: Mutex::new(std::collections::HashSet::new()),
            reclaimed_locks: Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// Held for the whole of crash recovery: identity re-establishment and the
    /// subsequent reclaim pass must never overlap another recovery.
    pub(crate) async fn recovery_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.recovery.lock().await
    }

    /// Start a backoff until `until`, remembering the busy status that caused
    /// it so callers can report the real server reply instead of guessing.
    pub(crate) fn defer_recovery(&self, until: tokio::time::Instant, status: crate::Nfs4ErrorCode) {
        *self
            .recovery_backoff
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((until, status));
    }

    pub(crate) fn clear_recovery_backoff(&self) {
        *self
            .recovery_backoff
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// The busy status while the backoff window is still open.
    ///
    /// Returns `None` once the window has lapsed, clearing it so a later caller
    /// retries recovery.
    pub(crate) fn active_recovery_backoff(&self) -> Option<crate::Nfs4ErrorCode> {
        let mut backoff = self
            .recovery_backoff
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match *backoff {
            Some((until, status)) if tokio::time::Instant::now() < until => Some(status),
            Some(_) => {
                *backoff = None;
                None
            }
            None => None,
        }
    }

    /// Claim the single retry-task slot; false when one is already running.
    pub(crate) fn claim_retry_task(&self) -> bool {
        self.retry_task_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn release_retry_task(&self) {
        self.retry_task_active.store(false, Ordering::Release);
    }

    pub(crate) fn health(&self) -> MountHealth {
        let lifecycle = match self.lifecycle.load(Ordering::Acquire) {
            value if value == MountLifecycleState::Ready as u8 => MountLifecycleState::Ready,
            value if value == MountLifecycleState::Suspect as u8 => MountLifecycleState::Suspect,
            value if value == MountLifecycleState::Reconnecting as u8 => {
                MountLifecycleState::Reconnecting
            }
            value if value == MountLifecycleState::Recovering as u8 => {
                MountLifecycleState::Recovering
            }
            value if value == MountLifecycleState::Reclaiming as u8 => {
                MountLifecycleState::Reclaiming
            }
            value if value == MountLifecycleState::LostState as u8 => {
                MountLifecycleState::LostState
            }
            value if value == MountLifecycleState::Closing as u8 => MountLifecycleState::Closing,
            value if value == MountLifecycleState::Closed as u8 => MountLifecycleState::Closed,
            _ => MountLifecycleState::LostState,
        };
        MountHealth {
            lifecycle,
            generation: self.generation.load(Ordering::Acquire),
            // Never report a healthy lease while the lifecycle is not Ready:
            // mark_ready's post-CAS stores can race with mark_lost, and this is
            // the only reader of the raw flag.
            lease_healthy: Some(
                self.healthy.load(Ordering::Acquire) && lifecycle == MountLifecycleState::Ready,
            ),
            lease_seconds: Some(self.lease_seconds),
            lease_renewals: self.renewals.load(Ordering::Acquire),
            callback_healthy: None,
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn gate_stateful(&self, operation: &str) -> crate::Result<()> {
        let lifecycle = self.health().lifecycle;
        if lifecycle == MountLifecycleState::Ready {
            return Ok(());
        }
        Err(self.gated(operation, lifecycle))
    }

    fn gated(&self, operation: &str, lifecycle: MountLifecycleState) -> NfsError {
        let recovery = if lifecycle == MountLifecycleState::LostState {
            RecoveryAction::Reopen
        } else {
            RecoveryAction::VerifyThenResume
        };
        NfsError::OperationOutcome(Box::new(OperationOutcomeError::new(
            OperationOutcome::Uncertain,
            OperationClass::ReplaySensitive,
            recovery,
            RequestContext {
                operation: operation.into(),
                protocol: NFSVersion::NFSv4p0,
                request_id: None,
            },
            NfsError::Rpc(format!(
                "NFSv4.0 state-dependent operation gated while mount is {:?}",
                lifecycle
            )),
        )))
    }

    pub(crate) fn begin_stateful(&self, operation: &str) -> crate::Result<u64> {
        loop {
            let before = self.generation();
            self.gate_stateful(operation)?;
            let after = self.generation();
            if before == after {
                return Ok(after);
            }
        }
    }

    pub(crate) fn finish_stateful(&self, generation: u64, operation: &str) -> crate::Result<()> {
        if self.validate_stateful(generation, operation).is_ok() {
            self.record_activity();
            return Ok(());
        }
        self.gate_stateful(operation)
    }

    /// Begin a teardown RPC that only removes state (LOCKU).
    ///
    /// Releasing state is safe while the mount is verifying its lease: the
    /// request can only shrink what the server holds. Refusing here would let a
    /// cancelled LOCK wedge `close` until the mount is lost. Recovery and
    /// reclaim stay gated because they are actively re-establishing state.
    ///
    /// A successful reply is authoritative even if the lifecycle changes while
    /// the request is in flight, so there is no matching finish gate.
    pub(crate) fn begin_teardown(&self, operation: &str) -> crate::Result<u64> {
        let lifecycle = self.health().lifecycle;
        match lifecycle {
            MountLifecycleState::Ready
            | MountLifecycleState::Suspect
            | MountLifecycleState::Reconnecting => Ok(self.generation()),
            _ => Err(self.gated(operation, lifecycle)),
        }
    }

    pub(crate) fn validate_stateful(&self, generation: u64, operation: &str) -> crate::Result<()> {
        if self.generation() == generation && self.health().lifecycle == MountLifecycleState::Ready
        {
            Ok(())
        } else {
            self.gate_stateful(operation)
        }
    }

    pub(crate) fn mark_ready(&self) {
        // A lost/closed mount must stay terminal: its state maps were cleared
        // and callers were told to reopen, so a later successful RENEW must not
        // silently resurrect it as Ready.
        if !self.enter_recovered() {
            return;
        }
        self.record_activity();
        self.renewals.fetch_add(1, Ordering::AcqRel);
        self.healthy.store(true, Ordering::Release);
    }

    /// Atomically promote to `Ready` unless the mount already became terminal.
    fn enter_recovered(&self) -> bool {
        let lost = MountLifecycleState::LostState as u8;
        let closing = MountLifecycleState::Closing as u8;
        let closed = MountLifecycleState::Closed as u8;
        let mut current = self.lifecycle.load(Ordering::Acquire);
        loop {
            if current == lost || current == closing || current == closed {
                return false;
            }
            match self.lifecycle.compare_exchange_weak(
                current,
                MountLifecycleState::Ready as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Recovery finished: resume service and extend the lease window. Unlike
    /// `mark_ready`, this does not count a renewal, which only RENEW validates.
    pub(crate) fn mark_recovered(&self) {
        self.record_activity();
        self.healthy.store(true, Ordering::Release);
        self.lifecycle
            .store(MountLifecycleState::Ready as u8, Ordering::Release);
    }

    pub(crate) fn record_activity(&self) {
        let lease_duration = Duration::from_secs(u64::from(self.lease_seconds));
        self.deadline
            .send_replace(tokio::time::Instant::now() + lease_duration);
    }

    /// Move to `next` unless the mount already reached a terminal state.
    ///
    /// `mark_ready` has its own CAS (`enter_recovered`); every other lifecycle
    /// transition goes through here so a late RENEW or recovery attempt cannot
    /// resurrect a mount whose state maps were already cleared.
    fn transition(&self, next: MountLifecycleState) -> bool {
        let next = next as u8;
        loop {
            let current = self.lifecycle.load(Ordering::Acquire);
            if current == MountLifecycleState::LostState as u8
                || current == MountLifecycleState::Closing as u8
                || current == MountLifecycleState::Closed as u8
            {
                return false;
            }
            if self
                .lifecycle
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Conservative lease deadline: the point after which the server may have
    /// discarded this client's state, so retrying recovery is pointless.
    pub(crate) fn lease_deadline(&self) -> tokio::time::Instant {
        *self.deadline.borrow()
    }

    fn mark_suspect(&self) {
        self.healthy.store(false, Ordering::Release);
        self.transition(MountLifecycleState::Suspect);
    }

    pub(crate) fn mark_reconnecting(&self) {
        self.healthy.store(false, Ordering::Release);
        self.transition(MountLifecycleState::Reconnecting);
    }

    pub(crate) fn mark_recovering(&self) {
        self.healthy.store(false, Ordering::Release);
        self.transition(MountLifecycleState::Recovering);
    }

    pub(crate) fn mark_reclaiming(&self) -> bool {
        self.transition(MountLifecycleState::Reclaiming)
    }

    /// Owners already reclaimed during the current recovery session.
    ///
    /// A busy reply (GRACE/DELAY) ends the pass and schedules a retry. Without
    /// this ledger the retry would send CLAIM_PREVIOUS with seqid 0 for lanes
    /// that already reclaimed, which the server answers with NFS4ERR_BAD_SEQID
    /// and the pass would drop. The set is cleared whenever a recovery session
    /// settles so the next session reclaims every lane again.
    pub(crate) fn open_reclaimed(&self, owner: u64) -> bool {
        self.reclaimed_opens
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&owner)
    }

    pub(crate) fn mark_open_reclaimed(&self, owner: u64) {
        self.reclaimed_opens
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(owner);
    }

    pub(crate) fn lock_reclaimed(&self, owner: u64) -> bool {
        self.reclaimed_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(&owner)
    }

    pub(crate) fn mark_lock_reclaimed(&self, owner: u64) {
        self.reclaimed_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(owner);
    }

    pub(crate) fn clear_reclaimed(&self) {
        self.reclaimed_opens
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.reclaimed_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    pub(crate) fn mark_closing(&self) {
        self.lifecycle
            .store(MountLifecycleState::Closing as u8, Ordering::Release);
    }

    pub(crate) fn mark_closed(&self) {
        self.healthy.store(false, Ordering::Release);
        self.lifecycle
            .store(MountLifecycleState::Closed as u8, Ordering::Release);
    }

    pub(crate) async fn publication_guard(&self) -> RwLockReadGuard<'_, ()> {
        self.publication.read().await
    }

    pub(crate) async fn mark_lost(&self) {
        let _publication = self.publication.write().await;
        self.healthy.store(false, Ordering::Release);
        self.lifecycle
            .store(MountLifecycleState::LostState as u8, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
}

pub(crate) struct LeaseRenewal {
    handle: Mutex<Option<JoinHandle<()>>>,
    stop: watch::Sender<bool>,
    rpc: rpc::Client,
}

pub(crate) type RecoveryHandler =
    Arc<dyn Fn() -> BoxFuture<'static, crate::Result<()>> + Send + Sync>;

impl LeaseRenewal {
    pub(crate) fn start(
        rpc: rpc::Client,
        auth: Auth,
        client_id: Arc<AtomicU64>,
        interval: Duration,
        state: Arc<LeaseState>,
        recovery: Option<RecoveryHandler>,
    ) -> Self {
        let (stop, mut stopping) = watch::channel(false);
        let renewal_rpc = rpc.clone();
        let mut deadline = state.deadline.subscribe();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(renewal_delay(interval, rand::random())) => {}
                    changed = stopping.changed() => {
                        if changed.is_err() || *stopping.borrow() {
                            return;
                        }
                        continue;
                    }
                }
                let request = CompoundBuilder::new("renew")
                    .renew(client_id.load(Ordering::Acquire))
                    .encode_with_header(&auth);
                let context = RequestContext {
                    operation: "renew".into(),
                    protocol: NFSVersion::NFSv4p0,
                    request_id: None,
                };
                let renew = async {
                    renewal_rpc
                        .call(request, ReplayPolicy::byte_identical(2), RENEW_TIMEOUT)
                        .await
                        .map_err(|error| {
                            classify_sent_nfs40_error(
                                OperationClass::SessionControl,
                                context,
                                error,
                            )
                        })
                        .and_then(decode_renew_response)
                };
                tokio::pin!(renew);
                let result = loop {
                    let expires_at = *deadline.borrow();
                    tokio::select! {
                        result = &mut renew => break result,
                        _ = tokio::time::sleep_until(expires_at) => {
                            // Fence state at the conservative deadline, but retain
                            // the sent RPC until it settles instead of cancelling it.
                            state.mark_lost().await;
                            let _ = renew.await;
                            return;
                        }
                        changed = deadline.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                    }
                };
                match result {
                    Ok(()) => {
                        state.mark_ready();
                    }
                    Err(NfsError::Nfs4(
                        crate::Nfs4ErrorCode::NFS4ERR_EXPIRED
                        | crate::Nfs4ErrorCode::NFS4ERR_STALE_CLIENTID,
                    )) => {
                        if let Some(recover) = &recovery {
                            state.mark_recovering();
                            if recover().await.is_ok() {
                                state.mark_ready();
                            }
                        } else {
                            state.mark_lost().await;
                        }
                    }
                    Err(_) => {
                        if tokio::time::Instant::now() >= *deadline.borrow() {
                            state.mark_lost().await;
                            return;
                        }
                        state.mark_suspect();
                    }
                }
                if *stopping.borrow() {
                    return;
                }
            }
        });
        Self {
            handle: Mutex::new(Some(handle)),
            stop,
            rpc,
        }
    }

    pub(crate) async fn stop(&self) {
        let _ = self.stop.send(true);
        let Some(mut handle) = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        else {
            return;
        };
        if tokio::time::timeout(RENEW_STOP_TIMEOUT, &mut handle)
            .await
            .is_err()
        {
            // Closing the transport gives every pending call an explicit
            // failure result; abort is only a last bound after that settlement.
            self.rpc.shutdown().await;
            if tokio::time::timeout(RENEW_FORCE_SETTLE_TIMEOUT, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
    }
}

impl Drop for LeaseRenewal {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        let handle = match self.handle.get_mut() {
            Ok(handle) => handle,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(handle) = handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_task_slot_has_a_single_owner() {
        let lease = LeaseState::ready(1, 30);
        assert!(lease.claim_retry_task());
        assert!(!lease.claim_retry_task());
        lease.release_retry_task();
        assert!(lease.claim_retry_task());
    }

    #[test]
    fn reclaiming_refuses_to_revive_a_terminal_mount() {
        let lease = LeaseState::ready(1, 30);
        lease.mark_closing();
        assert!(!lease.mark_reclaiming());
        assert_eq!(lease.health().lifecycle, MountLifecycleState::Closing);

        let lease = LeaseState::ready(1, 30);
        lease.mark_closed();
        assert!(!lease.mark_reclaiming());
        assert_eq!(lease.health().lifecycle, MountLifecycleState::Closed);
    }

    #[test]
    fn renewal_jitter_stays_within_the_conservative_window() {
        let interval = Duration::from_secs(30);
        assert_eq!(renewal_delay(interval, 0), Duration::from_secs(27));
        assert_eq!(renewal_delay(interval, 200), Duration::from_secs(33));
        for sample in 0..=u16::MAX {
            let delay = renewal_delay(interval, sample);
            assert!(delay >= Duration::from_secs(27));
            assert!(delay <= Duration::from_secs(33));
        }
    }

    #[tokio::test]
    async fn a_lost_mount_cannot_be_promoted_back_to_ready() {
        let lease = LeaseState::ready(1, 60);
        lease.mark_lost().await;
        assert_eq!(lease.generation(), 2);
        // A late successful RENEW must not resurrect a mount whose state maps
        // were cleared and whose callers were told to reopen.
        lease.mark_ready();
        assert_eq!(
            lease.health().lifecycle,
            MountLifecycleState::LostState,
            "LostState is terminal"
        );
        // Late recovery/renewal transitions must be ignored as well, not only
        // the final promotion to Ready.
        lease.mark_reconnecting();
        lease.mark_recovering();
        lease.mark_reclaiming();
        assert_eq!(
            lease.health().lifecycle,
            MountLifecycleState::LostState,
            "no lifecycle transition may resurrect a lost mount"
        );
        assert_eq!(lease.generation(), 2);
        assert_eq!(
            lease.health().lease_healthy,
            Some(false),
            "a lost mount must not report a healthy lease"
        );
    }
}
