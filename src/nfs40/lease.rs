use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::compound::{CompoundBuilder, decode_renew_response};
use crate::error::{NfsError, OperationClass, RequestContext, classify_sent_nfs40_error};
use crate::error::{OperationOutcome, OperationOutcomeError, RecoveryAction};
use crate::mount::{MountHealth, MountLifecycleState, NFSVersion};
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};

const RENEW_TIMEOUT: Duration = Duration::from_secs(5);
const RENEW_STOP_TIMEOUT: Duration = Duration::from_secs(15);

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
}

impl LeaseState {
    pub(crate) fn ready(generation: u64, lease_seconds: u32) -> Arc<Self> {
        Arc::new(Self {
            lifecycle: AtomicU8::new(MountLifecycleState::Ready as u8),
            generation: AtomicU64::new(generation),
            healthy: AtomicBool::new(true),
            lease_seconds,
            renewals: AtomicU64::new(0),
        })
    }

    pub(crate) fn health(&self) -> MountHealth {
        MountHealth {
            lifecycle: match self.lifecycle.load(Ordering::Acquire) {
                value if value == MountLifecycleState::Ready as u8 => MountLifecycleState::Ready,
                value if value == MountLifecycleState::Suspect as u8 => {
                    MountLifecycleState::Suspect
                }
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
                value if value == MountLifecycleState::Closing as u8 => {
                    MountLifecycleState::Closing
                }
                value if value == MountLifecycleState::Closed as u8 => MountLifecycleState::Closed,
                _ => MountLifecycleState::LostState,
            },
            generation: self.generation.load(Ordering::Acquire),
            lease_healthy: Some(self.healthy.load(Ordering::Acquire)),
            lease_seconds: Some(self.lease_seconds),
            lease_renewals: self.renewals.load(Ordering::Acquire),
            callback_healthy: None,
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) fn gate_stateful(&self, operation: &str) -> crate::Result<()> {
        let health = self.health();
        if health.lifecycle == MountLifecycleState::Ready {
            return Ok(());
        }
        let recovery = if health.lifecycle == MountLifecycleState::LostState {
            RecoveryAction::Reopen
        } else {
            RecoveryAction::VerifyThenResume
        };
        Err(NfsError::OperationOutcome(Box::new(
            OperationOutcomeError::new(
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
                    health.lifecycle
                )),
            ),
        )))
    }

    fn mark_ready(&self) {
        self.renewals.fetch_add(1, Ordering::AcqRel);
        self.healthy.store(true, Ordering::Release);
        self.lifecycle
            .store(MountLifecycleState::Ready as u8, Ordering::Release);
    }

    fn mark_suspect(&self) {
        self.healthy.store(false, Ordering::Release);
        self.lifecycle
            .store(MountLifecycleState::Suspect as u8, Ordering::Release);
    }

    pub(crate) fn mark_reconnecting(&self) {
        self.healthy.store(false, Ordering::Release);
        self.lifecycle
            .store(MountLifecycleState::Reconnecting as u8, Ordering::Release);
    }

    pub(crate) fn mark_recovering(&self) {
        self.healthy.store(false, Ordering::Release);
        self.lifecycle
            .store(MountLifecycleState::Recovering as u8, Ordering::Release);
    }

    pub(crate) fn mark_reclaiming(&self) {
        self.lifecycle
            .store(MountLifecycleState::Reclaiming as u8, Ordering::Release);
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

    pub(crate) fn mark_lost(&self) {
        self.healthy.store(false, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.lifecycle
            .store(MountLifecycleState::LostState as u8, Ordering::Release);
    }
}

pub(crate) struct LeaseRenewal {
    handle: Mutex<Option<JoinHandle<()>>>,
    stop: watch::Sender<bool>,
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
        let handle = tokio::spawn(async move {
            let lease_duration = Duration::from_secs(u64::from(state.lease_seconds));
            let mut expires_at = tokio::time::Instant::now() + lease_duration;
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
                let result = rpc
                    .call(request, ReplayPolicy::byte_identical(2), RENEW_TIMEOUT)
                    .await
                    .map_err(|error| {
                        classify_sent_nfs40_error(OperationClass::SessionControl, context, error)
                    })
                    .and_then(decode_renew_response);
                match result {
                    Ok(()) => {
                        expires_at = tokio::time::Instant::now() + lease_duration;
                        state.mark_ready();
                    }
                    Err(NfsError::Nfs4(
                        crate::Nfs4ErrorCode::NFS4ERR_EXPIRED
                        | crate::Nfs4ErrorCode::NFS4ERR_STALE_CLIENTID,
                    )) => {
                        if let Some(recover) = &recovery {
                            state.mark_recovering();
                            if recover().await.is_ok() {
                                expires_at = tokio::time::Instant::now() + lease_duration;
                                state.mark_ready();
                            }
                        } else {
                            state.mark_lost();
                        }
                    }
                    Err(_) => {
                        if tokio::time::Instant::now() >= expires_at {
                            state.mark_lost();
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
        }
    }

    pub(crate) async fn stop(&self) {
        let _ = self.stop.send(true);
        let Some(mut handle) = self.handle.lock().expect("lease task lock poisoned").take() else {
            return;
        };
        if tokio::time::timeout(RENEW_STOP_TIMEOUT, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Drop for LeaseRenewal {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(handle) = self
            .handle
            .get_mut()
            .expect("lease task lock poisoned")
            .take()
        {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
