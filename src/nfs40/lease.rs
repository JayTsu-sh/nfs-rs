use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use tokio::task::JoinHandle;

use super::compound::{CompoundBuilder, decode_renew_response};
use crate::error::{NfsError, OperationClass, RequestContext, classify_sent_nfs40_error};
use crate::mount::{MountHealth, MountLifecycleState, NFSVersion};
use crate::rpc::auth::Auth;
use crate::rpc::{self, ReplayPolicy};

const RENEW_TIMEOUT: Duration = Duration::from_secs(5);

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
}

pub(crate) struct LeaseRenewal {
    handle: JoinHandle<()>,
}

impl LeaseRenewal {
    pub(crate) fn start(
        rpc: rpc::Client,
        auth: Auth,
        client_id: u64,
        interval: Duration,
        state: Arc<LeaseState>,
    ) -> Self {
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(renewal_delay(interval, rand::random())).await;
                let request = CompoundBuilder::new("renew")
                    .renew(client_id)
                    .encode_with_header(&auth);
                let context = RequestContext {
                    operation: "renew".into(),
                    protocol: NFSVersion::NFSv4p0,
                    request_id: None,
                };
                let result = rpc
                    .call(request, ReplayPolicy::ONE_ATTEMPT, RENEW_TIMEOUT)
                    .await
                    .map_err(|error| {
                        classify_sent_nfs40_error(OperationClass::SessionControl, context, error)
                    })
                    .and_then(decode_renew_response);
                match result {
                    Ok(()) => state.mark_ready(),
                    Err(NfsError::Nfs4(_)) | Err(NfsError::OperationOutcome(_)) => {
                        state.mark_suspect()
                    }
                    Err(_) => state.mark_suspect(),
                }
            }
        });
        Self { handle }
    }
}

impl Drop for LeaseRenewal {
    fn drop(&mut self) {
        self.handle.abort();
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
