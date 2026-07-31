//! NFSv4.1 lease renewal via background SEQUENCE calls.
//!
//! RFC 5661 §8.3: The server maintains a lease for each client. If the lease
//! expires, the server may revoke the client's state (open files, locks, etc.).
//! The lease is renewed by any SEQUENCE operation, so a background task
//! periodically sends COMPOUND(SEQUENCE) to keep the session alive.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::compound::CompoundBuilder;
use super::session::SessionHolder;
use crate::rpc;
use crate::rpc::auth::Auth;

/// Background lease renewal task.
pub(crate) struct LeaseRenewal {
    handle: JoinHandle<()>,
}

impl LeaseRenewal {
    /// Start a background task that sends COMPOUND(SEQUENCE) every `interval`.
    ///
    /// The interval should be less than the server's lease time (typically 30-90s).
    /// Accepts `Arc<SessionHolder>` so that post-recovery session replacements are
    /// picked up automatically: each renewal iteration reads the current session
    /// from the holder rather than holding a stale `Arc<Session>`.
    pub fn start(
        rpc: rpc::Client,
        auth: Auth,
        session_holder: Arc<SessionHolder>,
        interval: Duration,
    ) -> Self {
        let handle = tokio::spawn(async move {
            debug!(interval_secs = interval.as_secs(), "lease renewal started");
            loop {
                tokio::time::sleep(interval).await;
                // Always get the latest session so post-recovery renewals use the
                // new session ID rather than the old stale one.
                let session = session_holder.get().await;
                match session.acquire_slot().await {
                    Ok(slot) => {
                        let builder = CompoundBuilder::new("renew").sequence(
                            session.id(),
                            slot.sequence_id,
                            slot.slot_id,
                            session.highest_slot_id(),
                        );
                        let mut buf = Vec::new();
                        builder.encode_with_header(&auth, &mut buf);
                        match rpc.call(buf, 1, Duration::from_secs(5)).await {
                            Ok(_) => {
                                slot.advance();
                                debug!("lease renewal: SEQUENCE successful");
                            }
                            Err(e) => {
                                warn!(error = %e, "lease renewal: SEQUENCE failed");
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "lease renewal: failed to acquire slot");
                    }
                };
            }
        });
        Self { handle }
    }

    /// Stop the background renewal task.
    pub fn stop(&self) {
        self.handle.abort();
    }
}

impl Drop for LeaseRenewal {
    fn drop(&mut self) {
        self.stop();
    }
}
