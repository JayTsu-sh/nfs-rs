//! NFSv4.1 delegation management (RFC 5661 §10.2).
//!
//! Delegations allow the server to grant the client local authority over a file,
//! reducing round-trips. When the server needs to recall a delegation (because
//! another client wants access), it sends CB_RECALL via the backchannel.
//!
//! This module tracks active delegations and handles recall notifications
//! by sending DELEGRETURN.

use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::callback::RecallNotification;

/// A single active delegation.
#[derive(Debug, Clone)]
pub(crate) struct Delegation {
    pub stateid: [u8; 16],
}

/// Manages active delegations and processes recall notifications.
pub(crate) struct DelegationManager {
    /// Map from file handle → active delegation.
    delegations: RwLock<HashMap<Bytes, Delegation>>,
}

impl DelegationManager {
    pub fn new() -> Self {
        Self {
            delegations: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new delegation received from an OPEN response.
    pub async fn register(&self, fh: &Bytes, stateid: [u8; 16]) {
        let mut map = self.delegations.write().await;
        debug!(fh_len = fh.len(), "delegation registered");
        map.insert(fh.clone(), Delegation { stateid });
    }

    /// Process a recall notification by returning the delegation.
    /// Returns the delegation stateid that should be sent via DELEGRETURN.
    pub async fn handle_recall(&self, notification: &RecallNotification) -> Option<Delegation> {
        let mut map = self.delegations.write().await;
        // Find delegation by stateid match
        let key = map.iter()
            .find(|(_, d)| d.stateid == notification.stateid)
            .map(|(k, _)| k.clone());
        if let Some(key) = key {
            let deleg = map.remove(&key);
            info!(fh_len = notification.fh.len(), "delegation recalled");
            deleg
        } else {
            warn!(stateid = ?notification.stateid, "recall for unknown delegation");
            None
        }
    }

    /// Return all delegations (used during umount/cleanup).
    pub async fn return_all(&self) -> Vec<(Bytes, Delegation)> {
        let mut map = self.delegations.write().await;
        map.drain().collect()
    }

    /// Find the file handle and full stateid for a delegation whose stateid starts
    /// with the given 8-byte prefix.
    /// Used by the public `delegreturn(u64)` API where only 8 bytes of stateid are available.
    pub async fn find_fh_by_stateid_prefix(&self, prefix: [u8; 8]) -> Option<(Bytes, [u8; 16])> {
        let map = self.delegations.read().await;
        map.iter()
            .find(|(_, d)| d.stateid[..8] == prefix)
            .map(|(k, d)| (k.clone(), d.stateid))
    }
}

#[cfg(test)]
impl DelegationManager {
    pub async fn remove(&self, fh: &Bytes) -> Option<Delegation> {
        let mut map = self.delegations.write().await;
        map.remove(fh)
    }

    pub async fn get(&self, fh: &Bytes) -> Option<Delegation> {
        let map = self.delegations.read().await;
        map.get(fh).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::callback::RecallNotification;

    #[tokio::test]
    async fn register_and_get_delegation() {
        let mgr = DelegationManager::new();
        let fh = Bytes::from_static(b"testfh");
        let sid = [1u8; 16];

        assert!(mgr.get(&fh).await.is_none());

        mgr.register(&fh, sid).await;
        let deleg = mgr.get(&fh).await.unwrap();
        assert_eq!(deleg.stateid, sid);
    }

    #[tokio::test]
    async fn remove_delegation() {
        let mgr = DelegationManager::new();
        let fh = Bytes::from_static(b"testfh");
        mgr.register(&fh, [2u8; 16]).await;

        let removed = mgr.remove(&fh).await;
        assert!(removed.is_some());
        assert!(mgr.get(&fh).await.is_none());
    }

    #[tokio::test]
    async fn handle_recall_by_stateid() {
        let mgr = DelegationManager::new();
        let fh = Bytes::from_static(b"myfile");
        let sid = [3u8; 16];
        mgr.register(&fh, sid).await;

        let recall = RecallNotification {
            stateid: sid,
            truncate: false,
            fh: fh.clone(),
        };
        let deleg = mgr.handle_recall(&recall).await;
        assert!(deleg.is_some());
        assert_eq!(deleg.unwrap().stateid, sid);
        // Should be removed after recall
        assert!(mgr.get(&fh).await.is_none());
    }

    #[tokio::test]
    async fn handle_recall_unknown_stateid() {
        let mgr = DelegationManager::new();
        let recall = RecallNotification {
            stateid: [99u8; 16],
            truncate: false,
            fh: Bytes::from_static(b"nope"),
        };
        assert!(mgr.handle_recall(&recall).await.is_none());
    }

    #[tokio::test]
    async fn return_all_delegations() {
        let mgr = DelegationManager::new();
        mgr.register(&Bytes::from_static(b"f1"), [1u8; 16]).await;
        mgr.register(&Bytes::from_static(b"f2"), [2u8; 16]).await;

        let all = mgr.return_all().await;
        assert_eq!(all.len(), 2);
        // All cleared
        assert!(mgr.get(&Bytes::from_static(b"f1")).await.is_none());
        assert!(mgr.get(&Bytes::from_static(b"f2")).await.is_none());
    }

    #[tokio::test]
    async fn multiple_delegations_independent() {
        let mgr = DelegationManager::new();
        let fh1 = Bytes::from_static(b"file1");
        let fh2 = Bytes::from_static(b"file2");
        mgr.register(&fh1, [10u8; 16]).await;
        mgr.register(&fh2, [20u8; 16]).await;

        mgr.remove(&fh1).await;
        assert!(mgr.get(&fh1).await.is_none());
        assert!(mgr.get(&fh2).await.is_some()); // fh2 unaffected
    }

    #[tokio::test]
    async fn find_fh_by_stateid_prefix_found() {
        let mgr = DelegationManager::new();
        let fh = Bytes::from_static(b"myfile");
        let mut sid = [0u8; 16];
        sid[0] = 0xAB; sid[1] = 0xCD; sid[2] = 0xEF;
        mgr.register(&fh, sid).await;

        let prefix: [u8; 8] = sid[..8].try_into().unwrap();
        let result = mgr.find_fh_by_stateid_prefix(prefix).await;
        assert!(result.is_some());
        let (found_fh, found_sid) = result.unwrap();
        assert_eq!(found_fh, fh);
        assert_eq!(found_sid, sid);
    }

    #[tokio::test]
    async fn find_fh_by_stateid_prefix_not_found() {
        let mgr = DelegationManager::new();
        let result = mgr.find_fh_by_stateid_prefix([0xFF; 8]).await;
        assert!(result.is_none());
    }
}
