//! NFSv4.1 state management — automatic OPEN/CLOSE stateid tracking.
//!
//! NFSv4.1 requires a stateid (obtained via OPEN) for READ/WRITE operations.
//! The StateManager transparently manages these stateids:
//! - `ensure_open(fh, access)`: returns cached stateid or opens a new one
//! - ref_count tracks concurrent users of each stateid
//! - `close(fh)`: decrements ref_count, sends CLOSE when it reaches 0

use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::debug;

/// Opaque NFSv4 stateid (16 bytes: seqid u32 + other [u8;12]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StateId {
    pub raw: [u8; 16],
}

impl StateId {
    /// Anonymous stateid (all-zeros) — used for operations that don't require OPEN.
    pub fn anonymous() -> Self {
        Self { raw: [0u8; 16] }
    }

    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        Self { raw: *bytes }
    }
}

/// Access mode for OPEN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccessMode {
    Read,
    Write,
    Both,
}

impl AccessMode {
    pub fn share_access(&self) -> u32 {
        match self {
            AccessMode::Read => 0x00000001,  // OPEN4_SHARE_ACCESS_READ
            AccessMode::Write => 0x00000002, // OPEN4_SHARE_ACCESS_WRITE
            AccessMode::Both => 0x00000003,  // OPEN4_SHARE_ACCESS_BOTH
        }
    }

    /// Check if this mode covers (is compatible with) the requested mode.
    pub fn covers(&self, requested: AccessMode) -> bool {
        matches!(
            (self, requested),
            (AccessMode::Both, _)
                | (AccessMode::Read, AccessMode::Read)
                | (AccessMode::Write, AccessMode::Write)
        )
    }

    /// Upgrade to cover both existing and requested access.
    pub fn upgrade(&self, requested: AccessMode) -> AccessMode {
        match (self, requested) {
            (AccessMode::Both, _) | (_, AccessMode::Both) => AccessMode::Both,
            (AccessMode::Read, AccessMode::Write) | (AccessMode::Write, AccessMode::Read) => {
                AccessMode::Both
            }
            _ => *self,
        }
    }
}

/// State for a single opened file.
struct OpenState {
    stateid: StateId,
    access: AccessMode,
    ref_count: u32,
}

/// Manages OPEN/CLOSE stateids for NFSv4.1 files.
///
/// File handles are used as keys. Multiple reads/writes on the same file
/// share a single OPEN stateid with ref_count tracking.
pub(crate) struct StateManager {
    /// Map from file handle → open state.
    /// Uses `tokio::sync::RwLock` per CLAUDE.md (async context, not Mutex).
    /// Keys are `Bytes` (zero-copy, ref-counted) instead of `Vec<u8>`.
    open_files: RwLock<HashMap<Bytes, OpenState>>,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            open_files: RwLock::new(HashMap::new()),
        }
    }

    /// Get the stateid for a file, or return anonymous stateid if not opened.
    /// Does NOT auto-open — use `acquire` for that.
    pub async fn get_stateid(&self, fh: &Bytes) -> StateId {
        let files = self.open_files.read().await;
        match files.get(fh) {
            Some(state) => state.stateid.clone(),
            None => StateId::anonymous(),
        }
    }

    /// Register an OPEN result. Called after a successful OPEN COMPOUND.
    pub async fn register_open(&self, fh: &Bytes, stateid: StateId, access: AccessMode) {
        let mut files = self.open_files.write().await;
        match files.get_mut(fh) {
            Some(existing) => {
                // Upgrade access mode and update stateid
                existing.access = existing.access.upgrade(access);
                existing.stateid = stateid;
                existing.ref_count += 1;
                debug!(
                    fh_len = fh.len(),
                    ref_count = existing.ref_count,
                    "state: reuse open"
                );
            }
            None => {
                files.insert(
                    fh.clone(),
                    OpenState {
                        stateid,
                        access,
                        ref_count: 1,
                    },
                );
                debug!(fh_len = fh.len(), "state: new open registered");
            }
        }
    }

    /// Release a reference. Returns true if ref_count reached 0 (caller should CLOSE).
    pub async fn release(&self, fh: &Bytes) -> Option<StateId> {
        let mut files = self.open_files.write().await;
        if let Some(state) = files.get_mut(fh) {
            state.ref_count = state.ref_count.saturating_sub(1);
            if state.ref_count == 0 {
                let sid = state.stateid.clone();
                files.remove(fh);
                debug!(fh_len = fh.len(), "state: last ref released, should CLOSE");
                return Some(sid);
            }
            debug!(
                fh_len = fh.len(),
                ref_count = state.ref_count,
                "state: ref released"
            );
        }
        None
    }

    /// Drain all open states, returning (fh, stateid) pairs for CLOSE.
    pub async fn drain(&self) -> Vec<(Bytes, StateId)> {
        let mut files = self.open_files.write().await;
        let pairs: Vec<(Bytes, StateId)> = files
            .iter()
            .map(|(fh, state)| (fh.clone(), state.stateid.clone()))
            .collect();
        files.clear();
        pairs
    }

    /// Remove all tracked state (used during umount or error recovery).
    pub async fn clear(&self) {
        let mut files = self.open_files.write().await;
        files.clear();
    }

    /// Return cached stateid if one exists with sufficient access, otherwise None.
    /// Use this instead of `get_stateid` when the caller needs a specific access mode —
    /// it avoids sending a read-only stateid for a write operation (NFS4ERR_OPENMODE).
    pub async fn has_open(&self, fh: &Bytes, access: AccessMode) -> Option<StateId> {
        let files = self.open_files.read().await;
        files.get(fh).and_then(|state| {
            if state.access.covers(access) {
                Some(state.stateid.clone())
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_get() {
        let mgr = StateManager::new();
        let fh = Bytes::from_static(b"test_fh");
        let sid = StateId::from_bytes(&[1u8; 16]);

        assert_eq!(mgr.get_stateid(&fh).await, StateId::anonymous());

        mgr.register_open(&fh, sid.clone(), AccessMode::Read).await;
        assert_eq!(mgr.get_stateid(&fh).await, sid);
    }

    #[tokio::test]
    async fn ref_count_tracking() {
        let mgr = StateManager::new();
        let fh = Bytes::from_static(b"test_fh");
        let sid = StateId::from_bytes(&[2u8; 16]);

        mgr.register_open(&fh, sid.clone(), AccessMode::Read).await;
        mgr.register_open(&fh, sid.clone(), AccessMode::Read).await;

        // First release: ref_count goes to 1
        assert!(mgr.release(&fh).await.is_none());
        // Second release: ref_count goes to 0, returns stateid for CLOSE
        assert_eq!(mgr.release(&fh).await, Some(sid));
        // Now anonymous
        assert_eq!(mgr.get_stateid(&fh).await, StateId::anonymous());
    }

    #[tokio::test]
    async fn access_upgrade() {
        let mgr = StateManager::new();
        let fh = Bytes::from_static(b"test_fh");
        let sid1 = StateId::from_bytes(&[3u8; 16]);
        let sid2 = StateId::from_bytes(&[4u8; 16]);

        mgr.register_open(&fh, sid1, AccessMode::Read).await;
        assert!(mgr.has_open(&fh, AccessMode::Read).await.is_some());
        assert!(mgr.has_open(&fh, AccessMode::Write).await.is_none());

        // Upgrade to Both
        mgr.register_open(&fh, sid2.clone(), AccessMode::Write)
            .await;
        assert!(mgr.has_open(&fh, AccessMode::Read).await.is_some());
        assert!(mgr.has_open(&fh, AccessMode::Write).await.is_some());
        assert!(mgr.has_open(&fh, AccessMode::Both).await.is_some());
    }

    #[tokio::test]
    async fn drain_returns_all_and_clears() {
        let mgr = StateManager::new();
        let fh1 = Bytes::from_static(b"f1");
        let fh2 = Bytes::from_static(b"f2");
        let sid1 = StateId::from_bytes(&[10u8; 16]);
        let sid2 = StateId::from_bytes(&[20u8; 16]);

        mgr.register_open(&fh1, sid1.clone(), AccessMode::Read)
            .await;
        mgr.register_open(&fh2, sid2.clone(), AccessMode::Write)
            .await;

        let pairs = mgr.drain().await;
        assert_eq!(pairs.len(), 2);
        // All cleared
        assert_eq!(mgr.get_stateid(&fh1).await, StateId::anonymous());
        assert_eq!(mgr.get_stateid(&fh2).await, StateId::anonymous());
    }
}
