use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, watch};

#[derive(Debug)]
pub(crate) struct OwnerLane {
    pub owner: u64,
    pub next_seqid: u32,
    pub stateid: [u8; 16],
    pub fh: Bytes,
    /// Directory handle and name used by OPEN, retained so a released state can
    /// be re-established with a fresh OPEN (CLAIM_NULL) without a path walk.
    pub dir_fh: Bytes,
    pub name: String,
    pub access: u32,
    pub write_verifier: Option<[u8; 8]>,
    /// Shared permits cover in-flight stateid I/O. CLOSE takes the exclusive
    /// permit after setting `closing`, so it cannot overtake an issued RPC.
    pub io_fence: Arc<RwLock<()>>,
    /// Fences the owner once CLOSE has started. NFSv4.0 CLOSE is a modifying
    /// operation and must never be issued twice when the first outcome may be
    /// uncertain.
    pub closing: bool,
}

#[derive(Debug)]
pub(crate) struct LockLane {
    pub owner: u64,
    pub open_owner: u64,
    pub owner_wire: Bytes,
    pub next_seqid: u32,
    pub stateid: [u8; 16],
    pub fh: Bytes,
    pub lock_type: u32,
    pub offset: u64,
    pub length: u64,
}

/// The lock state that recovery could not retain for a file handle.
///
/// Reads and writes on that handle stay gated with `StateRecoveryExhausted`
/// until a new lock restores the lost protection or the handle is closed, so an
/// application cannot keep writing through an open whose advisory lock is
/// already gone.
#[derive(Clone, Debug)]
pub(crate) struct LostLock {
    pub(crate) status: crate::Nfs4ErrorCode,
    pub(crate) lock_type: u32,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) owner: bytes::Bytes,
}

/// Whether `granted` restores the protection of a lost lock.
///
/// A WRITE lock (2) restores a lost READ (1) or WRITE lock; a READ lock does not
/// restore a lost WRITE lock. A zero length means "to the end of file", which
/// only another open-ended lock can cover.
fn restores(loss: &LostLock, lock_type: u32, offset: u64, length: u64) -> bool {
    if lock_type < loss.lock_type {
        return false;
    }
    let end = |start: u64, length: u64| {
        if length == 0 {
            None
        } else {
            start.checked_add(length)
        }
    };
    if offset > loss.offset {
        return false;
    }
    match (end(offset, length), end(loss.offset, loss.length)) {
        (Some(granted_end), Some(lost_end)) => granted_end >= lost_end,
        // A lock to EOF covers any lost range; a lost lock to EOF needs one.
        (None, _) => true,
        (_, None) => false,
    }
}

#[derive(Default)]
pub(crate) struct LockState {
    lanes: RwLock<HashMap<[u8; 16], Arc<Mutex<LockLane>>>>,
    aliases: RwLock<HashMap<[u8; 16], Arc<Mutex<LockLane>>>>,
    /// File handles with locks that recovery could not retain and that still
    /// gate I/O. Several locks on one handle can be lost independently, so each
    /// unresolved loss is kept until a new lock restores its protection.
    lost: RwLock<HashMap<Vec<u8>, Vec<LostLock>>>,
    acquisitions: Arc<LockAcquisitions>,
}

/// Tracks LOCK requests that were issued but have not finished settling in
/// their detached task.
///
/// A LOCK runs in its own task and only registers its lane after the reply
/// arrives. If the caller is cancelled, that task keeps running, so a cleanup
/// that snapshots the registry immediately can miss the lock and let a later
/// CLOSE collide with it (`LOCKS_HELD`). `release_locks` therefore captures a
/// barrier and waits for every acquisition issued before it.
struct LockAcquisitions {
    tickets: std::sync::Mutex<AcquisitionTickets>,
    settled: watch::Sender<u64>,
}

impl Default for LockAcquisitions {
    fn default() -> Self {
        let (settled, _receiver) = watch::channel(0u64);
        Self {
            tickets: std::sync::Mutex::default(),
            settled,
        }
    }
}

#[derive(Default)]
struct AcquisitionTickets {
    next: u64,
    pending: HashMap<u64, Bytes>,
}

impl LockAcquisitions {
    fn begin(self: &Arc<Self>, fh: Bytes) -> LockAcquisition {
        let mut tickets = self
            .tickets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let ticket = tickets.next;
        // A wrap only breaks the `ticket < barrier` comparison after 2^64
        // acquisitions on one mount, which is not reachable in practice.
        tickets.next = tickets.next.wrapping_add(1);
        tickets.pending.insert(ticket, fh);
        LockAcquisition {
            acquisitions: Arc::clone(self),
            ticket,
        }
    }

    fn barrier(&self) -> u64 {
        self.tickets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .next
    }

    /// Waits until every acquisition for `fh` with a ticket below `barrier`
    /// has settled. Acquisitions started after `barrier` are intentionally not
    /// awaited: cleanup only promises to cover requests already issued.
    async fn wait_issued_before(&self, barrier: u64, fh: &Bytes) {
        let mut settled = self.settled.subscribe();
        loop {
            let pending = {
                let tickets = self
                    .tickets
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                tickets
                    .pending
                    .iter()
                    .any(|(ticket, candidate)| *ticket < barrier && candidate == fh)
            };
            if !pending {
                return;
            }
            // `send_modify` works without receivers and every waiter subscribes
            // before checking `pending`, so this cannot happen while `self` is
            // alive; treat a closed channel as settled and return.
            if settled.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Holds one in-flight LOCK acquisition open until the detached task that owns
/// the request has registered its result (or failed).
pub(crate) struct LockAcquisition {
    acquisitions: Arc<LockAcquisitions>,
    ticket: u64,
}

impl Drop for LockAcquisition {
    fn drop(&mut self) {
        let removed = {
            let mut tickets = self
                .acquisitions
                .tickets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            tickets.pending.remove(&self.ticket).is_some()
        };
        if removed {
            self.acquisitions
                .settled
                .send_modify(|version| *version = version.wrapping_add(1));
        }
    }
}

impl LockState {
    /// Marks a LOCK as issued. The returned guard must live in the detached
    /// task that settles it, so cancellation of the caller cannot end the
    /// acquisition tracking early.
    #[must_use]
    pub(crate) fn begin_acquisition(self: &Arc<Self>, fh: Bytes) -> LockAcquisition {
        self.acquisitions.begin(fh)
    }

    /// Waits for LOCK acquisitions for `fh` that were issued before this call.
    pub(crate) async fn wait_for_acquisitions(&self, fh: &Bytes) {
        let barrier = self.acquisitions.barrier();
        self.acquisitions.wait_issued_before(barrier, fh).await;
    }
    pub(crate) async fn register(&self, lane: LockLane) -> Arc<Mutex<LockLane>> {
        let stateid = lane.stateid;
        let lane = Arc::new(Mutex::new(lane));
        let previous = self.lanes.write().await.insert(stateid, Arc::clone(&lane));
        if let Some(previous) = previous {
            self.retire_lock_stateid_collision(previous, &lane, stateid)
                .await;
        }
        lane
    }

    pub(crate) async fn by_stateid(&self, stateid: &[u8; 16]) -> Option<Arc<Mutex<LockLane>>> {
        if let Some(lane) = self.lanes.read().await.get(stateid).cloned() {
            return Some(lane);
        }
        self.aliases.read().await.get(stateid).cloned()
    }

    pub(crate) async fn remove(&self, stateid: &[u8; 16]) {
        let lane = if let Some(lane) = self.lanes.write().await.remove(stateid) {
            lane
        } else if let Some(lane) = self.aliases.write().await.remove(stateid) {
            lane
        } else {
            return;
        };
        self.lanes
            .write()
            .await
            .retain(|_, candidate| !Arc::ptr_eq(candidate, &lane));
        self.aliases
            .write()
            .await
            .retain(|_, candidate| !Arc::ptr_eq(candidate, &lane));
    }

    pub(crate) async fn has_fh(&self, fh: &Bytes) -> bool {
        let lanes = self
            .lanes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for lane in lanes {
            if lane.lock().await.fh == *fh {
                return true;
            }
        }
        false
    }

    /// Whether `lane` is still the registry entry for `stateid` (it was not
    /// released by a concurrent LOCKU racing with recovery).
    pub(crate) async fn is_current(&self, stateid: &[u8; 16], lane: &Arc<Mutex<LockLane>>) -> bool {
        self.by_stateid(stateid)
            .await
            .is_some_and(|current| Arc::ptr_eq(&current, lane))
    }

    /// Every lock lane currently tracked for `fh`, deduplicated by identity.
    pub(crate) async fn for_fh(&self, fh: &Bytes) -> Vec<Arc<Mutex<LockLane>>> {
        let lanes = self
            .lanes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut matched: Vec<Arc<Mutex<LockLane>>> = Vec::new();
        for lane in lanes {
            if lane.lock().await.fh != *fh {
                continue;
            }
            if !matched
                .iter()
                .any(|candidate| Arc::ptr_eq(candidate, &lane))
            {
                matched.push(lane);
            }
        }
        matched
    }

    pub(crate) async fn snapshot(&self) -> Vec<Arc<Mutex<LockLane>>> {
        let lanes = self
            .lanes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut ordered = Vec::with_capacity(lanes.len());
        for lane in lanes {
            let owner = lane.lock().await.owner;
            ordered.push((owner, lane));
        }
        ordered.sort_by_key(|(owner, _)| *owner);
        ordered.into_iter().map(|(_, lane)| lane).collect()
    }

    pub(crate) async fn rekey(
        &self,
        old_stateid: [u8; 16],
        new_stateid: [u8; 16],
        lane: Arc<Mutex<LockLane>>,
    ) {
        // Hold both maps for the update: a LOOKUP that runs between them would
        // miss `old_stateid` in `lanes` before the alias exists and report a
        // lock that is still live as unknown.
        let mut lanes = self.lanes.write().await;
        let mut aliases = self.aliases.write().await;
        aliases.insert(old_stateid, Arc::clone(&lane));
        lanes.remove(&old_stateid);
        let previous = lanes.insert(new_stateid, Arc::clone(&lane));
        drop(lanes);
        drop(aliases);
        if let Some(previous) = previous {
            self.retire_lock_stateid_collision(previous, &lane, new_stateid)
                .await;
        }
    }

    /// A conforming server never reuses a live lock stateid. If it does, the
    /// older lane would silently disappear from the registry while tokens
    /// carrying that stateid now resolve to the newer lock. Retire the stale
    /// lane so neither a snapshot nor an alias keeps addressing the reused
    /// stateid.
    async fn retire_lock_stateid_collision(
        &self,
        previous: Arc<Mutex<LockLane>>,
        current: &Arc<Mutex<LockLane>>,
        stateid: [u8; 16],
    ) {
        if Arc::ptr_eq(&previous, current) {
            return;
        }
        // Never await another lane here: `rekey` calls this while holding the
        // current lane, so two concurrent reclaim groups whose server-assigned
        // stateids swap would wait on each other forever while the recovery
        // guard is held. The alias purge below already stops `by_stateid` from
        // resolving to the retired lane; zeroing only hides it from stale
        // snapshots, so skipping it under contention is safe.
        if let Ok(mut previous) = previous.try_lock() {
            previous.stateid = [0; 16];
        }
        self.aliases
            .write()
            .await
            .retain(|_, candidate| !Arc::ptr_eq(candidate, &previous));
        tracing::warn!(
            target: "nfs_rs::stateid",
            stateid = ?stateid,
            "NFSv4.0 server reused a live lock stateid; retiring the older lock lane"
        );
    }

    /// Remember that recovery could not retain a lock for `fh`, so I/O on that
    /// handle must fail until the caller reacquires or closes it.
    pub(crate) async fn mark_lost(&self, fh: &[u8], lost: LostLock) {
        self.lost
            .write()
            .await
            .entry(fh.to_vec())
            .or_default()
            .push(lost);
    }

    /// An unresolved lost-lock record gating I/O on `fh`, if any.
    pub(crate) async fn lost(&self, fh: &[u8]) -> Option<LostLock> {
        self.lost.read().await.get(fh)?.first().cloned()
    }

    /// Resolve only the losses that a newly granted lock restores: its type must
    /// be at least as strong and its range must cover the lost range. A disjoint
    /// or weaker lock acknowledges nothing, and other losses on the handle stay.
    pub(crate) async fn resolve_lost(&self, fh: &[u8], lock_type: u32, offset: u64, length: u64) {
        let mut lost = self.lost.write().await;
        let Some(unresolved) = lost.get_mut(fh) else {
            return;
        };
        unresolved.retain(|loss| !restores(loss, lock_type, offset, length));
        if unresolved.is_empty() {
            lost.remove(fh);
        }
    }

    /// Lift the gate once the handle has no open state left.
    pub(crate) async fn clear_lost(&self, fh: &[u8]) {
        self.lost.write().await.remove(fh);
    }

    pub(crate) async fn clear(&self) {
        // In-flight acquisitions are left to their detached tasks; waiting for
        // them is bounded by the RPC timeout, and their lanes are retired by
        // the generation check once the reply arrives.
        self.lanes.write().await.clear();
        self.aliases.write().await.clear();
        self.lost.write().await.clear();
    }
}

#[derive(Default)]
pub(crate) struct OpenState {
    lanes: RwLock<HashMap<u64, Arc<Mutex<OwnerLane>>>>,
    by_fh: RwLock<HashMap<Bytes, Vec<u64>>>,
}

impl OpenState {
    pub(crate) async fn register(&self, lane: OwnerLane) -> Arc<Mutex<OwnerLane>> {
        let owner = lane.owner;
        let fh = lane.fh.clone();
        let lane = Arc::new(Mutex::new(lane));
        self.lanes.write().await.insert(owner, Arc::clone(&lane));
        self.by_fh.write().await.entry(fh).or_default().push(owner);
        lane
    }

    pub(crate) async fn by_owner(&self, owner: u64) -> Option<Arc<Mutex<OwnerLane>>> {
        self.lanes.read().await.get(&owner).cloned()
    }

    /// Whether `lane` is still the current owner entry (it was not replaced or
    /// removed by a close/cleanup racing with recovery).
    pub(crate) async fn is_current(&self, owner: u64, lane: &Arc<Mutex<OwnerLane>>) -> bool {
        self.by_owner(owner)
            .await
            .is_some_and(|current| Arc::ptr_eq(&current, lane))
    }

    pub(crate) async fn for_fh(
        &self,
        fh: &Bytes,
        required_access: u32,
    ) -> Option<Arc<Mutex<OwnerLane>>> {
        let owners = self.by_fh.read().await.get(fh)?.clone();
        for owner in owners.into_iter().rev() {
            // An owner may already be removed from `lanes` while its entry is
            // still visible in `by_fh`; skip it instead of failing the whole
            // lookup for a lane that is still usable.
            let Some(lane) = self.by_owner(owner).await else {
                continue;
            };
            if lane.lock().await.access & required_access != 0 {
                return Some(lane);
            }
        }
        None
    }

    pub(crate) async fn remove(&self, owner: u64, fh: &Bytes) {
        self.lanes.write().await.remove(&owner);
        let mut by_fh = self.by_fh.write().await;
        if let Some(owners) = by_fh.get_mut(fh) {
            owners.retain(|candidate| *candidate != owner);
            if owners.is_empty() {
                by_fh.remove(fh);
            }
        }
    }

    pub(crate) async fn snapshot(&self) -> Vec<Arc<Mutex<OwnerLane>>> {
        let mut owners = self.lanes.read().await.keys().copied().collect::<Vec<_>>();
        owners.sort_unstable();
        let lanes = self.lanes.read().await;
        owners
            .into_iter()
            .filter_map(|owner| lanes.get(&owner).cloned())
            .collect()
    }

    pub(crate) async fn clear(&self) {
        self.lanes.write().await.clear();
        self.by_fh.write().await.clear();
    }
}

pub(crate) fn encode_owner(issuer: u64, owner: u64) -> Bytes {
    Bytes::copy_from_slice(&[issuer.to_be_bytes(), owner.to_be_bytes()].concat())
}

pub(crate) fn decode_owner(state: &Bytes) -> Option<(u64, u64)> {
    if state.len() != 16 {
        return None;
    }
    let mut issuer = [0; 8];
    issuer.copy_from_slice(&state[..8]);
    let mut owner = [0; 8];
    owner.copy_from_slice(&state[8..]);
    Some((u64::from_be_bytes(issuer), u64::from_be_bytes(owner)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_file_keeps_independent_owner_lanes() {
        let state = OpenState::default();
        for owner in [11, 12] {
            state
                .register(OwnerLane {
                    owner,
                    next_seqid: 1,
                    stateid: [owner as u8; 16],
                    fh: Bytes::from_static(b"fh"),
                    dir_fh: Bytes::from_static(b"dir"),
                    name: "file".into(),
                    access: if owner == 11 {
                        crate::OPEN_READ
                    } else {
                        crate::OPEN_WRITE
                    },
                    write_verifier: None,
                    io_fence: Arc::new(RwLock::new(())),
                    closing: false,
                })
                .await;
        }
        assert_eq!(
            state.by_owner(11).await.unwrap().lock().await.stateid,
            [11; 16]
        );
        assert_eq!(
            state
                .for_fh(&Bytes::from_static(b"fh"), crate::OPEN_WRITE)
                .await
                .unwrap()
                .lock()
                .await
                .owner,
            12
        );
        state.remove(12, &Bytes::from_static(b"fh")).await;
        assert_eq!(
            state
                .for_fh(&Bytes::from_static(b"fh"), crate::OPEN_READ)
                .await
                .unwrap()
                .lock()
                .await
                .owner,
            11
        );
    }

    #[tokio::test]
    async fn stale_owner_entry_does_not_hide_a_live_lane() {
        let state = OpenState::default();
        for owner in [11, 12] {
            state
                .register(OwnerLane {
                    owner,
                    next_seqid: 1,
                    stateid: [owner as u8; 16],
                    fh: Bytes::from_static(b"fh"),
                    dir_fh: Bytes::from_static(b"dir"),
                    name: "file".into(),
                    access: crate::OPEN_WRITE,
                    write_verifier: None,
                    io_fence: Arc::new(RwLock::new(())),
                    closing: false,
                })
                .await;
        }
        // Simulate `remove(12)` racing a lookup: the lane is gone from `lanes`
        // but its owner is still listed under `by_fh`.
        state.lanes.write().await.remove(&12);
        let lane = state
            .for_fh(&Bytes::from_static(b"fh"), crate::OPEN_WRITE)
            .await
            .expect("the remaining owner lane stays usable");
        assert_eq!(lane.lock().await.owner, 11);
    }

    #[tokio::test]
    async fn rekey_keeps_the_old_stateid_resolvable() {
        let state = LockState::default();
        let lane = state
            .register(LockLane {
                owner: 7,
                open_owner: 11,
                owner_wire: Bytes::from_static(b"lock"),
                next_seqid: 1,
                stateid: [1; 16],
                fh: Bytes::from_static(b"fh"),
                lock_type: 1,
                offset: 0,
                length: 0,
            })
            .await;
        state.rekey([1; 16], [2; 16], Arc::clone(&lane)).await;

        // An in-flight LOCKU still carries the pre-reclaim stateid.
        assert!(state.by_stateid(&[1; 16]).await.is_some());
        assert!(state.by_stateid(&[2; 16]).await.is_some());

        state.remove(&[2; 16]).await;
        assert!(state.by_stateid(&[1; 16]).await.is_none());
    }

    #[tokio::test]
    async fn reused_lock_stateid_retires_the_older_lane() {
        let state = LockState::default();
        let make = |owner: u64, stateid: [u8; 16]| LockLane {
            owner,
            open_owner: 11,
            owner_wire: Bytes::from_static(b"lock"),
            next_seqid: 1,
            stateid,
            fh: Bytes::from_static(b"fh"),
            lock_type: 1,
            offset: 0,
            length: 0,
        };
        let older = state.register(make(7, [1; 16])).await;
        state.rekey([1; 16], [3; 16], Arc::clone(&older)).await;
        let newer = state.register(make(8, [3; 16])).await;
        let current = state.by_stateid(&[3; 16]).await.unwrap();
        assert!(Arc::ptr_eq(&current, &newer));
        assert_eq!(
            older.lock().await.stateid,
            [0; 16],
            "the shadowed lane must be retired so a snapshot cannot LOCKU the reused stateid"
        );
        assert!(
            state.by_stateid(&[1; 16]).await.is_none(),
            "aliases that still resolve to the retired lane must be dropped"
        );

        // rekey (reclaim) must apply the same policy.
        let third = state.register(make(9, [2; 16])).await;
        state.rekey([3; 16], [2; 16], Arc::clone(&newer)).await;
        let current = state.by_stateid(&[2; 16]).await.unwrap();
        assert!(Arc::ptr_eq(&current, &newer));
        assert_eq!(third.lock().await.stateid, [0; 16]);

        // A concurrent reclaim group may hold the lane being retired. Retiring
        // must not wait for it: with swapped server-assigned stateids, two
        // groups would otherwise deadlock while the recovery guard is held.
        let held = newer.lock().await;
        let fourth = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            state.register(make(10, [2; 16])),
        )
        .await
        .expect("retiring a lane held elsewhere must not block");
        drop(held);
        assert!(
            state
                .by_stateid(&[2; 16])
                .await
                .is_some_and(|lane| Arc::ptr_eq(&lane, &fourth)),
            "the newer lane must own the reused stateid even when the retired lane was busy"
        );
    }

    #[test]
    fn opaque_public_state_only_identifies_the_owner() {
        let encoded = encode_owner(9, 0x0102_0304_0506_0708);
        assert_eq!(decode_owner(&encoded), Some((9, 0x0102_0304_0506_0708)));
        assert_eq!(decode_owner(&Bytes::from_static(b"bad")), None);
    }

    #[test]
    fn only_a_lock_that_restores_the_lost_protection_resolves_a_loss() {
        let loss = |lock_type, offset, length| LostLock {
            status: crate::Nfs4ErrorCode::NFS4ERR_DENIED,
            lock_type,
            offset,
            length,
            owner: Bytes::new(),
        };
        let write = loss(2, 0, 4096);
        assert!(restores(&write, 2, 0, 4096), "the same lock");
        assert!(restores(&write, 2, 0, 8192), "a wider lock");
        assert!(restores(&write, 2, 0, 0), "a lock to EOF");
        assert!(!restores(&write, 2, 4096, 1), "a disjoint lock");
        assert!(!restores(&write, 2, 0, 1), "a partial overlap");
        assert!(
            !restores(&write, 1, 0, 4096),
            "a READ lock over a lost WRITE lock"
        );
        let read = loss(1, 0, 4096);
        assert!(
            restores(&read, 2, 0, 4096),
            "a WRITE lock over a lost READ lock"
        );
        assert!(!restores(&loss(2, 0, 0), 2, 0, 4096), "a lost lock to EOF");
        assert!(
            restores(&loss(2, 0, 0), 2, 0, 0),
            "only a lock to EOF covers it"
        );
        assert!(
            !restores(&loss(2, u64::MAX - 1, 8), 2, 0, u64::MAX),
            "an overflowing range is never reported as covered"
        );
    }
}
