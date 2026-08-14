use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug)]
pub(crate) struct OwnerLane {
    pub owner: u64,
    pub next_seqid: u32,
    pub stateid: [u8; 16],
    pub fh: Bytes,
    pub access: u32,
    pub write_verifier: Option<[u8; 8]>,
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

#[derive(Default)]
pub(crate) struct LockState {
    lanes: RwLock<HashMap<[u8; 16], Arc<Mutex<LockLane>>>>,
    aliases: RwLock<HashMap<[u8; 16], Arc<Mutex<LockLane>>>>,
}

impl LockState {
    pub(crate) async fn register(&self, lane: LockLane) -> Arc<Mutex<LockLane>> {
        let stateid = lane.stateid;
        let lane = Arc::new(Mutex::new(lane));
        self.lanes.write().await.insert(stateid, Arc::clone(&lane));
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
        let mut lanes = self.lanes.write().await;
        lanes.remove(&old_stateid);
        lanes.insert(new_stateid, Arc::clone(&lane));
        self.aliases.write().await.insert(old_stateid, lane);
    }

    pub(crate) async fn clear(&self) {
        self.lanes.write().await.clear();
        self.aliases.write().await.clear();
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

    pub(crate) async fn for_fh(
        &self,
        fh: &Bytes,
        required_access: u32,
    ) -> Option<Arc<Mutex<OwnerLane>>> {
        let owners = self.by_fh.read().await.get(fh)?.clone();
        for owner in owners.into_iter().rev() {
            let lane = self.by_owner(owner).await?;
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
                    access: if owner == 11 {
                        crate::OPEN_READ
                    } else {
                        crate::OPEN_WRITE
                    },
                    write_verifier: None,
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

    #[test]
    fn opaque_public_state_only_identifies_the_owner() {
        let encoded = encode_owner(9, 0x0102_0304_0506_0708);
        assert_eq!(decode_owner(&encoded), Some((9, 0x0102_0304_0506_0708)));
        assert_eq!(decode_owner(&Bytes::from_static(b"bad")), None);
    }
}
