use bytes::Bytes;

use super::mount::{extract_stateid, Mount41};
use super::state::StateId;
use crate::error::{NfsError, Result};

impl Mount41 {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn setattr(
        &self,
        fh: Bytes,
        _guard_ctime: Option<crate::Time>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<crate::Time>,
        mtime: Option<crate::Time>,
    ) -> Result<()> {
        // Build fattr4 bitmap + attr_vals for the requested attributes
        let (attrmask, attr_vals) = encode_setattr(mode, uid, gid, size, atime, mtime);
        let stateid = [0u8; 16]; // anonymous stateid
        let resp = self.compound("setattr", |b| {
            b.putfh(&fh).setattr(&stateid, &attrmask, &attr_vals)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        // SETATTR4res has status + bitmap (not a union with void default)
        let setattr_op = resp.results.get(2)
            .ok_or_else(|| NfsError::Xdr("SETATTR response missing op at index 2".to_string()))?;
        if !matches!(setattr_op.status, super::fastxdr::nfsstat4::NFS4_OK) {
            return Err(NfsError::Nfs4(setattr_op.status));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn setattr_path(
        &self,
        path: &str,
        specify_guard: bool,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<crate::Time>,
        mtime: Option<crate::Time>,
    ) -> Result<()> {
        let obj = self.lookup_path(path).await?;
        let guard = if specify_guard {
            let attr = self.getattr(obj.fh.clone()).await?;
            Some(attr.ctime)
        } else {
            None
        };
        self.setattr(obj.fh, guard, mode, uid, gid, size, atime, mtime).await
    }

    pub(crate) async fn lock(
        &self,
        fh: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        // LOCK requires a valid open stateid — file must be opened first
        let sid = self.state.get_stateid(&fh).await;
        if sid == StateId::anonymous() {
            return Err(NfsError::InvalidInput(
                "file must be opened before locking (call open() first)".to_string(),
            ));
        }
        let open_stateid = sid.raw;
        let client_id = self.session_holder.get().await.client_id();

        let resp = self.compound("lock", |b| {
            b.putfh(&fh).lock(
                lock_type,
                false, // reclaim
                offset,
                length,
                true,  // new_lock_owner
                &open_stateid,
                0,     // lock_seqid
                0,     // open_seqid
                b"nfs-rs-lock",
                client_id,
            )
        }).await?;
        resp.op_ok(1)?; // PUTFH
        let lock_op = resp.op_ok(2)?;
        let mut data = lock_op.data.clone();
        // LOCK4resok: lock_stateid (stateid4 = 16 bytes)
        let stateid_raw = extract_stateid(&mut data)?;
        Ok(Bytes::copy_from_slice(&stateid_raw))
    }

    pub(crate) async fn locku(
        &self,
        fh: Bytes,
        lock_stateid: Bytes,
        lock_type: u32,
        offset: u64,
        length: u64,
    ) -> Result<()> {
        if lock_stateid.len() < 16 {
            return Err(NfsError::InvalidInput("lock_stateid must be 16 bytes".to_string()));
        }
        let mut sid = [0u8; 16];
        sid.copy_from_slice(&lock_stateid[..16]);

        let resp = self.compound("locku", |b| {
            b.putfh(&fh).locku(lock_type, 0, &sid, offset, length)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // LOCKU
        Ok(())
    }

    pub(crate) async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<()> {
        let resp = self.compound("commit", |b| {
            b.putfh(&fh).commit(offset, count)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // COMMIT
        Ok(())
    }

    pub(crate) async fn commit_path(&self, path: &str, offset: u64, count: u32) -> Result<()> {
        let obj = self.lookup_path(path).await?;
        self.commit(obj.fh, offset, count).await
    }
}

/// Encode setattr attributes into a bitmap + attr_vals pair.
pub(super) fn encode_setattr(
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime: Option<crate::Time>,
    mtime: Option<crate::Time>,
) -> (Vec<u32>, Vec<u8>) {
    let mut word0: u32 = 0;
    let mut word1: u32 = 0;
    let mut vals = Vec::new();

    // Attributes must be encoded in bitmap order (ascending attribute number).
    // NFSv4.1 numbering: filehandle inserted at 19, shifting all subsequent attrs +1.
    // size = attr #4 (word 0)
    if let Some(s) = size {
        word0 |= 1 << 4;
        vals.extend_from_slice(&s.to_be_bytes());
    }
    // mode = attr #33 (word 1, bit 1)
    if let Some(m) = mode {
        word1 |= 1 << 1;
        vals.extend_from_slice(&m.to_be_bytes());
    }
    // owner = attr #36 (word 1, bit 4)
    if let Some(u) = uid {
        word1 |= 1 << 4;
        let s = u.to_string();
        let bytes = s.as_bytes();
        vals.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        vals.extend_from_slice(bytes);
        let pad = (4 - bytes.len() % 4) % 4;
        vals.extend(std::iter::repeat_n(0, pad));
    }
    // owner_group = attr #37 (word 1, bit 5)
    if let Some(g) = gid {
        word1 |= 1 << 5;
        let s = g.to_string();
        let bytes = s.as_bytes();
        vals.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        vals.extend_from_slice(bytes);
        let pad = (4 - bytes.len() % 4) % 4;
        vals.extend(std::iter::repeat_n(0, pad));
    }
    // time_access_set = attr #48 (word 1, bit 16)
    if let Some(t) = atime {
        word1 |= 1 << 16;
        // set_to_client_time4: SET_TO_CLIENT_TIME = 1
        vals.extend_from_slice(&1u32.to_be_bytes());
        vals.extend_from_slice(&(t.seconds as i64).to_be_bytes());
        vals.extend_from_slice(&t.nseconds.to_be_bytes());
    }
    // time_modify_set = attr #54 (word 1, bit 22)
    if let Some(t) = mtime {
        word1 |= 1 << 22;
        vals.extend_from_slice(&1u32.to_be_bytes());
        vals.extend_from_slice(&(t.seconds as i64).to_be_bytes());
        vals.extend_from_slice(&t.nseconds.to_be_bytes());
    }

    let attrmask = if word1 != 0 { vec![word0, word1] } else if word0 != 0 { vec![word0] } else { vec![] };
    (attrmask, vals)
}
