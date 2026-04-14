use bytes::{Buf, Bytes};

use super::mount::{decode_string_from_bytes, Mount41};
use super::state::{AccessMode, StateId};
use crate::error::{NfsError, Result};

impl Mount41 {
    pub(crate) async fn access(&self, fh: Bytes, mode: u32) -> Result<u32> {
        let resp = self.compound("access", |b| {
            b.putfh(&fh).access(mode)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        let access_op = resp.op_ok(2)?;
        let mut data = access_op.data.clone();
        if data.remaining() < 8 {
            return Err(NfsError::Xdr("ACCESS result too short".to_string()));
        }
        let _supported = data.get_u32();
        let access = data.get_u32();
        Ok(access)
    }

    pub(crate) async fn access_path(&self, path: &str, mode: u32) -> Result<u32> {
        let obj = self.lookup_path(path).await?;
        self.access(obj.fh, mode).await
    }

    pub(crate) async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        // Try pNFS parallel read first
        if let Some(result) = self.pnfs_read(&fh, offset, count).await {
            return result;
        }
        // Fallback: MDS read
        self.mds_read(&fh, offset, count).await
    }

    /// Direct MDS read (used as fallback when pNFS is unavailable).
    async fn mds_read(&self, fh: &Bytes, offset: u64, count: u32) -> Result<Bytes> {
        // Use a cached read stateid if available; anonymous otherwise.
        // has_open guards against accidentally using a write-only stateid for reads.
        let sid = self.state.has_open(fh, AccessMode::Read).await
            .unwrap_or_else(StateId::anonymous);
        let stateid = sid.raw;
        let resp = self.compound_data("read", count as usize, |b| {
            b.putfh(fh).read(&stateid, offset, count)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        let read_op = resp.op_ok(2)?;
        let mut data = read_op.data.clone();
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("READ result too short".to_string()));
        }
        let _eof = data.get_u32(); // bool eof
        // data<>: length-prefixed opaque
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("READ data length missing".to_string()));
        }
        let data_len = data.get_u32() as usize;
        if data.remaining() < data_len {
            return Err(NfsError::Xdr("READ data truncated".to_string()));
        }
        Ok(data.slice(..data_len))
    }

    pub(crate) async fn read_path(&self, path: &str, offset: u64, count: u32) -> Result<Bytes> {
        let obj = self.lookup_path(path).await?;
        self.read(obj.fh, offset, count).await
    }

    pub(crate) async fn readlink(&self, fh: Bytes) -> Result<String> {
        let resp = self.compound("readlink", |b| {
            b.putfh(&fh).readlink()
        }).await?;
        resp.op_ok(1)?; // PUTFH
        let readlink_op = resp.op_ok(2)?;
        let mut data = readlink_op.data.clone();
        // READLINK4resok: link (utf8string = opaque<>)
        decode_string_from_bytes(&mut data)
    }

    pub(crate) async fn readlink_path(&self, path: &str) -> Result<String> {
        let obj = self.lookup_path(path).await?;
        self.readlink(obj.fh).await
    }
}
