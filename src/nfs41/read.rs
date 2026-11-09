use bytes::{Buf, Bytes};

use super::mount::Mount41;
use super::state::{AccessMode, StateId};
use crate::error::{NfsError, Result};
use crate::nfs4::compound::take_opaque;

impl Mount41 {
    pub(crate) async fn access(&self, fh: Bytes, mode: u32) -> Result<u32> {
        let resp = self
            .compound("access", |b| b.putfh(&fh).access(mode))
            .await?;
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
        crate::retry::scope(self.read_budgeted(fh, offset, count)).await
    }

    async fn read_budgeted(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        if count == 0 {
            return Ok(Bytes::new());
        }
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
        let sid = self
            .state
            .has_open(fh, AccessMode::Read)
            .await
            .unwrap_or_else(StateId::anonymous);
        let stateid = sid.raw;
        let resp = self
            .compound_data("read", count as usize, |b| {
                b.require_generation(sid.generation)
                    .putfh(fh)
                    .read(&stateid, offset, count)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let read_op = resp.op_ok(2)?;
        let mut data = read_op.data.clone();
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("READ result too short".to_string()));
        }
        let eof = data.get_u32() != 0;
        // data<>: length-prefixed opaque
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("READ data length missing".to_string()));
        }
        let data_len = data.get_u32() as usize;
        if data.remaining() < data_len {
            return Err(NfsError::Xdr("READ data truncated".to_string()));
        }
        crate::mount::read_reply(data.slice(..data_len), eof)
    }

    pub(crate) async fn read_path(&self, path: &str, offset: u64, count: u32) -> Result<Bytes> {
        let obj = self.lookup_path(path).await?;
        self.read(obj.fh, offset, count).await
    }

    pub(crate) async fn readlink(&self, fh: Bytes) -> Result<String> {
        let resp = self
            .compound("readlink", |b| b.putfh(&fh).readlink())
            .await?;
        resp.op_ok(1)?; // PUTFH
        let readlink_op = resp.op_ok(2)?;
        let mut data = readlink_op.data.clone();
        // RFC 8881 §18.24: preserve target identity; the String interface
        // rejects unrepresentable bytes instead of following a replacement name.
        let link = take_opaque(&mut data, "READLINK target")?;
        std::str::from_utf8(&link)
            .map(str::to_owned)
            .map_err(|error| NfsError::Xdr(format!("READLINK target is not UTF-8: {error}")))
    }

    pub(crate) async fn readlink_path(&self, path: &str) -> Result<String> {
        let obj = self.lookup_path(path).await?;
        self.readlink(obj.fh).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs41::MinorVersion;
    use crate::nfs41::test_support::{ok, opaque, putfh, serve_observed};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn readlink_preserves_utf8_identity_v41_v42() {
        // RFC 8881 §18.24: READLINK text is interpreted by the client;
        // replacing invalid bytes would silently change the target identity.
        for minor in [MinorVersion::V1, MinorVersion::V2] {
            for target in [
                b"../target".as_slice(),
                "目录/target".as_bytes(),
                "�".as_bytes(),
                b"\xff",
                b"\xc0\xaf",
            ] {
                let wire_target = target.to_vec();
                let (mount, server) = serve_observed(
                    move |tag, mut args| {
                        assert_eq!(tag, "readlink");
                        putfh(&mut args);
                        assert_eq!(args.get_u32(), 27);
                        vec![ok(22, vec![]), ok(27, opaque(&wire_target))]
                    },
                    minor,
                    Arc::new(Mutex::new(vec![])),
                )
                .await;
                let result = mount.readlink(Bytes::from_static(b"link")).await;
                match std::str::from_utf8(target) {
                    Ok(expected) => assert_eq!(result.unwrap(), expected),
                    Err(_) => assert!(matches!(result, Err(NfsError::Xdr(_))), "{result:?}"),
                }
                server.abort();
            }
        }
    }
}
