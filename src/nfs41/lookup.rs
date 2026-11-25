use bytes::Bytes;

use super::mount::{Mount41, decode_fh};
use crate::error::Result;
use crate::mount;
use crate::nfs4::attrs::{decode_getattr_response, standard_getattr_bitmap};

impl Mount41 {
    pub(crate) async fn lookup(&self, dir_fh: Bytes, filename: &[u8]) -> Result<mount::ObjRes> {
        self.lookup_raw(dir_fh, Bytes::copy_from_slice(filename))
            .await
    }

    pub(crate) async fn lookup_raw(&self, dir_fh: Bytes, raw_name: Bytes) -> Result<mount::ObjRes> {
        let bitmap = standard_getattr_bitmap();
        let resp = self
            .compound("lookup", |b| {
                b.putfh(&dir_fh).lookup(&raw_name).getfh().getattr(&bitmap)
            })
            .await?;
        // ops: [SEQUENCE(0), PUTFH(1), LOOKUP(2), GETFH(3), GETATTR(4)]
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // LOOKUP
        let getfh = resp.op_ok(3)?;
        let mut fh_data = getfh.data.clone();
        let fh = decode_fh(&mut fh_data)?;
        let getattr = resp.op_ok(4)?;
        let mut attr_data = getattr.data.clone();
        let attr = decode_getattr_response(&mut attr_data)?;
        Ok(mount::ObjRes {
            fh,
            attr: Some(attr),
        })
    }

    pub(crate) async fn lookup_path(&self, path: &[u8]) -> Result<mount::ObjRes> {
        let components = crate::path_components(path)?;
        if components.is_empty() {
            return Ok(mount::ObjRes {
                fh: self.root_fh.clone(),
                attr: None,
            });
        }
        let bitmap = standard_getattr_bitmap();
        let resp = self
            .compound("lookup_path", |mut b| {
                b = b.putfh(&self.root_fh);
                for c in &components {
                    b = b.lookup(c);
                }
                b.getfh().getattr(&bitmap)
            })
            .await?;
        // ops: [SEQUENCE(0), PUTFH(1), LOOKUP*n(2..2+n-1), GETFH(2+n), GETATTR(3+n)]
        let n = components.len();
        for i in 0..n {
            resp.op_ok(2 + i)?; // each LOOKUP
        }
        let getfh = resp.op_ok(2 + n)?;
        let mut fh_data = getfh.data.clone();
        let fh = decode_fh(&mut fh_data)?;
        let getattr = resp.op_ok(3 + n)?;
        let mut attr_data = getattr.data.clone();
        let attr = decode_getattr_response(&mut attr_data)?;
        Ok(mount::ObjRes {
            fh,
            attr: Some(attr),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NfsError;
    use crate::nfs4::Nfs4ErrorCode;
    use crate::nfs4::compound::take_opaque;
    use crate::nfs41::test_support::{Reply, ok, putfh, serve};
    use bytes::Buf;

    #[tokio::test]
    async fn lookup_preserves_server_rejection_of_raw_name() {
        let (mount, server) = serve(|tag, mut args| {
            assert_eq!(tag, "lookup");
            putfh(&mut args);
            assert_eq!(args.get_u32(), 15);
            assert_eq!(take_opaque(&mut args, "name").unwrap().as_ref(), b"\xff");
            vec![
                ok(22, vec![]),
                Reply {
                    opcode: 15,
                    status: 10040,
                    data: vec![],
                },
            ]
        })
        .await;
        let error = mount.lookup(Bytes::new(), b"\xff").await.unwrap_err();
        assert!(matches!(
            error,
            NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_BADCHAR)
        ));
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }
}
