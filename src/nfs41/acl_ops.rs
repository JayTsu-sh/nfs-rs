//! Mount41 implementation of ACL operations (getacl, setacl, aclsupport).

use bytes::Bytes;

use super::mount::Mount41;
use crate::error::{NfsError, Result};
use crate::mount;
use crate::nfs4::acl;

impl Mount41 {
    pub(crate) async fn getacl(&self, fh: Bytes) -> Result<mount::Acl> {
        let bitmap: [u32; 1] = [1 << 12]; // FATTR4_ACL
        let resp = self
            .compound("getacl", |b| b.putfh(&fh).getattr(&bitmap))
            .await;
        let resp = acl::attrnotsupp_as_unsupported("GETACL", resp)?;
        resp.op_ok(1)?; // PUTFH
        let getattr = resp.op_ok(2)?;
        let mut data = getattr.data.clone();
        acl::decode_getattr_acl(&mut data)
    }

    pub(crate) async fn setacl(&self, fh: Bytes, acl: &mount::Acl) -> Result<()> {
        let (attrmask, attr_vals) = acl::encode_setattr_acl(acl);
        // Anonymous stateid is acceptable for SETATTR when no conflicting share reservations
        // exist (RFC 5661 §8.2.3). Consistent with how setattr.rs handles the same case.
        let stateid = [0u8; 16];
        let resp = self
            .compound("setacl", |b| {
                b.putfh(&fh).setattr(&stateid, &attrmask, &attr_vals)
            })
            .await;
        let resp = acl::attrnotsupp_as_unsupported("SETACL", resp)?;
        resp.op_ok(1)?; // PUTFH
        let setattr_op = resp
            .results
            .get(2)
            .ok_or_else(|| NfsError::Xdr("SETATTR response missing op at index 2".to_string()))?;
        if !matches!(setattr_op.status, crate::nfs4::fastxdr::nfsstat4::NFS4_OK) {
            return Err(NfsError::Nfs4(setattr_op.status));
        }
        Ok(())
    }

    pub(crate) async fn aclsupport(&self, fh: Bytes) -> Result<mount::AclSupport> {
        let bitmap: [u32; 1] = [1 << 13]; // FATTR4_ACLSUPPORT
        let resp = self
            .compound("aclsupport", |b| b.putfh(&fh).getattr(&bitmap))
            .await?;
        resp.op_ok(1)?; // PUTFH
        let getattr = resp.op_ok(2)?;
        let mut data = getattr.data.clone();
        acl::decode_getattr_aclsupport(&mut data)
    }
}
