use bytes::Bytes;

use super::mount::Mount41;
use crate::error::Result;
use crate::mount;
use crate::nfs4::attrs::{decode_getattr_response, standard_getattr_bitmap};

impl Mount41 {
    pub(crate) async fn getattr(&self, fh: Bytes) -> Result<mount::Attr> {
        let bitmap = standard_getattr_bitmap();
        let resp = self
            .compound("getattr", |b| b.putfh(&fh).getattr(&bitmap))
            .await?;
        resp.op_ok(1)?; // PUTFH
        let getattr = resp.op_ok(2)?;
        let mut data = getattr.data.clone();
        decode_getattr_response(&mut data)
    }

    pub(crate) async fn getattr_path(&self, path: &str) -> Result<mount::Attr> {
        let obj = self.lookup_path(path).await?;
        self.getattr(obj.fh).await
    }
}
