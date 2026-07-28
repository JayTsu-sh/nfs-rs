use bytes::Bytes;

use super::attrs::{decode_getattr_response, standard_getattr_bitmap};
use super::mount::{Mount41, decode_fh};
use crate::error::Result;
use crate::mount;

impl Mount41 {
    pub(crate) async fn lookup(&self, dir_fh: Bytes, filename: &str) -> Result<mount::ObjRes> {
        let bitmap = standard_getattr_bitmap();
        let resp = self
            .compound("lookup", |b| {
                b.putfh(&dir_fh).lookup(filename).getfh().getattr(&bitmap)
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

    pub(crate) async fn lookup_path(&self, path: &str) -> Result<mount::ObjRes> {
        let components: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
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
