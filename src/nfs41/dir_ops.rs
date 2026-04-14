use bytes::{Buf, Bytes};
use tracing::debug;

use super::attrs::{decode_getattr_response, standard_getattr_bitmap};
use super::compound::OpResponse;
use super::mount::{decode_fh, Mount41};
use crate::error::Result;
use crate::mount;

/// Parse change_info4 from an operation result.
/// Returns (atomic, before_changeid, after_changeid).
fn parse_change_info(op: &OpResponse) -> Option<(bool, u64, u64)> {
    let mut data = op.data.clone();
    if data.remaining() < 20 {
        return None;
    }
    let atomic = data.get_u32() != 0;
    let before = data.get_u64();
    let after = data.get_u64();
    Some((atomic, before, after))
}

impl Mount41 {
    pub(crate) async fn remove(&self, dir_fh: Bytes, filename: &str) -> Result<()> {
        let resp = self.compound("remove", |b| {
            b.putfh(&dir_fh).remove(filename)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // REMOVE
        if let Some(remove_op) = resp.results.get(2) {
            if let Some((atomic, before, after)) = parse_change_info(remove_op) {
                debug!(atomic, before, after, name = filename, "REMOVE change_info");
            }
        }
        Ok(())
    }

    pub(crate) async fn remove_path(&self, path: &str) -> Result<()> {
        let (dir, name) = crate::split_path(path)?;
        let dir_obj = self.lookup_path(&dir).await?;
        self.remove(dir_obj.fh, &name).await
    }

    pub(crate) async fn rmdir(&self, dir_fh: Bytes, dirname: &str) -> Result<()> {
        // NFSv4 REMOVE works for both files and directories
        self.remove(dir_fh, dirname).await
    }

    pub(crate) async fn rmdir_path(&self, path: &str) -> Result<()> {
        self.remove_path(path).await
    }

    pub(crate) async fn rename(
        &self,
        from_dir_fh: Bytes,
        from_filename: &str,
        to_dir_fh: Bytes,
        to_filename: &str,
    ) -> Result<()> {
        // NFSv4 RENAME: PUTFH(src_dir) + SAVEFH + PUTFH(dst_dir) + RENAME(old, new)
        let resp = self.compound("rename", |b| {
            b.putfh(&from_dir_fh)
             .savefh()
             .putfh(&to_dir_fh)
             .rename(from_filename, to_filename)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // SAVEFH
        resp.op_ok(3)?; // PUTFH
        resp.op_ok(4)?; // RENAME
        if let Some(rename_op) = resp.results.get(4) {
            let mut data = rename_op.data.clone();
            if data.remaining() >= 40 {
                let src_atomic = data.get_u32() != 0;
                let src_before = data.get_u64();
                let src_after = data.get_u64();
                let dst_atomic = data.get_u32() != 0;
                let dst_before = data.get_u64();
                let dst_after = data.get_u64();
                debug!(
                    src_atomic, src_before, src_after,
                    dst_atomic, dst_before, dst_after,
                    from = from_filename, to = to_filename,
                    "RENAME change_info"
                );
            }
        }
        Ok(())
    }

    pub(crate) async fn rename_path(&self, from_path: &str, to_path: &str) -> Result<()> {
        let (from_dir, from_name) = crate::split_path(from_path)?;
        let (to_dir, to_name) = crate::split_path(to_path)?;
        let from_obj = self.lookup_path(&from_dir).await?;
        let to_obj = self.lookup_path(&to_dir).await?;
        self.rename(from_obj.fh, &from_name, to_obj.fh, &to_name).await
    }

    pub(crate) async fn link(
        &self,
        src_fh: Bytes,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<mount::Attr> {
        // NFSv4 LINK: PUTFH(src) + SAVEFH + PUTFH(dst_dir) + LINK(newname)
        let bitmap = standard_getattr_bitmap();
        let resp = self.compound("link", |b| {
            b.putfh(&src_fh)
             .savefh()
             .putfh(&dst_dir_fh)
             .link(dst_filename)
             .putfh(&src_fh)
             .getattr(&bitmap)
        }).await?;
        resp.op_ok(1)?; // PUTFH(src)
        resp.op_ok(2)?; // SAVEFH
        resp.op_ok(3)?; // PUTFH(dst_dir)
        resp.op_ok(4)?; // LINK
        if let Some(link_op) = resp.results.get(4) {
            if let Some((atomic, before, after)) = parse_change_info(link_op) {
                debug!(atomic, before, after, name = dst_filename, "LINK change_info");
            }
        }
        resp.op_ok(5)?; // PUTFH(src) again for getattr
        let getattr = resp.op_ok(6)?;
        let mut data = getattr.data.clone();
        decode_getattr_response(&mut data)
    }

    pub(crate) async fn link_path(&self, src_path: &str, dst_path: &str) -> Result<mount::Attr> {
        let src_obj = self.lookup_path(src_path).await?;
        let (dst_dir, dst_name) = crate::split_path(dst_path)?;
        let dst_dir_obj = self.lookup_path(&dst_dir).await?;
        self.link(src_obj.fh, dst_dir_obj.fh, &dst_name).await
    }

    pub(crate) async fn symlink(
        &self,
        target: &str,
        dst_dir_fh: Bytes,
        dst_filename: &str,
    ) -> Result<mount::ObjRes> {
        // NFSv4 symlink: CREATE with NF4LNK type
        let bitmap = standard_getattr_bitmap();
        let resp = self.compound("symlink", |b| {
            b.putfh(&dst_dir_fh)
             .create_symlink(dst_filename, target, &[], &[])
             .getfh()
             .getattr(&bitmap)
        }).await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // CREATE(NF4LNK)
        let getfh = resp.op_ok(3)?;
        let mut fh_data = getfh.data.clone();
        let fh = decode_fh(&mut fh_data)?;
        let getattr = resp.op_ok(4)?;
        let mut attr_data = getattr.data.clone();
        let attr = decode_getattr_response(&mut attr_data)?;
        Ok(mount::ObjRes { fh, attr: Some(attr) })
    }

    pub(crate) async fn symlink_path(
        &self,
        target: &str,
        dst_path: &str,
    ) -> Result<mount::ObjRes> {
        let (dst_dir, dst_name) = crate::split_path(dst_path)?;
        let dst_dir_obj = self.lookup_path(&dst_dir).await?;
        self.symlink(target, dst_dir_obj.fh, &dst_name).await
    }
}
