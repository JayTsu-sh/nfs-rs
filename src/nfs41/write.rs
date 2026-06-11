use bytes::{Buf, Bytes};
use tracing::{debug, warn};

use super::attrs::{decode_getattr_response, standard_getattr_bitmap};
use super::compound::OpenArgs;
use super::mount::{decode_fh, extract_stateid, Mount41};
use super::setattr::encode_setattr;
use super::state::{AccessMode, StateId};
use crate::error::{NfsError, Result};
use crate::mount;

impl Mount41 {
    pub(crate) async fn write(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
        // Try pNFS parallel write first
        if let Some(result) = self.pnfs_write(&fh, offset, data.clone()).await {
            return result;
        }
        // Fallback: MDS write
        self.mds_write(&fh, offset, data).await
    }

    /// Direct MDS write (used as fallback when pNFS is unavailable).
    async fn mds_write(&self, fh: &Bytes, offset: u64, data: Bytes) -> Result<u32> {
        let sid = self
            .state
            .has_open(fh, AccessMode::Write)
            .await
            .unwrap_or_else(StateId::anonymous);
        let stateid = sid.raw;
        let data_len = data.len() as u32;
        let fh_ref = fh.clone();
        let resp = self
            .compound_write("write", data, |b| {
                b.putfh(&fh_ref)
                    .write_header(&stateid, offset, 2 /* FILE_SYNC4 */, data_len)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let write_op = resp.op_ok(2)?;
        let mut d = write_op.data.clone();
        if d.remaining() < 16 {
            return Err(NfsError::Xdr("WRITE result too short".to_string()));
        }
        let count = d.get_u32();
        let committed = d.get_u32();
        d.advance(8); // writeverf — used in COMMIT response, not here
                      // RFC 5661 §18.32.3: if stability was downgraded (committed != FILE_SYNC4),
                      // COMMIT to make the data durable before returning success to the caller.
        if committed != 2
        /* FILE_SYNC4 */
        {
            self.commit(fh.clone(), offset, count).await?;
        }
        Ok(count)
    }

    pub(crate) async fn write_path(&self, path: &str, offset: u64, data: Bytes) -> Result<u32> {
        let obj = self.lookup_path(path).await?;
        self.write(obj.fh, offset, data).await
    }

    pub(crate) async fn open(
        &self,
        dir_fh: Bytes,
        filename: &str,
        access: u32,
    ) -> Result<mount::ObjRes> {
        let access_mode = match access {
            crate::OPEN_READ => AccessMode::Read,
            crate::OPEN_WRITE => AccessMode::Write,
            _ => AccessMode::Both,
        };

        let bitmap = standard_getattr_bitmap();
        let open_args = OpenArgs {
            seqid: 0,
            share_access: access_mode.share_access(),
            share_deny: 0, // OPEN4_SHARE_DENY_NONE
            client_id: self.session_holder.get().await.client_id(),
            owner: Bytes::from_static(b"nfs-rs"),
            create: false,
            create_attrs_mask: vec![],
            create_attrs_vals: vec![],
            claim_file: filename.to_string(),
        };
        let resp = self
            .compound("open", |b| {
                b.putfh(&dir_fh).open(&open_args).getfh().getattr(&bitmap)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let open_op = resp.op_ok(2)?; // OPEN
        let mut open_data = open_op.data.clone();
        let stateid = extract_stateid(&mut open_data)?;

        let getfh = resp.op_ok(3)?; // GETFH
        let mut fh_data = getfh.data.clone();
        let fh = decode_fh(&mut fh_data)?;
        let getattr = resp.op_ok(4)?; // GETATTR
        let mut attr_data = getattr.data.clone();
        let attr = decode_getattr_response(&mut attr_data)?;

        // Register in StateManager for READ/WRITE to use
        self.state
            .register_open(&fh, StateId::from_bytes(&stateid), access_mode)
            .await;

        Ok(mount::ObjRes {
            fh,
            attr: Some(attr),
        })
    }

    pub(crate) async fn open_path(&self, path: &str, access: u32) -> Result<mount::ObjRes> {
        let (dir, name) = crate::split_path(path)?;
        let dir_obj = self.lookup_path(&dir).await?;
        self.open(dir_obj.fh, &name, access).await
    }

    pub(crate) async fn close_file(&self, fh: Bytes) -> Result<()> {
        // Return pNFS layout before CLOSE if server requested return_on_close
        if let Some(layout) = self.layout_manager.get_layout(&fh).await {
            if layout.return_on_close {
                self.layoutreturn_file(&fh).await;
            }
        }
        // Release ref in StateManager; if ref_count hits 0, send CLOSE to server
        if let Some(sid) = self.state.release(&fh).await {
            let _ = self
                .compound("close", |b| b.putfh(&fh).close(0, &sid.raw))
                .await;
        }
        Ok(())
    }

    pub(crate) async fn create(
        &self,
        dir_fh: Bytes,
        filename: &str,
        mode: Option<u32>,
    ) -> Result<mount::ObjRes> {
        // 不在 OPEN createattrs 传 mode，避免 NFS4ERR_ATTRNOTSUPP；创建后 SETATTR 设置。
        let bitmap = standard_getattr_bitmap();
        let open_args = OpenArgs {
            seqid: 0,
            share_access: 0x00000003, // OPEN4_SHARE_ACCESS_BOTH
            share_deny: 0,            // OPEN4_SHARE_DENY_NONE
            client_id: self.session_holder.get().await.client_id(),
            owner: Bytes::from_static(b"nfs-rs-create"),
            create: true,
            create_attrs_mask: vec![],
            create_attrs_vals: vec![],
            claim_file: filename.to_string(),
        };
        let resp = self
            .compound("create", |b| {
                b.putfh(&dir_fh).open(&open_args).getfh().getattr(&bitmap)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let open_op = resp.op_ok(2)?; // OPEN
                                      // Extract stateid from OPEN result for CLOSE
        let mut open_data = open_op.data.clone();
        let stateid = extract_stateid(&mut open_data)?;
        let getfh = resp.op_ok(3)?;
        let mut fh_data = getfh.data.clone();
        let fh = decode_fh(&mut fh_data)?;
        let getattr = resp.op_ok(4)?;
        let mut attr_data = getattr.data.clone();
        let mut attr = decode_getattr_response(&mut attr_data)?;
        // SETATTR with open stateid (before CLOSE) — RFC 5661 §18.30.4:
        // 使用 open stateid 避免与 delegation 冲突，且保证原子性。
        if let Some(m) = mode {
            let (attrmask, attr_vals) = encode_setattr(Some(m), None, None, None, None, None);
            if let Err(e) = self
                .compound("setattr", |b| {
                    b.putfh(&fh).setattr(&stateid, &attrmask, &attr_vals)
                })
                .await
            {
                warn!(error = %e, mode = m, "create: SETATTR mode failed after file creation");
            } else {
                attr.file_mode = m;
            }
        }
        // 保持文件 open 并注册 stateid，由调用方 close_file() 时发 CLOSE 释放
        // （umount 时 drain 兜底）。后续 WRITE 因此持有真实 open stateid——
        // RFC 8881 §13.9.1 禁止 DS I/O 使用 special stateid，提前 CLOSE 会导致
        // pNFS 写全部 NFS4ERR_BAD_STATEID 回退 MDS。
        self.state
            .register_open(&fh, StateId::from_bytes(&stateid), AccessMode::Both)
            .await;
        Ok(mount::ObjRes {
            fh,
            attr: Some(attr),
        })
    }

    pub(crate) async fn create_path(&self, path: &str, mode: Option<u32>) -> Result<mount::ObjRes> {
        let (dir, name) = crate::split_path(path)?;
        let dir_obj = self.lookup_path(&dir).await?;
        self.create(dir_obj.fh, &name, mode).await
    }

    pub(crate) async fn mkdir(
        &self,
        dir_fh: Bytes,
        dirname: &str,
        mode: u32,
    ) -> Result<mount::ObjRes> {
        // 不在 CREATE createattrs 传 mode，避免 NFS4ERR_ATTRNOTSUPP；创建后 SETATTR 设置。
        let bitmap = standard_getattr_bitmap();
        let resp = self
            .compound("mkdir", |b| {
                b.putfh(&dir_fh)
                    .create(2 /* NF4DIR */, dirname, &[], &[])
                    .getfh()
                    .getattr(&bitmap)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // CREATE
                        // Log CREATE change_info
        if let Some(create_op) = resp.results.get(2) {
            let mut cdata = create_op.data.clone();
            if cdata.remaining() >= 20 {
                let atomic = cdata.get_u32() != 0;
                let before = cdata.get_u64();
                let after = cdata.get_u64();
                debug!(atomic, before, after, name = dirname, "CREATE change_info");
            }
        }
        let getfh = resp.op_ok(3)?;
        let mut fh_data = getfh.data.clone();
        let fh = decode_fh(&mut fh_data)?;
        let getattr = resp.op_ok(4)?;
        let mut attr_data = getattr.data.clone();
        let mut attr = decode_getattr_response(&mut attr_data)?;
        // 创建后通过 SETATTR 设置 mode，保持与 v3 行为一致。
        // 目录已创建成功，SETATTR 失败只记 warning，不影响整体结果。
        if let Err(e) = self
            .setattr(fh.clone(), None, Some(mode), None, None, None, None, None)
            .await
        {
            warn!(error = %e, mode, "mkdir: SETATTR mode failed after dir creation");
        } else {
            attr.file_mode = mode;
        }
        Ok(mount::ObjRes {
            fh,
            attr: Some(attr),
        })
    }

    pub(crate) async fn mkdir_path(&self, path: &str, mode: u32) -> Result<mount::ObjRes> {
        let (dir, name) = crate::split_path(path)?;
        let dir_obj = self.lookup_path(&dir).await?;
        self.mkdir(dir_obj.fh, &name, mode).await
    }
}
