//! pNFS I/O: parallel reads and writes to data servers (RFC 5661 §12-13).
//!
//! This module adds pNFS layout-aware I/O methods to `Mount41`. When a file
//! has a granted layout, reads and writes are striped across data servers in
//! parallel. If pNFS is unavailable or fails, callers fall back to MDS I/O.

use std::collections::HashSet;
use std::net::SocketAddr;

use bytes::{Buf, Bytes, BytesMut};
use tracing::{debug, info};

use super::compound::CompoundResponse;
use super::fastxdr::nfsstat4;
use super::layout::{Layout, LayoutContent, LayoutSegment};
use super::mount::Mount41;
use super::state::{AccessMode, StateId};
use crate::error::{NfsError, Result};

/// Find the layout segment covering a given file offset.
fn find_covering_segment(layout: &Layout, offset: u64) -> Option<&LayoutSegment> {
    layout.segments.iter().find(|seg| {
        offset >= seg.offset
            && (seg.length == 0xFFFF_FFFF_FFFF_FFFF || offset < seg.offset + seg.length)
    })
}

impl Mount41 {
    /// Get layout for a file, fetching from MDS if not cached.
    /// Returns None if pNFS layouts are unavailable (caller should fall back to MDS I/O).
    pub(crate) async fn get_or_fetch_layout(&self, fh: &Bytes, iomode: u32) -> Option<Layout> {
        // RFC 5661 §18.35.3：server 未在 EXCHANGE_ID 中声明 USE_PNFS_MDS，
        // 整个 mount 禁用 pNFS，跳过 LAYOUTGET（省每文件一次注定失败的 RTT）
        if !self.session_holder.get().await.pnfs_mds() {
            return None;
        }

        // 1. Check cache
        if let Some(layout) = self.layout_manager.get_layout(fh).await {
            return Some(layout);
        }

        // 2. LAYOUTGET to MDS: COMPOUND(SEQUENCE, PUTFH, LAYOUTGET)
        // iomode 1=READ, 2=RW — use matching access mode to avoid NFS4ERR_OPENMODE.
        let access = if iomode == 1 {
            AccessMode::Read
        } else {
            AccessMode::Write
        };
        let sid = self
            .state
            .has_open(fh, access)
            .await
            .unwrap_or_else(StateId::anonymous);
        let result = self
            .compound("layoutget", |b| {
                b.putfh(fh).layoutget(
                    false,                 // signal_layout_avail
                    1,                     // LAYOUT4_NFSV4_1_FILES
                    iomode,                // 1=READ, 2=RW
                    0,                     // offset = whole file
                    0xFFFF_FFFF_FFFF_FFFF, // length = whole file
                    0,                     // min_length
                    &sid.raw,
                    1024 * 1024, // max_count (1 MiB)
                )
            })
            .await;

        match result {
            Ok(resp) => {
                // LAYOUTGET result is after SEQUENCE=0, PUTFH=1 → index 2
                let op = resp.op_ok(2).ok()?;
                let mut data = op.data.clone();
                let layout = super::layout::decode_layoutget_response(&mut data).ok()?;
                // Fetch device info for layout segments
                self.fetch_devices_for_layout(&layout).await;
                self.layout_manager.store_layout(fh, layout.clone()).await;
                Some(layout)
            }
            Err(e) => {
                debug!(error = %e, "LAYOUTGET failed, falling back to MDS I/O");
                None
            }
        }
    }

    /// 该文件的 pNFS 设备是否退化（所有 DS 地址都等于 MDS）。
    ///
    /// 退化时 DS I/O 与 MDS I/O 网络路径完全等价，走 pNFS 只多付
    /// LAYOUTCOMMIT 等管理开销，I/O 路径应回退 MDS。判定按 device
    /// 而非 mount 级——FlexGroup 等多 device 拓扑下，不同文件可能
    /// 落在不同节点，其中部分 device 退化、部分不退化。
    fn device_degenerate(&self, device: &super::layout::DeviceInfo) -> bool {
        let degenerate = super::layout::is_degenerate_device(device, &self.server_addr);
        if degenerate && self.layout_manager.should_log_degenerate() {
            info!(
                "pNFS degenerate device: data servers resolve to the MDS, using MDS I/O for affected files"
            );
        }
        degenerate
    }

    /// 该设备引用的任一非 MDS 的 DS 首选地址已被标记不可达时返回 true，
    /// 调用方直接回退 MDS I/O（layout 保留缓存，避免反复 LAYOUTGET）。
    async fn device_ds_unreachable(&self, device: &super::layout::DeviceInfo) -> bool {
        for paths in &device.ds_addrs {
            if let Some(addr) = paths.first()
                && *addr != self.server_addr
                && self.layout_manager.is_ds_unreachable(addr).await
            {
                return true;
            }
        }
        false
    }

    /// Fetch GETDEVICEINFO for each unique device_id referenced by a layout.
    async fn fetch_devices_for_layout(&self, layout: &Layout) {
        let mut seen = HashSet::new();
        for seg in &layout.segments {
            if let LayoutContent::FilesLayout { device_id, .. } = &seg.content {
                if !seen.insert(*device_id) {
                    continue;
                }
                if self.layout_manager.get_device(device_id).await.is_some() {
                    continue;
                }
                // GETDEVICEINFO: COMPOUND(SEQUENCE, PUTROOTFH, GETDEVICEINFO)
                match self
                    .compound("getdeviceinfo", |b| {
                        b.putrootfh().getdeviceinfo(device_id, 1, 1024 * 1024)
                    })
                    .await
                {
                    Ok(resp) => {
                        // GETDEVICEINFO is after SEQUENCE=0, PUTROOTFH=1 → index 2
                        if let Ok(op) = resp.op_ok(2) {
                            let mut data = op.data.clone();
                            if let Ok(mut info) =
                                super::layout::decode_getdeviceinfo_response(&mut data)
                            {
                                // multipath 地址按与 MDS 的网络接近度排序后再缓存，
                                // 避免 DS I/O 选到客户端不可达网段的 LIF
                                super::layout::sort_multipath_by_affinity(
                                    &mut info,
                                    &self.server_addr,
                                );
                                self.layout_manager.store_device(*device_id, info).await;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "GETDEVICEINFO failed");
                    }
                }
            }
        }
    }

    // ─── DS chunk I/O ───────────────────────────────────────────────────────

    /// 对单个 stripe chunk 发 DS READ（COMPOUND: SEQUENCE, PUTFH, READ）。
    /// MDS 即 DS 时复用主 session（避免对同一 server 重复建 client-id）；
    /// 否则走 DS 自己的 session，NFS4ERR_BADSESSION/DEADSESSION 时重建一次。
    async fn ds_read_chunk(
        &self,
        ds_addr: SocketAddr,
        ds_fh: &Bytes,
        stateid: &[u8; 16],
        offset: u64,
        count: u32,
    ) -> Result<CompoundResponse> {
        if ds_addr == self.server_addr {
            return self
                .compound_data("ds-read-mds", count as usize, |b| {
                    b.putfh(ds_fh).read(stateid, offset, count)
                })
                .await;
        }
        let ds = self
            .layout_manager
            .get_data_server(ds_addr, &self.auth, &self.client_identity)
            .await?;
        let result = Mount41::compound_ds(&ds, &self.auth, "ds-read", count as usize, |b| {
            b.putfh(ds_fh).read(stateid, offset, count)
        })
        .await;
        match result {
            Err(NfsError::Nfs4(nfsstat4::NFS4ERR_BADSESSION | nfsstat4::NFS4ERR_DEADSESSION)) => {
                // DS session 失效（如长时间空闲后过期）：重建一次再试
                self.layout_manager.remove_data_server(ds_addr).await;
                let ds = self
                    .layout_manager
                    .get_data_server(ds_addr, &self.auth, &self.client_identity)
                    .await?;
                Mount41::compound_ds(&ds, &self.auth, "ds-read", count as usize, |b| {
                    b.putfh(ds_fh).read(stateid, offset, count)
                })
                .await
            }
            other => other,
        }
    }

    /// 对单个 stripe chunk 发 DS WRITE（COMPOUND: SEQUENCE, PUTFH, WRITE），
    /// 路由与 session 失效处理同 [`Self::ds_read_chunk`]。
    async fn ds_write_chunk(
        &self,
        ds_addr: SocketAddr,
        ds_fh: &Bytes,
        stateid: &[u8; 16],
        ds_off: u64,
        data: Bytes,
    ) -> Result<CompoundResponse> {
        let len = data.len() as u32;
        if ds_addr == self.server_addr {
            return self
                .compound_write("ds-write-mds", data, |b| {
                    b.putfh(ds_fh)
                        .write_header(stateid, ds_off, 2 /* FILE_SYNC4 */, len)
                })
                .await;
        }
        let ds = self
            .layout_manager
            .get_data_server(ds_addr, &self.auth, &self.client_identity)
            .await?;
        let result = Mount41::compound_ds_write(&ds, &self.auth, "ds-write", data.clone(), |b| {
            b.putfh(ds_fh)
                .write_header(stateid, ds_off, 2 /* FILE_SYNC4 */, len)
        })
        .await;
        match result {
            Err(NfsError::Nfs4(nfsstat4::NFS4ERR_BADSESSION | nfsstat4::NFS4ERR_DEADSESSION)) => {
                self.layout_manager.remove_data_server(ds_addr).await;
                let ds = self
                    .layout_manager
                    .get_data_server(ds_addr, &self.auth, &self.client_identity)
                    .await?;
                Mount41::compound_ds_write(&ds, &self.auth, "ds-write", data, |b| {
                    b.putfh(ds_fh)
                        .write_header(stateid, ds_off, 2 /* FILE_SYNC4 */, len)
                })
                .await
            }
            other => other,
        }
    }

    // ─── pNFS Read ──────────────────────────────────────────────────────────

    /// Attempt a pNFS parallel read.
    /// Returns `None` if layout is unavailable (caller should fall back to MDS).
    /// Returns `Some(Ok(data))` on success, `Some(Err(e))` is never returned —
    /// on DS error the layout is evicted and `None` is returned for MDS fallback.
    pub(crate) async fn pnfs_read(
        &self,
        fh: &Bytes,
        offset: u64,
        count: u32,
    ) -> Option<Result<Bytes>> {
        let layout = self.get_or_fetch_layout(fh, 1 /* IOMODE_READ */).await?;
        let seg = find_covering_segment(&layout, offset)?;
        let (device_id, stripe_unit, is_dense, first_stripe_index, pattern_offset, fh_list) =
            match &seg.content {
                LayoutContent::FilesLayout {
                    device_id,
                    stripe_unit,
                    is_dense,
                    first_stripe_index,
                    pattern_offset,
                    fh_list,
                    ..
                } => (
                    *device_id,
                    *stripe_unit,
                    *is_dense,
                    *first_stripe_index,
                    *pattern_offset,
                    fh_list,
                ),
                _ => return None,
            };

        if stripe_unit == 0 || fh_list.is_empty() {
            return None;
        }
        let device = self.layout_manager.get_device(&device_id).await?;
        if device.ds_addrs.len() < fh_list.len() {
            return None;
        }
        // 退化设备（DS == MDS）：DS 路径无收益，回退 MDS I/O
        if self.device_degenerate(&device) {
            return None;
        }
        // DS 已知不可达：回退 MDS I/O（layout 保留，不再反复尝试）
        if self.device_ds_unreachable(&device).await {
            return None;
        }

        // RFC 8881 §13.9.1：DS 上的 READ 使用 open/delegation stateid，
        // 而非 layout stateid（layout stateid 仅用于 LAYOUTCOMMIT/LAYOUTRETURN）
        let io_stateid = self
            .state
            .has_open(fh, AccessMode::Read)
            .await
            .unwrap_or_else(StateId::anonymous)
            .raw;

        let num_ds = fh_list.len() as u32;
        let chunks = super::layout::split_into_stripes(
            offset,
            count,
            stripe_unit,
            is_dense,
            first_stripe_index,
            num_ds,
            pattern_offset,
        );

        // Issue parallel reads to data servers
        let futures: Vec<_> = chunks
            .iter()
            .map(|chunk| {
                // fh_list is indexed by stripe position (ds_index)
                // ds_addrs is indexed by physical DS (needs stripe_indices indirection)
                let ds_fh_res = fh_list
                    .get(chunk.ds_index as usize)
                    .cloned()
                    .ok_or_else(|| {
                        NfsError::Rpc(format!("fh_list index {} out of range", chunk.ds_index))
                    });
                let ds_phys_idx = device
                    .stripe_indices
                    .get(chunk.ds_index as usize)
                    .copied()
                    .unwrap_or(chunk.ds_index) as usize;
                let ds_addr_res = device
                    .ds_addrs
                    .get(ds_phys_idx)
                    .and_then(|a| a.first())
                    .copied()
                    .ok_or_else(|| NfsError::Rpc(format!("DS index {} out of range", ds_phys_idx)));
                let chunk_len = chunk.length;
                let chunk_ds_offset = chunk.ds_offset;
                async move {
                    let ds_fh = ds_fh_res?;
                    let ds_addr = ds_addr_res?;
                    let resp = self
                        .ds_read_chunk(ds_addr, &ds_fh, &io_stateid, chunk_ds_offset, chunk_len)
                        .await?;
                    // 主 session 复用与独立 DS session 两条路径的 op 布局一致：
                    // SEQUENCE=0, PUTFH=1, READ=2
                    resp.op_ok(1)?; // PUTFH
                    let read_op = resp.op_ok(2)?; // READ
                    let mut data = read_op.data.clone();
                    // READ4resok: eof(4) + data<>
                    if data.remaining() < 4 {
                        return Err(NfsError::Xdr("DS READ result too short".to_string()));
                    }
                    let _eof = data.get_u32();
                    if data.remaining() < 4 {
                        return Err(NfsError::Xdr("DS READ data length missing".to_string()));
                    }
                    let data_len = data.get_u32() as usize;
                    if data.remaining() < data_len {
                        return Err(NfsError::Xdr("DS READ data truncated".to_string()));
                    }
                    Ok::<Bytes, NfsError>(data.slice(..data_len))
                }
            })
            .collect();

        match futures::future::try_join_all(futures).await {
            Ok(results) => {
                if results.len() == 1 {
                    Some(Ok(results.into_iter().next().unwrap_or_default()))
                } else {
                    // Concatenate stripe results in order
                    let total_len: usize = results.iter().map(|b| b.len()).sum();
                    let mut combined = BytesMut::with_capacity(total_len);
                    for chunk_data in results {
                        combined.extend_from_slice(&chunk_data);
                    }
                    Some(Ok(combined.freeze()))
                }
            }
            Err(e) => {
                // On DS error, evict layout and return None to fall back to MDS
                self.layout_manager.remove_layout(fh).await;
                debug!(error = %e, "pNFS read failed, falling back to MDS");
                None
            }
        }
    }

    // ─── pNFS Write ─────────────────────────────────────────────────────────

    /// Attempt a pNFS parallel write.
    /// Returns `None` if layout is unavailable (caller should fall back to MDS).
    pub(crate) async fn pnfs_write(
        &self,
        fh: &Bytes,
        offset: u64,
        data: Bytes,
    ) -> Option<Result<u32>> {
        let layout = self.get_or_fetch_layout(fh, 2 /* IOMODE_RW */).await?;
        let seg = find_covering_segment(&layout, offset)?;
        let (device_id, stripe_unit, is_dense, first_stripe_index, pattern_offset, fh_list) =
            match &seg.content {
                LayoutContent::FilesLayout {
                    device_id,
                    stripe_unit,
                    is_dense,
                    first_stripe_index,
                    pattern_offset,
                    fh_list,
                    ..
                } => (
                    *device_id,
                    *stripe_unit,
                    *is_dense,
                    *first_stripe_index,
                    *pattern_offset,
                    fh_list,
                ),
                _ => return None,
            };

        if stripe_unit == 0 || fh_list.is_empty() {
            return None;
        }
        let device = self.layout_manager.get_device(&device_id).await?;
        if device.ds_addrs.len() < fh_list.len() {
            return None;
        }
        // 退化设备（DS == MDS）：DS 路径无收益，回退 MDS I/O
        if self.device_degenerate(&device) {
            return None;
        }
        // DS 已知不可达：回退 MDS I/O（layout 保留，不再反复尝试）
        if self.device_ds_unreachable(&device).await {
            return None;
        }

        // RFC 8881 §13.9.1：DS 上的 WRITE 使用 open/delegation stateid，
        // 而非 layout stateid（layout stateid 仅用于 LAYOUTCOMMIT/LAYOUTRETURN）
        let io_stateid = self
            .state
            .has_open(fh, AccessMode::Write)
            .await
            .unwrap_or_else(StateId::anonymous)
            .raw;

        let num_ds = fh_list.len() as u32;
        let data_len = data.len();
        let chunks = super::layout::split_into_stripes(
            offset,
            data_len as u32,
            stripe_unit,
            is_dense,
            first_stripe_index,
            num_ds,
            pattern_offset,
        );

        // Issue parallel writes to data servers
        let futures: Vec<_> = chunks
            .iter()
            .map(|chunk| {
                // fh_list is indexed by stripe position (ds_index)
                // ds_addrs is indexed by physical DS (needs stripe_indices indirection)
                let ds_fh_res = fh_list
                    .get(chunk.ds_index as usize)
                    .cloned()
                    .ok_or_else(|| {
                        NfsError::Rpc(format!("fh_list index {} out of range", chunk.ds_index))
                    });
                let ds_phys_idx = device
                    .stripe_indices
                    .get(chunk.ds_index as usize)
                    .copied()
                    .unwrap_or(chunk.ds_index) as usize;
                let ds_addr_res = device
                    .ds_addrs
                    .get(ds_phys_idx)
                    .and_then(|a| a.first())
                    .copied()
                    .ok_or_else(|| NfsError::Rpc(format!("DS index {} out of range", ds_phys_idx)));
                // Zero-copy slice of the write data for this stripe chunk
                let chunk_start = (chunk.file_offset - offset) as usize;
                let chunk_data = data.slice(chunk_start..chunk_start + chunk.length as usize);
                let ds_off = chunk.ds_offset;
                async move {
                    let ds_fh = ds_fh_res?;
                    let ds_addr = ds_addr_res?;
                    let resp = self
                        .ds_write_chunk(ds_addr, &ds_fh, &io_stateid, ds_off, chunk_data)
                        .await?;
                    // SEQUENCE=0, PUTFH=1, WRITE=2（两条路径布局一致）
                    resp.op_ok(1)?; // PUTFH
                    let write_op = resp.op_ok(2)?; // WRITE
                    let mut d = write_op.data.clone();
                    if d.remaining() < 16 {
                        return Err(NfsError::Xdr("DS WRITE result too short".to_string()));
                    }
                    let written = d.get_u32();
                    let committed = d.get_u32();
                    // writeverf: 8 bytes
                    d.advance(8);
                    // needs_commit=true if DS downgraded write stability
                    Ok::<(u32, bool), NfsError>((written, committed != 2 /* FILE_SYNC4 */))
                }
            })
            .collect();

        match futures::future::try_join_all(futures).await {
            Ok(results) => {
                let total: u32 = results.iter().map(|(n, _)| n).sum();
                let needs_commit = results.iter().any(|(_, c)| *c);
                // RFC 5661 §18.42.3：LAYOUTCOMMIT 不必每次 WRITE 后发，只需在
                // LAYOUTRETURN/CLOSE 前提交。这里仅累积 dirty 范围，由
                // flush_layoutcommit 在 close/layoutreturn 时一次性发送，
                // 避免每个 wsize 块一次串行 MDS RTT。
                if data_len > 0 {
                    self.layout_manager
                        .mark_dirty(fh, offset, offset + data_len as u64)
                        .await;
                }
                // RFC 5661 §18.32.3: if any DS downgraded write stability, COMMIT to MDS.
                if needs_commit {
                    let _ = self.commit(fh.clone(), offset, total).await;
                }
                Some(Ok(total))
            }
            Err(e) => {
                // 驱逐 layout 前先提交此前成功写入的范围
                self.flush_layoutcommit(fh).await;
                self.layout_manager.remove_layout(fh).await;
                debug!(error = %e, "pNFS write failed, falling back to MDS");
                None
            }
        }
    }

    // ─── pNFS Layout Commit ──────────────────────────────────────────────

    /// 将累积的 dirty 范围通过一次 LAYOUTCOMMIT 提交给 MDS（best-effort）。
    /// 在 CLOSE / LAYOUTRETURN 前调用；无 dirty 范围或 layout 已不在缓存时为 no-op。
    pub(crate) async fn flush_layoutcommit(&self, fh: &Bytes) {
        let Some((start, end)) = self.layout_manager.take_dirty(fh).await else {
            return;
        };
        let Some(layout) = self.layout_manager.get_layout(fh).await else {
            // layout 已被驱逐（recall 等）：无法 LAYOUTCOMMIT，丢弃 dirty 记录
            return;
        };
        let result = self
            .compound("layoutcommit", |b| {
                b.putfh(fh).layoutcommit(
                    start,
                    end - start,
                    false,
                    &layout.stateid,
                    Some(end - 1),
                    1, // LAYOUT4_NFSV4_1_FILES
                )
            })
            .await;
        if let Err(e) = result {
            debug!(error = %e, "LAYOUTCOMMIT flush failed");
        }
    }

    // ─── pNFS Layout Return ──────────────────────────────────────────────

    /// Return a layout to the metadata server (LAYOUTRETURN4_FILE).
    /// Removes the layout from the local cache and notifies the server.
    /// Errors are logged but not propagated — layout return is best-effort.
    pub(crate) async fn layoutreturn_file(&self, fh: &Bytes) {
        // RFC 5661 §18.42.3：LAYOUTCOMMIT 必须在 LAYOUTRETURN 之前
        self.flush_layoutcommit(fh).await;
        let layout = match self.layout_manager.remove_layout(fh).await {
            Some(l) => l,
            None => return,
        };
        // Use the first segment's iomode; for whole-file layouts this is correct.
        // If multiple iomodes exist, IOMODE_ANY (3) tells the server to return all.
        let iomode = if layout.segments.len() == 1 {
            layout.segments[0].iomode as u32
        } else {
            3 // LAYOUTIOMODE4_ANY
        };
        let result = self
            .compound("layoutreturn", |b| {
                b.putfh(fh).layoutreturn(
                    false, // reclaim
                    1,     // LAYOUT4_NFSV4_1_FILES
                    iomode,
                    1,                     // LAYOUTRETURN4_FILE
                    0,                     // offset = whole file
                    0xFFFF_FFFF_FFFF_FFFF, // length = whole file
                    &layout.stateid,
                )
            })
            .await;
        match result {
            Ok(resp) => {
                if let Err(e) = resp.op_ok(2) {
                    debug!(error = %e, "LAYOUTRETURN op failed");
                }
            }
            Err(e) => {
                debug!(error = %e, "LAYOUTRETURN compound failed");
            }
        }
    }

    /// Return all cached layouts to the server (used during umount).
    pub(crate) async fn layoutreturn_all(&self) {
        let layouts = self.layout_manager.drain_layouts().await;
        for (fh, layout) in &layouts {
            // layout 已从缓存 drain，用 drain 出的 stateid 提交 dirty 范围
            if let Some((start, end)) = self.layout_manager.take_dirty(fh).await {
                let _ = self
                    .compound("layoutcommit", |b| {
                        b.putfh(fh).layoutcommit(
                            start,
                            end - start,
                            false,
                            &layout.stateid,
                            Some(end - 1),
                            1, // LAYOUT4_NFSV4_1_FILES
                        )
                    })
                    .await;
            }
            let iomode = if layout.segments.len() == 1 {
                layout.segments[0].iomode as u32
            } else {
                3 // LAYOUTIOMODE4_ANY
            };
            let _ = self
                .compound("layoutreturn", |b| {
                    b.putfh(fh).layoutreturn(
                        false,
                        1, // LAYOUT4_NFSV4_1_FILES
                        iomode,
                        1, // LAYOUTRETURN4_FILE
                        0,
                        0xFFFF_FFFF_FFFF_FFFF,
                        &layout.stateid,
                    )
                })
                .await;
        }
        self.layout_manager.clear().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs41::layout::{IoMode, Layout, LayoutContent, LayoutSegment, LayoutType};

    #[test]
    fn find_covering_segment_whole_file() {
        let layout = Layout {
            stateid: [0u8; 16],
            return_on_close: false,
            segments: vec![LayoutSegment {
                offset: 0,
                length: 0xFFFF_FFFF_FFFF_FFFF,
                iomode: IoMode::Read,
                layout_type: LayoutType::NfsV41Files,
                content: LayoutContent::Opaque(Bytes::new()),
            }],
        };
        assert!(find_covering_segment(&layout, 0).is_some());
        assert!(find_covering_segment(&layout, 1_000_000).is_some());
    }

    #[test]
    fn find_covering_segment_bounded() {
        let layout = Layout {
            stateid: [0u8; 16],
            return_on_close: false,
            segments: vec![LayoutSegment {
                offset: 100,
                length: 500,
                iomode: IoMode::Read,
                layout_type: LayoutType::NfsV41Files,
                content: LayoutContent::Opaque(Bytes::new()),
            }],
        };
        assert!(find_covering_segment(&layout, 99).is_none());
        assert!(find_covering_segment(&layout, 100).is_some());
        assert!(find_covering_segment(&layout, 599).is_some());
        assert!(find_covering_segment(&layout, 600).is_none());
    }

    #[test]
    fn find_covering_segment_empty() {
        let layout = Layout {
            stateid: [0u8; 16],
            return_on_close: false,
            segments: vec![],
        };
        assert!(find_covering_segment(&layout, 0).is_none());
    }

    #[test]
    fn find_covering_segment_multiple() {
        let layout = Layout {
            stateid: [0u8; 16],
            return_on_close: false,
            segments: vec![
                LayoutSegment {
                    offset: 0,
                    length: 1000,
                    iomode: IoMode::Read,
                    layout_type: LayoutType::NfsV41Files,
                    content: LayoutContent::Opaque(Bytes::new()),
                },
                LayoutSegment {
                    offset: 1000,
                    length: 1000,
                    iomode: IoMode::Read,
                    layout_type: LayoutType::NfsV41Files,
                    content: LayoutContent::Opaque(Bytes::new()),
                },
            ],
        };
        let seg = find_covering_segment(&layout, 500);
        assert!(seg.is_some());
        assert_eq!(seg.map(|s| s.offset), Some(0));
        let seg2 = find_covering_segment(&layout, 1500);
        assert!(seg2.is_some());
        assert_eq!(seg2.map(|s| s.offset), Some(1000));
    }
}
