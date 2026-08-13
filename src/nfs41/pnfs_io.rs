//! pNFS I/O: parallel reads and writes to data servers (RFC 5661 §12-13).
//!
//! This module adds pNFS layout-aware I/O methods to `Mount41`. When a file
//! has a granted layout, reads and writes are striped across data servers in
//! parallel. If pNFS is unavailable or fails, callers fall back to MDS I/O.

use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;

use bytes::{Buf, Bytes, BytesMut};
use tracing::{debug, info};

use super::Nfs4ErrorCode;
use super::compound::CompoundResponse;
use super::fastxdr::nfsstat4;
#[cfg(test)]
use super::layout::LayoutManager;
use super::layout::{IoMode, Layout, LayoutContent, LayoutSegment};
use super::mount::Mount41;
use super::state::{AccessMode, StateId};
use crate::error::{
    NfsError, OperationClass, OperationOutcome, OperationOutcomeError, RecoveryAction,
    RequestContext, Result,
};

/// Whether pNFS WRITE transmitted a DS mutation.
///
/// Only `NotAttempted` permits the caller to fall back to an MDS WRITE. Once a
/// DS batch starts, every result remains on the `Attempted` path so an
/// ambiguous mutation can never be silently overwritten through the MDS.
pub(crate) enum PnfsWriteOutcome {
    NotAttempted,
    Attempted(Result<u32>),
}

struct PlannedDsWrite {
    stripe_index: usize,
    ds_fh: Bytes,
    ds_addr: SocketAddr,
    ds_offset: u64,
    data: Bytes,
}

struct DsWriteCompletion<T> {
    stripe_index: usize,
    ds_addr: SocketAddr,
    result: Result<T>,
}

fn ds_batch_diagnostic<T>(completions: &[DsWriteCompletion<T>]) -> String {
    completions
        .iter()
        .map(|completion| match &completion.result {
            Ok(_) => format!(
                "stripe={} ds={} attempted=true outcome=success",
                completion.stripe_index, completion.ds_addr
            ),
            Err(error) => format!(
                "stripe={} ds={} attempted=true outcome=error error={error}",
                completion.stripe_index, completion.ds_addr
            ),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn uncertain_pnfs_write(context: RequestContext, source: NfsError) -> NfsError {
    NfsError::OperationOutcome(Box::new(OperationOutcomeError::new(
        OperationOutcome::Uncertain,
        OperationClass::ReplaySensitive,
        RecoveryAction::VerifyThenResume,
        context,
        source,
    )))
}

/// Wait for every request in an already-issued DS batch before reporting its
/// aggregate result. Unlike `try_join_all`, an early error cannot cancel a
/// sibling WRITE whose request may already be on the wire. Errors are selected
/// in plan order, so diagnostics do not depend on network completion order.
async fn settle_ds_batch<T, F>(futures: Vec<(usize, SocketAddr, F)>) -> Vec<DsWriteCompletion<T>>
where
    F: Future<Output = Result<T>>,
{
    futures::future::join_all(futures.into_iter().map(
        |(stripe_index, ds_addr, future)| async move {
            DsWriteCompletion {
                stripe_index,
                ds_addr,
                result: future.await,
            }
        },
    ))
    .await
}

#[cfg(test)]
async fn invalidate_layout_after_ds_error(
    layout_manager: &LayoutManager,
    fh: &Bytes,
    error: &NfsError,
) {
    if matches!(error, NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_STALE)) {
        layout_manager.remove_layout(fh).await;
        layout_manager.invalidate_dirty(fh).await;
    }
}

/// Find the layout segment covering a given file offset.
fn find_covering_segment(layout: &Layout, offset: u64) -> Option<&LayoutSegment> {
    layout
        .segments
        .iter()
        .find(|segment| segment.covers(offset))
}

impl Mount41 {
    async fn preflight_write_data_servers(
        &self,
        writes: &[PlannedDsWrite],
        generation: u64,
    ) -> Result<()> {
        let addresses = writes
            .iter()
            .map(|write| write.ds_addr)
            .filter(|address| *address != self.server_addr)
            .collect::<HashSet<_>>();
        futures::future::try_join_all(addresses.into_iter().map(|address| async move {
            self.layout_manager
                .get_data_server(address, &self.auth, &self.client_identity, generation)
                .await
                .map(|_| ())
        }))
        .await?;
        Ok(())
    }

    /// Get layout for a file, fetching from MDS if not cached.
    /// Returns None if pNFS layouts are unavailable (caller should fall back to MDS I/O).
    pub(crate) async fn get_or_fetch_layout(
        &self,
        fh: &Bytes,
        iomode: IoMode,
        offset: u64,
    ) -> Option<Layout> {
        self.fetch_layout(fh, iomode, offset, false).await
    }

    async fn fetch_layout(
        &self,
        fh: &Bytes,
        iomode: IoMode,
        offset: u64,
        force_update: bool,
    ) -> Option<Layout> {
        // RFC 5661 §18.35.3：server 未在 EXCHANGE_ID 中声明 USE_PNFS_MDS，
        // 整个 mount 禁用 pNFS，跳过 LAYOUTGET（省每文件一次注定失败的 RTT）
        if !self.session_holder.get().await.pnfs_mds() {
            return None;
        }

        // 1. Check cache
        if !force_update
            && let Some(layout) = self.layout_manager.get_layout_covering(fh, offset).await
        {
            return Some(layout);
        }

        // 2. LAYOUTGET to MDS: COMPOUND(SEQUENCE, PUTFH, LAYOUTGET)
        // iomode 1=READ, 2=RW — use matching access mode to avoid NFS4ERR_OPENMODE.
        let access = match iomode {
            IoMode::Read => AccessMode::Read,
            IoMode::ReadWrite => AccessMode::Write,
        };
        let cached = self.layout_manager.get_layout(fh).await;
        let sid = self
            .state
            .has_open(fh, access)
            .await
            .unwrap_or_else(StateId::anonymous);
        let request_stateid = cached
            .as_ref()
            .map(|layout| layout.stateid)
            .unwrap_or(sid.raw);
        let result = self
            .compound("layoutget", |b| {
                b.require_generation(sid.generation).putfh(fh).layoutget(
                    false, // signal_layout_avail
                    1,     // LAYOUT4_NFSV4_1_FILES
                    iomode as u32,
                    offset,
                    u64::MAX - offset,
                    0, // min_length
                    &request_stateid,
                    1024 * 1024, // max_count (1 MiB)
                )
            })
            .await;

        match result {
            Ok(resp) => {
                // LAYOUTGET result is after SEQUENCE=0, PUTFH=1 → index 2
                let op = resp.op_ok(2).ok()?;
                let mut data = op.data.clone();
                let mut layout = super::layout::decode_layoutget_response(&mut data).ok()?;
                layout.generation = resp.session_generation;
                let accepted = if cached.is_some() {
                    self.layout_manager.merge_layout(fh, layout.clone()).await;
                    self.layout_manager.get_layout(fh).await.is_some()
                } else {
                    self.layout_manager
                        .store_layout_at(fh, resp.session_generation, layout.clone())
                        .await
                };
                if accepted {
                    // Only an accepted layout may populate generation-owned caches.
                    self.fetch_devices_for_layout(&layout).await;
                    self.layout_manager.get_layout(fh).await
                } else {
                    debug!(
                        response_generation = resp.session_generation,
                        active_generation = self.layout_manager.generation(),
                        "discarding stale LAYOUTGET response"
                    );
                    None
                }
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
                        b.require_generation(layout.generation)
                            .putrootfh()
                            .getdeviceinfo(device_id, 1, 1024 * 1024)
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
                                if !self
                                    .layout_manager
                                    .store_device_at(*device_id, layout.generation, info)
                                    .await
                                {
                                    debug!(
                                        layout_generation = layout.generation,
                                        "discarding stale GETDEVICEINFO response"
                                    );
                                }
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
        generation: u64,
        offset: u64,
        count: u32,
    ) -> Result<CompoundResponse> {
        if ds_addr == self.server_addr {
            return self
                .compound_data("ds-read-mds", count as usize, |b| {
                    b.require_generation(generation)
                        .putfh(ds_fh)
                        .read(stateid, offset, count)
                })
                .await;
        }
        let ds = self
            .layout_manager
            .get_data_server(ds_addr, &self.auth, &self.client_identity, generation)
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
                    .get_data_server(ds_addr, &self.auth, &self.client_identity, generation)
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
        generation: u64,
        ds_off: u64,
        data: Bytes,
    ) -> Result<CompoundResponse> {
        let len = data.len() as u32;
        if ds_addr == self.server_addr {
            return self
                .compound_write("ds-write-mds", data, |b| {
                    b.require_generation(generation)
                        .putfh(ds_fh)
                        .write_header(stateid, ds_off, 2 /* FILE_SYNC4 */, len)
                })
                .await;
        }
        let ds = self
            .layout_manager
            .get_data_server(ds_addr, &self.auth, &self.client_identity, generation)
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
                    .get_data_server(ds_addr, &self.auth, &self.client_identity, generation)
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
        let layout = self.get_or_fetch_layout(fh, IoMode::Read, offset).await?;
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
                        .ds_read_chunk(
                            ds_addr,
                            &ds_fh,
                            &io_stateid,
                            layout.generation,
                            chunk_ds_offset,
                            chunk_len,
                        )
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
                if layout.generation != self.layout_manager.generation() {
                    return Some(Err(NfsError::Rpc(
                        "discarding pNFS READ result from stale session generation".to_string(),
                    )));
                }
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
    /// Returns `NotAttempted` only while MDS fallback is provably safe. After
    /// any DS batch starts, failures are returned as an uncertain attempted
    /// mutation and must be verified by the migration consumer.
    pub(crate) async fn pnfs_write(
        &self,
        fh: &Bytes,
        offset: u64,
        data: Bytes,
    ) -> PnfsWriteOutcome {
        let Some(layout) = self
            .get_or_fetch_layout(fh, IoMode::ReadWrite, offset)
            .await
        else {
            return PnfsWriteOutcome::NotAttempted;
        };
        let Some(seg) = find_covering_segment(&layout, offset) else {
            return PnfsWriteOutcome::NotAttempted;
        };
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
                _ => return PnfsWriteOutcome::NotAttempted,
            };

        if stripe_unit == 0 || fh_list.is_empty() {
            return PnfsWriteOutcome::NotAttempted;
        }
        let Some(device) = self.layout_manager.get_device(&device_id).await else {
            return PnfsWriteOutcome::NotAttempted;
        };
        if device.ds_addrs.len() < fh_list.len() {
            return PnfsWriteOutcome::NotAttempted;
        }
        // 退化设备（DS == MDS）：DS 路径无收益，回退 MDS I/O
        if self.device_degenerate(&device) {
            return PnfsWriteOutcome::NotAttempted;
        }
        // DS 已知不可达：回退 MDS I/O（layout 保留，不再反复尝试）
        if self.device_ds_unreachable(&device).await {
            return PnfsWriteOutcome::NotAttempted;
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

        // Resolve the complete write plan before any DS mutation. Bytes::slice
        // keeps stripe payloads zero-copy.
        let writes = match chunks
            .iter()
            .enumerate()
            .map(|(stripe_index, chunk)| {
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
                Ok::<PlannedDsWrite, NfsError>(PlannedDsWrite {
                    stripe_index,
                    ds_fh: ds_fh_res?,
                    ds_addr: ds_addr_res?,
                    ds_offset: chunk.ds_offset,
                    data: chunk_data,
                })
            })
            .collect::<Result<Vec<_>>>()
        {
            Ok(writes) => writes,
            Err(error) => {
                debug!(error = %error, "pNFS WRITE plan invalid before send; using MDS");
                return PnfsWriteOutcome::NotAttempted;
            }
        };

        // Phase 1: establish every required DS session before transmitting any
        // WRITE. A failure here proves that this logical write made no DS
        // mutation, so MDS fallback is safe.
        if let Err(error) = self
            .preflight_write_data_servers(&writes, layout.generation)
            .await
        {
            debug!(error = %error, "pNFS DS preflight failed before send; using MDS");
            return PnfsWriteOutcome::NotAttempted;
        }

        // Phase 2: after this boundary, any error is potentially post-send and
        // must remain uncertain rather than falling back to MDS.
        let futures: Vec<_> = writes
            .into_iter()
            .map(|write| {
                let stripe_index = write.stripe_index;
                let ds_addr = write.ds_addr;
                let future = async move {
                    let resp = self
                        .ds_write_chunk(
                            write.ds_addr,
                            &write.ds_fh,
                            &io_stateid,
                            layout.generation,
                            write.ds_offset,
                            write.data,
                        )
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
                };
                (stripe_index, ds_addr, future)
            })
            .collect();

        let completions = settle_ds_batch(futures).await;
        if completions
            .iter()
            .all(|completion| completion.result.is_ok())
        {
            let results: Vec<_> = completions
                .into_iter()
                .filter_map(|completion| completion.result.ok())
                .collect();
            if layout.generation != self.layout_manager.generation() {
                // This is aggregate pNFS batch context, so slot/sequence are
                // intentionally zero rather than claiming one DS request.
                let active_session = self.session_holder.get().await;
                let context = RequestContext {
                    operation: "pnfs_write".to_string(),
                    session_id: *active_session.id(),
                    slot_id: 0,
                    sequence_id: 0,
                };
                return PnfsWriteOutcome::Attempted(Err(uncertain_pnfs_write(
                    context,
                    NfsError::Rpc(
                        "pNFS WRITE outcome crossed a session generation boundary".to_string(),
                    ),
                )));
            }
            let total: u32 = results.iter().map(|(n, _)| n).sum();
            let needs_commit = results.iter().any(|(_, c)| *c);
            // RFC 5661 §18.42.3：LAYOUTCOMMIT 不必每次 WRITE 后发，只需在
            // LAYOUTRETURN/CLOSE 前提交。这里仅累积 dirty 范围，由
            // flush_layoutcommit 在 close/layoutreturn 时一次性发送，
            // 避免每个 wsize 块一次串行 MDS RTT。
            if data_len > 0 {
                self.layout_manager
                    .mark_dirty_at(fh, layout.generation, offset, offset + data_len as u64)
                    .await;
            }
            // RFC 5661 §18.32.3: if any DS downgraded write stability, COMMIT to MDS.
            if needs_commit {
                let _ = self.commit(fh.clone(), offset, total).await;
            }
            PnfsWriteOutcome::Attempted(Ok(total))
        } else {
            if data_len > 0 {
                self.layout_manager
                    .mark_dirty_at(fh, layout.generation, offset, offset + data_len as u64)
                    .await;
            }
            if completions.iter().any(|completion| {
                matches!(
                    completion.result,
                    Err(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_STALE))
                )
            }) {
                self.layout_manager.remove_layout(fh).await;
                self.layout_manager.invalidate_dirty(fh).await;
            }
            let diagnostic = ds_batch_diagnostic(&completions);
            // Preserve hot-path performance: aggregate diagnostic context
            // is only materialized when the DS batch actually fails.
            let active_session = self.session_holder.get().await;
            let context = RequestContext {
                operation: "pnfs_write".to_string(),
                session_id: *active_session.id(),
                slot_id: 0,
                sequence_id: 0,
            };
            debug!(
                diagnostic,
                "pNFS write result is uncertain; refusing MDS fallback"
            );
            PnfsWriteOutcome::Attempted(Err(uncertain_pnfs_write(
                context,
                NfsError::Rpc(format!("pNFS DS WRITE results: {diagnostic}")),
            )))
        }
    }

    // ─── pNFS Layout Commit ──────────────────────────────────────────────

    /// Commit a versioned snapshot of the accumulated dirty range. The range
    /// remains pending across transport errors, operation errors, and task
    /// cancellation, and is acknowledged only after authoritative success.
    pub(crate) async fn flush_layoutcommit(&self, fh: &Bytes) -> Result<()> {
        let Some(dirty) = self.layout_manager.snapshot_dirty(fh).await else {
            return Ok(());
        };
        let Some(layout) = self.layout_manager.get_layout(fh).await else {
            return Err(NfsError::Rpc(
                "cannot LAYOUTCOMMIT dirty range without an active layout".to_string(),
            ));
        };
        let response = self
            .compound("layoutcommit", |b| {
                b.putfh(fh).layoutcommit(
                    dirty.start,
                    dirty.end - dirty.start,
                    false,
                    &layout.stateid,
                    Some(dirty.end - 1),
                    1, // LAYOUT4_NFSV4_1_FILES
                )
            })
            .await?;
        response.op_ok(1)?; // PUTFH
        response.op_ok(2)?; // LAYOUTCOMMIT
        if !self.layout_manager.acknowledge_dirty(fh, dirty).await {
            return Err(NfsError::Rpc(
                "pNFS dirty range changed during LAYOUTCOMMIT; retry before CLOSE".to_string(),
            ));
        }
        Ok(())
    }

    // ─── pNFS Layout Return ──────────────────────────────────────────────

    /// Return a layout to the metadata server (LAYOUTRETURN4_FILE).
    /// Removes the layout from the local cache and notifies the server.
    /// A failed commit/return is propagated so CLOSE cannot release state while
    /// layout changes are still pending.
    pub(crate) async fn layoutreturn_file(&self, fh: &Bytes) -> Result<()> {
        // RFC 5661 §18.42.3：LAYOUTCOMMIT 必须在 LAYOUTRETURN 之前
        self.flush_layoutcommit(fh).await?;
        let layout = match self.layout_manager.get_layout(fh).await {
            Some(l) => l,
            None => return Ok(()),
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
                    iomode as u32,
                    1,                     // LAYOUTRETURN4_FILE
                    0,                     // offset = whole file
                    0xFFFF_FFFF_FFFF_FFFF, // length = whole file
                    &layout.stateid,
                )
            })
            .await;
        match result {
            Ok(resp) => {
                resp.op_ok(1)?;
                resp.op_ok(2)?;
            }
            Err(e) => return Err(e),
        }
        self.layout_manager.remove_layout(fh).await;
        Ok(())
    }

    pub(crate) async fn refresh_layout_for_write(&self, fh: &Bytes, offset: u64) -> Result<()> {
        if self.layout_manager.get_layout(fh).await.is_none()
            || !self.layout_manager.layout_refresh_due(fh, offset).await
        {
            return Ok(());
        }

        let _io_guard = self.layout_manager.write_file_io(fh).await;
        if self.layout_manager.get_layout(fh).await.is_some()
            && self.layout_manager.layout_refresh_due(fh, offset).await
        {
            self.flush_layoutcommit(fh).await?;
            if self
                .fetch_layout(fh, IoMode::ReadWrite, offset, true)
                .await
                .is_some()
            {
                self.layout_manager.record_layout_refresh(fh, offset).await;
            }
        }
        Ok(())
    }

    /// Return all cached layouts to the server (used during umount).
    pub(crate) async fn layoutreturn_all(&self) -> Result<()> {
        let layouts = self.layout_manager.all_layouts().await;
        for (fh, _) in layouts {
            let _io_guard = self.layout_manager.write_file_io(&fh).await;
            self.layoutreturn_file(&fh).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs41::layout::{IoMode, Layout, LayoutContent, LayoutSegment, LayoutType};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type TestDsFuture = Pin<Box<dyn Future<Output = Result<u32>> + Send>>;

    #[test]
    fn find_covering_segment_whole_file() {
        let layout = Layout {
            generation: 1,
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
            generation: 1,
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
    fn find_covering_segment_handles_a_range_ending_past_u64_max() {
        let layout = Layout {
            generation: 1,
            stateid: [0; 16],
            return_on_close: false,
            segments: vec![LayoutSegment {
                offset: u64::MAX - 10,
                length: 20,
                iomode: IoMode::ReadWrite,
                layout_type: LayoutType::NfsV41Files,
                content: LayoutContent::Opaque(Bytes::new()),
            }],
        };

        assert!(find_covering_segment(&layout, u64::MAX).is_some());
    }

    #[test]
    fn find_covering_segment_empty() {
        let layout = Layout {
            generation: 1,
            stateid: [0u8; 16],
            return_on_close: false,
            segments: vec![],
        };
        assert!(find_covering_segment(&layout, 0).is_none());
    }

    #[test]
    fn find_covering_segment_multiple() {
        let layout = Layout {
            generation: 1,
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

    #[test]
    fn attempted_ds_error_is_uncertain_and_requires_verification() {
        let context = RequestContext {
            operation: "pnfs_write".to_string(),
            session_id: [7; 16],
            slot_id: 0,
            sequence_id: 0,
        };
        let error = uncertain_pnfs_write(
            context,
            NfsError::Rpc("DS connection reset after send".to_string()),
        );
        let outcome = error
            .operation_outcome()
            .expect("attempted pNFS WRITE must have structured guidance");
        assert_eq!(outcome.outcome, OperationOutcome::Uncertain);
        assert_eq!(outcome.operation_class, OperationClass::ReplaySensitive);
        assert_eq!(outcome.recovery, RecoveryAction::VerifyThenResume);
        assert_eq!(outcome.context().operation, "pnfs_write");
    }

    #[tokio::test]
    async fn stale_ds_write_evicts_layout_and_invalidates_old_dirty_range() {
        let manager = LayoutManager::new(true);
        let fh = Bytes::from_static(b"multipart-file");
        let layout = Layout {
            generation: manager.generation(),
            stateid: [7; 16],
            return_on_close: false,
            segments: vec![],
        };
        manager.store_layout(&fh, layout).await;
        manager.mark_dirty(&fh, 0, 4096).await;

        invalidate_layout_after_ds_error(
            &manager,
            &fh,
            &NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_STALE),
        )
        .await;

        assert!(manager.get_layout(&fh).await.is_none());
        assert_eq!(manager.take_dirty(&fh).await, None);
    }

    #[tokio::test]
    async fn transport_ds_write_error_retains_layout_for_verification() {
        let manager = LayoutManager::new(true);
        let fh = Bytes::from_static(b"ordinary-file");
        let layout = Layout {
            generation: manager.generation(),
            stateid: [8; 16],
            return_on_close: false,
            segments: vec![],
        };
        manager.store_layout(&fh, layout).await;

        invalidate_layout_after_ds_error(
            &manager,
            &fh,
            &NfsError::Rpc("connection reset after send".to_string()),
        )
        .await;

        assert!(manager.get_layout(&fh).await.is_some());
    }

    #[tokio::test]
    async fn ds_batch_waits_for_success_when_failure_completes_first() {
        let (failure_seen_tx, failure_seen_rx) = tokio::sync::oneshot::channel();
        let (release_success_tx, release_success_rx) = tokio::sync::oneshot::channel();
        let success_count = Arc::new(AtomicUsize::new(0));
        let success_count_task = Arc::clone(&success_count);
        let futures: Vec<(usize, SocketAddr, TestDsFuture)> = vec![
            (
                0,
                "192.0.2.10:2049".parse().unwrap(),
                Box::pin(async move {
                    let _ = failure_seen_tx.send(());
                    Err(NfsError::Rpc("DS 0 failed".to_string()))
                }),
            ),
            (
                1,
                "192.0.2.11:2049".parse().unwrap(),
                Box::pin(async move {
                    let _ = release_success_rx.await;
                    success_count_task.fetch_add(1, Ordering::SeqCst);
                    Ok(17)
                }),
            ),
        ];

        let batch = tokio::spawn(settle_ds_batch(futures));
        assert!(failure_seen_rx.await.is_ok());
        tokio::task::yield_now().await;
        assert!(
            !batch.is_finished(),
            "early DS failure cancelled a sibling WRITE"
        );
        assert_eq!(success_count.load(Ordering::SeqCst), 0);
        assert!(release_success_tx.send(()).is_ok());
        let completions = batch.await.unwrap();
        assert!(
            matches!(completions[0].result, Err(NfsError::Rpc(ref message)) if message == "DS 0 failed")
        );
        assert!(matches!(completions[1].result, Ok(17)));
        let diagnostic = ds_batch_diagnostic(&completions);
        assert_eq!(
            diagnostic,
            "stripe=0 ds=192.0.2.10:2049 attempted=true outcome=error error=RPC error: DS 0 failed; stripe=1 ds=192.0.2.11:2049 attempted=true outcome=success"
        );
        assert!(!diagnostic.contains("file-handle"));
        assert!(!diagnostic.contains("payload"));
        assert_eq!(success_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn ds_batch_waits_for_failure_when_success_completes_first() {
        let (success_seen_tx, success_seen_rx) = tokio::sync::oneshot::channel();
        let (release_failure_tx, release_failure_rx) = tokio::sync::oneshot::channel();
        let futures: Vec<(usize, SocketAddr, TestDsFuture)> = vec![
            (
                0,
                "192.0.2.10:2049".parse().unwrap(),
                Box::pin(async move {
                    let _ = success_seen_tx.send(());
                    Ok(23)
                }),
            ),
            (
                1,
                "192.0.2.11:2049".parse().unwrap(),
                Box::pin(async move {
                    let _ = release_failure_rx.await;
                    Err(NfsError::Rpc("DS 1 failed".to_string()))
                }),
            ),
        ];

        let batch = tokio::spawn(settle_ds_batch(futures));
        assert!(success_seen_rx.await.is_ok());
        tokio::task::yield_now().await;
        assert!(
            !batch.is_finished(),
            "successful stripe hid a pending DS WRITE"
        );
        assert!(release_failure_tx.send(()).is_ok());
        let completions = batch.await.unwrap();
        assert!(matches!(completions[0].result, Ok(23)));
        assert!(
            matches!(completions[1].result, Err(NfsError::Rpc(ref message)) if message == "DS 1 failed")
        );
    }

    #[tokio::test]
    async fn cancelling_ds_batch_drops_every_pending_write() {
        struct DropCount(Arc<AtomicUsize>);
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let futures: Vec<(usize, SocketAddr, TestDsFuture)> = (0..2)
            .map(|stripe_index| {
                let started = Arc::clone(&started);
                let guard = DropCount(Arc::clone(&dropped));
                let future = Box::pin(async move {
                    let _guard = guard;
                    started.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<Result<u32>>().await
                }) as TestDsFuture;
                (stripe_index, "192.0.2.10:2049".parse().unwrap(), future)
            })
            .collect();

        let batch = tokio::spawn(settle_ds_batch(futures));
        while started.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
        batch.abort();
        let _ = batch.await;
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }
}
