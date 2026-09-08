//! pNFS I/O: parallel reads and writes to data servers (RFC 5661 §12-13).
//!
//! This module adds pNFS layout-aware I/O methods to `Mount41`. When a file
//! has a granted layout, reads and writes are striped across data servers in
//! parallel. If pNFS is unavailable or fails, callers fall back to MDS I/O.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::{Buf, Bytes, BytesMut};
use tracing::{debug, info};

use super::Nfs4ErrorCode;
use super::compound::CompoundResponse;
#[cfg(test)]
use super::layout::LayoutManager;
use super::layout::{IoMode, Layout, LayoutContent, LayoutSegment};
use super::mount::{Mount41, effective_rsize, effective_wsize};
use super::state::{AccessMode, StateId};
use crate::NFSVersion;
use crate::error::{
    NfsError, OperationClass, OperationOutcome, OperationOutcomeError, RecoveryAction,
    RequestContext, Result,
};
use crate::mount::write_verifier_changed;
use crate::nfs4::fastxdr::nfsstat4;

/// Whether pNFS WRITE transmitted a DS mutation.
///
/// Only `NotAttempted` permits the caller to fall back to an MDS WRITE. Once a
/// DS batch starts, every result remains on the `Attempted` path so an
/// ambiguous mutation can never be silently overwritten through the MDS.
pub(crate) enum PnfsWriteOutcome {
    NotAttempted,
    Attempted(Result<crate::WriteOutcome>),
}

struct PlannedDsWrite {
    file_offset: u64,
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

/// One data server's WRITE reply plus what a follow-up COMMIT needs.
#[derive(Clone, Debug)]
struct DsWriteReply {
    file_offset: u64,
    written: u32,
    /// FILE_SYNC needs no further data or layout commit. DATA_SYNC needs
    /// metadata synchronization; UNSTABLE also needs data COMMIT.
    committed: crate::WriteCommitted,
    data: Bytes,
    verifier: [u8; 8],
    ds_addr: SocketAddr,
    ds_fh: Bytes,
    ds_offset: u64,
}

/// Retained by both the caller and layout manager until data and metadata
/// are committed. Recall/close can finish pending writes before returning a layout.
#[derive(Debug)]
pub(crate) struct PendingWrite {
    fh: Bytes,
    offset: u64,
    count: u32,
    generation: u64,
    commit_thru_mds: bool,
    replies: Vec<DsWriteReply>,
    done: AtomicBool,
    failed: AtomicBool,
}

impl PendingWrite {
    fn committed(&self) -> crate::WriteCommitted {
        self.replies
            .iter()
            .map(|reply| reply.committed)
            .min()
            .unwrap_or(crate::WriteCommitted::Unstable)
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
}

/// A commit is failed only if it exits without settling the retained writes.
/// Drop also records cancellation, while the caller still holds its I/O gate.
struct PendingCommitGuard<'a> {
    pending: &'a [&'a Arc<PendingWrite>],
}

impl<'a> PendingCommitGuard<'a> {
    fn new(pending: &'a [&'a Arc<PendingWrite>]) -> Self {
        Self { pending }
    }
}

impl Drop for PendingCommitGuard<'_> {
    fn drop(&mut self) {
        for write in self.pending {
            if !write.is_done() {
                write.failed.store(true, Ordering::Release);
            }
        }
    }
}

#[derive(Debug)]
struct CommitTarget {
    offset: u64,
    count: u32,
    expected: Vec<[u8; 8]>,
}
#[derive(Debug, Default)]
struct CommitPlan {
    mds: Option<CommitTarget>,
    ds: Vec<(SocketAddr, Bytes, CommitTarget)>,
    layout: Option<(u64, u64)>,
}

fn plan_commit_batch(pending: &[&Arc<PendingWrite>], replies: &[Vec<DsWriteReply>]) -> CommitPlan {
    let mut mds_expected = Vec::new();
    let mut mds_start = u64::MAX;
    let mut mds_end = 0;
    let mut ds_groups: BTreeMap<(SocketAddr, Bytes), Vec<&DsWriteReply>> = BTreeMap::new();
    let mut layout_needed = false;
    for (write, batch) in pending.iter().zip(replies) {
        if write.commit_thru_mds {
            for reply in batch
                .iter()
                .filter(|r| r.committed != crate::WriteCommitted::FileSync)
            {
                mds_expected.push(reply.verifier);
            }
            mds_start = mds_start.min(write.offset);
            mds_end = mds_end.max(write.offset + u64::from(write.count));
        } else {
            layout_needed |= batch
                .iter()
                .any(|r| r.committed != crate::WriteCommitted::FileSync);
            for reply in batch
                .iter()
                .filter(|r| r.committed == crate::WriteCommitted::Unstable)
            {
                ds_groups
                    .entry((reply.ds_addr, reply.ds_fh.clone()))
                    .or_default()
                    .push(reply);
            }
        }
    }
    let mds = if mds_expected.is_empty() {
        None
    } else {
        let (offset, count) = wire_commit_range(mds_start, mds_end);
        Some(CommitTarget {
            offset,
            count,
            expected: mds_expected,
        })
    };
    let ds = ds_groups
        .into_iter()
        .map(|((addr, fh), group)| {
            let start = group.iter().map(|r| r.ds_offset).min().unwrap_or(0);
            let end = group
                .iter()
                .map(|r| r.ds_offset + u64::from(r.written))
                .max()
                .unwrap_or(0);
            let (offset, count) = wire_commit_range(start, end);
            (
                addr,
                fh,
                CommitTarget {
                    offset,
                    count,
                    expected: group.iter().map(|r| r.verifier).collect(),
                },
            )
        })
        .collect();
    let layout = if layout_needed {
        Some((
            pending.iter().map(|w| w.offset).min().unwrap_or(0),
            pending
                .iter()
                .map(|w| w.offset + u64::from(w.count))
                .max()
                .unwrap_or(0),
        ))
    } else {
        None
    };
    CommitPlan { mds, ds, layout }
}

#[async_trait::async_trait]
trait CommitIo: Sync {
    async fn mds_commit(&self, fh: &Bytes, offset: u64, count: u32) -> Result<[u8; 8]>;
    async fn ds_commit(
        &self,
        addr: SocketAddr,
        fh: &Bytes,
        generation: u64,
        offset: u64,
        count: u32,
    ) -> Result<[u8; 8]>;
    async fn layout_commit(&self, fh: &Bytes, generation: u64, start: u64, end: u64) -> Result<()>;
}

#[async_trait::async_trait]
impl CommitIo for Mount41 {
    async fn mds_commit(&self, fh: &Bytes, offset: u64, count: u32) -> Result<[u8; 8]> {
        self.commit_with_verifier(fh.clone(), offset, count)
            .await?
            .ok_or_else(|| NfsError::Rpc("missing MDS commit verifier".into()))
    }
    async fn ds_commit(
        &self,
        addr: SocketAddr,
        fh: &Bytes,
        generation: u64,
        offset: u64,
        count: u32,
    ) -> Result<[u8; 8]> {
        self.ds_commit_chunk(addr, fh, generation, offset, count)
            .await
    }
    async fn layout_commit(&self, fh: &Bytes, generation: u64, start: u64, end: u64) -> Result<()> {
        self.layout_manager
            .mark_dirty_at(fh, generation, start, end)
            .await;
        self.layoutcommit_dirty(fh).await?;
        Ok(())
    }
}

async fn execute_commit_plan(
    io: &impl CommitIo,
    fh: &Bytes,
    generation: u64,
    plan: &CommitPlan,
) -> Result<()> {
    if let Some(target) = &plan.mds {
        let actual = io.mds_commit(fh, target.offset, target.count).await?;
        check_commit_verifier(target.expected.iter(), actual)?;
    }
    for (addr, ds_fh, target) in &plan.ds {
        let actual = io
            .ds_commit(*addr, ds_fh, generation, target.offset, target.count)
            .await?;
        check_commit_verifier(target.expected.iter(), actual)?;
    }
    if let Some((start, end)) = plan.layout {
        io.layout_commit(fh, generation, start, end).await?;
    }
    Ok(())
}

/// RFC 5661 §13.7: a COMMIT verifier that differs from any WRITE verifier it
/// covers means the server may have lost the uncommitted data.
fn check_commit_verifier<'a>(
    written: impl IntoIterator<Item = &'a [u8; 8]>,
    committed: [u8; 8],
) -> Result<()> {
    if written.into_iter().any(|verifier| *verifier != committed) {
        return Err(write_verifier_changed(NFSVersion::NFSv4p1));
    }
    Ok(())
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
        let count = count.min(effective_rsize(
            u64::from(self.rsize),
            ds.session.max_response_size(),
        )?);
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
                let count = count.min(effective_rsize(
                    u64::from(self.rsize),
                    ds.session.max_response_size(),
                )?);
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
                        .write_header(stateid, ds_off, 0 /* UNSTABLE4 */, len)
                })
                .await;
        }
        let ds = self
            .layout_manager
            .get_data_server(ds_addr, &self.auth, &self.client_identity, generation)
            .await?;
        let len = data
            .len()
            .min(effective_wsize(u64::from(self.wsize), ds.session.max_request_size())? as usize)
            as u32;
        let data = data.slice(..len as usize);
        let result = Mount41::compound_ds_write(&ds, &self.auth, "ds-write", data.clone(), |b| {
            b.putfh(ds_fh)
                .write_header(stateid, ds_off, 0 /* UNSTABLE4 */, len)
        })
        .await;
        match result {
            Err(NfsError::Nfs4(nfsstat4::NFS4ERR_BADSESSION | nfsstat4::NFS4ERR_DEADSESSION)) => {
                self.layout_manager.remove_data_server(ds_addr).await;
                let ds = self
                    .layout_manager
                    .get_data_server(ds_addr, &self.auth, &self.client_identity, generation)
                    .await?;
                let len = data.len().min(effective_wsize(
                    u64::from(self.wsize),
                    ds.session.max_request_size(),
                )? as usize) as u32;
                let data = data.slice(..len as usize);
                Mount41::compound_ds_write(&ds, &self.auth, "ds-write", data, |b| {
                    b.putfh(ds_fh)
                        .write_header(stateid, ds_off, 0 /* UNSTABLE4 */, len)
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
                    let mut combined = BytesMut::with_capacity(chunk_len as usize);
                    while combined.len() < chunk_len as usize {
                        let remaining = chunk_len as usize - combined.len();
                        let resp = self
                            .ds_read_chunk(
                                ds_addr,
                                &ds_fh,
                                &io_stateid,
                                layout.generation,
                                chunk_ds_offset + combined.len() as u64,
                                remaining as u32,
                            )
                            .await?;
                        resp.op_ok(1)?;
                        let mut data = resp.op_ok(2)?.data.clone();
                        if data.remaining() < 8 {
                            return Err(NfsError::Xdr("DS READ result too short".into()));
                        }
                        let eof = data.get_u32() != 0;
                        let data_len = data.get_u32() as usize;
                        if data_len > remaining || data.remaining() < data_len {
                            return Err(NfsError::Xdr("invalid DS READ data length".into()));
                        }
                        combined.extend_from_slice(&data[..data_len]);
                        if eof {
                            break;
                        }
                        if data_len == 0 {
                            return Err(NfsError::Rpc("DS READ made no progress".into()));
                        }
                    }
                    let short = combined.len() < chunk_len as usize;
                    Ok::<(Bytes, bool), NfsError>((combined.freeze(), short))
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
                    Some(Ok(results.into_iter().next().unwrap_or_default().0))
                } else {
                    // Concatenate stripe results in order
                    let total_len: usize = results.iter().map(|(b, _)| b.len()).sum();
                    let mut combined = BytesMut::with_capacity(total_len);
                    for (chunk_data, short) in results {
                        combined.extend_from_slice(&chunk_data);
                        if short {
                            break;
                        }
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
        let (
            device_id,
            stripe_unit,
            is_dense,
            commit_thru_mds,
            first_stripe_index,
            pattern_offset,
            fh_list,
        ) = match &seg.content {
            LayoutContent::FilesLayout {
                device_id,
                stripe_unit,
                is_dense,
                commit_thru_mds,
                first_stripe_index,
                pattern_offset,
                fh_list,
            } => (
                *device_id,
                *stripe_unit,
                *is_dense,
                *commit_thru_mds,
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

        let covered = seg.offset.saturating_add(seg.length).saturating_sub(offset);
        let data = data.slice(
            ..data
                .len()
                .min(usize::try_from(covered).unwrap_or(usize::MAX)),
        );
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
                    file_offset: chunk.file_offset,
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
                    self.write_ds_complete(&write, &io_stateid, layout.generation)
                        .await
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
                    protocol: crate::NFSVersion::NFSv4p1,
                    request_id: Some(crate::error::RequestId::nfs41(*active_session.id(), 0, 0)),
                };
                return PnfsWriteOutcome::Attempted(Err(uncertain_pnfs_write(
                    context,
                    NfsError::Rpc(
                        "pNFS WRITE outcome crossed a session generation boundary".to_string(),
                    ),
                )));
            }
            let total: u32 = results.iter().map(|reply| reply.written).sum();
            let pending = Arc::new(PendingWrite {
                fh: fh.clone(),
                offset,
                count: total,
                generation: layout.generation,
                commit_thru_mds,
                replies: results,
                done: AtomicBool::new(false),
                failed: AtomicBool::new(false),
            });
            // Register before releasing the file I/O guard, so recall cannot
            // return a layout while its DS writes are still uncommitted.
            self.layout_manager
                .register_write(fh, pending.clone())
                .await;
            PnfsWriteOutcome::Attempted(Ok(crate::WriteOutcome {
                count: total,
                committed: pending.committed(),
                verifier: None,
                pnfs: Some(pending),
            }))
        } else {
            if completions.iter().any(|completion| {
                matches!(
                    completion.result,
                    Err(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_STALE))
                )
            }) {
                self.layout_manager.remove_layout(fh).await;
                self.layout_manager.invalidate_dirty(fh).await;
            }
            let known: Vec<_> = completions
                .iter()
                .filter_map(|c| c.result.as_ref().ok().cloned())
                .collect();
            if !known.is_empty() {
                self.layout_manager
                    .register_write(
                        fh,
                        Arc::new(PendingWrite {
                            fh: fh.clone(),
                            offset,
                            count: (known
                                .iter()
                                .map(|r| r.file_offset + u64::from(r.written))
                                .max()
                                .unwrap_or(offset)
                                - offset) as u32,
                            generation: layout.generation,
                            commit_thru_mds,
                            replies: known,
                            done: AtomicBool::new(false),
                            failed: AtomicBool::new(true),
                        }),
                    )
                    .await;
            }
            let diagnostic = ds_batch_diagnostic(&completions);
            // Preserve hot-path performance: aggregate diagnostic context
            // is only materialized when the DS batch actually fails.
            let active_session = self.session_holder.get().await;
            let context = RequestContext {
                operation: "pnfs_write".to_string(),
                protocol: crate::NFSVersion::NFSv4p1,
                request_id: Some(crate::error::RequestId::nfs41(*active_session.id(), 0, 0)),
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

    /// Finish short writes on one stripe before reporting a contiguous logical
    /// byte count. Returning the sum of short stripes would conceal holes.
    async fn write_ds_complete(
        &self,
        write: &PlannedDsWrite,
        stateid: &[u8; 16],
        generation: u64,
    ) -> Result<DsWriteReply> {
        for attempt in 0..3 {
            match self.write_ds_attempt(write, stateid, generation).await {
                Err(error)
                    if attempt < 2
                        && error
                            .operation_outcome()
                            .is_some_and(|o| o.context().operation == "write_verifier") =>
                {
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("last stripe write attempt always returns")
    }

    async fn write_ds_attempt(
        &self,
        write: &PlannedDsWrite,
        stateid: &[u8; 16],
        generation: u64,
    ) -> Result<DsWriteReply> {
        let mut done = 0usize;
        let mut level = 2;
        let mut verifier = None;
        while done < write.data.len() {
            let resp = self
                .ds_write_chunk(
                    write.ds_addr,
                    &write.ds_fh,
                    stateid,
                    generation,
                    write.ds_offset + done as u64,
                    write.data.slice(done..),
                )
                .await?;
            resp.op_ok(1)?;
            let mut d = resp.op_ok(2)?.data.clone();
            if d.remaining() < 16 {
                return Err(NfsError::Xdr("DS WRITE result too short".into()));
            }
            let n = d.get_u32() as usize;
            let committed = d.get_u32();
            if n == 0 || n > write.data.len() - done || committed > 2 {
                return Err(NfsError::Rpc("invalid DS WRITE acknowledgement".into()));
            }
            let mut current = [0; 8];
            d.copy_to_slice(&mut current);
            if verifier.is_some_and(|v| v != current) {
                return Err(write_verifier_changed(NFSVersion::NFSv4p1));
            }
            verifier = Some(current);
            level = level.min(committed);
            done += n;
        }
        Ok(DsWriteReply {
            file_offset: write.file_offset,
            written: done as u32,
            committed: crate::WriteCommitted::try_from(level)?,
            verifier: verifier.unwrap_or_default(),
            ds_addr: write.ds_addr,
            ds_fh: write.ds_fh.clone(),
            ds_offset: write.ds_offset,
            data: write.data.clone(),
        })
    }

    /// The caller holds the file I/O gate (shared for ordinary commits,
    /// exclusive for recall/close). DS identities and offsets come from the
    /// original WRITE receipts, never from a refreshed stripe mapping.
    pub(crate) async fn commit_pending_writes(
        &self,
        fh: &Bytes,
        pending: &[Arc<PendingWrite>],
    ) -> Result<()> {
        let _commit_guard =
            crate::fileio::file_gate(self as *const Self as usize, fh.clone(), 1).await;
        let pending: Vec<_> = pending.iter().filter(|w| !w.is_done()).collect();
        let _attempt = PendingCommitGuard::new(&pending);
        if pending.is_empty() {
            return Ok(());
        }
        for write in &pending {
            if write.fh != *fh || write.generation != self.layout_manager.generation() {
                return Err(self
                    .uncertain_after_ds_write(NfsError::Rpc(
                        "pNFS commit crossed layout generation".into(),
                    ))
                    .await);
            }
        }
        let generation = pending[0].generation;
        let mut replies: Vec<Vec<DsWriteReply>> =
            pending.iter().map(|w| w.replies.clone()).collect();
        for attempt in 0..3 {
            let result = self
                .commit_ds_batch(fh, &pending, &replies, generation)
                .await;
            match result {
                Ok(()) => {
                    if generation != self.layout_manager.generation() {
                        return Err(self
                            .uncertain_after_ds_write(NfsError::Rpc(
                                "pNFS generation changed during commit".into(),
                            ))
                            .await);
                    }
                    for write in &pending {
                        write.done.store(true, Ordering::Release);
                    }
                    self.layout_manager.prune_completed_writes(fh).await;
                    return Ok(());
                }
                Err(error)
                    if attempt < 2
                        && error
                            .operation_outcome()
                            .is_some_and(|o| o.context().operation == "write_verifier") =>
                {
                    let sid = self
                        .state
                        .has_open(fh, AccessMode::Write)
                        .await
                        .unwrap_or_else(StateId::anonymous);
                    for batch in &mut replies {
                        for reply in batch {
                            let plan = PlannedDsWrite {
                                file_offset: reply.file_offset,
                                stripe_index: 0,
                                ds_fh: reply.ds_fh.clone(),
                                ds_addr: reply.ds_addr,
                                ds_offset: reply.ds_offset,
                                data: reply.data.clone(),
                            };
                            *reply = self
                                .write_ds_complete(&plan, &sid.raw, generation)
                                .await
                                .map_err(|e| {
                                    uncertain_pnfs_write(
                                        RequestContext {
                                            operation: "pnfs_commit".into(),
                                            protocol: NFSVersion::NFSv4p1,
                                            request_id: None,
                                        },
                                        e,
                                    )
                                })?;
                        }
                    }
                }
                Err(error) => return Err(self.uncertain_after_ds_write(error).await),
            }
        }
        unreachable!("bounded commit loop returns on its last attempt")
    }

    async fn commit_ds_batch(
        &self,
        fh: &Bytes,
        pending: &[&Arc<PendingWrite>],
        replies: &[Vec<DsWriteReply>],
        generation: u64,
    ) -> Result<()> {
        let plan = plan_commit_batch(pending, replies);
        execute_commit_plan(self, fh, generation, &plan).await
    }

    /// COMMIT one stripe on its data server (COMPOUND: SEQUENCE, PUTFH, COMMIT)
    /// and return the server's write verifier.
    async fn ds_commit_chunk(
        &self,
        ds_addr: SocketAddr,
        ds_fh: &Bytes,
        generation: u64,
        ds_off: u64,
        count: u32,
    ) -> Result<[u8; 8]> {
        let resp = if ds_addr == self.server_addr {
            self.compound("ds-commit-mds", |b| {
                b.require_generation(generation)
                    .putfh(ds_fh)
                    .commit(ds_off, count)
            })
            .await?
        } else {
            let ds = self
                .layout_manager
                .get_data_server(ds_addr, &self.auth, &self.client_identity, generation)
                .await?;
            Mount41::compound_ds(&ds, &self.auth, "ds-commit", 0, |b| {
                b.putfh(ds_fh).commit(ds_off, count)
            })
            .await?
        };
        resp.op_ok(1)?; // PUTFH
        let commit_op = resp.op_ok(2)?; // COMMIT
        let mut d = commit_op.data.clone();
        if d.remaining() < 8 {
            return Err(NfsError::Xdr("DS COMMIT result too short".to_string()));
        }
        let mut verifier = [0u8; 8];
        d.copy_to_slice(&mut verifier);
        Ok(verifier)
    }

    async fn uncertain_after_ds_write(&self, source: NfsError) -> NfsError {
        let active_session = self.session_holder.get().await;
        let context = RequestContext {
            operation: "pnfs_write".to_string(),
            protocol: NFSVersion::NFSv4p1,
            request_id: Some(crate::error::RequestId::nfs41(*active_session.id(), 0, 0)),
        };
        uncertain_pnfs_write(context, source)
    }

    // ─── pNFS Layout Commit ──────────────────────────────────────────────

    /// Commit a versioned snapshot of the accumulated dirty range. The range
    /// remains pending across transport errors, operation errors, and task
    /// cancellation, and is acknowledged only after authoritative success.
    /// Fails if a concurrent WRITE extended the range meanwhile, because the
    /// caller is about to CLOSE or return the layout.
    pub(crate) async fn flush_layoutcommit(&self, fh: &Bytes) -> Result<()> {
        let pending = self.layout_manager.pending_writes(fh).await;
        self.commit_pending_writes(fh, &pending).await?;
        if self.layoutcommit_dirty(fh).await? {
            Ok(())
        } else {
            Err(NfsError::Rpc(
                "pNFS dirty range changed during LAYOUTCOMMIT; retry before CLOSE".to_string(),
            ))
        }
    }

    /// LAYOUTCOMMIT the current dirty snapshot. Returns whether the snapshot
    /// was acknowledged unchanged; `false` means a concurrent WRITE extended
    /// the range, which stays pending for the next call.
    async fn layoutcommit_dirty(&self, fh: &Bytes) -> Result<bool> {
        let Some(dirty) = self.layout_manager.snapshot_dirty(fh).await else {
            return Ok(true);
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
        Ok(self.layout_manager.acknowledge_dirty(fh, dirty).await)
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

fn wire_commit_range(start: u64, end: u64) -> (u64, u32) {
    u32::try_from(end - start).map_or((0, 0), |count| (start, count))
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

    fn reply(stable: bool, verifier: u8) -> DsWriteReply {
        DsWriteReply {
            file_offset: 0,
            written: 4,
            committed: if stable {
                crate::WriteCommitted::FileSync
            } else {
                crate::WriteCommitted::Unstable
            },
            data: Bytes::from_static(b"data"),
            verifier: [verifier; 8],
            ds_addr: "127.0.0.1:2049".parse().unwrap(),
            ds_fh: Bytes::from_static(b"ds-fh"),
            ds_offset: 0,
        }
    }

    #[derive(Default)]
    struct FakeCommit {
        calls: std::sync::Mutex<Vec<String>>,
        mismatch: bool,
        fail_layout: bool,
    }
    #[async_trait::async_trait]
    impl CommitIo for FakeCommit {
        async fn mds_commit(&self, _fh: &Bytes, offset: u64, count: u32) -> Result<[u8; 8]> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("mds:{offset}:{count}"));
            Ok([if self.mismatch { 99 } else { 1 }; 8])
        }
        async fn ds_commit(
            &self,
            addr: SocketAddr,
            fh: &Bytes,
            _generation: u64,
            offset: u64,
            count: u32,
        ) -> Result<[u8; 8]> {
            self.calls.lock().unwrap().push(format!(
                "ds:{}:{}:{offset}:{count}",
                addr.port(),
                String::from_utf8_lossy(fh)
            ));
            Ok([if self.mismatch {
                99
            } else {
                (addr.port() - 2048) as u8
            }; 8])
        }
        async fn layout_commit(
            &self,
            _fh: &Bytes,
            _generation: u64,
            start: u64,
            end: u64,
        ) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("layout:{start}:{end}"));
            if self.fail_layout {
                Err(NfsError::Rpc("layout failure".into()))
            } else {
                Ok(())
            }
        }
    }
    fn pending(through_mds: bool, offset: u64, replies: Vec<DsWriteReply>) -> Arc<PendingWrite> {
        Arc::new(PendingWrite {
            fh: Bytes::from_static(b"file"),
            offset,
            count: replies.iter().map(|r| r.written).sum(),
            generation: 0,
            commit_thru_mds: through_mds,
            replies,
            done: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        })
    }
    fn test_plan(writes: &[Arc<PendingWrite>]) -> CommitPlan {
        plan_commit_batch(
            &writes.iter().collect::<Vec<_>>(),
            &writes.iter().map(|w| w.replies.clone()).collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn batch_groups_ds_filehandles_and_commits_metadata_last() {
        let mut first = reply(false, 1);
        first.ds_addr.set_port(2049);
        first.ds_offset = 4;
        let mut second = first.clone();
        second.ds_offset = 12;
        let mut third = reply(false, 2);
        third.ds_addr.set_port(2050);
        third.ds_offset = 0;
        let writes = [
            pending(false, 0, vec![first]),
            pending(false, 4, vec![second, third]),
        ];
        let io = FakeCommit::default();
        execute_commit_plan(&io, &writes[0].fh, 0, &test_plan(&writes))
            .await
            .unwrap();
        assert_eq!(
            *io.calls.lock().unwrap(),
            ["ds:2049:ds-fh:4:12", "ds:2050:ds-fh:0:4", "layout:0:12"]
        );
    }

    #[tokio::test]
    async fn commit_through_mds_batches_all_chunks_without_ds_commits() {
        let writes = [
            pending(true, 0, vec![reply(false, 1)]),
            pending(true, 4, vec![reply(false, 1)]),
        ];
        let io = FakeCommit::default();
        execute_commit_plan(&io, &writes[0].fh, 0, &test_plan(&writes))
            .await
            .unwrap();
        assert_eq!(*io.calls.lock().unwrap(), ["mds:0:8"]);
    }

    #[test]
    fn successful_commit_is_not_reported_as_failure_while_in_progress() {
        let write = pending(false, 0, vec![reply(false, 1)]);
        let writes = [&write];
        {
            let _attempt = PendingCommitGuard::new(&writes);
            assert!(
                !write.failed(),
                "an in-progress commit is not a failed commit"
            );
            write.done.store(true, Ordering::Release);
        }
        assert!(write.is_done());
        assert!(!write.failed());
    }

    #[tokio::test]
    async fn cancelled_commit_marks_unsettled_writes_failed() {
        let write = pending(false, 0, vec![reply(false, 1)]);
        let copy = write.clone();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let writes = [&copy];
            let _attempt = PendingCommitGuard::new(&writes);
            entered.send(()).unwrap();
            futures::future::pending::<()>().await;
        });
        waiting.await.unwrap();
        assert!(!write.failed());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(write.failed());
        assert!(!write.is_done());
    }

    #[tokio::test]
    async fn incoming_write_waits_for_healthy_recall_commit() {
        let manager = Arc::new(LayoutManager::new(true));
        let fh = Bytes::from_static(b"recall-commit");
        let write = pending(false, 0, vec![reply(false, 1)]);
        let writes = [&write];
        let recall_guard = manager.write_file_io(&fh).await;
        let attempt = PendingCommitGuard::new(&writes);
        assert!(!write.failed());
        let reader_manager = manager.clone();
        let reader_write = write.clone();
        let mut incoming = tokio::spawn(async move {
            let _guard = reader_manager.read_file_io(&fh).await;
            !reader_write.failed() || reader_write.is_done()
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut incoming)
                .await
                .is_err()
        );
        write.done.store(true, Ordering::Release);
        drop(attempt);
        drop(recall_guard);
        assert!(incoming.await.unwrap());
    }

    #[test]
    fn failed_commit_marks_only_unsettled_writes() {
        let done = pending(false, 0, vec![reply(false, 1)]);
        done.done.store(true, Ordering::Release);
        let unfinished = pending(false, 4, vec![reply(false, 1)]);
        let writes = [&done, &unfinished];
        drop(PendingCommitGuard::new(&writes));
        assert!(!done.failed());
        assert!(unfinished.failed());
    }

    #[test]
    fn outcome_commitment_reports_weakest_ds_reply() {
        use crate::WriteCommitted::*;
        for (levels, expected) in [
            (vec![FileSync, FileSync], FileSync),
            (vec![FileSync, DataSync], DataSync),
            (vec![DataSync, Unstable, FileSync], Unstable),
        ] {
            let replies = levels
                .into_iter()
                .map(|committed| {
                    let mut r = reply(false, 1);
                    r.committed = committed;
                    r
                })
                .collect();
            assert_eq!(pending(false, 0, replies).committed(), expected);
        }
    }

    #[tokio::test]
    async fn data_sync_requires_only_layout_when_not_committing_through_mds() {
        let mut r = reply(false, 1);
        r.committed = crate::WriteCommitted::DataSync;
        let writes = [pending(false, 10, vec![r])];
        let io = FakeCommit::default();
        execute_commit_plan(&io, &writes[0].fh, 0, &test_plan(&writes))
            .await
            .unwrap();
        assert_eq!(*io.calls.lock().unwrap(), ["layout:10:14"]);
    }

    #[tokio::test]
    async fn file_sync_replies_need_no_data_or_layout_commit() {
        for flag in [true, false] {
            let writes = [pending(flag, 0, vec![reply(true, 1)])];
            let io = FakeCommit::default();
            execute_commit_plan(&io, &writes[0].fh, 0, &test_plan(&writes))
                .await
                .unwrap();
            assert!(io.calls.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn verifier_failure_prevents_layout_commit_and_layout_failure_propagates() {
        let mut r = reply(false, 1);
        r.ds_addr.set_port(2049);
        let writes = [pending(false, 0, vec![r])];
        let io = FakeCommit {
            mismatch: true,
            ..Default::default()
        };
        assert!(
            execute_commit_plan(&io, &writes[0].fh, 0, &test_plan(&writes))
                .await
                .is_err()
        );
        assert_eq!(io.calls.lock().unwrap().len(), 1);
        let io = FakeCommit {
            fail_layout: true,
            ..Default::default()
        };
        assert!(
            execute_commit_plan(&io, &writes[0].fh, 0, &test_plan(&writes))
                .await
                .is_err()
        );
        assert_eq!(io.calls.lock().unwrap().len(), 2);
    }

    #[test]
    fn huge_commit_range_uses_whole_file_without_truncation() {
        assert_eq!(wire_commit_range(4, u64::from(u32::MAX) + 10), (0, 0));
    }

    #[test]
    fn commit_verifier_matching_every_write_verifier_is_durable() {
        let written = [[1u8; 8], [1u8; 8]];
        assert!(check_commit_verifier(written.iter(), [1u8; 8]).is_ok());
        assert!(check_commit_verifier(std::iter::empty(), [9u8; 8]).is_ok());
    }

    #[test]
    fn commit_verifier_mismatch_is_an_uncertain_write() {
        let written = [[1u8; 8], [2u8; 8]];
        let error = check_commit_verifier(written.iter(), [1u8; 8]).unwrap_err();
        let outcome = error
            .operation_outcome()
            .expect("verifier mismatch must carry structured guidance");
        assert_eq!(outcome.outcome, OperationOutcome::Uncertain);
        assert_eq!(outcome.recovery, RecoveryAction::VerifyThenResume);
        assert_eq!(outcome.context().operation, "write_verifier");
    }

    #[test]
    fn attempted_ds_error_is_uncertain_and_requires_verification() {
        let context = RequestContext {
            operation: "pnfs_write".to_string(),
            protocol: crate::NFSVersion::NFSv4p1,
            request_id: Some(crate::error::RequestId::nfs41([7; 16], 0, 0)),
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
