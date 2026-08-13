//! pNFS layout management (RFC 5661 §12).
//!
//! pNFS allows clients to perform I/O directly to data servers, bypassing the
//! metadata server for data operations. The metadata server grants layouts via
//! LAYOUTGET, which describe how file data is distributed across data servers.
//!
//! Layout types:
//! - LAYOUT4_NFSV4_1_FILES (1): file-based layout with stripe patterns
//! - LAYOUT4_OSD2_OBJECTS (2): object storage (not implemented)
//! - LAYOUT4_BLOCK_VOLUME (3): block devices (not implemented)

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::{Buf, Bytes};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use tracing::{debug, info, warn};

use super::compound::CompoundBuilder;
use super::session::{ClientIdentity, Session};
use crate::error::{NfsError, Result};
use crate::rpc;
use crate::rpc::auth::Auth;

/// DS 控制操作（DESTROY_SESSION/DESTROY_CLIENTID）的超时。
const DS_TEARDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// DS 连接 + 会话建立的整体超时（不可达地址快速失败并拉黑）。
const DS_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

type DataServerInitKey = (u64, SocketAddr);
type DataServerInitGate = Mutex<()>;

fn should_negative_cache_ds_error(error: &NfsError) -> bool {
    matches!(
        error,
        NfsError::Io(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::HostUnreachable
                    | std::io::ErrorKind::NetworkUnreachable
                    | std::io::ErrorKind::TimedOut
            )
    )
}

/// pNFS layout types.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum LayoutType {
    NfsV41Files = 1,
    Osd2Objects = 2,
    BlockVolume = 3,
}

/// I/O mode for layouts.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum IoMode {
    Read = 1,
    ReadWrite = 2,
}

/// A layout segment describing how data is distributed.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Protocol struct: all fields used during decode + future extensions
pub(crate) struct LayoutSegment {
    pub offset: u64,
    pub length: u64,
    pub iomode: IoMode,
    pub layout_type: LayoutType,
    /// For LAYOUT4_NFSV4_1_FILES: the nfsv4_1_file_layout4 content
    pub content: LayoutContent,
}

impl LayoutSegment {
    pub(crate) fn covers(&self, offset: u64) -> bool {
        offset >= self.offset
            && (self.length == u64::MAX
                || self
                    .offset
                    .checked_add(self.length)
                    .is_none_or(|end| offset < end))
    }
}

/// Decoded layout content (type-specific).
#[derive(Debug, Clone)]
#[allow(dead_code)] // Protocol enum: all variants/fields used during decode + future extensions
pub(crate) enum LayoutContent {
    /// nfsv4_1_file_layout4 (RFC 5661 §13.3)
    FilesLayout {
        device_id: [u8; 16],
        /// nfl_util low 30 bits: stripe unit size in bytes.
        stripe_unit: u32,
        /// nfl_util bit 30: NFL4_UFLG_DENSE flag.
        is_dense: bool,
        /// NFSv4.1 file layout uses striping across data servers.
        /// first_stripe_index indicates which DS gets the first stripe.
        first_stripe_index: u32,
        /// Offset within the file where the pattern starts.
        pattern_offset: u64,
        /// File handles on data servers (one per DS in the stripe).
        fh_list: Vec<Bytes>,
    },
    /// Opaque content for unsupported layout types.
    Opaque(Bytes),
}

/// A granted layout for a file.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Protocol struct: all fields decoded from wire format
pub(crate) struct Layout {
    /// MDS session generation that granted this layout.
    pub generation: u64,
    pub stateid: [u8; 16],
    pub return_on_close: bool,
    pub segments: Vec<LayoutSegment>,
}

fn layout_segments_overlap(left: &LayoutSegment, right: &LayoutSegment) -> bool {
    let left_end = if left.length == u64::MAX {
        u64::MAX
    } else {
        left.offset.saturating_add(left.length)
    };
    let right_end = if right.length == u64::MAX {
        u64::MAX
    } else {
        right.offset.saturating_add(right.length)
    };
    left.offset < right_end && right.offset < left_end
}

/// Resolved pNFS device: per-DS network addresses.
#[derive(Debug, Clone)]
pub(crate) struct DeviceInfo {
    /// Indirection table: stripe_indices[stripe_pos] -> index into ds_addrs.
    /// RFC 5661 §13.3: maps stripe position to DS multipath_list entry.
    pub stripe_indices: Vec<u32>,
    /// Data server address lists. ds_addrs[i] = multipath addresses for physical DS i.
    pub ds_addrs: Vec<Vec<SocketAddr>>,
}

/// A single stripe chunk for DS I/O.
#[derive(Debug, Clone)]
pub(crate) struct StripeChunk {
    pub ds_index: u32,
    pub file_offset: u64,
    pub ds_offset: u64,
    pub length: u32,
}

/// A connection to a pNFS data server with its own NFSv4.1 session
/// (RFC 8881 §13.1：DS 上的 READ/WRITE 同样要求 SEQUENCE)。
#[derive(Clone)]
pub(crate) struct DsConnection {
    pub client: rpc::Client,
    pub session: Arc<Session>,
    /// MDS generation for which this DS connection was established.
    pub owner_generation: u64,
}

impl DsConnection {
    /// Best-effort 销毁 DS 的 session 与 client-id（umount 或竞态重复连接时）。
    /// DESTROY_SESSION/DESTROY_CLIENTID 都不带 SEQUENCE，失败仅记日志。
    pub async fn destroy(&self, auth: &Auth) {
        let builder = CompoundBuilder::new("ds_destroy_session").destroy_session(self.session.id());
        let mut buf = Vec::new();
        builder.encode_with_header(auth, &mut buf);
        if let Err(e) = self.client.call(buf, 1, DS_TEARDOWN_TIMEOUT).await {
            debug!(error = %e, "DS DESTROY_SESSION failed (may already be destroyed)");
        }
        let builder =
            CompoundBuilder::new("ds_destroy_clientid").destroy_client_id(self.session.client_id());
        let mut buf = Vec::new();
        builder.encode_with_header(auth, &mut buf);
        let _ = self.client.call(buf, 1, DS_TEARDOWN_TIMEOUT).await;
    }
}

/// Manages active layouts and data server connections.
pub(crate) struct LayoutManager {
    /// Layouts indexed by file handle.
    layouts: RwLock<HashMap<Bytes, Layout>>,
    /// Cached connections to data servers (each with its own session).
    data_servers: RwLock<HashMap<SocketAddr, DsConnection>>,
    /// Singleflight gates for first-use DS session establishment. The key
    /// includes the MDS generation so recovery never waits behind stale work.
    data_server_init_gates: Mutex<HashMap<DataServerInitKey, std::sync::Weak<DataServerInitGate>>>,
    /// Cached device info indexed by device ID.
    device_cache: RwLock<HashMap<[u8; 16], (u64, DeviceInfo)>>,
    /// 已通过 layout 写入但尚未 LAYOUTCOMMIT 的字节范围（fh → (start, end)）。
    /// LAYOUTCOMMIT 聚合到 close/layoutreturn 前一次性发送，而非每次 WRITE 后。
    dirty: RwLock<HashMap<Bytes, DirtyRange>>,
    /// Next sequential file offset at which the client proactively returns
    /// and reacquires this file's layout. Entries are per file so a large-file
    /// lifecycle transition never disturbs layouts for sibling files.
    layout_refresh_offsets: RwLock<HashMap<Bytes, u64>>,
    /// Per-file I/O gates. WRITEs take a shared guard; recall/CLOSE take an
    /// exclusive guard so different files and same-file parallel WRITEs remain
    /// concurrent while layout lifecycle transitions are serialized.
    file_io_gates: Mutex<HashMap<Bytes, std::sync::Weak<RwLock<()>>>>,
    /// 退化拓扑 info 提示是否已打印（每个 mount 只提示一次，避免刷屏）。
    degenerate_logged: AtomicBool,
    /// 连接失败的 DS 地址负缓存（mount 生命周期内不再尝试）。
    /// server 可能下发客户端不可达网段的 DS LIF（如 ONTAP 多网段拓扑），
    /// 不拉黑会导致每次 I/O 都重复付一次连接超时。
    unreachable_ds: RwLock<HashSet<SocketAddr>>,
    /// Whether to use ephemeral (non-privileged) source ports for DS connections.
    noresvport: bool,
    active_generation: AtomicU64,
}

pub(crate) const PNFS_LAYOUT_REFRESH_INTERVAL: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DirtyRange {
    pub generation: u64,
    pub start: u64,
    pub end: u64,
    revision: u64,
}

impl LayoutManager {
    pub fn new(noresvport: bool) -> Self {
        Self {
            layouts: RwLock::new(HashMap::new()),
            data_servers: RwLock::new(HashMap::new()),
            data_server_init_gates: Mutex::new(HashMap::new()),
            device_cache: RwLock::new(HashMap::new()),
            dirty: RwLock::new(HashMap::new()),
            layout_refresh_offsets: RwLock::new(HashMap::new()),
            file_io_gates: Mutex::new(HashMap::new()),
            degenerate_logged: AtomicBool::new(false),
            unreachable_ds: RwLock::new(HashSet::new()),
            noresvport,
            active_generation: AtomicU64::new(1),
        }
    }

    pub async fn transition_to(&self, generation: u64) {
        // Publish the fence before the first cancellation point. Every cache
        // lookup below validates ownership, so partially completed cleanup is
        // safe if the recovering request is cancelled.
        self.active_generation.store(generation, Ordering::Release);
        let mut layouts = self.layouts.write().await;
        let mut servers = self.data_servers.write().await;
        let mut devices = self.device_cache.write().await;
        let mut dirty = self.dirty.write().await;
        layouts.clear();
        servers.clear();
        self.data_server_init_gates.lock().await.clear();
        devices.clear();
        dirty.clear();
        self.unreachable_ds.write().await.clear();
        self.layout_refresh_offsets.write().await.clear();
    }

    pub fn generation(&self) -> u64 {
        self.active_generation.load(Ordering::Acquire)
    }

    async fn data_server_init_gate(
        &self,
        addr: SocketAddr,
        generation: u64,
    ) -> Arc<DataServerInitGate> {
        let mut gates = self.data_server_init_gates.lock().await;
        if let Some(gate) = gates
            .get(&(generation, addr))
            .and_then(std::sync::Weak::upgrade)
        {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(Mutex::new(()));
        gates.insert((generation, addr), Arc::downgrade(&gate));
        gate
    }

    async fn file_io_gate(&self, fh: &Bytes) -> Arc<RwLock<()>> {
        let mut gates = self.file_io_gates.lock().await;
        if let Some(gate) = gates.get(fh).and_then(std::sync::Weak::upgrade) {
            return gate;
        }
        gates.retain(|_, gate| gate.strong_count() > 0);
        let gate = Arc::new(RwLock::new(()));
        gates.insert(fh.clone(), Arc::downgrade(&gate));
        gate
    }

    pub async fn read_file_io(&self, fh: &Bytes) -> OwnedRwLockReadGuard<()> {
        self.file_io_gate(fh).await.read_owned().await
    }

    pub async fn write_file_io(&self, fh: &Bytes) -> OwnedRwLockWriteGuard<()> {
        self.file_io_gate(fh).await.write_owned().await
    }

    /// 把一个连接失败的 DS 地址记入负缓存（mount 生命周期内不再尝试）。
    pub async fn mark_ds_unreachable(&self, addr: SocketAddr) {
        let mut set = self.unreachable_ds.write().await;
        if set.insert(addr) {
            warn!(addr = %addr, "marking pNFS data server unreachable, affected files fall back to MDS I/O");
        }
    }

    /// DS 地址是否已被标记为不可达。
    pub async fn is_ds_unreachable(&self, addr: &SocketAddr) -> bool {
        let set = self.unreachable_ds.read().await;
        set.contains(addr)
    }

    /// 首次调用返回 true（用于退化拓扑只打一次 info 日志），之后返回 false。
    pub fn should_log_degenerate(&self) -> bool {
        self.degenerate_logged
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// Store a layout for a file.
    #[cfg(test)]
    pub async fn store_layout(&self, fh: &Bytes, layout: Layout) {
        let mut map = self.layouts.write().await;
        debug!(
            fh_len = fh.len(),
            segments = layout.segments.len(),
            "layout stored"
        );
        map.insert(fh.clone(), layout);
    }

    pub async fn store_layout_at(&self, fh: &Bytes, generation: u64, mut layout: Layout) -> bool {
        let mut map = self.layouts.write().await;
        if generation != self.active_generation.load(Ordering::Acquire) {
            return false;
        }
        layout.generation = generation;
        map.insert(fh.clone(), layout);
        true
    }

    /// Get the layout for a file, if any.
    pub async fn get_layout(&self, fh: &Bytes) -> Option<Layout> {
        let map = self.layouts.read().await;
        let generation = self.generation();
        map.get(fh)
            .filter(|layout| layout.generation == generation)
            .cloned()
    }

    /// Return a cached layout only when one of its segments covers `offset`.
    /// A bounded segment must not suppress a LAYOUTGET for a later multipart
    /// file range.
    pub async fn get_layout_covering(&self, fh: &Bytes, offset: u64) -> Option<Layout> {
        self.get_layout(fh)
            .await
            .filter(|layout| layout.segments.iter().any(|segment| segment.covers(offset)))
    }

    pub async fn merge_layout(&self, fh: &Bytes, mut update: Layout) {
        let mut layouts = self.layouts.write().await;
        if update.generation != self.generation() {
            return;
        }
        if let Some(current) = layouts.get_mut(fh) {
            current.segments.retain(|old| {
                !update
                    .segments
                    .iter()
                    .any(|new| layout_segments_overlap(old, new))
            });
            current.segments.append(&mut update.segments);
            current.segments.sort_by_key(|segment| segment.offset);
            current.stateid = update.stateid;
            current.return_on_close |= update.return_on_close;
        } else {
            layouts.insert(fh.clone(), update);
        }
    }

    /// Remove the layout for a file.
    pub async fn remove_layout(&self, fh: &Bytes) -> Option<Layout> {
        let mut map = self.layouts.write().await;
        let removed = map.remove(fh);
        self.layout_refresh_offsets.write().await.remove(fh);
        removed
    }

    pub async fn layout_refresh_due(&self, fh: &Bytes, offset: u64) -> bool {
        let offsets = self.layout_refresh_offsets.read().await;
        offset
            >= offsets
                .get(fh)
                .copied()
                .unwrap_or(PNFS_LAYOUT_REFRESH_INTERVAL)
    }

    pub async fn record_layout_refresh(&self, fh: &Bytes, offset: u64) {
        let next = offset
            .saturating_div(PNFS_LAYOUT_REFRESH_INTERVAL)
            .saturating_add(1)
            .saturating_mul(PNFS_LAYOUT_REFRESH_INTERVAL);
        self.layout_refresh_offsets
            .write()
            .await
            .insert(fh.clone(), next);
    }

    pub async fn all_layouts(&self) -> Vec<(Bytes, Layout)> {
        let map = self.layouts.read().await;
        map.iter()
            .map(|(fh, layout)| (fh.clone(), layout.clone()))
            .collect()
    }

    /// Remove all layouts and device cache atomically.
    ///
    /// Both locks are acquired before clearing to prevent a window where one map
    /// is empty but the other still has stale entries.
    /// Lock order: layouts -> device_cache -> dirty (consistent with all other callers to prevent deadlock).
    pub async fn clear(&self) {
        let mut map = self.layouts.write().await;
        let mut devices = self.device_cache.write().await;
        let mut dirty = self.dirty.write().await;
        map.clear();
        devices.clear();
        dirty.clear();
        self.layout_refresh_offsets.write().await.clear();
    }

    /// 记录一段已通过 layout 写入、待 LAYOUTCOMMIT 的范围；与已有范围 merge（min/max）。
    pub async fn mark_dirty_at(&self, fh: &Bytes, generation: u64, start: u64, end: u64) -> bool {
        let mut dirty = self.dirty.write().await;
        if generation != self.active_generation.load(Ordering::Acquire) {
            return false;
        }
        dirty
            .entry(fh.clone())
            .and_modify(|range| {
                range.start = range.start.min(start);
                range.end = range.end.max(end);
                range.revision = range.revision.wrapping_add(1);
            })
            .or_insert(DirtyRange {
                generation,
                start,
                end,
                revision: 0,
            });
        true
    }

    #[cfg(test)]
    pub async fn mark_dirty(&self, fh: &Bytes, start: u64, end: u64) {
        let _ = self.mark_dirty_at(fh, self.generation(), start, end).await;
    }

    /// Snapshot a pending range without removing it. The revision lets a
    /// successful LAYOUTCOMMIT acknowledge only the exact state it sent.
    pub async fn snapshot_dirty(&self, fh: &Bytes) -> Option<DirtyRange> {
        let dirty = self.dirty.read().await;
        dirty
            .get(fh)
            .copied()
            .filter(|range| range.generation == self.active_generation.load(Ordering::Acquire))
    }

    /// Clear a range only if no concurrent WRITE extended it while the
    /// LAYOUTCOMMIT was in flight.
    pub async fn acknowledge_dirty(&self, fh: &Bytes, committed: DirtyRange) -> bool {
        let mut dirty = self.dirty.write().await;
        let unchanged = dirty.get(fh).is_some_and(|current| *current == committed)
            && committed.generation == self.active_generation.load(Ordering::Acquire);
        if unchanged {
            dirty.remove(fh);
        }
        unchanged
    }

    /// Discard dirty metadata that can no longer be committed with its
    /// originating layout stateid. The caller must already be returning an
    /// uncertain outcome that requires data verification before resuming.
    pub async fn invalidate_dirty(&self, fh: &Bytes) {
        self.dirty.write().await.remove(fh);
    }

    /// Test/teardown helper that removes a pending range immediately.
    #[cfg(test)]
    pub async fn take_dirty(&self, fh: &Bytes) -> Option<(u64, u64)> {
        let mut dirty = self.dirty.write().await;
        dirty.remove(fh).and_then(|range| {
            (range.generation == self.active_generation.load(Ordering::Acquire))
                .then_some((range.start, range.end))
        })
    }

    /// Store a device info entry in the cache.
    #[cfg(test)]
    pub async fn store_device(&self, device_id: [u8; 16], info: DeviceInfo) {
        let generation = self.generation();
        let _ = self.store_device_at(device_id, generation, info).await;
    }

    pub async fn store_device_at(
        &self,
        device_id: [u8; 16],
        generation: u64,
        info: DeviceInfo,
    ) -> bool {
        let mut cache = self.device_cache.write().await;
        if generation != self.active_generation.load(Ordering::Acquire) {
            return false;
        }
        debug!(device_id = ?device_id, ds_count = info.ds_addrs.len(), "device info cached");
        cache.insert(device_id, (generation, info));
        true
    }

    /// Get a device info entry from the cache.
    pub async fn get_device(&self, device_id: &[u8; 16]) -> Option<DeviceInfo> {
        let cache = self.device_cache.read().await;
        let generation = self.generation();
        cache
            .get(device_id)
            .filter(|(owner, _)| *owner == generation)
            .map(|(_, info)| info.clone())
    }

    /// Get or create a connection (with session) to a data server.
    ///
    /// 明确的 endpoint 连接失败（含超时）会进入不可达负缓存；NFS/RPC
    /// session 协议错误不会污染地址可达性。
    pub async fn get_data_server(
        &self,
        addr: SocketAddr,
        auth: &Auth,
        client_identity: &ClientIdentity,
        owner_generation: u64,
    ) -> Result<DsConnection> {
        if owner_generation != self.generation() {
            return Err(NfsError::Rpc(
                "refusing data-server I/O for stale MDS generation".to_string(),
            ));
        }
        // Fast path: read lock only
        {
            let servers = self.data_servers.read().await;
            if let Some(conn) = servers
                .get(&addr)
                .filter(|conn| conn.owner_generation == owner_generation)
            {
                return Ok(conn.clone());
            }
        }
        let init_gate = self.data_server_init_gate(addr, owner_generation).await;
        let _init_guard = init_gate.lock().await;
        if owner_generation != self.generation() {
            return Err(NfsError::Rpc(
                "refusing data-server initialization for stale MDS generation".to_string(),
            ));
        }
        // A same-address waiter may have populated the cache while this task
        // waited for the singleflight leader.
        {
            let servers = self.data_servers.read().await;
            if let Some(conn) = servers
                .get(&addr)
                .filter(|conn| conn.owner_generation == owner_generation)
            {
                return Ok(conn.clone());
            }
        }
        if self.is_ds_unreachable(&addr).await {
            return Err(NfsError::Rpc(format!(
                "pNFS data server {addr} marked unreachable"
            )));
        }
        // TCP connect + session establishment holds only this address and
        // generation's singleflight gate, so other DS endpoints remain fully
        // concurrent while same-DS cold-start waiters share one session.
        // 整体包超时：server 可能下发不可达网段的 DS 地址（SYN 黑洞时
        // 系统级 connect 超时可达 20s+），失败后拉黑避免每次 I/O 重付。
        let connect_and_establish = async {
            let mux = rpc::StreamMux::connect(addr, self.noresvport).await?;
            let new_client = rpc::Client::new(mux, None);
            // RFC 8881 §13.1：DS 需要自己的 client-id + session（EXCHANGE_ID USE_PNFS_DS）
            let session = Session::establish_ds(&new_client, auth, client_identity).await?;
            Ok::<DsConnection, NfsError>(DsConnection {
                client: new_client,
                session: Arc::new(session),
                owner_generation,
            })
        };
        let conn = match tokio::time::timeout(DS_CONNECT_TIMEOUT, connect_and_establish).await {
            Ok(Ok(conn)) => conn,
            Ok(Err(e)) => {
                if should_negative_cache_ds_error(&e) {
                    self.mark_ds_unreachable(addr).await;
                }
                return Err(e);
            }
            Err(_) => {
                self.mark_ds_unreachable(addr).await;
                return Err(NfsError::Rpc(format!(
                    "pNFS data server {addr} connect timed out"
                )));
            }
        };
        // The per-address initialization gate makes this the only publisher
        // for this DS and generation.
        let mut servers = self.data_servers.write().await;
        if self.generation() != owner_generation {
            drop(servers);
            let auth = auth.clone();
            tokio::spawn(async move { conn.destroy(&auth).await });
            return Err(NfsError::Rpc(
                "discarding data-server connection from stale MDS generation".to_string(),
            ));
        }
        servers.insert(addr, conn.clone());
        info!(addr = %addr, "connected to pNFS data server (session established)");
        Ok(conn)
    }

    /// Remove a cached DS connection (e.g. after NFS4ERR_BADSESSION from the DS).
    pub async fn remove_data_server(&self, addr: SocketAddr) -> Option<DsConnection> {
        let mut servers = self.data_servers.write().await;
        servers.remove(&addr)
    }

    /// Drain all DS connections for teardown (umount).
    pub async fn drain_data_servers(&self) -> Vec<(SocketAddr, DsConnection)> {
        let mut servers = self.data_servers.write().await;
        servers.drain().collect()
    }
}

/// 判定设备是否为退化 pNFS 拓扑：所有 DS 的首选地址（与 DS I/O 的
/// 选址逻辑一致，取 multipath list 第一个）都等于 MDS 地址。
/// 空 ds_addrs 保守返回 false。
pub(crate) fn is_degenerate_device(device: &DeviceInfo, server_addr: &SocketAddr) -> bool {
    !device.ds_addrs.is_empty()
        && device
            .ds_addrs
            .iter()
            .all(|paths| paths.first() == Some(server_addr))
}

/// IP 地址与 MDS 地址的公共前缀位数（不同地址家族视为 0）。
fn addr_prefix_len(a: &SocketAddr, b: &SocketAddr) -> u32 {
    match (a.ip(), b.ip()) {
        (IpAddr::V4(x), IpAddr::V4(y)) => (u32::from(x) ^ u32::from(y)).leading_zeros(),
        (IpAddr::V6(x), IpAddr::V6(y)) => (u128::from(x) ^ u128::from(y)).leading_zeros(),
        _ => 0,
    }
}

/// 按与 MDS 地址的网络接近度对每个 DS 的 multipath 地址排序（降序）：
/// 与 MDS 完全相同的地址最优（复用主 session），其次按 IP 公共前缀
/// 长度排序——server 可能在 multipath 里返回客户端不可达网段的 LIF
/// （如 ONTAP 把同节点上该 SVM 的所有 LIF 都放进列表），盲取第一个
/// 会把 DS I/O 指向不可达地址。
pub(crate) fn sort_multipath_by_affinity(info: &mut DeviceInfo, server_addr: &SocketAddr) {
    for paths in &mut info.ds_addrs {
        paths.sort_by_key(|addr| {
            let exact = *addr == *server_addr;
            let same_family = addr.is_ipv4() == server_addr.is_ipv4();
            // sort_by_key 升序：取反让 (exact, 同家族, 前缀长) 大者排前
            (
                !exact,
                !same_family,
                u32::MAX - addr_prefix_len(addr, server_addr),
            )
        });
    }
}

/// Decode a LAYOUTGET response into a Layout.
pub(crate) fn decode_layoutget_response(data: &mut Bytes) -> Result<Layout> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "LAYOUTGET return_on_close truncated".to_string(),
        ));
    }
    let return_on_close = data.get_u32() != 0;

    // stateid4
    if data.remaining() < 16 {
        return Err(NfsError::Xdr("LAYOUTGET stateid truncated".to_string()));
    }
    let mut stateid = [0u8; 16];
    data.copy_to_slice(&mut stateid);

    // layout4<>: array of layout segments
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "LAYOUTGET segments array truncated".to_string(),
        ));
    }
    let num_segments = data.get_u32() as usize;
    if num_segments > 1024 {
        return Err(NfsError::Xdr(format!(
            "LAYOUTGET has {} segments, max 1024",
            num_segments
        )));
    }
    let mut segments = Vec::with_capacity(num_segments);

    for _ in 0..num_segments {
        if data.remaining() < 24 {
            return Err(NfsError::Xdr(
                "layout4 segment header truncated".to_string(),
            ));
        }
        let offset = data.get_u64();
        let length = data.get_u64();
        let iomode_val = data.get_u32();
        let layout_type_val = data.get_u32();

        let iomode = match iomode_val {
            1 => IoMode::Read,
            2 => IoMode::ReadWrite,
            _ => IoMode::Read,
        };
        let layout_type = match layout_type_val {
            1 => LayoutType::NfsV41Files,
            2 => LayoutType::Osd2Objects,
            3 => LayoutType::BlockVolume,
            _ => LayoutType::NfsV41Files,
        };

        // layout_content4: opaque<>
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("layout_content length truncated".to_string()));
        }
        let content_len = data.get_u32() as usize;
        let padded = (content_len + 3) & !3;
        if data.remaining() < padded {
            return Err(NfsError::Xdr("layout_content data truncated".to_string()));
        }
        let mut content_data = data.split_to(content_len);
        let pad = padded - content_len;
        if data.remaining() >= pad {
            data.advance(pad);
        }

        let content = if layout_type == LayoutType::NfsV41Files {
            decode_files_layout(&mut content_data)?
        } else {
            LayoutContent::Opaque(content_data)
        };

        segments.push(LayoutSegment {
            offset,
            length,
            iomode,
            layout_type,
            content,
        });
    }

    Ok(Layout {
        generation: 0,
        stateid,
        return_on_close,
        segments,
    })
}

/// Decode nfsv4_1_file_layout4 content (RFC 5661 §13.3).
fn decode_files_layout(data: &mut Bytes) -> Result<LayoutContent> {
    // deviceid4 (16 bytes)
    if data.remaining() < 16 {
        return Err(NfsError::Xdr("files_layout deviceid truncated".to_string()));
    }
    let mut device_id = [0u8; 16];
    data.copy_to_slice(&mut device_id);

    // nfl_util: uint32 — low 30 bits = stripe unit, bit 30 = NFL4_UFLG_DENSE
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("files_layout nfl_util truncated".to_string()));
    }
    let nfl_util = data.get_u32();
    let stripe_unit = nfl_util & 0x3FFF_FFFF;
    let is_dense = (nfl_util & 0x4000_0000) != 0;

    // nfl_first_stripe_index: uint32
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "files_layout first_stripe_index truncated".to_string(),
        ));
    }
    let first_stripe_index = data.get_u32();

    // nfl_pattern_offset: offset4 (uint64)
    if data.remaining() < 8 {
        return Err(NfsError::Xdr(
            "files_layout pattern_offset truncated".to_string(),
        ));
    }
    let pattern_offset = data.get_u64();

    // nfl_fh_list: nfs_fh4<>
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "files_layout fh_list length truncated".to_string(),
        ));
    }
    let num_fhs = data.get_u32() as usize;
    if num_fhs > 4096 {
        return Err(NfsError::Xdr(format!(
            "files_layout has {} FHs, max 4096",
            num_fhs
        )));
    }
    let mut fh_list = Vec::with_capacity(num_fhs);
    for _ in 0..num_fhs {
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("files_layout fh truncated".to_string()));
        }
        let fh_len = data.get_u32() as usize;
        let padded = (fh_len + 3) & !3;
        if data.remaining() < padded {
            return Err(NfsError::Xdr("files_layout fh data truncated".to_string()));
        }
        let fh = data.slice(..fh_len);
        data.advance(padded);
        fh_list.push(fh);
    }

    Ok(LayoutContent::FilesLayout {
        device_id,
        stripe_unit,
        is_dense,
        first_stripe_index,
        pattern_offset,
        fh_list,
    })
}

/// Calculate which DS index handles a given file offset.
pub(crate) fn stripe_ds_index(
    offset: u64,
    stripe_unit: u32,
    first_stripe_index: u32,
    num_ds: u32,
) -> u32 {
    let su = stripe_unit as u64;
    let stripe_num = offset / su;
    ((stripe_num + first_stripe_index as u64) % num_ds as u64) as u32
}

/// Calculate the offset on the data server for a given file offset.
pub(crate) fn ds_offset(
    file_offset: u64,
    stripe_unit: u32,
    _first_stripe_index: u32,
    num_ds: u32,
    is_dense: bool,
) -> u64 {
    let su = stripe_unit as u64;
    let stripe_num = file_offset / su;
    let offset_in_stripe = file_offset % su;
    if is_dense {
        // Dense: DS holds every num_ds-th stripe, compacted without gaps
        let ds_stripe_num = stripe_num / num_ds as u64;
        ds_stripe_num * su + offset_in_stripe
    } else {
        // Sparse: DS preserves original file offsets (with holes for other DSs).
        // RFC 5661 §13.3.2
        let _ = (su, stripe_num, offset_in_stripe); // suppress unused warnings
        file_offset
    }
}

/// Split a byte range [offset, offset+count) into per-stripe chunks.
pub(crate) fn split_into_stripes(
    offset: u64,
    count: u32,
    stripe_unit: u32,
    is_dense: bool,
    first_stripe_index: u32,
    num_ds: u32,
    pattern_offset: u64,
) -> Vec<StripeChunk> {
    let mut chunks = Vec::new();
    if stripe_unit == 0 || num_ds == 0 {
        return chunks;
    }
    let su = stripe_unit as u64;
    let end = offset + count as u64;
    let mut pos = offset;
    while pos < end {
        // How many bytes until end of current stripe?
        let stripe_end = ((pos / su) + 1) * su;
        let chunk_end = end.min(stripe_end);
        let chunk_len = (chunk_end - pos) as u32;
        // Adjust for pattern_offset: stripe numbering starts at pattern_offset
        let adjusted = pos.saturating_sub(pattern_offset);
        let ds_idx = stripe_ds_index(adjusted, stripe_unit, first_stripe_index, num_ds);
        let ds_off = ds_offset(adjusted, stripe_unit, first_stripe_index, num_ds, is_dense);
        chunks.push(StripeChunk {
            ds_index: ds_idx,
            file_offset: pos,
            ds_offset: ds_off,
            length: chunk_len,
        });
        pos = chunk_end;
    }
    chunks
}

/// Parse a netaddr4 (r_netid, r_addr) into a SocketAddr.
///
/// For "tcp":  r_addr = "h1.h2.h3.h4.p1.p2" (IPv4 dotted-decimal + 2 port octets)
/// For "tcp6": r_addr = "h1:h2:...:h8.p1.p2" (IPv6 colon-hex + ".p1.p2" port suffix)
/// Both end with two dot-separated port octets where port = p1*256 + p2.
fn parse_netaddr4(r_netid: &str, r_addr: &str) -> Result<SocketAddr> {
    let parse_u8 = |s: &str| -> Result<u8> {
        s.parse::<u8>()
            .map_err(|_| NfsError::Xdr(format!("invalid address octet: {}", s)))
    };

    if r_netid == "tcp" {
        // IPv4: "h1.h2.h3.h4.p1.p2"
        let parts: Vec<&str> = r_addr.split('.').collect();
        if parts.len() != 6 {
            return Err(NfsError::Xdr(format!(
                "invalid tcp r_addr (expected 6 dot-separated fields): {}",
                r_addr
            )));
        }
        let ip = Ipv4Addr::new(
            parse_u8(parts[0])?,
            parse_u8(parts[1])?,
            parse_u8(parts[2])?,
            parse_u8(parts[3])?,
        );
        let port = parse_u8(parts[4])? as u16 * 256 + parse_u8(parts[5])? as u16;
        Ok(SocketAddr::new(IpAddr::V4(ip), port))
    } else if r_netid == "tcp6" {
        // IPv6: "h1:h2:...:h8.p1.p2"
        // The last two dot-separated tokens are port octets; the rest is the IPv6 address.
        // Find the second-to-last '.' which separates IPv6 from the first port octet.
        let dot2 = {
            let last = r_addr.rfind('.').ok_or_else(|| {
                NfsError::Xdr(format!("invalid tcp6 r_addr (missing port): {}", r_addr))
            })?;
            r_addr[..last].rfind('.').ok_or_else(|| {
                NfsError::Xdr(format!(
                    "invalid tcp6 r_addr (missing port octets): {}",
                    r_addr
                ))
            })?
        };
        let ip_part = &r_addr[..dot2];
        let port_part = &r_addr[dot2 + 1..];
        let port_octets: Vec<&str> = port_part.split('.').collect();
        if port_octets.len() != 2 {
            return Err(NfsError::Xdr(format!(
                "invalid tcp6 r_addr port part: {}",
                r_addr
            )));
        }
        let p1 = parse_u8(port_octets[0])? as u16;
        let p2 = parse_u8(port_octets[1])? as u16;
        let port = p1 * 256 + p2;
        let ip: Ipv6Addr = ip_part
            .parse()
            .map_err(|_| NfsError::Xdr(format!("invalid IPv6 address: {}", ip_part)))?;
        Ok(SocketAddr::new(IpAddr::V6(ip), port))
    } else {
        Err(NfsError::Xdr(format!("unsupported netid: {}", r_netid)))
    }
}

/// Read an XDR string (uint32 length + data + padding).
fn read_xdr_string(data: &mut Bytes) -> Result<String> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("XDR string length truncated".to_string()));
    }
    let len = data.get_u32() as usize;
    let padded = (len + 3) & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr("XDR string data truncated".to_string()));
    }
    let s = std::str::from_utf8(&data[..len])
        .map_err(|_| NfsError::Xdr("XDR string not valid UTF-8".to_string()))?
        .to_string();
    data.advance(padded);
    Ok(s)
}

/// Decode a GETDEVICEINFO response (for files layout type) into a DeviceInfo.
///
/// The response contains:
/// - `da_addr_body` (opaque): nfsv4_1_file_layout_ds_addr4
///   - `stripe_indices`: uint32[] (maps layout FH indices to DS indices)
///   - `multipath_ds_list`: multipath_list4[]
///     - each: netaddr4[] (r_netid + r_addr)
pub(crate) fn decode_getdeviceinfo_response(data: &mut Bytes) -> Result<DeviceInfo> {
    // device_addr4: layout_type (uint32) + da_addr_body (opaque<>)
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "GETDEVICEINFO layout_type truncated".to_string(),
        ));
    }
    let layout_type = data.get_u32();
    if layout_type != 1 {
        return Err(NfsError::Xdr(format!(
            "GETDEVICEINFO unsupported layout type: {}",
            layout_type
        )));
    }

    // da_addr_body: opaque<>
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "GETDEVICEINFO da_addr_body length truncated".to_string(),
        ));
    }
    let body_len = data.get_u32() as usize;
    let padded = (body_len + 3) & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr(
            "GETDEVICEINFO da_addr_body truncated".to_string(),
        ));
    }
    let mut body = data.split_to(body_len);
    let pad = padded - body_len;
    if data.remaining() >= pad {
        data.advance(pad);
    }

    // nfsv4_1_file_layout_ds_addr4:
    // stripe_indices: uint32<>
    if body.remaining() < 4 {
        return Err(NfsError::Xdr(
            "GETDEVICEINFO stripe_indices length truncated".to_string(),
        ));
    }
    let num_indices = body.get_u32() as usize;
    if num_indices > 4096 {
        return Err(NfsError::Xdr(format!(
            "GETDEVICEINFO has {} stripe_indices, max 4096",
            num_indices
        )));
    }
    if body.remaining() < num_indices * 4 {
        return Err(NfsError::Xdr(
            "GETDEVICEINFO stripe_indices data truncated".to_string(),
        ));
    }
    let mut stripe_indices = Vec::with_capacity(num_indices);
    for _ in 0..num_indices {
        stripe_indices.push(body.get_u32());
    }

    // multipath_ds_list: multipath_list4<>
    if body.remaining() < 4 {
        return Err(NfsError::Xdr(
            "GETDEVICEINFO multipath_ds_list length truncated".to_string(),
        ));
    }
    let num_ds = body.get_u32() as usize;
    if num_ds > 4096 {
        return Err(NfsError::Xdr(format!(
            "GETDEVICEINFO has {} data servers, max 4096",
            num_ds
        )));
    }

    let mut ds_addrs = Vec::with_capacity(num_ds);
    for _ in 0..num_ds {
        // multipath_list4: netaddr4<>
        if body.remaining() < 4 {
            return Err(NfsError::Xdr(
                "GETDEVICEINFO multipath_list length truncated".to_string(),
            ));
        }
        let num_addrs = body.get_u32() as usize;
        if num_addrs > 256 {
            return Err(NfsError::Xdr(format!(
                "GETDEVICEINFO DS has {} addresses, max 256",
                num_addrs
            )));
        }
        let mut addrs = Vec::with_capacity(num_addrs);
        for _ in 0..num_addrs {
            let r_netid = read_xdr_string(&mut body)?;
            let r_addr = read_xdr_string(&mut body)?;
            let addr = parse_netaddr4(&r_netid, &r_addr)?;
            addrs.push(addr);
        }
        ds_addrs.push(addrs);
    }

    Ok(DeviceInfo {
        stripe_indices,
        ds_addrs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_be_bytes());
    }
    fn put_u64(buf: &mut Vec<u8>, v: u64) {
        buf.extend_from_slice(&v.to_be_bytes());
    }

    #[tokio::test]
    async fn layout_manager_store_and_get() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"file1");
        let layout = Layout {
            generation: 1,
            stateid: [1u8; 16],
            return_on_close: true,
            segments: vec![],
        };
        mgr.store_layout(&fh, layout).await;
        let got = mgr.get_layout(&fh).await.unwrap();
        assert_eq!(got.stateid, [1u8; 16]);
        assert!(got.return_on_close);
    }

    #[tokio::test]
    async fn transition_rejects_late_layout_from_old_generation() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"generation-layout");
        let layout = Layout {
            generation: 1,
            stateid: [2; 16],
            return_on_close: false,
            segments: vec![],
        };
        assert!(
            mgr.store_layout_at(&fh, 1, layout.clone()).await,
            "initial generation should accept layout"
        );
        mgr.transition_to(2).await;
        assert!(mgr.get_layout(&fh).await.is_none());
        assert!(!mgr.store_layout_at(&fh, 1, layout.clone()).await);
        assert!(mgr.get_layout(&fh).await.is_none());
        assert!(mgr.store_layout_at(&fh, 2, layout).await);
        assert!(mgr.get_layout(&fh).await.is_some());
    }

    #[tokio::test]
    async fn layout_manager_remove() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"file2");
        mgr.store_layout(
            &fh,
            Layout {
                generation: 1,
                stateid: [2u8; 16],
                return_on_close: false,
                segments: vec![],
            },
        )
        .await;
        let removed = mgr.remove_layout(&fh).await;
        assert!(removed.is_some());
        assert!(mgr.get_layout(&fh).await.is_none());
    }

    #[test]
    fn degenerate_device_single_ds_is_mds() {
        let mds: SocketAddr = "10.0.0.1:2049".parse().unwrap();
        let device = DeviceInfo {
            stripe_indices: vec![0],
            ds_addrs: vec![vec![mds]],
        };
        assert!(is_degenerate_device(&device, &mds));
    }

    #[test]
    fn degenerate_device_other_ds() {
        let mds: SocketAddr = "10.0.0.1:2049".parse().unwrap();
        let other: SocketAddr = "10.0.0.2:2049".parse().unwrap();
        // 任一 DS ≠ MDS → 非退化
        let device = DeviceInfo {
            stripe_indices: vec![0, 1],
            ds_addrs: vec![vec![mds], vec![other]],
        };
        assert!(!is_degenerate_device(&device, &mds));
    }

    #[test]
    fn degenerate_device_empty_ds_list() {
        let mds: SocketAddr = "10.0.0.1:2049".parse().unwrap();
        let device = DeviceInfo {
            stripe_indices: vec![],
            ds_addrs: vec![],
        };
        // 空 ds_addrs 保守不判退化
        assert!(!is_degenerate_device(&device, &mds));
    }

    #[test]
    fn degenerate_device_port_mismatch() {
        // 端口不同视为不同端点（与 ds_write_chunk 的完整 SocketAddr 比较一致）
        let mds: SocketAddr = "10.0.0.1:2049".parse().unwrap();
        let same_ip_other_port: SocketAddr = "10.0.0.1:20490".parse().unwrap();
        let device = DeviceInfo {
            stripe_indices: vec![0],
            ds_addrs: vec![vec![same_ip_other_port]],
        };
        assert!(!is_degenerate_device(&device, &mds));
    }

    #[test]
    fn multipath_sort_exact_match_first() {
        // 复现 ONTAP FlexGroup 场景：multipath = [不可达网段 LIF, MDS 本身]
        let mds: SocketAddr = "10.128.61.201:2049".parse().unwrap();
        let mut info = DeviceInfo {
            stripe_indices: vec![0],
            ds_addrs: vec![vec![
                "192.168.13.132:2049".parse().unwrap(),
                "10.128.61.201:2049".parse().unwrap(),
            ]],
        };
        sort_multipath_by_affinity(&mut info, &mds);
        assert_eq!(info.ds_addrs[0][0], mds);
        // 排序后退化判定应命中
        assert!(is_degenerate_device(&info, &mds));
    }

    #[test]
    fn multipath_sort_prefix_affinity() {
        // 非退化 DS：选与 MDS 同网段的地址而非异网段地址
        let mds: SocketAddr = "10.128.61.201:2049".parse().unwrap();
        let near: SocketAddr = "10.128.61.200:2049".parse().unwrap();
        let far: SocketAddr = "192.168.13.131:2049".parse().unwrap();
        let mut info = DeviceInfo {
            stripe_indices: vec![0],
            ds_addrs: vec![vec![far, near]],
        };
        sort_multipath_by_affinity(&mut info, &mds);
        assert_eq!(info.ds_addrs[0][0], near);
        assert!(!is_degenerate_device(&info, &mds));
    }

    #[test]
    fn multipath_sort_keeps_v4_over_v6_mismatch() {
        // 不同地址家族视为 0 前缀，排在同家族地址之后
        let mds: SocketAddr = "10.128.61.201:2049".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::1]:2049".parse().unwrap();
        let v4: SocketAddr = "172.16.0.1:2049".parse().unwrap();
        let mut info = DeviceInfo {
            stripe_indices: vec![0],
            ds_addrs: vec![vec![v6, v4]],
        };
        sort_multipath_by_affinity(&mut info, &mds);
        assert_eq!(info.ds_addrs[0][0], v4);
    }

    #[tokio::test]
    async fn unreachable_ds_mark_and_check() {
        let mgr = LayoutManager::new(false);
        let addr: SocketAddr = "192.168.13.131:2049".parse().unwrap();
        assert!(!mgr.is_ds_unreachable(&addr).await);
        mgr.mark_ds_unreachable(addr).await;
        assert!(mgr.is_ds_unreachable(&addr).await);
        // 重复标记幂等
        mgr.mark_ds_unreachable(addr).await;
        assert!(mgr.is_ds_unreachable(&addr).await);
    }

    #[tokio::test]
    async fn concurrent_cold_start_for_one_ds_is_singleflight() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let manager = Arc::new(LayoutManager::new(true));
        let address: SocketAddr = "192.0.2.10:2049".parse().unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..16 {
            let manager = Arc::clone(&manager);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            tasks.spawn(async move {
                let gate = manager.data_server_init_gate(address, 1).await;
                let _guard = gate.lock().await;
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn session_protocol_errors_do_not_poison_ds_reachability() {
        assert!(!should_negative_cache_ds_error(&NfsError::Nfs4(
            crate::Nfs4ErrorCode::NFS4ERR_BADSESSION,
        )));
        assert!(!should_negative_cache_ds_error(&NfsError::Rpc(
            "CREATE_SESSION sequence race".to_string(),
        )));
    }

    #[test]
    fn endpoint_connectivity_errors_are_negative_cached() {
        for kind in [
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::HostUnreachable,
            std::io::ErrorKind::NetworkUnreachable,
            std::io::ErrorKind::TimedOut,
        ] {
            assert!(should_negative_cache_ds_error(&NfsError::Io(
                std::io::Error::from(kind),
            )));
        }
    }

    #[tokio::test]
    async fn degenerate_log_only_once() {
        let mgr = LayoutManager::new(false);
        assert!(mgr.should_log_degenerate());
        assert!(!mgr.should_log_degenerate());
    }

    #[tokio::test]
    async fn dirty_range_merge() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"dirty1");
        mgr.mark_dirty(&fh, 4096, 8192).await;
        mgr.mark_dirty(&fh, 0, 4096).await;
        mgr.mark_dirty(&fh, 16384, 20480).await;
        assert_eq!(mgr.take_dirty(&fh).await, Some((0, 20480)));
    }

    #[tokio::test]
    async fn dirty_take_removes() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"dirty2");
        mgr.mark_dirty(&fh, 100, 200).await;
        assert_eq!(mgr.take_dirty(&fh).await, Some((100, 200)));
        assert_eq!(mgr.take_dirty(&fh).await, None);
    }

    #[tokio::test]
    async fn dirty_snapshot_survives_unacknowledged_commit() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"dirty-snapshot");
        mgr.mark_dirty(&fh, 100, 200).await;

        let snapshot = mgr.snapshot_dirty(&fh).await.expect("dirty snapshot");
        // Dropping/cancelling the would-be LAYOUTCOMMIT does not mutate state.
        assert_eq!(mgr.snapshot_dirty(&fh).await, Some(snapshot));
        assert_eq!(mgr.take_dirty(&fh).await, Some((100, 200)));
    }

    #[tokio::test]
    async fn dirty_acknowledgement_preserves_concurrent_write() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"dirty-concurrent");
        mgr.mark_dirty(&fh, 100, 200).await;
        let snapshot = mgr.snapshot_dirty(&fh).await.expect("dirty snapshot");

        mgr.mark_dirty(&fh, 300, 400).await;
        assert!(!mgr.acknowledge_dirty(&fh, snapshot).await);
        assert_eq!(mgr.take_dirty(&fh).await, Some((100, 400)));
    }

    #[tokio::test]
    async fn dirty_acknowledgement_clears_only_matching_snapshot() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"dirty-ack");
        mgr.mark_dirty(&fh, 0, 4096).await;
        let snapshot = mgr.snapshot_dirty(&fh).await.expect("dirty snapshot");

        assert!(mgr.acknowledge_dirty(&fh, snapshot).await);
        assert_eq!(mgr.snapshot_dirty(&fh).await, None);
    }

    #[tokio::test]
    async fn file_io_gate_allows_parallel_writes_and_serializes_recall() {
        let mgr = Arc::new(LayoutManager::new(false));
        let fh = Bytes::from_static(b"gate-shared-exclusive");
        let first_write = mgr.read_file_io(&fh).await;
        let second_write =
            tokio::time::timeout(std::time::Duration::from_millis(100), mgr.read_file_io(&fh))
                .await
                .expect("same-file WRITEs must remain parallel");

        let recall_mgr = Arc::clone(&mgr);
        let recall_fh = fh.clone();
        let recall = tokio::spawn(async move { recall_mgr.write_file_io(&recall_fh).await });
        tokio::task::yield_now().await;
        assert!(!recall.is_finished());

        drop(first_write);
        drop(second_write);
        let recall_guard = tokio::time::timeout(std::time::Duration::from_secs(1), recall)
            .await
            .expect("recall gate timeout")
            .expect("recall task failed");
        drop(recall_guard);
    }

    #[tokio::test]
    async fn file_io_gate_does_not_serialize_different_files() {
        let mgr = LayoutManager::new(false);
        let first = mgr.write_file_io(&Bytes::from_static(b"gate-file-a")).await;
        let second = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            mgr.write_file_io(&Bytes::from_static(b"gate-file-b")),
        )
        .await
        .expect("different files must not share an exclusive gate");
        drop(first);
        drop(second);
    }

    #[tokio::test]
    async fn dirty_cleared_on_clear() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"dirty3");
        mgr.mark_dirty(&fh, 0, 1).await;
        mgr.clear().await;
        assert_eq!(mgr.take_dirty(&fh).await, None);
    }

    #[tokio::test]
    async fn transition_rejects_late_device_and_dirty_updates() {
        let mgr = LayoutManager::new(false);
        let fh = Bytes::from_static(b"stale-side-caches");
        let device_id = [7; 16];
        let info = DeviceInfo {
            stripe_indices: vec![0],
            ds_addrs: vec![vec!["192.0.2.10:2049".parse().unwrap()]],
        };

        mgr.transition_to(2).await;
        assert!(!mgr.store_device_at(device_id, 1, info.clone()).await);
        assert!(mgr.get_device(&device_id).await.is_none());
        assert!(!mgr.mark_dirty_at(&fh, 1, 0, 4096).await);
        assert_eq!(mgr.take_dirty(&fh).await, None);

        assert!(mgr.store_device_at(device_id, 2, info).await);
        assert!(mgr.get_device(&device_id).await.is_some());
        assert!(mgr.mark_dirty_at(&fh, 2, 0, 4096).await);
        assert_eq!(mgr.take_dirty(&fh).await, Some((0, 4096)));
    }

    #[tokio::test]
    async fn layout_manager_clear() {
        let mgr = LayoutManager::new(false);
        mgr.store_layout(
            &Bytes::from_static(b"a"),
            Layout {
                generation: 1,
                stateid: [0u8; 16],
                return_on_close: false,
                segments: vec![],
            },
        )
        .await;
        mgr.store_layout(
            &Bytes::from_static(b"b"),
            Layout {
                generation: 1,
                stateid: [0u8; 16],
                return_on_close: false,
                segments: vec![],
            },
        )
        .await;
        mgr.clear().await;
        assert!(mgr.get_layout(&Bytes::from_static(b"a")).await.is_none());
        assert!(mgr.get_layout(&Bytes::from_static(b"b")).await.is_none());
    }

    #[test]
    fn decode_layoutget_empty_segments() {
        let mut buf = Vec::new();
        put_u32(&mut buf, 1); // return_on_close = true
        buf.extend_from_slice(&[5u8; 16]); // stateid
        put_u32(&mut buf, 0); // 0 segments

        let mut bytes = Bytes::from(buf);
        let layout = decode_layoutget_response(&mut bytes).unwrap();
        assert!(layout.return_on_close);
        assert_eq!(layout.stateid, [5u8; 16]);
        assert!(layout.segments.is_empty());
    }

    #[test]
    fn decode_layoutget_one_segment_opaque() {
        let mut buf = Vec::new();
        put_u32(&mut buf, 0); // return_on_close = false
        buf.extend_from_slice(&[7u8; 16]); // stateid
        put_u32(&mut buf, 1); // 1 segment
        // segment: offset(8) + length(8) + iomode(4) + layout_type(4)
        put_u64(&mut buf, 0); // offset
        put_u64(&mut buf, 0xFFFFFFFFFFFFFFFF); // length = whole file
        put_u32(&mut buf, 2); // iomode = RW
        put_u32(&mut buf, 2); // LayoutType::Osd2Objects → Opaque content
        // layout_content: opaque (must be padded to 4-byte boundary)
        let content = vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22];
        put_u32(&mut buf, content.len() as u32);
        buf.extend_from_slice(&content);

        let mut bytes = Bytes::from(buf);
        let layout = decode_layoutget_response(&mut bytes).unwrap();
        assert_eq!(layout.segments.len(), 1);
        assert_eq!(layout.segments[0].iomode, IoMode::ReadWrite);
        assert!(matches!(
            layout.segments[0].content,
            LayoutContent::Opaque(_)
        ));
    }

    #[test]
    fn decode_files_layout_basic() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0xAAu8; 16]); // device_id
        put_u32(&mut buf, 65536); // nfl_util (stripe unit)
        put_u32(&mut buf, 0); // first_stripe_index
        put_u64(&mut buf, 0); // pattern_offset
        put_u32(&mut buf, 2); // 2 file handles
        // fh1: 4 bytes
        put_u32(&mut buf, 4);
        buf.extend_from_slice(&[1, 2, 3, 4]);
        // fh2: 4 bytes
        put_u32(&mut buf, 4);
        buf.extend_from_slice(&[5, 6, 7, 8]);

        let mut bytes = Bytes::from(buf);
        let content = decode_files_layout(&mut bytes).unwrap();
        match content {
            LayoutContent::FilesLayout {
                device_id,
                stripe_unit,
                is_dense,
                first_stripe_index,
                pattern_offset,
                fh_list,
            } => {
                assert_eq!(device_id, [0xAAu8; 16]);
                assert_eq!(stripe_unit, 65536);
                assert!(!is_dense);
                assert_eq!(first_stripe_index, 0);
                assert_eq!(pattern_offset, 0);
                assert_eq!(fh_list.len(), 2);
                assert_eq!(fh_list[0].as_ref(), &[1, 2, 3, 4]);
                assert_eq!(fh_list[1].as_ref(), &[5, 6, 7, 8]);
            }
            _ => panic!("expected FilesLayout"),
        }
    }

    #[test]
    fn decode_layoutget_truncated() {
        let buf = vec![0u8; 5]; // too short
        let mut bytes = Bytes::from(buf);
        assert!(decode_layoutget_response(&mut bytes).is_err());
    }

    #[test]
    fn iomode_values() {
        assert_eq!(IoMode::Read as u32, 1);
        assert_eq!(IoMode::ReadWrite as u32, 2);
    }

    #[test]
    fn layout_type_values() {
        assert_eq!(LayoutType::NfsV41Files as u32, 1);
        assert_eq!(LayoutType::Osd2Objects as u32, 2);
        assert_eq!(LayoutType::BlockVolume as u32, 3);
    }

    #[test]
    fn decode_files_layout_dense_flag() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0xBBu8; 16]); // device_id
        // nfl_util with dense flag (bit 30) set + stripe_unit = 4096
        let nfl_util: u32 = 0x4000_0000 | 4096;
        put_u32(&mut buf, nfl_util);
        put_u32(&mut buf, 2); // first_stripe_index
        put_u64(&mut buf, 1024); // pattern_offset
        put_u32(&mut buf, 1); // 1 file handle
        put_u32(&mut buf, 4);
        buf.extend_from_slice(&[9, 10, 11, 12]);

        let mut bytes = Bytes::from(buf);
        let content = decode_files_layout(&mut bytes).unwrap();
        match content {
            LayoutContent::FilesLayout {
                stripe_unit,
                is_dense,
                first_stripe_index,
                pattern_offset,
                ..
            } => {
                assert_eq!(stripe_unit, 4096);
                assert!(is_dense);
                assert_eq!(first_stripe_index, 2);
                assert_eq!(pattern_offset, 1024);
            }
            _ => panic!("expected FilesLayout"),
        }
    }

    #[tokio::test]
    async fn device_cache_store_and_get() {
        let mgr = LayoutManager::new(false);
        let dev_id = [0xCCu8; 16];
        let info = DeviceInfo {
            stripe_indices: vec![0],
            ds_addrs: vec![vec!["10.0.0.1:2049".parse().unwrap()]],
        };
        mgr.store_device(dev_id, info).await;
        let got = mgr.get_device(&dev_id).await;
        assert!(got.is_some());
        let got = got.unwrap();
        assert_eq!(got.ds_addrs.len(), 1);
        assert_eq!(
            got.ds_addrs[0][0],
            "10.0.0.1:2049".parse::<SocketAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn device_cache_cleared_on_clear() {
        let mgr = LayoutManager::new(false);
        let dev_id = [0xDDu8; 16];
        mgr.store_device(
            dev_id,
            DeviceInfo {
                stripe_indices: vec![],
                ds_addrs: vec![],
            },
        )
        .await;
        mgr.clear().await;
        assert!(mgr.get_device(&dev_id).await.is_none());
    }

    // --- parse_netaddr4 tests ---

    #[test]
    fn test_parse_netaddr4_basic() {
        // 192.168.1.1 port 2049: 2049 = 8*256 + 1
        let addr = parse_netaddr4("tcp", "192.168.1.1.8.1").unwrap();
        assert_eq!(
            addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 2049)
        );
    }

    #[test]
    fn test_parse_netaddr4_high_port() {
        // 10.0.0.1 port 8080: 8080 = 31*256 + 144
        let addr = parse_netaddr4("tcp", "10.0.0.1.31.144").unwrap();
        assert_eq!(
            addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080)
        );
    }

    #[test]
    fn test_parse_netaddr4_tcp6() {
        // Full IPv6 address: 2001:db8::1, port 2049 (8*256+1)
        let addr = parse_netaddr4("tcp6", "2001:db8::1.8.1").unwrap();
        assert_eq!(addr.port(), 2049);
        assert_eq!(addr.ip().to_string(), "2001:db8::1");
    }

    #[test]
    fn test_parse_netaddr4_unsupported_netid() {
        assert!(parse_netaddr4("udp", "10.0.0.1.8.1").is_err());
    }

    #[test]
    fn test_parse_netaddr4_invalid_addr() {
        assert!(parse_netaddr4("tcp", "10.0.0.1.8").is_err()); // only 5 parts
        assert!(parse_netaddr4("tcp", "10.0.0.1.8.1.2").is_err()); // 7 parts
        assert!(parse_netaddr4("tcp", "999.0.0.1.8.1").is_err()); // octet > 255
    }

    #[test]
    fn parse_netaddr4_ipv4() {
        let addr = parse_netaddr4("tcp", "192.168.1.1.8.1").unwrap();
        assert_eq!(addr.port(), 8 * 256 + 1); // = 2049
        assert_eq!(addr.ip().to_string(), "192.168.1.1");
    }

    #[test]
    fn parse_netaddr4_ipv6_loopback() {
        // "::1.8.1" = IPv6 loopback (::1), port = 8*256+1 = 2049
        let addr = parse_netaddr4("tcp6", "::1.8.1").unwrap();
        assert_eq!(addr.port(), 2049);
        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn parse_netaddr4_invalid_netid() {
        assert!(parse_netaddr4("udp", "1.2.3.4.0.1").is_err());
    }

    #[test]
    fn parse_netaddr4_ipv4_invalid() {
        assert!(parse_netaddr4("tcp", "999.1.1.1.0.1").is_err()); // 999 not a valid u8
    }

    // --- stripe_ds_index tests ---

    #[test]
    fn test_stripe_ds_index_basic() {
        // 4 DSes, stripe_unit=4096, first_stripe_index=0
        // offset 0 → stripe 0 → DS 0
        assert_eq!(stripe_ds_index(0, 4096, 0, 4), 0);
        // offset 4096 → stripe 1 → DS 1
        assert_eq!(stripe_ds_index(4096, 4096, 0, 4), 1);
        // offset 8192 → stripe 2 → DS 2
        assert_eq!(stripe_ds_index(8192, 4096, 0, 4), 2);
        // offset 16384 → stripe 4 → DS 0 (wraps)
        assert_eq!(stripe_ds_index(16384, 4096, 0, 4), 0);
    }

    #[test]
    fn test_stripe_ds_index_with_first_stripe_offset() {
        // first_stripe_index=2, 4 DSes
        // offset 0 → stripe 0 → (0+2)%4 = DS 2
        assert_eq!(stripe_ds_index(0, 4096, 2, 4), 2);
        // offset 4096 → stripe 1 → (1+2)%4 = DS 3
        assert_eq!(stripe_ds_index(4096, 4096, 2, 4), 3);
        // offset 8192 → stripe 2 → (2+2)%4 = DS 0
        assert_eq!(stripe_ds_index(8192, 4096, 2, 4), 0);
    }

    // --- ds_offset tests ---

    #[test]
    fn test_ds_offset_dense() {
        // Dense: DS holds every num_ds-th stripe, compacted without gaps.
        // 4 DSes, stripe_unit=4096:
        // offset 0 → stripe 0, ds_stripe=0/4=0, in_stripe=0 → 0
        assert_eq!(ds_offset(0, 4096, 0, 4, true), 0);
        // offset 12345 → stripe 3 (12345/4096=3), ds_stripe=3/4=0, in_stripe=57 → 57
        assert_eq!(ds_offset(12345, 4096, 0, 4, true), 57);
        // offset 16384 → stripe 4, ds_stripe=4/4=1, in_stripe=0 → 4096
        assert_eq!(ds_offset(16384, 4096, 0, 4, true), 4096);
    }

    #[test]
    fn test_ds_offset_sparse() {
        // Sparse layout: DS offset == file_offset (DS preserves original file positions).
        // RFC 5661 §13.3.2
        assert_eq!(ds_offset(0, 4096, 0, 4, false), 0);
        assert_eq!(ds_offset(4096, 4096, 0, 4, false), 4096);
        assert_eq!(ds_offset(8192, 4096, 0, 4, false), 8192);
        assert_eq!(ds_offset(16484, 4096, 0, 4, false), 16484);
    }

    // --- split_into_stripes tests ---

    #[test]
    fn test_split_into_stripes_single_stripe() {
        // Entire range within one stripe
        let chunks = split_into_stripes(0, 1000, 4096, false, 0, 4, 0);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].ds_index, 0);
        assert_eq!(chunks[0].file_offset, 0);
        assert_eq!(chunks[0].length, 1000);
    }

    #[test]
    fn test_split_into_stripes_crosses_boundary() {
        // Range 3000..5000 crosses the 4096 boundary
        let chunks = split_into_stripes(3000, 2000, 4096, false, 0, 4, 0);
        assert_eq!(chunks.len(), 2);
        // First chunk: 3000..4096 (1096 bytes) on DS 0
        assert_eq!(chunks[0].ds_index, 0);
        assert_eq!(chunks[0].file_offset, 3000);
        assert_eq!(chunks[0].length, 1096);
        // Second chunk: 4096..5000 (904 bytes) on DS 1
        assert_eq!(chunks[1].ds_index, 1);
        assert_eq!(chunks[1].file_offset, 4096);
        assert_eq!(chunks[1].length, 904);
    }

    #[test]
    fn test_split_into_stripes_multiple() {
        // 3 full stripes: 0..12288 with stripe_unit=4096, 4 DSes
        let chunks = split_into_stripes(0, 12288, 4096, false, 0, 4, 0);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].ds_index, 0);
        assert_eq!(chunks[1].ds_index, 1);
        assert_eq!(chunks[2].ds_index, 2);
        for c in &chunks {
            assert_eq!(c.length, 4096);
        }
    }

    // --- decode_getdeviceinfo_response tests ---

    /// Helper to write an XDR string into a buffer.
    fn put_xdr_string(buf: &mut Vec<u8>, s: &str) {
        let bytes = s.as_bytes();
        put_u32(buf, bytes.len() as u32);
        buf.extend_from_slice(bytes);
        // XDR padding to 4-byte boundary
        let pad = (4 - (bytes.len() % 4)) % 4;
        for _ in 0..pad {
            buf.push(0);
        }
    }

    #[test]
    fn test_decode_getdeviceinfo_single_ds() {
        // Build da_addr_body first
        let mut body = Vec::new();
        // stripe_indices: 1 index (value 0)
        put_u32(&mut body, 1);
        put_u32(&mut body, 0);
        // multipath_ds_list: 1 DS
        put_u32(&mut body, 1);
        // DS 0: 1 address
        put_u32(&mut body, 1);
        put_xdr_string(&mut body, "tcp");
        put_xdr_string(&mut body, "192.168.1.1.8.1"); // port 2049

        // Build outer: layout_type + opaque body
        let mut buf = Vec::new();
        put_u32(&mut buf, 1); // LAYOUT4_NFSV4_1_FILES
        put_u32(&mut buf, body.len() as u32);
        buf.extend_from_slice(&body);
        // Pad if needed
        let pad = (4 - (body.len() % 4)) % 4;
        buf.extend(std::iter::repeat_n(0, pad));

        let mut bytes = Bytes::from(buf);
        let info = decode_getdeviceinfo_response(&mut bytes).unwrap();
        assert_eq!(info.ds_addrs.len(), 1);
        assert_eq!(info.ds_addrs[0].len(), 1);
        assert_eq!(
            info.ds_addrs[0][0],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 2049)
        );
    }

    #[test]
    fn test_decode_getdeviceinfo_multiple_ds() {
        let mut body = Vec::new();
        // stripe_indices: 2 indices
        put_u32(&mut body, 2);
        put_u32(&mut body, 0);
        put_u32(&mut body, 1);
        // multipath_ds_list: 2 DSes
        put_u32(&mut body, 2);
        // DS 0: 1 address
        put_u32(&mut body, 1);
        put_xdr_string(&mut body, "tcp");
        put_xdr_string(&mut body, "10.0.0.1.8.1"); // 10.0.0.1:2049
        // DS 1: 2 addresses (multipath)
        put_u32(&mut body, 2);
        put_xdr_string(&mut body, "tcp");
        put_xdr_string(&mut body, "10.0.0.2.8.1"); // 10.0.0.2:2049
        put_xdr_string(&mut body, "tcp");
        put_xdr_string(&mut body, "10.0.0.3.8.1"); // 10.0.0.3:2049

        let mut buf = Vec::new();
        put_u32(&mut buf, 1);
        put_u32(&mut buf, body.len() as u32);
        buf.extend_from_slice(&body);
        let pad = (4 - (body.len() % 4)) % 4;
        buf.extend(std::iter::repeat_n(0, pad));

        let mut bytes = Bytes::from(buf);
        let info = decode_getdeviceinfo_response(&mut bytes).unwrap();
        assert_eq!(info.ds_addrs.len(), 2);
        assert_eq!(info.ds_addrs[0].len(), 1);
        assert_eq!(info.ds_addrs[1].len(), 2);
        assert_eq!(
            info.ds_addrs[1][0],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 2049)
        );
        assert_eq!(
            info.ds_addrs[1][1],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 2049)
        );
    }

    #[test]
    fn test_decode_getdeviceinfo_unsupported_layout_type() {
        let mut buf = Vec::new();
        put_u32(&mut buf, 2); // OSD layout type
        put_u32(&mut buf, 0); // empty body

        let mut bytes = Bytes::from(buf);
        assert!(decode_getdeviceinfo_response(&mut bytes).is_err());
    }

    #[test]
    fn test_decode_getdeviceinfo_truncated() {
        let buf = vec![0u8; 2]; // too short
        let mut bytes = Bytes::from(buf);
        assert!(decode_getdeviceinfo_response(&mut bytes).is_err());
    }

    #[test]
    fn ds_offset_dense_multi_ds() {
        // 3 DSs, stripe_unit=4096
        // Byte 0 → DS0 stripe 0, DS offset = 0
        assert_eq!(ds_offset(0, 4096, 0, 3, true), 0);
        // Byte 4096 → DS1 stripe 0, DS offset = 0 (dense: DS1 has its own compacted space)
        assert_eq!(ds_offset(4096, 4096, 0, 3, true), 0);
        // Byte 8192 → DS2 stripe 0, DS offset = 0
        assert_eq!(ds_offset(8192, 4096, 0, 3, true), 0);
        // Byte 12288 → DS0 stripe 1, DS offset = 4096
        assert_eq!(ds_offset(12288, 4096, 0, 3, true), 4096);
        // Byte 4097 → DS1 stripe 0, DS offset = 1
        assert_eq!(ds_offset(4097, 4096, 0, 3, true), 1);
    }

    #[test]
    fn split_into_stripes_pattern_offset_zero() {
        // pattern_offset=0, 2 DSs
        let chunks = split_into_stripes(0, 8192, 4096, false, 0, 2, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].ds_index, 0);
        assert_eq!(chunks[1].ds_index, 1);
    }

    #[test]
    fn split_into_stripes_pattern_offset_nonzero() {
        // pattern_offset=4096: stripe pattern starts at byte 4096
        // Range [4096, 8192): adjusted offset = 0, so DS0, ds_offset=0
        let chunks = split_into_stripes(4096, 4096, 4096, false, 0, 2, 4096);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].ds_index, 0);
        assert_eq!(chunks[0].ds_offset, 0);
    }

    #[tokio::test]
    async fn layout_refresh_crossing_is_recorded_once_per_window() {
        let manager = LayoutManager::new(true);
        let fh = Bytes::from_static(b"large-file");

        assert!(
            !manager
                .layout_refresh_due(&fh, PNFS_LAYOUT_REFRESH_INTERVAL - 1)
                .await
        );
        assert!(
            manager
                .layout_refresh_due(&fh, PNFS_LAYOUT_REFRESH_INTERVAL)
                .await
        );
        manager
            .record_layout_refresh(&fh, PNFS_LAYOUT_REFRESH_INTERVAL)
            .await;
        assert!(
            !manager
                .layout_refresh_due(&fh, PNFS_LAYOUT_REFRESH_INTERVAL + 1)
                .await
        );
        assert!(
            manager
                .layout_refresh_due(&fh, PNFS_LAYOUT_REFRESH_INTERVAL * 2)
                .await
        );
    }

    #[tokio::test]
    async fn layout_refresh_offsets_are_independent_per_file() {
        let manager = LayoutManager::new(true);
        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"second");
        manager
            .record_layout_refresh(&first, PNFS_LAYOUT_REFRESH_INTERVAL)
            .await;

        assert!(
            !manager
                .layout_refresh_due(&first, PNFS_LAYOUT_REFRESH_INTERVAL + 1)
                .await
        );
        assert!(
            manager
                .layout_refresh_due(&second, PNFS_LAYOUT_REFRESH_INTERVAL + 1)
                .await
        );
    }

    #[tokio::test]
    async fn removing_layout_resets_its_refresh_lifecycle() {
        let manager = LayoutManager::new(true);
        let fh = Bytes::from_static(b"reopened-file");
        let layout = Layout {
            generation: manager.generation(),
            stateid: [9; 16],
            return_on_close: false,
            segments: vec![],
        };
        manager.store_layout(&fh, layout).await;
        manager
            .record_layout_refresh(&fh, PNFS_LAYOUT_REFRESH_INTERVAL)
            .await;
        assert!(
            !manager
                .layout_refresh_due(&fh, PNFS_LAYOUT_REFRESH_INTERVAL + 1)
                .await
        );

        manager.remove_layout(&fh).await;

        assert!(
            manager
                .layout_refresh_due(&fh, PNFS_LAYOUT_REFRESH_INTERVAL + 1)
                .await
        );
    }

    #[tokio::test]
    async fn cached_layout_is_returned_only_for_a_covered_offset() {
        let manager = LayoutManager::new(true);
        let fh = Bytes::from_static(b"multipart-file");
        manager
            .store_layout(
                &fh,
                Layout {
                    generation: manager.generation(),
                    stateid: [7; 16],
                    return_on_close: false,
                    segments: vec![LayoutSegment {
                        offset: 1024,
                        length: 2048,
                        iomode: IoMode::ReadWrite,
                        layout_type: LayoutType::NfsV41Files,
                        content: LayoutContent::Opaque(Bytes::new()),
                    }],
                },
            )
            .await;

        assert!(manager.get_layout_covering(&fh, 1024).await.is_some());
        assert!(manager.get_layout_covering(&fh, 3071).await.is_some());
        assert!(manager.get_layout_covering(&fh, 3072).await.is_none());
    }

    #[tokio::test]
    async fn layout_update_replaces_overlapping_segment_and_keeps_other_ranges() {
        let manager = LayoutManager::new(true);
        let fh = Bytes::from_static(b"growing-multipart-file");
        let segment = |offset, length, marker| LayoutSegment {
            offset,
            length,
            iomode: IoMode::ReadWrite,
            layout_type: LayoutType::NfsV41Files,
            content: LayoutContent::Opaque(Bytes::from(vec![marker])),
        };
        manager
            .store_layout(
                &fh,
                Layout {
                    generation: manager.generation(),
                    stateid: [1; 16],
                    return_on_close: false,
                    segments: vec![segment(0, 1024, 1), segment(2048, 1024, 2)],
                },
            )
            .await;

        manager
            .merge_layout(
                &fh,
                Layout {
                    generation: manager.generation(),
                    stateid: [3; 16],
                    return_on_close: true,
                    segments: vec![segment(512, 2048, 3)],
                },
            )
            .await;

        let merged = manager.get_layout(&fh).await.unwrap();
        assert_eq!(merged.stateid, [3; 16]);
        assert!(merged.return_on_close);
        assert_eq!(merged.segments.len(), 1);
        assert_eq!(merged.segments[0].offset, 512);
        assert_eq!(merged.segments[0].length, 2048);
    }
}
