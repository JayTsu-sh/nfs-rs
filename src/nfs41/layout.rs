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

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::{Buf, Bytes};
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::error::{NfsError, Result};
use crate::rpc;

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
    pub stateid: [u8; 16],
    pub return_on_close: bool,
    pub segments: Vec<LayoutSegment>,
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

/// Manages active layouts and data server connections.
pub(crate) struct LayoutManager {
    /// Layouts indexed by file handle.
    layouts: RwLock<HashMap<Bytes, Layout>>,
    /// Cached connections to data servers.
    data_servers: RwLock<HashMap<SocketAddr, rpc::Client>>,
    /// Cached device info indexed by device ID.
    device_cache: RwLock<HashMap<[u8; 16], DeviceInfo>>,
}

impl LayoutManager {
    pub fn new() -> Self {
        Self {
            layouts: RwLock::new(HashMap::new()),
            data_servers: RwLock::new(HashMap::new()),
            device_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Store a layout for a file.
    pub async fn store_layout(&self, fh: &Bytes, layout: Layout) {
        let mut map = self.layouts.write().await;
        debug!(fh_len = fh.len(), segments = layout.segments.len(), "layout stored");
        map.insert(fh.clone(), layout);
    }

    /// Get the layout for a file, if any.
    pub async fn get_layout(&self, fh: &Bytes) -> Option<Layout> {
        let map = self.layouts.read().await;
        map.get(fh).cloned()
    }

    /// Remove the layout for a file.
    pub async fn remove_layout(&self, fh: &Bytes) -> Option<Layout> {
        let mut map = self.layouts.write().await;
        map.remove(fh)
    }

    /// Drain all layouts, returning them for LAYOUTRETURN.
    pub async fn drain_layouts(&self) -> Vec<(Bytes, Layout)> {
        let mut map = self.layouts.write().await;
        map.drain().collect()
    }

    /// Remove all layouts and device cache atomically.
    ///
    /// Both locks are acquired before clearing to prevent a window where one map
    /// is empty but the other still has stale entries.
    /// Lock order: layouts -> device_cache (consistent with all other callers to prevent deadlock).
    pub async fn clear(&self) {
        let mut map = self.layouts.write().await;
        let mut devices = self.device_cache.write().await;
        map.clear();
        devices.clear();
    }

    /// Store a device info entry in the cache.
    pub async fn store_device(&self, device_id: [u8; 16], info: DeviceInfo) {
        let mut cache = self.device_cache.write().await;
        debug!(device_id = ?device_id, ds_count = info.ds_addrs.len(), "device info cached");
        cache.insert(device_id, info);
    }

    /// Get a device info entry from the cache.
    pub async fn get_device(&self, device_id: &[u8; 16]) -> Option<DeviceInfo> {
        let cache = self.device_cache.read().await;
        cache.get(device_id).cloned()
    }

    /// Get or create a connection to a data server.
    pub async fn get_data_server(&self, addr: SocketAddr) -> Result<rpc::Client> {
        // Fast path: read lock only
        {
            let servers = self.data_servers.read().await;
            if let Some(client) = servers.get(&addr) {
                return Ok(client.clone());
            }
        }
        // TCP connect OUTSIDE any lock — can be slow without stalling other I/O
        let mux = rpc::StreamMux::connect(addr).await?;
        let new_client = rpc::Client::new(mux, None);
        // Acquire write lock and re-check (another task may have connected concurrently)
        let mut servers = self.data_servers.write().await;
        if let Some(existing) = servers.get(&addr) {
            // Race: another task already connected; use existing connection
            return Ok(existing.clone());
        }
        servers.insert(addr, new_client.clone());
        info!(addr = %addr, "connected to pNFS data server");
        Ok(new_client)
    }
}

/// Decode a LAYOUTGET response into a Layout.
pub(crate) fn decode_layoutget_response(data: &mut Bytes) -> Result<Layout> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("LAYOUTGET return_on_close truncated".to_string()));
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
        return Err(NfsError::Xdr("LAYOUTGET segments array truncated".to_string()));
    }
    let num_segments = data.get_u32() as usize;
    if num_segments > 1024 {
        return Err(NfsError::Xdr(format!("LAYOUTGET has {} segments, max 1024", num_segments)));
    }
    let mut segments = Vec::with_capacity(num_segments);

    for _ in 0..num_segments {
        if data.remaining() < 24 {
            return Err(NfsError::Xdr("layout4 segment header truncated".to_string()));
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
        return Err(NfsError::Xdr("files_layout first_stripe_index truncated".to_string()));
    }
    let first_stripe_index = data.get_u32();

    // nfl_pattern_offset: offset4 (uint64)
    if data.remaining() < 8 {
        return Err(NfsError::Xdr("files_layout pattern_offset truncated".to_string()));
    }
    let pattern_offset = data.get_u64();

    // nfl_fh_list: nfs_fh4<>
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("files_layout fh_list length truncated".to_string()));
    }
    let num_fhs = data.get_u32() as usize;
    if num_fhs > 4096 {
        return Err(NfsError::Xdr(format!("files_layout has {} FHs, max 4096", num_fhs)));
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
        return Err(NfsError::Xdr("GETDEVICEINFO layout_type truncated".to_string()));
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
        return Err(NfsError::Xdr("GETDEVICEINFO da_addr_body length truncated".to_string()));
    }
    let body_len = data.get_u32() as usize;
    let padded = (body_len + 3) & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr("GETDEVICEINFO da_addr_body truncated".to_string()));
    }
    let mut body = data.split_to(body_len);
    let pad = padded - body_len;
    if data.remaining() >= pad {
        data.advance(pad);
    }

    // nfsv4_1_file_layout_ds_addr4:
    // stripe_indices: uint32<>
    if body.remaining() < 4 {
        return Err(NfsError::Xdr("GETDEVICEINFO stripe_indices length truncated".to_string()));
    }
    let num_indices = body.get_u32() as usize;
    if num_indices > 4096 {
        return Err(NfsError::Xdr(format!(
            "GETDEVICEINFO has {} stripe_indices, max 4096",
            num_indices
        )));
    }
    if body.remaining() < num_indices * 4 {
        return Err(NfsError::Xdr("GETDEVICEINFO stripe_indices data truncated".to_string()));
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

    Ok(DeviceInfo { stripe_indices, ds_addrs })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_be_bytes()); }
    fn put_u64(buf: &mut Vec<u8>, v: u64) { buf.extend_from_slice(&v.to_be_bytes()); }

    #[tokio::test]
    async fn layout_manager_store_and_get() {
        let mgr = LayoutManager::new();
        let fh = Bytes::from_static(b"file1");
        let layout = Layout {
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
    async fn layout_manager_remove() {
        let mgr = LayoutManager::new();
        let fh = Bytes::from_static(b"file2");
        mgr.store_layout(&fh, Layout { stateid: [2u8; 16], return_on_close: false, segments: vec![] }).await;
        let removed = mgr.remove_layout(&fh).await;
        assert!(removed.is_some());
        assert!(mgr.get_layout(&fh).await.is_none());
    }

    #[tokio::test]
    async fn layout_manager_clear() {
        let mgr = LayoutManager::new();
        mgr.store_layout(&Bytes::from_static(b"a"), Layout { stateid: [0u8; 16], return_on_close: false, segments: vec![] }).await;
        mgr.store_layout(&Bytes::from_static(b"b"), Layout { stateid: [0u8; 16], return_on_close: false, segments: vec![] }).await;
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
        assert!(matches!(layout.segments[0].content, LayoutContent::Opaque(_)));
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
            LayoutContent::FilesLayout { device_id, stripe_unit, is_dense, first_stripe_index, pattern_offset, fh_list } => {
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
            LayoutContent::FilesLayout { stripe_unit, is_dense, first_stripe_index, pattern_offset, .. } => {
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
        let mgr = LayoutManager::new();
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
        assert_eq!(got.ds_addrs[0][0], "10.0.0.1:2049".parse::<SocketAddr>().unwrap());
    }

    #[tokio::test]
    async fn device_cache_cleared_on_clear() {
        let mgr = LayoutManager::new();
        let dev_id = [0xDDu8; 16];
        mgr.store_device(dev_id, DeviceInfo { stripe_indices: vec![], ds_addrs: vec![] }).await;
        mgr.clear().await;
        assert!(mgr.get_device(&dev_id).await.is_none());
    }

    // --- parse_netaddr4 tests ---

    #[test]
    fn test_parse_netaddr4_basic() {
        // 192.168.1.1 port 2049: 2049 = 8*256 + 1
        let addr = parse_netaddr4("tcp", "192.168.1.1.8.1").unwrap();
        assert_eq!(addr, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 2049));
    }

    #[test]
    fn test_parse_netaddr4_high_port() {
        // 10.0.0.1 port 8080: 8080 = 31*256 + 144
        let addr = parse_netaddr4("tcp", "10.0.0.1.31.144").unwrap();
        assert_eq!(addr, SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080));
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
        for _ in 0..pad {
            buf.push(0);
        }

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
        for _ in 0..pad {
            buf.push(0);
        }

        let mut bytes = Bytes::from(buf);
        let info = decode_getdeviceinfo_response(&mut bytes).unwrap();
        assert_eq!(info.ds_addrs.len(), 2);
        assert_eq!(info.ds_addrs[0].len(), 1);
        assert_eq!(info.ds_addrs[1].len(), 2);
        assert_eq!(info.ds_addrs[1][0], SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 2049));
        assert_eq!(info.ds_addrs[1][1], SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 2049));
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
}
