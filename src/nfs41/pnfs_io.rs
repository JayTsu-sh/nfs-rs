//! pNFS I/O: parallel reads and writes to data servers (RFC 5661 §12-13).
//!
//! This module adds pNFS layout-aware I/O methods to `Mount41`. When a file
//! has a granted layout, reads and writes are striped across data servers in
//! parallel. If pNFS is unavailable or fails, callers fall back to MDS I/O.

use std::collections::HashSet;

use bytes::{Buf, Bytes, BytesMut};
use tracing::debug;

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
    pub(crate) async fn get_or_fetch_layout(
        &self,
        fh: &Bytes,
        iomode: u32,
    ) -> Option<Layout> {
        // 1. Check cache
        if let Some(layout) = self.layout_manager.get_layout(fh).await {
            return Some(layout);
        }

        // 2. LAYOUTGET to MDS: COMPOUND(SEQUENCE, PUTFH, LAYOUTGET)
        // iomode 1=READ, 2=RW — use matching access mode to avoid NFS4ERR_OPENMODE.
        let access = if iomode == 1 { AccessMode::Read } else { AccessMode::Write };
        let sid = self.state.has_open(fh, access).await
            .unwrap_or_else(StateId::anonymous);
        let result = self
            .compound("layoutget", |b| {
                b.putfh(fh).layoutget(
                    false,                      // signal_layout_avail
                    1,                          // LAYOUT4_NFSV4_1_FILES
                    iomode,                     // 1=READ, 2=RW
                    0,                          // offset = whole file
                    0xFFFF_FFFF_FFFF_FFFF,      // length = whole file
                    0,                          // min_length
                    &sid.raw,
                    1024 * 1024,                // max_count (1 MiB)
                )
            })
            .await;

        match result {
            Ok(resp) => {
                // LAYOUTGET result is after SEQUENCE=0, PUTFH=1 → index 2
                let op = resp.op_ok(2).ok()?;
                let mut data = op.data.clone();
                let layout =
                    super::layout::decode_layoutget_response(&mut data).ok()?;
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
                        b.putrootfh()
                            .getdeviceinfo(device_id, 1, 1024 * 1024)
                    })
                    .await
                {
                    Ok(resp) => {
                        // GETDEVICEINFO is after SEQUENCE=0, PUTROOTFH=1 → index 2
                        if let Ok(op) = resp.op_ok(2) {
                            let mut data = op.data.clone();
                            if let Ok(info) =
                                super::layout::decode_getdeviceinfo_response(&mut data)
                            {
                                self.layout_manager
                                    .store_device(*device_id, info)
                                    .await;
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
                } => (*device_id, *stripe_unit, *is_dense, *first_stripe_index, *pattern_offset, fh_list),
                _ => return None,
            };

        if stripe_unit == 0 || fh_list.is_empty() {
            return None;
        }
        let device = self.layout_manager.get_device(&device_id).await?;
        if device.ds_addrs.len() < fh_list.len() {
            return None;
        }

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
                    .ok_or_else(|| NfsError::Rpc(format!("fh_list index {} out of range", chunk.ds_index)));
                let ds_phys_idx = device.stripe_indices
                    .get(chunk.ds_index as usize)
                    .copied()
                    .unwrap_or(chunk.ds_index) as usize;
                let ds_addr_res = device
                    .ds_addrs
                    .get(ds_phys_idx)
                    .and_then(|a| a.first())
                    .copied()
                    .ok_or_else(|| NfsError::Rpc(format!("DS index {} out of range", ds_phys_idx)));
                let layout_stateid = layout.stateid;
                let auth = self.auth.clone();
                let lm = self.layout_manager.clone();
                let chunk_len = chunk.length;
                let chunk_ds_offset = chunk.ds_offset;
                async move {
                    let ds_fh = ds_fh_res?;
                    let ds_addr = ds_addr_res?;
                    let ds_client = lm.get_data_server(ds_addr).await?;
                    let resp = Mount41::compound_ds(
                        &ds_client,
                        &auth,
                        "ds-read",
                        chunk_len as usize,
                        |b| b.putfh(&ds_fh).read(&layout_stateid, chunk_ds_offset, chunk_len),
                    )
                    .await?;
                    resp.op_ok(0)?; // PUTFH (no SEQUENCE on DS)
                    let read_op = resp.op_ok(1)?; // READ
                    let mut data = read_op.data.clone();
                    // READ4resok: eof(4) + data<>
                    if data.remaining() < 4 {
                        return Err(NfsError::Xdr(
                            "DS READ result too short".to_string(),
                        ));
                    }
                    let _eof = data.get_u32();
                    if data.remaining() < 4 {
                        return Err(NfsError::Xdr(
                            "DS READ data length missing".to_string(),
                        ));
                    }
                    let data_len = data.get_u32() as usize;
                    if data.remaining() < data_len {
                        return Err(NfsError::Xdr(
                            "DS READ data truncated".to_string(),
                        ));
                    }
                    Ok::<Bytes, NfsError>(data.slice(..data_len))
                }
            })
            .collect();

        match futures::future::try_join_all(futures).await {
            Ok(results) => {
                if results.len() == 1 {
                    Some(Ok(results
                        .into_iter()
                        .next()
                        .unwrap_or_default()))
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
                } => (*device_id, *stripe_unit, *is_dense, *first_stripe_index, *pattern_offset, fh_list),
                _ => return None,
            };

        if stripe_unit == 0 || fh_list.is_empty() {
            return None;
        }
        let device = self.layout_manager.get_device(&device_id).await?;
        if device.ds_addrs.len() < fh_list.len() {
            return None;
        }

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
                    .ok_or_else(|| NfsError::Rpc(format!("fh_list index {} out of range", chunk.ds_index)));
                let ds_phys_idx = device.stripe_indices
                    .get(chunk.ds_index as usize)
                    .copied()
                    .unwrap_or(chunk.ds_index) as usize;
                let ds_addr_res = device
                    .ds_addrs
                    .get(ds_phys_idx)
                    .and_then(|a| a.first())
                    .copied()
                    .ok_or_else(|| NfsError::Rpc(format!("DS index {} out of range", ds_phys_idx)));
                let layout_stateid = layout.stateid;
                let auth = self.auth.clone();
                let lm = self.layout_manager.clone();
                // Zero-copy slice of the write data for this stripe chunk
                let chunk_start = (chunk.file_offset - offset) as usize;
                let chunk_data = data.slice(chunk_start..chunk_start + chunk.length as usize);
                let chunk_len = chunk.length;
                let ds_off = chunk.ds_offset;
                async move {
                    let ds_fh = ds_fh_res?;
                    let ds_addr = ds_addr_res?;
                    let ds_client = lm.get_data_server(ds_addr).await?;
                    let resp = Mount41::compound_ds_write(
                        &ds_client,
                        &auth,
                        "ds-write",
                        chunk_data,
                        |b| {
                            b.putfh(&ds_fh)
                                .write_header(&layout_stateid, ds_off, 2 /* FILE_SYNC4 */, chunk_len)
                        },
                    )
                    .await?;
                    resp.op_ok(0)?; // PUTFH
                    let write_op = resp.op_ok(1)?; // WRITE
                    let mut d = write_op.data.clone();
                    if d.remaining() < 16 {
                        return Err(NfsError::Xdr(
                            "DS WRITE result too short".to_string(),
                        ));
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
                // LAYOUTCOMMIT to MDS: inform MDS that data was written via layout
                let _ = self
                    .compound("layoutcommit", |b| {
                        b.putfh(fh).layoutcommit(
                            offset,
                            data_len as u64,
                            false,
                            &layout.stateid,
                            Some(offset + data_len as u64 - 1),
                            1, // LAYOUT4_NFSV4_1_FILES
                        )
                    })
                    .await;
                // RFC 5661 §18.32.3: if any DS downgraded write stability, COMMIT to MDS.
                if needs_commit {
                    let _ = self.commit(fh.clone(), offset, total).await;
                }
                Some(Ok(total))
            }
            Err(e) => {
                self.layout_manager.remove_layout(fh).await;
                debug!(error = %e, "pNFS write failed, falling back to MDS");
                None
            }
        }
    }

    // ─── pNFS Layout Return ──────────────────────────────────────────────

    /// Return a layout to the metadata server (LAYOUTRETURN4_FILE).
    /// Removes the layout from the local cache and notifies the server.
    /// Errors are logged but not propagated — layout return is best-effort.
    pub(crate) async fn layoutreturn_file(&self, fh: &Bytes) {
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
                    false,                          // reclaim
                    1,                              // LAYOUT4_NFSV4_1_FILES
                    iomode,
                    1,                              // LAYOUTRETURN4_FILE
                    0,                              // offset = whole file
                    0xFFFF_FFFF_FFFF_FFFF,          // length = whole file
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
    use crate::nfs41::layout::{
        IoMode, Layout, LayoutContent, LayoutSegment, LayoutType,
    };

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
