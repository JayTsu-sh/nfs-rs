//! RFC 7862/7863 operations over the shared session engine.
use super::mount::Mount41;
use super::state::{AccessMode, StateId};
use crate::{
    CopyResult, Nfs42Attributes, NfsError, ReadPlusContent, ReadPlusResult, Result, SeekContent,
    SeekResult,
};
use bytes::{Buf, Bytes};
use std::sync::Arc;

pub(super) fn word(buf: &mut Bytes) -> Result<u32> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("NFSv4.2 u32 truncated".into()));
    }
    Ok(buf.get_u32())
}

pub(super) fn decode_u64(buf: &mut Bytes) -> Result<u64> {
    if buf.remaining() < 8 {
        return Err(NfsError::Xdr("NFSv4.2 u64 truncated".into()));
    }
    Ok(buf.get_u64())
}

pub(super) fn decode_bool(buf: &mut Bytes) -> Result<bool> {
    match word(buf)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(NfsError::Xdr("invalid NFSv4.2 XDR boolean".into())),
    }
}

pub(super) fn opaque(buf: &mut Bytes) -> Result<Bytes> {
    crate::nfs4::compound::take_opaque(buf, "NFSv4.2 opaque")
}

pub(super) fn result_data(
    response: &super::compound::CompoundResponse,
    index: usize,
    opcode: u32,
) -> Result<Bytes> {
    let op = response.op_ok(index)?;
    if op.opcode != opcode {
        return Err(NfsError::Xdr("NFSv4.2 response opcode mismatch".into()));
    }
    Ok(op.data.clone())
}

pub(super) struct CopyReply {
    callback: Option<[u8; 16]>,
    count: u64,
    committed: crate::WriteCommitted,
    verifier: [u8; 8],
    consecutive: bool,
    synchronous: bool,
}

pub(super) fn decode_copy(buf: &mut Bytes) -> Result<CopyReply> {
    let callback = match word(buf)? {
        0 => None,
        1 if buf.remaining() >= 16 => {
            let mut id = [0; 16];
            buf.copy_to_slice(&mut id);
            Some(id)
        }
        _ => return Err(NfsError::Xdr("invalid COPY callback stateid array".into())),
    };
    let count = decode_u64(buf)?;
    let committed = crate::WriteCommitted::try_from(word(buf)?)?;
    if buf.remaining() < 8 {
        return Err(NfsError::Xdr("COPY verifier truncated".into()));
    }
    let mut verifier = [0; 8];
    buf.copy_to_slice(&mut verifier);
    Ok(CopyReply {
        callback,
        count,
        committed,
        verifier,
        consecutive: decode_bool(buf)?,
        synchronous: decode_bool(buf)?,
    })
}

pub(super) fn decode_read_plus(buf: &mut Bytes) -> Result<ReadPlusResult> {
    let eof = decode_bool(buf)?;
    let count = word(buf)? as usize;
    if count > 4096 || count > buf.remaining() / 16 {
        return Err(NfsError::Xdr(
            "READ_PLUS excessive/truncated contents array".into(),
        ));
    }
    let mut contents = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = word(buf)?;
        let offset = decode_u64(buf)?;
        contents.push(match kind {
            0 => ReadPlusContent::Data {
                offset,
                data: opaque(buf)?,
            },
            1 => ReadPlusContent::Hole {
                offset,
                length: decode_u64(buf)?,
            },
            _ => return Err(NfsError::Xdr("READ_PLUS unknown content kind".into())),
        });
    }
    Ok(ReadPlusResult { eof, contents })
}

// Whole-hole extents may start before and end after the requested interval
// (RFC 7862 §15.10.3). Validate contiguity, then clip without allocating zeros.
pub(super) fn clip_read_plus(
    reply: ReadPlusResult,
    offset: u64,
    count: u32,
) -> Result<ReadPlusResult> {
    let end = offset
        .checked_add(u64::from(count))
        .ok_or_else(|| NfsError::InvalidInput("READ_PLUS range overflow".into()))?;
    let mut cursor = offset;
    let mut previous_end = None;
    let mut clipped_eof = reply.eof;
    let mut contents = Vec::new();
    for extent in reply.contents {
        let (start, length) = match &extent {
            ReadPlusContent::Data { offset, data } => (*offset, data.len() as u64),
            ReadPlusContent::Hole { offset, length } => (*offset, *length),
        };
        let extent_end = start
            .checked_add(length)
            .ok_or_else(|| NfsError::Xdr("READ_PLUS extent overflow".into()))?;
        if length == 0
            || previous_end.is_some_and(|last| last != start)
            || start > cursor
            || extent_end <= cursor
            || cursor == end
        {
            return Err(NfsError::Xdr(
                "READ_PLUS gaps, overlaps or zero progress".into(),
            ));
        }
        if matches!(extent, ReadPlusContent::Data { .. }) && (start < offset || extent_end > end) {
            return Err(NfsError::Xdr(
                "READ_PLUS data outside requested range".into(),
            ));
        }
        if extent_end > end {
            clipped_eof = false;
        }
        let clipped_end = extent_end.min(end);
        contents.push(match extent {
            ReadPlusContent::Data { data, .. } => ReadPlusContent::Data {
                offset: cursor,
                data,
            },
            ReadPlusContent::Hole { .. } => ReadPlusContent::Hole {
                offset: cursor,
                length: clipped_end - cursor,
            },
        });
        cursor = clipped_end;
        previous_end = Some(extent_end);
    }
    if contents.is_empty() && !reply.eof && count != 0 {
        return Err(NfsError::Xdr("READ_PLUS empty non-EOF response".into()));
    }
    Ok(ReadPlusResult {
        eof: clipped_eof,
        contents,
    })
}

/// Keep admitted mutations and per-file barriers alive if their waiter is cancelled.
/// Stable handle ordering prevents reciprocal COPY/CLONE and CLOSE deadlocks.
pub(super) async fn run_owned<T, F, Fut>(
    mount: Arc<Mount41>,
    core: Arc<crate::client_core::ClientCore>,
    source: Option<Bytes>,
    destination: Bytes,
    work: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(Arc<Mount41>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<T>> + Send + 'static,
{
    let mut handles = vec![destination.clone()];
    if let Some(source) = source {
        handles.push(source);
    }
    handles.sort();
    handles.dedup();
    let mut guards = Vec::new();
    for fh in handles {
        guards.push(mount.writes.barrier(&fh).await);
        if let Some(error) = mount.writes.error(&fh) {
            return Err(error);
        }
    }
    // A ready notification is not acknowledgement: cancellation can discard a
    // queued result. Keep the result and barriers until the waiter consumes it
    // or the task publishes its abandoned uncertainty (RFC 8881 §13.7).
    let result = Arc::new(std::sync::Mutex::new(None));
    let slot = result.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let abandoned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_abandoned = abandoned.clone();
    let task_mount = mount.clone();
    let task_destination = destination.clone();
    core.spawn_owned(async move {
        let _guards = guards;
        let mut task = super::write_owner::WriteTaskGuard::for_operation(
            task_mount.writes.clone(),
            task_destination.clone(),
            task_abandoned,
            "NFSv4.2 mutation",
        );
        let completed = work(task_mount.clone()).await;
        *slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(completed);
        let _ = sender.send(());
        if ack_rx.await.is_err() {
            let unclaimed = slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(Err(error)) = unclaimed
                && error
                    .operation_outcome()
                    .is_some_and(|o| o.outcome == crate::OperationOutcome::Uncertain)
            {
                task_mount.writes.record(task_destination, error);
            }
        }
        task.settled();
    })?;
    let mut waiter = super::write_owner::WriteWaiter::new(
        mount.writes.clone(),
        destination.clone(),
        abandoned,
        ack_tx,
    );
    if receiver.await.is_err() {
        waiter.acknowledge();
        return Err(mount.writes.error(&destination).unwrap_or_else(|| {
            mount.mutation_error(
                "v42",
                NfsError::Rpc("NFSv4.2 operation task ended without result".into()),
            )
        }));
    }
    let value = result
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    waiter.acknowledge();
    value.unwrap_or_else(|| {
        Err(NfsError::Rpc(
            "NFSv4.2 operation result already consumed".into(),
        ))
    })
}

impl Mount41 {
    pub(super) fn require_v42(&self) -> Result<()> {
        if self.client_identity.minor_version != super::MinorVersion::V2 {
            return Err(NfsError::Unsupported("operation requires NFSv4.2".into()));
        }
        Ok(())
    }

    async fn open_state(&self, fh: &Bytes, access: AccessMode) -> Result<StateId> {
        self.state.has_open(fh, access).await.ok_or_else(|| {
            NfsError::InvalidInput(
                "NFSv4.2 file operation requires an OPEN with the requested access".into(),
            )
        })
    }

    pub(super) async fn v42_space(
        &self,
        fh: Bytes,
        offset: u64,
        length: u64,
        allocate: bool,
    ) -> Result<()> {
        self.require_v42()?;
        if length == 0 || offset.checked_add(length).is_none() {
            return Err(NfsError::InvalidInput(
                "space operation requires a nonempty, nonoverflowing range".into(),
            ));
        }
        let _io = self.layout_manager.write_file_io(&fh).await;
        self.layoutreturn_file(&fh).await?;
        let sid = self.open_state(&fh, AccessMode::Write).await?;
        let tag = if allocate { "allocate" } else { "deallocate" };
        let resp = self
            .compound(tag, |b| {
                b.require_generation(sid.generation)
                    .putfh(&fh)
                    .space(allocate, &sid.raw, offset, length)
            })
            .await?;
        result_data(&resp, 2, if allocate { 59 } else { 62 })
            .map_err(|e| self.mutation_error(tag, e))?;
        Ok(())
    }

    pub(super) async fn v42_seek(
        &self,
        fh: Bytes,
        offset: u64,
        what: SeekContent,
    ) -> Result<SeekResult> {
        self.require_v42()?;
        let _barrier = self.writes.barrier(&fh).await;
        if let Some(error) = self.writes.error(&fh) {
            return Err(error);
        }
        let _io = self.layout_manager.write_file_io(&fh).await;
        self.flush_layoutcommit(&fh).await?;
        let sid = self.open_state(&fh, AccessMode::Read).await?;
        let resp = self
            .compound("seek", |b| {
                b.require_generation(sid.generation)
                    .putfh(&fh)
                    .seek(&sid.raw, offset, what)
            })
            .await?;
        let mut data = result_data(&resp, 2, 69)?;
        let result = SeekResult {
            eof: decode_bool(&mut data)?,
            offset: decode_u64(&mut data)?,
        };
        if result.offset < offset {
            return Err(NfsError::Xdr(
                "SEEK returned an offset before request".into(),
            ));
        }
        Ok(result)
    }

    pub(super) async fn v42_read_plus(
        &self,
        fh: Bytes,
        offset: u64,
        count: u32,
    ) -> Result<ReadPlusResult> {
        self.require_v42()?;
        let count = count.min(self.rsize);
        if offset.checked_add(u64::from(count)).is_none() {
            return Err(NfsError::InvalidInput("READ_PLUS range overflow".into()));
        }
        let _barrier = self.writes.barrier(&fh).await;
        if let Some(error) = self.writes.error(&fh) {
            return Err(error);
        }
        let _io = self.layout_manager.write_file_io(&fh).await;
        self.flush_layoutcommit(&fh).await?;
        let sid = self.open_state(&fh, AccessMode::Read).await?;
        let resp = self
            .compound_data("read_plus", count as usize, |b| {
                b.require_generation(sid.generation)
                    .putfh(&fh)
                    .read_plus(&sid.raw, offset, count)
            })
            .await?;
        clip_read_plus(
            decode_read_plus(&mut result_data(&resp, 2, 68)?)?,
            offset,
            count,
        )
    }

    pub(super) fn mutation_error(&self, operation: &str, error: NfsError) -> NfsError {
        crate::nfs41::pnfs_io::uncertain_pnfs_write(
            crate::RequestContext {
                operation: operation.into(),
                protocol: self.protocol_version(),
                request_id: None,
            },
            error,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn v42_copy(
        &self,
        source: Bytes,
        destination: Bytes,
        source_offset: u64,
        destination_offset: u64,
        count: u64,
        clone: bool,
    ) -> Result<CopyResult> {
        self.require_v42()?;
        if source_offset.checked_add(count).is_none()
            || destination_offset.checked_add(count).is_none()
            || (!clone && source == destination)
        {
            return Err(NfsError::InvalidInput(
                "invalid COPY/CLONE range or identical COPY handles".into(),
            ));
        }
        let mut handles = vec![source.clone(), destination.clone()];
        handles.sort();
        handles.dedup();
        let mut guards = Vec::new();
        for fh in &handles {
            guards.push(self.layout_manager.write_file_io(fh).await);
            self.layoutreturn_file(fh).await?;
        }
        let src = self.open_state(&source, AccessMode::Read).await?;
        let dst = self.open_state(&destination, AccessMode::Write).await?;
        if src.generation != dst.generation {
            return Err(NfsError::InvalidInput(
                "COPY/CLONE OPEN generation mismatch".into(),
            ));
        }
        if source == destination {
            let length = if count == 0 {
                self.getattr(source.clone())
                    .await?
                    .filesize
                    .saturating_sub(source_offset)
            } else {
                count
            };
            let source_end = source_offset
                .checked_add(length)
                .ok_or_else(|| NfsError::InvalidInput("CLONE range overflow".into()))?;
            let destination_end = destination_offset
                .checked_add(length)
                .ok_or_else(|| NfsError::InvalidInput("CLONE range overflow".into()))?;
            if source_offset < destination_end && destination_offset < source_end {
                return Err(NfsError::InvalidInput("overlapping same-file CLONE".into()));
            }
        }
        let tag = if clone { "clone" } else { "copy" };
        let resp = self
            .compound(tag, |b| {
                b.require_generation(src.generation)
                    .putfh(&source)
                    .savefh()
                    .putfh(&destination)
                    .copy_range(
                        clone,
                        &src.raw,
                        &dst.raw,
                        source_offset,
                        destination_offset,
                        count,
                    )
            })
            .await?;
        let data = result_data(&resp, 4, if clone { 71 } else { 60 })
            .map_err(|e| self.mutation_error(tag, e))?;
        if clone {
            return Ok(CopyResult { count });
        }
        let reply = decode_copy(&mut data.clone()).map_err(|e| self.mutation_error(tag, e))?;
        if let Some(callback) = reply.callback {
            // A nonconforming asynchronous success must not leave unowned offload state.
            let _ = self
                .compound("offload_cancel", |b| {
                    b.require_generation(dst.generation)
                        .putfh(&destination)
                        .offload_cancel(&callback)
                })
                .await;
            return Err(self.mutation_error(tag, NfsError::Rpc("server returned asynchronous COPY despite synchronous requirement; cancellation attempted".into())));
        }
        if !reply.synchronous
            || !reply.consecutive
            || (count != 0 && (reply.count == 0 || reply.count > count))
            || destination_offset.checked_add(reply.count).is_none()
        {
            return Err(
                self.mutation_error(tag, NfsError::Xdr("invalid synchronous COPY result".into()))
            );
        }
        if reply.committed != crate::WriteCommitted::FileSync {
            let (offset, length) =
                u32::try_from(reply.count).map_or((0, 0), |n| (destination_offset, n));
            let actual = self
                .commit_with_verifier(destination.clone(), offset, length)
                .await
                .map_err(|e| self.mutation_error(tag, e))?;
            if actual != Some(reply.verifier) {
                return Err(crate::mount::write_verifier_changed(
                    self.protocol_version(),
                ));
            }
        }
        Ok(CopyResult { count: reply.count })
    }

    async fn v42_getattr(&self, fh: &Bytes, bitmap: &[u32]) -> Result<(Vec<u32>, Bytes)> {
        let reply = self
            .compound("v42_attrs", |b| b.putfh(fh).getattr(bitmap))
            .await?;
        let mut data = result_data(&reply, 2, 9)?;
        let words = word(&mut data)? as usize;
        if words > 16 || data.remaining() < words * 4 {
            return Err(NfsError::Xdr(
                "NFSv4.2 attribute bitmap truncated/excessive".into(),
            ));
        }
        let bitmap = (0..words).map(|_| data.get_u32()).collect();
        Ok((bitmap, opaque(&mut data)?))
    }

    pub(super) async fn v42_attributes(&self, fh: Bytes) -> Result<Nfs42Attributes> {
        self.require_v42()?;
        let (bitmap, mut values) = self.v42_getattr(&fh, &[1]).await?;
        if bitmap.first().is_none_or(|w| w & 1 == 0) {
            return Err(NfsError::Xdr("supported_attrs omitted".into()));
        }
        let words = word(&mut values)? as usize;
        if words > 16 || values.remaining() != words * 4 {
            return Err(NfsError::Xdr("invalid supported_attrs bitmap".into()));
        }
        let mut result = Nfs42Attributes {
            supported_attrs: (0..words).map(|_| values.get_u32()).collect(),
            ..Default::default()
        };
        let requested = (u32::from(result.supports_attribute(77)) << 13)
            | (u32::from(result.supports_attribute(82)) << 18);
        if requested != 0 {
            let (bitmap, mut values) = self.v42_getattr(&fh, &[0, 0, requested]).await?;
            let actual = bitmap.get(2).copied().unwrap_or(0);
            if actual & (1 << 13) != 0 {
                result.clone_blksize = Some(word(&mut values)?);
            }
            if actual & (1 << 18) != 0 {
                result.xattr_support = decode_bool(&mut values)?;
            }
            if !values.is_empty() {
                return Err(NfsError::Xdr("unexpected NFSv4.2 attribute values".into()));
            }
        }
        Ok(result)
    }
}
