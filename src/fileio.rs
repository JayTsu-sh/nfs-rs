// Copyright 2025 NetApp Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Bounded concurrent reads and per-call durable writes on top of a [`Mount`].
//! Each write sends UNSTABLE chunks and completes one batch commit before
//! returning. There is no deferred writeback or byte-based commit threshold.

use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, stream};
use tokio::sync::Mutex;

use crate::error::{NfsError, Result};
use crate::mount::{Mount, WriteOutcome};

/// Serialize batches sharing a mount and file, including separate Python
/// handles. Weak entries do not retain mounts or file handles indefinitely.
pub(crate) async fn file_gate(
    identity: usize,
    fh: Bytes,
    kind: u8,
) -> tokio::sync::OwnedMutexGuard<()> {
    type Gates = std::collections::HashMap<(usize, Bytes, u8), std::sync::Weak<Mutex<()>>>;
    static GATES: std::sync::OnceLock<std::sync::Mutex<Gates>> = std::sync::OnceLock::new();
    let gate = {
        let mut gates = GATES
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let key = (identity, fh, kind);
        if let Some(gate) = gates.get(&key).and_then(std::sync::Weak::upgrade) {
            gate
        } else {
            gates.retain(|_, gate| gate.strong_count() > 0);
            let gate = Arc::new(Mutex::new(()));
            gates.insert(key, Arc::downgrade(&gate));
            gate
        }
    };
    gate.lock_owned().await
}

#[async_trait]
pub(crate) trait WriteIo: Send + Sync {
    async fn begin_batch(&self, _fh: Bytes) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        None
    }
    fn write_chunk_size(&self) -> u32;
    fn protocol(&self) -> crate::NFSVersion;
    async fn write_unstable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<WriteOutcome>;
    async fn commit_batch(
        &self,
        fh: Bytes,
        offset: u64,
        count: u32,
        writes: &[WriteOutcome],
    ) -> Result<()>;
}

#[async_trait]
pub(crate) trait ReadIo: Send + Sync {
    fn read_chunk_size(&self) -> u32;
    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes>;
}

#[async_trait]
pub(crate) trait ChunkIo: WriteIo + ReadIo + 'static {
    async fn close(&self, fh: Bytes) -> Result<()>;
}

struct MountWriter<'a, M: Mount + ?Sized>(&'a M);
#[async_trait]
impl<M: Mount + ?Sized> WriteIo for MountWriter<'_, M> {
    async fn begin_batch(&self, fh: Bytes) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        Some(file_gate(self.0 as *const M as *const () as usize, fh, 0).await)
    }
    fn write_chunk_size(&self) -> u32 {
        self.0.get_max_write_size().max(1)
    }
    fn protocol(&self) -> crate::NFSVersion {
        self.0.version()
    }
    async fn write_unstable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<WriteOutcome> {
        self.0.write(fh, offset, data).await
    }
    async fn commit_batch(
        &self,
        fh: Bytes,
        offset: u64,
        count: u32,
        writes: &[WriteOutcome],
    ) -> Result<()> {
        self.0.commit_write_batch(fh, offset, count, writes).await
    }
}

#[async_trait]
impl WriteIo for Arc<dyn Mount> {
    async fn begin_batch(&self, fh: Bytes) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        Some(file_gate(Arc::as_ptr(self) as *const () as usize, fh, 0).await)
    }
    fn write_chunk_size(&self) -> u32 {
        self.get_max_write_size().max(1)
    }
    fn protocol(&self) -> crate::NFSVersion {
        self.version()
    }
    async fn write_unstable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<WriteOutcome> {
        self.write(fh, offset, data).await
    }
    async fn commit_batch(
        &self,
        fh: Bytes,
        offset: u64,
        count: u32,
        writes: &[WriteOutcome],
    ) -> Result<()> {
        self.commit_write_batch(fh, offset, count, writes).await
    }
}

#[async_trait]
impl ReadIo for Arc<dyn Mount> {
    fn read_chunk_size(&self) -> u32 {
        self.get_max_read_size().max(1)
    }
    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        Mount::read(self.as_ref(), fh, offset, count).await
    }
}

#[async_trait]
impl ChunkIo for Arc<dyn Mount> {
    async fn close(&self, fh: Bytes) -> Result<()> {
        Mount::close(self.as_ref(), fh).await
    }
}

/// Write all bytes and finish their data/metadata commit before returning.
/// Uses at most 8 concurrent disjoint chunks, completing short writes before
/// a single batch commit. Retains the payload for bounded verifier recovery. Calls on the same mount
/// and file handle are serialized, including calls through separate adapters.
/// Callers must coordinate overlapping writes made through other mounts.
/// Cancellation may leave a partially executed write; adapters should settle
/// this future even if their caller is cancelled.
pub async fn write_all<M: Mount + ?Sized>(
    mount: &M,
    fh: Bytes,
    offset: u64,
    data: Bytes,
) -> Result<u64> {
    write_all_with(&MountWriter(mount), fh, offset, data).await
}

const WRITE_CONCURRENCY: usize = 8;

/// Complete one disjoint range, retaining every short-write acknowledgement.
/// Return partial progress along with errors so sibling results can be settled.
async fn write_chunk_with<I: WriteIo + ?Sized>(
    io: &I,
    fh: Bytes,
    offset: u64,
    data: Bytes,
) -> (u64, usize, Vec<WriteOutcome>, Option<NfsError>) {
    let mut done = 0;
    let mut receipts = Vec::new();
    while done < data.len() {
        let outcome = match io
            .write_unstable(fh.clone(), offset + done as u64, data.slice(done..))
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => return (offset, done, receipts, Some(error)),
        };
        let n = outcome.count as usize;
        if n == 0 || n > data.len() - done {
            let error = write_failure(
                NfsError::Rpc("server returned an invalid write count".into()),
                io.protocol(),
                done as u64,
                true,
                false,
            );
            return (offset, done, receipts, Some(error));
        }
        done += n;
        receipts.push(outcome);
    }
    (offset, done, receipts, None)
}

pub(crate) async fn write_all_with<I: WriteIo + ?Sized>(
    io: &I,
    fh: Bytes,
    offset: u64,
    data: Bytes,
) -> Result<u64> {
    let len = data.len() as u64;
    offset
        .checked_add(len)
        .ok_or_else(|| NfsError::InvalidInput("write range overflows u64".into()))?;
    if data.is_empty() {
        return Ok(0);
    }
    let _batch = io.begin_batch(fh.clone()).await;
    for attempt in 0..3 {
        let chunk = io.write_chunk_size().max(1) as usize;
        let mut ranges = (0..data.len()).step_by(chunk);
        let mut active = FuturesUnordered::new();
        let write_range = |start: usize| {
            let end = data.len().min(start.saturating_add(chunk));
            write_chunk_with(
                io,
                fh.clone(),
                offset + start as u64,
                data.slice(start..end),
            )
        };
        for start in ranges.by_ref().take(WRITE_CONCURRENCY) {
            active.push(write_range(start));
        }
        let mut accepted = 0usize;
        let mut receipts = Vec::new();
        let mut failure: Option<(u64, NfsError)> = None;
        let mut uncertain = false;
        while let Some((start, done, chunk_receipts, error)) = active.next().await {
            accepted += done;
            receipts.extend(chunk_receipts);
            if let Some(error) = error {
                uncertain |= error
                    .operation_outcome()
                    .is_some_and(|outcome| outcome.outcome == crate::OperationOutcome::Uncertain);
                // Deterministic diagnostics among attempted ranges. Stop
                // admitting new ranges, but settle every active chunk.
                if failure.as_ref().is_none_or(|(at, _)| start < *at) {
                    failure = Some((start, error));
                }
            }
            if failure.is_none()
                && let Some(start) = ranges.next()
            {
                active.push(write_range(start));
            }
        }
        if let Some((_, error)) = failure {
            return Err(write_failure(
                error,
                io.protocol(),
                accepted as u64,
                false,
                uncertain,
            ));
        }
        let (commit_offset, count) = u32::try_from(len).map_or((0, 0), |n| (offset, n));
        match io
            .commit_batch(fh.clone(), commit_offset, count, &receipts)
            .await
        {
            Ok(()) => return Ok(len),
            Err(e)
                if attempt < 2
                    && e.operation_outcome()
                        .is_some_and(|o| o.context().operation == "write_verifier") =>
            {
                continue;
            }
            Err(e) => return Err(write_failure(e, io.protocol(), len, true, false)),
        }
    }
    unreachable!("last commit attempt always returns")
}

fn write_failure(
    error: NfsError,
    protocol: crate::NFSVersion,
    accepted: u64,
    commit: bool,
    uncertain: bool,
) -> NfsError {
    // Concurrent ranges may have holes: completed_bytes is the sum of
    // acknowledged bytes, not a contiguous resume offset or a durability
    // guarantee. Once bytes have been accepted, a failure leaves the batch
    // uncertain even when a later chunk received a definite protocol error.
    // A sibling can have been transmitted without acknowledging any bytes.
    // Its uncertainty must survive selection of a different diagnostic error.
    if accepted == 0 && !commit && !uncertain {
        return error;
    }
    NfsError::OperationOutcome(Box::new(
        crate::OperationOutcomeError::new(
            crate::OperationOutcome::Uncertain,
            crate::OperationClass::ReplaySensitive,
            crate::RecoveryAction::VerifyThenResume,
            crate::RequestContext {
                operation: if commit { "commit" } else { "write" }.into(),
                protocol,
                request_id: None,
            },
            error,
        )
        .with_completed_bytes(accepted),
    ))
}

// Bound response memory and RPC pressure independently of the caller's buffer size.
pub(crate) const READ_CONCURRENCY: usize = 8;

/// Fill only the requested range. Consume responses in offset order so EOF
/// or an error cannot leave a reported byte count that skips a hole.
/// Short non-empty responses are continued rather than treated as EOF.
pub(crate) async fn read_into_with<I, F>(
    io: &I,
    fh: Bytes,
    offset: u64,
    len: usize,
    mut fill: F,
) -> Result<usize>
where
    I: ReadIo + ?Sized,
    F: FnMut(usize, &[u8]) -> Result<()> + Send,
{
    read_chunks_with(io, fh, offset, len, |at, piece| fill(at, &piece)).await
}

/// Deliver owned response buffers in offset order without copying their payload.
pub(crate) async fn read_chunks_with<I, F>(
    io: &I,
    fh: Bytes,
    offset: u64,
    len: usize,
    mut fill: F,
) -> Result<usize>
where
    I: ReadIo + ?Sized,
    F: FnMut(usize, Bytes) -> Result<()> + Send,
{
    offset
        .checked_add(
            u64::try_from(len)
                .map_err(|_| NfsError::InvalidInput("read buffer is too large".into()))?,
        )
        .ok_or_else(|| NfsError::InvalidInput("read range overflows u64".into()))?;
    let chunk = io.read_chunk_size().max(1) as usize;
    let mut reads = stream::iter((0..len).step_by(chunk))
        .map(|start| {
            let fh = fh.clone();
            async move {
                let want = chunk.min(len - start);
                let mut got = 0;
                let mut pieces = Vec::new();
                while got < want {
                    let data = io
                        .read(
                            fh.clone(),
                            offset + start as u64 + got as u64,
                            (want - got) as u32,
                        )
                        .await?;
                    if data.len() > want - got {
                        return Err(NfsError::Rpc(
                            "server returned more READ data than requested".into(),
                        ));
                    }
                    if data.is_empty() {
                        break;
                    }
                    got += data.len();
                    pieces.push(data);
                }
                Ok::<_, NfsError>((start, want, got, pieces))
            }
        })
        .buffered(READ_CONCURRENCY);
    let mut completed = 0;
    while let Some(result) = reads.next().await {
        let (start, want, got, pieces) = result?;
        let mut at = start;
        for piece in pieces {
            let len = piece.len();
            fill(at, piece)?;
            at += len;
        }
        completed += got;
        if got < want {
            break;
        }
    }
    Ok(completed)
}

/// File convenience wrapper. Reads only the requested range, without prefetch
/// or a retained cache; writes are durable before returning successfully.
pub struct BufferedFile {
    io: Arc<dyn ChunkIo>,
    fh: Bytes,
    writes: tokio::sync::RwLock<()>,
}

impl std::fmt::Debug for BufferedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedFile").finish_non_exhaustive()
    }
}

impl BufferedFile {
    pub fn new(mount: Arc<dyn Mount>, fh: Bytes) -> Self {
        Self::with_io(Arc::new(mount), fh)
    }

    pub(crate) fn with_io(io: Arc<dyn ChunkIo>, fh: Bytes) -> Self {
        Self {
            io,
            fh,
            writes: tokio::sync::RwLock::new(()),
        }
    }

    pub async fn read_at(&self, offset: u64, len: u32) -> Result<Bytes> {
        let _guard = self.writes.read().await;
        let mut data = BytesMut::new();
        read_into_with(
            self.io.as_ref(),
            self.fh.clone(),
            offset,
            len as usize,
            |_, piece| {
                data.extend_from_slice(piece);
                Ok(())
            },
        )
        .await?;
        Ok(data.freeze())
    }

    pub async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        let _guard = self.writes.write().await;
        write_all_with(self.io.as_ref(), self.fh.clone(), offset, data)
            .await
            .map(|_| ())
    }

    pub async fn flush(&self) -> Result<()> {
        let _guard = self.writes.write().await;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let _guard = self.writes.write().await;
        self.io.close(self.fh.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use tokio::sync::Mutex as AsyncMutex;

    #[derive(Default)]
    struct Fake {
        data: AsyncMutex<Vec<u8>>,
        reads: AtomicUsize,
        unstable_writes: AtomicUsize,
        commits: AtomicUsize,
        closes: AtomicUsize,
        max_concurrent_reads: AtomicUsize,
        concurrent_reads: AtomicUsize,
        max_concurrent_writes: AtomicUsize,
        concurrent_writes: AtomicUsize,
        verifier: AtomicU32,
        fail_unstable_at: Option<u64>,
        fail_write_offset: Option<u64>,
        write_error: Option<fn(u64) -> NfsError>,
        write_delay: Option<fn(u64) -> u64>,
        report_stable: bool,
        commit_verifier_bump: bool,
        change_once: bool,
        chunk_size: u32,
        short_write: usize,
        short_read: usize,
        fail_read_at: Option<u64>,
        /// Per-offset READ latency in milliseconds (default 5 ms).
        read_delay: Option<fn(u64) -> u64>,
    }

    #[async_trait]
    impl ReadIo for Fake {
        fn read_chunk_size(&self) -> u32 {
            4
        }
        async fn read(&self, _fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
            let now = self.concurrent_reads.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_concurrent_reads.fetch_max(now, Ordering::SeqCst);
            let delay = self.read_delay.map_or(5, |f| f(offset));
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.fail_read_at == Some(offset) {
                self.concurrent_reads.fetch_sub(1, Ordering::SeqCst);
                return Err(NfsError::Rpc("scripted read failure".into()));
            }
            let count = if self.short_read > 0 {
                count.min(self.short_read as u32)
            } else {
                count
            };
            let data = self.data.lock().await;
            let start = (offset as usize).min(data.len());
            let end = (start + count as usize).min(data.len());
            self.concurrent_reads.fetch_sub(1, Ordering::SeqCst);
            Ok(Bytes::copy_from_slice(&data[start..end]))
        }
    }
    #[async_trait]
    impl ChunkIo for Fake {
        async fn close(&self, _fh: Bytes) -> Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[async_trait]
    impl WriteIo for Fake {
        fn write_chunk_size(&self) -> u32 {
            if self.chunk_size == 0 {
                4
            } else {
                self.chunk_size
            }
        }
        fn protocol(&self) -> crate::NFSVersion {
            crate::NFSVersion::NFSv3
        }
        async fn write_unstable(
            &self,
            _fh: Bytes,
            offset: u64,
            data: Bytes,
        ) -> Result<WriteOutcome> {
            if self.fail_unstable_at.is_some_and(|at| offset >= at) {
                return Err(NfsError::Rpc("scripted write failure".to_string()));
            }
            let data = if self.short_write > 0 {
                data.slice(..data.len().min(self.short_write))
            } else {
                data
            };
            let now = self.concurrent_writes.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_concurrent_writes.fetch_max(now, Ordering::SeqCst);
            let delay = self.write_delay.map_or(5, |f| f(offset));
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            if let Some(error) = self.write_error {
                self.concurrent_writes.fetch_sub(1, Ordering::SeqCst);
                return Err(error(offset));
            }
            if self.fail_write_offset == Some(offset) {
                self.concurrent_writes.fetch_sub(1, Ordering::SeqCst);
                return Err(NfsError::Rpc("scripted write failure".into()));
            }
            self.unstable_writes.fetch_add(1, Ordering::SeqCst);
            self.store(offset, &data).await;
            self.concurrent_writes.fetch_sub(1, Ordering::SeqCst);
            let v = self.verifier.load(Ordering::SeqCst);
            Ok(WriteOutcome {
                pnfs: None,
                count: data.len() as u32,
                committed: if self.report_stable {
                    crate::WriteCommitted::FileSync
                } else {
                    crate::WriteCommitted::Unstable
                },
                verifier: Some([v as u8; 8]),
            })
        }
        async fn commit_batch(
            &self,
            _fh: Bytes,
            _offset: u64,
            _count: u32,
            writes: &[WriteOutcome],
        ) -> Result<()> {
            assert_eq!(
                self.concurrent_writes.load(Ordering::SeqCst),
                0,
                "COMMIT raced an active WRITE"
            );
            if writes
                .iter()
                .all(|w| w.committed == crate::WriteCommitted::FileSync)
            {
                return Ok(());
            }
            let call = self.commits.fetch_add(1, Ordering::SeqCst);
            let v = if self.commit_verifier_bump || (self.change_once && call == 0) {
                self.verifier.fetch_add(1, Ordering::SeqCst) + 1
            } else {
                self.verifier.load(Ordering::SeqCst)
            };
            crate::mount::verify_write_batch(self.protocol(), writes, Some([v as u8; 8]))
        }
    }

    impl Fake {
        async fn store(&self, offset: u64, data: &[u8]) {
            let mut file = self.data.lock().await;
            let end = offset as usize + data.len();
            if file.len() < end {
                file.resize(end, 0);
            }
            file[offset as usize..end].copy_from_slice(data);
        }
    }

    fn file(fake: Arc<Fake>) -> BufferedFile {
        BufferedFile::with_io(fake, Bytes::from_static(b"fh"))
    }

    #[tokio::test]
    async fn concurrent_reads_fill_requested_range_without_prefetch() {
        let fake = Fake {
            data: AsyncMutex::new((0..200u8).collect()),
            read_delay: Some(|offset| if offset == 0 { 20 } else { 1 }),
            ..Default::default()
        };
        let mut target = [255; 100];
        let n = read_into_with(&fake, Bytes::new(), 0, target.len(), |at, data| {
            target[at..at + data.len()].copy_from_slice(data);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(n, 100);
        assert_eq!(target.as_slice(), &(0..100u8).collect::<Vec<_>>());
        assert_eq!(fake.reads.load(Ordering::SeqCst), 25);
        let peak = fake.max_concurrent_reads.load(Ordering::SeqCst);
        assert!(peak > 1 && peak <= READ_CONCURRENCY);
    }

    #[tokio::test]
    async fn owned_read_chunks_are_ordered_with_actual_concurrency() {
        for (len, expected_peak) in [(3, 1), (12, 3), (40, 8)] {
            let fake = Fake {
                data: AsyncMutex::new((0..40u8).collect()),
                read_delay: Some(|offset| if offset == 0 { 20 } else { 1 }),
                short_read: 2,
                ..Default::default()
            };
            let mut pieces = Vec::new();
            let mut next = 0;
            let count = read_chunks_with(&fake, Bytes::new(), 0, len, |at, piece| {
                assert_eq!(at, next);
                next += piece.len();
                pieces.push(piece);
                Ok(())
            })
            .await
            .unwrap();
            assert_eq!(count, len);
            assert_eq!(pieces.concat(), (0..len as u8).collect::<Vec<_>>());
            assert_eq!(
                fake.max_concurrent_reads.load(Ordering::SeqCst),
                expected_peak
            );
        }
    }

    #[tokio::test]
    async fn short_reads_are_completed_and_eof_leaves_buffer_tail_untouched() {
        let fake = Fake {
            data: AsyncMutex::new((0..25u8).collect()),
            short_read: 2,
            ..Default::default()
        };
        let mut target = [255; 32];
        let n = read_into_with(&fake, Bytes::new(), 3, target.len(), |at, data| {
            target[at..at + data.len()].copy_from_slice(data);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(n, 22);
        assert_eq!(&target[..n], &(3..25u8).collect::<Vec<_>>());
        assert_eq!(&target[n..], &[255; 10]);
    }

    #[tokio::test]
    async fn read_error_never_copies_data_beyond_a_hole() {
        let fake = Fake {
            data: AsyncMutex::new((0..40u8).collect()),
            fail_read_at: Some(4),
            ..Default::default()
        };
        let mut target = [255; 20];
        assert!(
            read_into_with(&fake, Bytes::new(), 0, target.len(), |at, data| {
                target[at..at + data.len()].copy_from_slice(data);
                Ok(())
            })
            .await
            .is_err()
        );
        assert_eq!(&target[..4], &[0, 1, 2, 3]);
        assert_eq!(&target[4..], &[255; 16]);
    }

    #[tokio::test]
    async fn empty_and_overflow_reads_send_no_requests() {
        let fake = Fake::default();
        assert_eq!(
            read_into_with(&fake, Bytes::new(), 0, 0, |_, _| panic!("no data"))
                .await
                .unwrap(),
            0
        );
        assert!(
            read_into_with(&fake, Bytes::new(), u64::MAX, 1, |_, _| panic!("no data"))
                .await
                .is_err()
        );
        assert_eq!(fake.reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn large_read_spans_chunks_and_stops_at_eof() {
        let fake = Arc::new(Fake {
            data: AsyncMutex::new((0..10u8).collect()),
            ..Default::default()
        });
        let f = file(fake);
        let all = f.read_at(0, 64).await.unwrap();
        assert_eq!(&all[..], &(0..10u8).collect::<Vec<_>>()[..]);
        assert!(f.read_at(10, 4).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn writes_use_at_most_eight_chunks_and_commit_after_all_short_writes() {
        let fake = Fake {
            short_write: 2,
            write_delay: Some(|offset| if offset == 7 { 20 } else { 2 }),
            ..Default::default()
        };
        let payload = Bytes::from((0..200u8).collect::<Vec<_>>());
        assert_eq!(
            write_all_with(&fake, Bytes::new(), 7, payload.clone())
                .await
                .unwrap(),
            200
        );
        assert_eq!(&fake.data.lock().await[7..], payload.as_ref());
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 100);
        assert_eq!(
            fake.max_concurrent_writes.load(Ordering::SeqCst),
            WRITE_CONCURRENCY
        );
        assert_eq!(fake.commits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn sibling_uncertainty_survives_lowest_offset_definite_error_without_acknowledgements() {
        for delay in [
            (|offset| if offset == 0 { 1 } else { 10 }) as fn(u64) -> u64,
            (|offset| if offset == 0 { 10 } else { 1 }) as fn(u64) -> u64,
        ] {
            let fake = Fake {
                write_delay: Some(delay),
                write_error: Some(|offset| {
                    if offset == 0 {
                        return NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOSPC);
                    }
                    NfsError::OperationOutcome(Box::new(crate::OperationOutcomeError::new(
                        crate::OperationOutcome::Uncertain,
                        crate::OperationClass::ReplaySensitive,
                        crate::RecoveryAction::VerifyThenResume,
                        crate::RequestContext {
                            operation: "write".into(),
                            protocol: crate::NFSVersion::NFSv3,
                            request_id: None,
                        },
                        NfsError::Rpc("reply lost after transmission".into()),
                    )))
                }),
                ..Default::default()
            };
            let error = write_all_with(&fake, Bytes::new(), 0, Bytes::from(vec![9; 32]))
                .await
                .unwrap_err();
            let outcome = error
                .operation_outcome()
                .expect("sibling may have modified the file");
            assert_eq!(outcome.outcome, crate::OperationOutcome::Uncertain);
            assert_eq!(outcome.completed_bytes, Some(0));
            assert!(matches!(
                *outcome.source,
                NfsError::Nfs3(crate::nfs3::ErrorCode::NFS3ERR_NOSPC)
            ));
            assert_eq!(fake.concurrent_writes.load(Ordering::SeqCst), 0);
            assert_eq!(fake.commits.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn failed_chunk_stops_admission_settles_siblings_and_skips_commit() {
        let fake = Fake {
            fail_write_offset: Some(0),
            write_delay: Some(|offset| if offset == 0 { 1 } else { 20 }),
            ..Default::default()
        };
        let error = write_all_with(&fake, Bytes::new(), 0, Bytes::from(vec![9; 200]))
            .await
            .unwrap_err();
        assert_eq!(fake.concurrent_writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            fake.unstable_writes.load(Ordering::SeqCst),
            WRITE_CONCURRENCY - 1
        );
        assert_eq!(fake.commits.load(Ordering::SeqCst), 0);
        let outcome = error.operation_outcome().unwrap();
        assert_eq!(outcome.outcome, crate::OperationOutcome::Uncertain);
        assert_eq!(
            outcome.completed_bytes,
            Some(((WRITE_CONCURRENCY - 1) * 4) as u64)
        );
        let data = fake.data.lock().await;
        assert_eq!(&data[..4], &[0; 4]);
        assert_eq!(&data[4..], &[9; (WRITE_CONCURRENCY - 1) * 4]);
    }

    #[tokio::test]
    async fn small_writes_each_commit_before_return() {
        let fake = Arc::new(Fake::default());
        let f = file(fake.clone());
        f.write_at(0, Bytes::from_static(b"a")).await.unwrap();
        f.write_at(1, Bytes::from_static(b"b")).await.unwrap();
        f.write_at(2, Bytes::from_static(b"c")).await.unwrap();
        f.flush().await.unwrap();
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 3);
        assert_eq!(fake.commits.load(Ordering::SeqCst), 3);
        assert_eq!(&fake.data.lock().await[..], b"abc");
    }

    #[tokio::test]
    async fn each_write_commits_before_return_and_flush_does_not_repeat() {
        let fake = Arc::new(Fake::default());
        let f = file(fake.clone());
        f.write_at(0, Bytes::from(vec![42; 32])).await.unwrap();
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 8);
        assert_eq!(fake.commits.load(Ordering::SeqCst), 1);
        f.write_at(32, Bytes::from_static(b"tail")).await.unwrap();
        assert_eq!(fake.commits.load(Ordering::SeqCst), 2);
        f.flush().await.unwrap();
        f.close().await.unwrap();
        assert_eq!(fake.commits.load(Ordering::SeqCst), 2);
        assert_eq!(fake.closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stable_replies_skip_commit() {
        let fake = Arc::new(Fake {
            report_stable: true,
            ..Default::default()
        });
        file(fake.clone())
            .write_at(0, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        assert_eq!(fake.commits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn verifier_recovery_is_bounded_and_never_uses_file_sync() {
        let fake = Arc::new(Fake {
            commit_verifier_bump: true,
            ..Default::default()
        });
        assert!(
            file(fake.clone())
                .write_at(0, Bytes::from_static(b"abcdefgh"))
                .await
                .is_err()
        );
        assert_eq!(fake.commits.load(Ordering::SeqCst), 3);
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn partial_failure_is_reported_by_write_as_uncertain() {
        let fake = Arc::new(Fake {
            fail_unstable_at: Some(4),
            ..Default::default()
        });
        let error = file(fake.clone())
            .write_at(0, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::Uncertain
        );
        assert_eq!(fake.commits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reads_after_write_observe_new_data() {
        let fake = Arc::new(Fake {
            data: AsyncMutex::new(vec![0; 16]),
            ..Default::default()
        });
        let f = file(fake.clone());
        f.read_at(0, 4).await.unwrap();
        f.write_at(4, Bytes::from_static(b"changed!"))
            .await
            .unwrap();
        assert_eq!(&f.read_at(4, 8).await.unwrap()[..], b"changed!");
        assert_eq!(fake.commits.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn large_call_has_one_commit_and_short_writes_are_completed() {
        let fake = Arc::new(Fake {
            chunk_size: 1024 * 1024,
            ..Default::default()
        });
        let data = Bytes::from(vec![7; 17 * 1024 * 1024]);
        file(fake.clone()).write_at(0, data.clone()).await.unwrap();
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 17);
        assert_eq!(fake.commits.load(Ordering::SeqCst), 1);
        assert_eq!(fake.data.lock().await.as_slice(), data.as_ref());

        let fake = Arc::new(Fake {
            short_write: 2,
            ..Default::default()
        });
        file(fake.clone())
            .write_at(0, Bytes::from_static(b"abcdefghij"))
            .await
            .unwrap();
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 5);
        assert_eq!(fake.commits.load(Ordering::SeqCst), 1);
        assert_eq!(fake.data.lock().await.as_slice(), b"abcdefghij");
    }

    #[tokio::test]
    async fn verifier_change_rewrites_retained_data_and_recommits() {
        let fake = Arc::new(Fake {
            change_once: true,
            ..Default::default()
        });
        file(fake.clone())
            .write_at(0, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 4);
        assert_eq!(fake.commits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn overflow_is_rejected_before_any_write() {
        let fake = Arc::new(Fake::default());
        assert!(
            file(fake.clone())
                .write_at(u64::MAX, Bytes::from_static(b"a"))
                .await
                .is_err()
        );
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 0);
    }
}
