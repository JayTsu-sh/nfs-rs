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

//! Sequential read-ahead and write-behind on top of a [`Mount`].
//!
//! [`BufferedFile`] keeps a window of READ RPCs in flight ahead of a
//! sequential reader and a window of UNSTABLE WRITE RPCs in flight behind a
//! writer, so a single caller issuing one chunk at a time saturates the link
//! the same way a queue depth of `N` would. Data written through the
//! write-behind window is only durable after [`BufferedFile::flush`]
//! (RFC 1813 §3.3.7 / RFC 5661 §18.32: UNSTABLE writes need a COMMIT).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::error::{NfsError, Result};
use crate::mount::{Mount, WriteOutcome};

/// Tunables for [`BufferedFile`]. Parsed from the `readahead` / `writeback`
/// URL query parameters and exposed by [`Mount::io_options`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoOptions {
    /// Chunks (of the negotiated read size) kept in flight ahead of a
    /// sequential reader. `0` disables read-ahead.
    pub readahead: u32,
    /// UNSTABLE WRITE chunks kept in flight behind a writer. `0` makes every
    /// write synchronous and FILE_SYNC, which is the historical behaviour.
    pub writeback: u32,
    /// Uncommitted bytes after which a COMMIT is issued automatically.
    pub commit_threshold: u64,
}

impl Default for IoOptions {
    fn default() -> Self {
        Self {
            readahead: 8,
            writeback: 0,
            commit_threshold: 16 * 1024 * 1024,
        }
    }
}

/// The subset of [`Mount`] a [`BufferedFile`] needs. Kept as a separate
/// trait so the buffering logic can be unit-tested against a fake server.
#[async_trait]
pub(crate) trait ChunkIo: Send + Sync + 'static {
    fn read_chunk_size(&self) -> u32;
    fn write_chunk_size(&self) -> u32;
    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes>;
    async fn write_unstable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<WriteOutcome>;
    async fn write_stable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32>;
    async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<Option<[u8; 8]>>;
}

#[async_trait]
impl ChunkIo for Arc<dyn Mount> {
    fn read_chunk_size(&self) -> u32 {
        self.get_max_read_size().max(1)
    }

    fn write_chunk_size(&self) -> u32 {
        self.get_max_write_size().max(1)
    }

    async fn read(&self, fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
        Mount::read(self.as_ref(), fh, offset, count).await
    }

    async fn write_unstable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<WriteOutcome> {
        Mount::write_unstable(self.as_ref(), fh, offset, data).await
    }

    async fn write_stable(&self, fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
        Mount::write(self.as_ref(), fh, offset, data).await
    }

    async fn commit(&self, fh: Bytes, offset: u64, count: u32) -> Result<Option<[u8; 8]>> {
        Mount::commit_with_verifier(self.as_ref(), fh, offset, count).await
    }
}

struct ReadState {
    /// Offset the next sequential read is expected at.
    next: u64,
    /// In-flight read-ahead chunks keyed by file offset. Each covers
    /// `read_chunk` bytes unless the file ends first.
    pending: BTreeMap<u64, JoinHandle<Result<Bytes>>>,
    /// Known end of file; nothing is prefetched at or beyond it.
    eof: Option<u64>,
}

struct WriteState {
    /// In-flight UNSTABLE writes, oldest first. Handles are detached (never
    /// aborted) so a cancelled caller cannot leave a session slot dangling.
    inflight: Vec<JoinHandle<Result<WriteOutcome>>>,
    /// Small contiguous writes waiting to fill a chunk: `(offset, data)`.
    staging: Option<(u64, BytesMut)>,
    /// Data sent UNSTABLE since the last successful COMMIT, retained so it
    /// can be resent if the server's write verifier changes.
    uncommitted: Vec<(u64, Bytes)>,
    uncommitted_bytes: u64,
    /// Write verifier observed on the first UNSTABLE reply since the last COMMIT.
    verifier: Option<[u8; 8]>,
    verifier_changed: bool,
    /// At least one reply since the last COMMIT was not FILE_SYNC.
    needs_commit: bool,
    /// First failure of a write-behind task, reported on the next call.
    failed: Option<NfsError>,
}

/// A file handle with sequential read-ahead and write-behind.
///
/// Reads and writes may be issued concurrently from multiple tasks; the file
/// serialises bookkeeping but never holds a lock across the network.
pub struct BufferedFile {
    io: Arc<dyn ChunkIo>,
    fh: Bytes,
    opts: IoOptions,
    read_chunk: u32,
    write_chunk: u32,
    reads: Mutex<ReadState>,
    writes: Mutex<WriteState>,
    write_permits: Arc<Semaphore>,
}

impl std::fmt::Debug for BufferedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedFile")
            .field("opts", &self.opts)
            .field("read_chunk", &self.read_chunk)
            .field("write_chunk", &self.write_chunk)
            .finish()
    }
}

impl BufferedFile {
    /// Wrap `fh` (obtained from `open`/`create` on `mount`) using the mount's
    /// negotiated transfer sizes and `opts`.
    pub fn new(mount: Arc<dyn Mount>, fh: Bytes, opts: IoOptions) -> Self {
        Self::with_io(Arc::new(mount), fh, opts)
    }

    pub(crate) fn with_io(io: Arc<dyn ChunkIo>, fh: Bytes, opts: IoOptions) -> Self {
        let read_chunk = io.read_chunk_size();
        let write_chunk = io.write_chunk_size();
        let permits = opts.writeback.max(1) as usize;
        Self {
            io,
            fh,
            opts,
            read_chunk,
            write_chunk,
            reads: Mutex::new(ReadState {
                next: 0,
                pending: BTreeMap::new(),
                eof: None,
            }),
            writes: Mutex::new(WriteState {
                inflight: Vec::new(),
                staging: None,
                uncommitted: Vec::new(),
                uncommitted_bytes: 0,
                verifier: None,
                verifier_changed: false,
                needs_commit: false,
                failed: None,
            }),
            write_permits: Arc::new(Semaphore::new(permits)),
        }
    }

    pub fn options(&self) -> IoOptions {
        self.opts
    }

    /// Read up to `len` bytes at `offset`. Returns fewer bytes only at end of file.
    pub async fn read_at(&self, offset: u64, len: u32) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        self.settle_writes().await?;
        if self.opts.readahead == 0 {
            return self.read_direct(offset, len).await;
        }
        let mut pieces: Vec<Bytes> = Vec::new();
        let mut cur = offset;
        let mut remaining = len;
        while remaining > 0 {
            let want = remaining.min(self.read_chunk);
            let piece = self.read_chunk_ahead(cur, want).await?;
            let got = piece.len() as u32;
            cur = cur.saturating_add(u64::from(got));
            remaining -= got.min(remaining);
            pieces.push(piece);
            if got < want {
                break;
            }
        }
        Ok(concat(pieces, len as usize))
    }

    async fn read_direct(&self, offset: u64, len: u32) -> Result<Bytes> {
        let mut pieces: Vec<Bytes> = Vec::new();
        let mut cur = offset;
        let mut remaining = len;
        while remaining > 0 {
            let want = remaining.min(self.read_chunk);
            let piece = self.io.read(self.fh.clone(), cur, want).await?;
            let got = (piece.len() as u32).min(want);
            cur = cur.saturating_add(u64::from(got));
            remaining -= got;
            let short = got < want;
            pieces.push(piece.slice(..got as usize));
            if short {
                break;
            }
        }
        Ok(concat(pieces, len as usize))
    }

    /// Serve one chunk at `cur` from the read-ahead window (or directly) and
    /// keep the window topped up ahead of the furthest reader.
    ///
    /// `next` is the frontier (one past the furthest chunk requested so far).
    /// An access within one window of the frontier counts as sequential, so
    /// several concurrent readers walking the same file out of order all
    /// share the window; a far jump discards it and starts over.
    async fn read_chunk_ahead(&self, cur: u64, want: u32) -> Result<Bytes> {
        let window = u64::from(self.read_chunk).saturating_mul(u64::from(self.opts.readahead));
        let hit = {
            let mut state = self.reads.lock().await;
            let hit = state.pending.remove(&cur);
            let end = cur.saturating_add(u64::from(want));
            if hit.is_none() && cur.abs_diff(state.next) > window {
                // Random access: forget the old window (tasks finish on their own).
                state.pending.clear();
                state.next = end;
            } else {
                state.next = state.next.max(end);
                let floor = cur.saturating_sub(window);
                state.pending.retain(|&key, _| key >= floor);
                self.top_up(&mut state);
            }
            hit
        };
        let piece = match hit {
            Some(handle) => handle.await.map_err(join_error)??,
            None => self.io.read(self.fh.clone(), cur, want).await?,
        };
        let piece = if piece.len() > want as usize {
            piece.slice(..want as usize)
        } else {
            piece
        };
        if piece.len() < want as usize {
            let mut state = self.reads.lock().await;
            let eof = cur.saturating_add(piece.len() as u64);
            state.eof = Some(eof);
            state.pending.retain(|&key, _| key < eof);
        }
        Ok(piece)
    }

    /// Spawn reads from the frontier upward until `readahead` chunks are pending.
    fn top_up(&self, state: &mut ReadState) {
        let chunk = u64::from(self.read_chunk);
        let mut key = state.next;
        while state.pending.len() < self.opts.readahead as usize {
            if state.eof.is_some_and(|eof| key >= eof) {
                break;
            }
            if !state.pending.contains_key(&key) {
                let io = Arc::clone(&self.io);
                let fh = self.fh.clone();
                let count = self.read_chunk;
                state.pending.insert(
                    key,
                    tokio::spawn(async move { io.read(fh, key, count).await }),
                );
            }
            key = key.saturating_add(chunk);
        }
    }

    async fn invalidate_reads(&self) {
        let mut state = self.reads.lock().await;
        state.pending.clear();
        state.eof = None;
    }

    /// Write `data` at `offset`. With `writeback > 0` this returns once the
    /// data is queued; durability and errors are reported by [`Self::flush`].
    pub async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.invalidate_reads().await;
        if self.opts.writeback == 0 {
            return self.write_all_stable(offset, data).await;
        }
        let mut state = self.writes.lock().await;
        if let Some(error) = state.failed.take() {
            return Err(error);
        }
        reap_finished(&mut state).await;
        let mut offset = offset;
        let mut data = data;
        // Extend a contiguous staging buffer first.
        let contiguous = state
            .staging
            .as_ref()
            .is_some_and(|(start, buf)| start.saturating_add(buf.len() as u64) == offset);
        if contiguous {
            let chunk = self.write_chunk as usize;
            let full = if let Some((_, buf)) = state.staging.as_mut() {
                let take = (chunk - buf.len()).min(data.len());
                buf.extend_from_slice(&data.split_to(take));
                offset = offset.saturating_add(take as u64);
                buf.len() == chunk
            } else {
                false
            };
            if full && let Some((start, buf)) = state.staging.take() {
                self.emit(&mut state, start, buf.freeze()).await?;
            }
        } else if let Some((start, buf)) = state.staging.take() {
            self.emit(&mut state, start, buf.freeze()).await?;
        }
        // Full chunks go out as-is (zero copy); the tail waits in staging.
        while data.len() >= self.write_chunk as usize {
            let chunk = data.split_to(self.write_chunk as usize);
            self.emit(&mut state, offset, chunk).await?;
            offset = offset.saturating_add(u64::from(self.write_chunk));
        }
        if !data.is_empty() {
            let mut buf = BytesMut::with_capacity(self.write_chunk as usize);
            buf.extend_from_slice(&data);
            state.staging = Some((offset, buf));
        }
        Ok(())
    }

    /// Queue one chunk on the write-behind window, blocking while the window is full.
    async fn emit(&self, state: &mut WriteState, offset: u64, chunk: Bytes) -> Result<()> {
        let permit = Arc::clone(&self.write_permits)
            .acquire_owned()
            .await
            .map_err(|_| NfsError::Rpc("write-behind window closed".to_string()))?;
        reap_finished(state).await;
        if let Some(error) = state.failed.take() {
            return Err(error);
        }
        let io = Arc::clone(&self.io);
        let fh = self.fh.clone();
        let data = chunk.clone();
        state.inflight.push(tokio::spawn(async move {
            let _permit = permit;
            write_all_unstable(io.as_ref(), fh, offset, data).await
        }));
        state.uncommitted_bytes = state.uncommitted_bytes.saturating_add(chunk.len() as u64);
        state.uncommitted.push((offset, chunk));
        if state.uncommitted_bytes >= self.opts.commit_threshold {
            self.commit_locked(state).await?;
        }
        Ok(())
    }

    /// Push staged data out and make everything written so far durable.
    pub async fn flush(&self) -> Result<()> {
        if self.opts.writeback == 0 {
            return Ok(());
        }
        let mut state = self.writes.lock().await;
        if let Some(error) = state.failed.take() {
            return Err(error);
        }
        reap_finished(&mut state).await;
        if let Some((start, buf)) = state.staging.take() {
            if state.inflight.is_empty() && state.uncommitted.is_empty() {
                // Lone small write: one FILE_SYNC round trip beats WRITE + COMMIT.
                return self.write_all_stable(start, buf.freeze()).await;
            }
            self.emit(&mut state, start, buf.freeze()).await?;
        }
        self.commit_locked(&mut state).await
    }

    /// Make in-flight writes visible to a subsequent read (no COMMIT needed).
    async fn settle_writes(&self) -> Result<()> {
        if self.opts.writeback == 0 {
            return Ok(());
        }
        let mut state = self.writes.lock().await;
        if state.staging.is_none() && state.inflight.is_empty() {
            return Ok(());
        }
        if let Some((start, buf)) = state.staging.take() {
            self.emit(&mut state, start, buf.freeze()).await?;
        }
        drain_inflight(&mut state).await;
        match state.failed.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn commit_locked(&self, state: &mut WriteState) -> Result<()> {
        drain_inflight(state).await;
        if let Some(error) = state.failed.take() {
            state.uncommitted.clear();
            state.uncommitted_bytes = 0;
            return Err(error);
        }
        if state.uncommitted.is_empty() {
            return Ok(());
        }
        let mut resend = state.verifier_changed;
        if state.needs_commit {
            let (offset, count) = commit_range(&state.uncommitted);
            let verifier = self.io.commit(self.fh.clone(), offset, count).await?;
            if let (Some(expected), Some(actual)) = (state.verifier, verifier)
                && expected != actual
            {
                resend = true;
            }
        }
        if resend {
            warn!(
                bytes = state.uncommitted_bytes,
                "write verifier changed; resending uncommitted data FILE_SYNC"
            );
            for (offset, data) in std::mem::take(&mut state.uncommitted) {
                self.write_all_stable(offset, data).await?;
            }
        }
        state.uncommitted.clear();
        state.uncommitted_bytes = 0;
        state.verifier = None;
        state.verifier_changed = false;
        state.needs_commit = false;
        Ok(())
    }

    async fn write_all_stable(&self, offset: u64, data: Bytes) -> Result<()> {
        let mut done = 0usize;
        while done < data.len() {
            let n = self
                .io
                .write_stable(self.fh.clone(), offset + done as u64, data.slice(done..))
                .await? as usize;
            if n == 0 || n > data.len() - done {
                return Err(NfsError::Rpc(
                    "server returned an invalid write count".to_string(),
                ));
            }
            done += n;
        }
        Ok(())
    }
}

async fn write_all_unstable(
    io: &dyn ChunkIo,
    fh: Bytes,
    offset: u64,
    data: Bytes,
) -> Result<WriteOutcome> {
    let mut done = 0usize;
    let mut stable = true;
    let mut verifier = None;
    while done < data.len() {
        let out = io
            .write_unstable(fh.clone(), offset + done as u64, data.slice(done..))
            .await?;
        let n = out.count as usize;
        if n == 0 || n > data.len() - done {
            return Err(NfsError::Rpc(
                "server returned an invalid write count".to_string(),
            ));
        }
        done += n;
        stable &= out.stable;
        if out.verifier.is_some() {
            verifier = out.verifier;
        }
    }
    Ok(WriteOutcome {
        count: done as u32,
        stable,
        verifier,
    })
}

type TaskResult = std::result::Result<Result<WriteOutcome>, tokio::task::JoinError>;

fn record_outcome(state: &mut WriteState, result: TaskResult) {
    match result {
        Ok(Ok(outcome)) => {
            if !outcome.stable {
                state.needs_commit = true;
            }
            if let Some(verifier) = outcome.verifier {
                match state.verifier {
                    None => state.verifier = Some(verifier),
                    Some(expected) if expected != verifier => state.verifier_changed = true,
                    Some(_) => {}
                }
            }
        }
        Ok(Err(error)) => {
            state.failed.get_or_insert(error);
        }
        Err(error) => {
            state.failed.get_or_insert(join_error(error));
        }
    }
}

/// Collect outcomes of tasks that already finished, without waiting on the rest.
async fn reap_finished(state: &mut WriteState) {
    let (done, pending): (Vec<_>, Vec<_>) = std::mem::take(&mut state.inflight)
        .into_iter()
        .partition(|handle| handle.is_finished());
    state.inflight = pending;
    for handle in done {
        let result = handle.await;
        record_outcome(state, result);
    }
}

async fn drain_inflight(state: &mut WriteState) {
    for handle in std::mem::take(&mut state.inflight) {
        let result = handle.await;
        record_outcome(state, result);
    }
}

fn commit_range(uncommitted: &[(u64, Bytes)]) -> (u64, u32) {
    let start = uncommitted.iter().map(|(o, _)| *o).min().unwrap_or(0);
    let end = uncommitted
        .iter()
        .map(|(o, d)| o.saturating_add(d.len() as u64))
        .max()
        .unwrap_or(0);
    match u32::try_from(end.saturating_sub(start)) {
        Ok(count) => (start, count),
        Err(_) => (0, 0),
    }
}

fn join_error(error: tokio::task::JoinError) -> NfsError {
    NfsError::Rpc(format!("buffered I/O task failed: {error}"))
}

fn concat(pieces: Vec<Bytes>, capacity: usize) -> Bytes {
    match pieces.len() {
        0 => Bytes::new(),
        1 => pieces.into_iter().next().unwrap_or_default(),
        _ => {
            let mut buf = BytesMut::with_capacity(capacity);
            for piece in pieces {
                buf.extend_from_slice(&piece);
            }
            buf.freeze()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use tokio::sync::Mutex as AsyncMutex;

    #[derive(Default)]
    struct Fake {
        data: AsyncMutex<Vec<u8>>,
        reads: AtomicUsize,
        unstable_writes: AtomicUsize,
        stable_writes: AtomicUsize,
        commits: AtomicUsize,
        max_concurrent_reads: AtomicUsize,
        concurrent_reads: AtomicUsize,
        max_concurrent_writes: AtomicUsize,
        concurrent_writes: AtomicUsize,
        verifier: AtomicU32,
        fail_unstable_at: Option<u64>,
        report_stable: bool,
        commit_verifier_bump: bool,
    }

    #[async_trait]
    impl ChunkIo for Fake {
        fn read_chunk_size(&self) -> u32 {
            4
        }
        fn write_chunk_size(&self) -> u32 {
            4
        }
        async fn read(&self, _fh: Bytes, offset: u64, count: u32) -> Result<Bytes> {
            let now = self.concurrent_reads.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_concurrent_reads.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            self.reads.fetch_add(1, Ordering::SeqCst);
            let data = self.data.lock().await;
            let start = (offset as usize).min(data.len());
            let end = (start + count as usize).min(data.len());
            self.concurrent_reads.fetch_sub(1, Ordering::SeqCst);
            Ok(Bytes::copy_from_slice(&data[start..end]))
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
            let now = self.concurrent_writes.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_concurrent_writes.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            self.unstable_writes.fetch_add(1, Ordering::SeqCst);
            self.store(offset, &data).await;
            self.concurrent_writes.fetch_sub(1, Ordering::SeqCst);
            let v = self.verifier.load(Ordering::SeqCst);
            Ok(WriteOutcome {
                count: data.len() as u32,
                stable: self.report_stable,
                verifier: Some([v as u8; 8]),
            })
        }
        async fn write_stable(&self, _fh: Bytes, offset: u64, data: Bytes) -> Result<u32> {
            self.stable_writes.fetch_add(1, Ordering::SeqCst);
            self.store(offset, &data).await;
            Ok(data.len() as u32)
        }
        async fn commit(&self, _fh: Bytes, _offset: u64, _count: u32) -> Result<Option<[u8; 8]>> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            let v = self.verifier.load(Ordering::SeqCst) + u32::from(self.commit_verifier_bump);
            Ok(Some([v as u8; 8]))
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

    fn file(fake: Arc<Fake>, readahead: u32, writeback: u32) -> BufferedFile {
        BufferedFile::with_io(
            fake,
            Bytes::from_static(b"fh"),
            IoOptions {
                readahead,
                writeback,
                commit_threshold: 16,
            },
        )
    }

    #[tokio::test]
    async fn sequential_reads_prefetch_and_return_data() {
        let fake = Arc::new(Fake {
            data: AsyncMutex::new((0..40u8).collect()),
            ..Default::default()
        });
        let f = file(fake.clone(), 3, 0);
        let mut got = Vec::new();
        let mut off = 0;
        loop {
            let piece = f.read_at(off, 4).await.unwrap();
            if piece.is_empty() {
                break;
            }
            got.extend_from_slice(&piece);
            off += piece.len() as u64;
        }
        assert_eq!(got, (0..40u8).collect::<Vec<_>>());
        assert!(fake.max_concurrent_reads.load(Ordering::SeqCst) >= 2);
        // 10 data chunks plus at most `readahead` empty probes past the end.
        assert!(fake.reads.load(Ordering::SeqCst) <= 14);
    }

    #[tokio::test]
    async fn random_reads_do_not_prefetch() {
        let fake = Arc::new(Fake {
            data: AsyncMutex::new((0..80u8).collect()),
            ..Default::default()
        });
        // chunk 4 × readahead 3 = 12-byte window; every jump below is wider.
        let f = file(fake.clone(), 3, 0);
        assert_eq!(&f.read_at(40, 4).await.unwrap()[..], &[40, 41, 42, 43]);
        assert_eq!(&f.read_at(4, 4).await.unwrap()[..], &[4, 5, 6, 7]);
        assert_eq!(&f.read_at(64, 2).await.unwrap()[..], &[64, 65]);
        // Let any stray prefetch tasks finish before counting.
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        assert_eq!(fake.reads.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn out_of_order_concurrent_readers_share_the_window() {
        let data: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
        let fake = Arc::new(Fake {
            data: AsyncMutex::new(data.clone()),
            ..Default::default()
        });
        let f = Arc::new(file(fake.clone(), 8, 0));
        let mut tasks = Vec::new();
        for k in 0..8u64 {
            let f = Arc::clone(&f);
            tasks.push(tokio::spawn(async move {
                let mut got = Vec::new();
                for i in (k..1024).step_by(8) {
                    // Jitter so the eight readers arrive out of order.
                    tokio::time::sleep(std::time::Duration::from_micros((i * 7) % 300)).await;
                    got.push((i * 4, f.read_at(i * 4, 4).await.unwrap()));
                }
                got
            }));
        }
        let mut all: Vec<(u64, Bytes)> = Vec::new();
        for task in tasks {
            all.extend(task.await.unwrap());
        }
        all.sort_by_key(|(offset, _)| *offset);
        let joined: Vec<u8> = all.iter().flat_map(|(_, b)| b.iter().copied()).collect();
        assert_eq!(joined, data);
        // 1024 chunks; the window must not thrash into wholesale refetching.
        assert!(fake.reads.load(Ordering::SeqCst) < 1024 + 64);
    }

    #[tokio::test]
    async fn large_read_spans_chunks_and_stops_at_eof() {
        let fake = Arc::new(Fake {
            data: AsyncMutex::new((0..10u8).collect()),
            ..Default::default()
        });
        let f = file(fake, 2, 0);
        let all = f.read_at(0, 64).await.unwrap();
        assert_eq!(&all[..], &(0..10u8).collect::<Vec<_>>()[..]);
        assert!(f.read_at(10, 4).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn small_writes_coalesce_into_one_stable_write() {
        let fake = Arc::new(Fake::default());
        let f = file(fake.clone(), 0, 4);
        f.write_at(0, Bytes::from_static(b"a")).await.unwrap();
        f.write_at(1, Bytes::from_static(b"b")).await.unwrap();
        f.write_at(2, Bytes::from_static(b"c")).await.unwrap();
        f.flush().await.unwrap();
        assert_eq!(fake.stable_writes.load(Ordering::SeqCst), 1);
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 0);
        assert_eq!(fake.commits.load(Ordering::SeqCst), 0);
        assert_eq!(&fake.data.lock().await[..], b"abc");
    }

    #[tokio::test]
    async fn chunked_writes_pipeline_and_commit_on_flush() {
        let fake = Arc::new(Fake::default());
        let f = file(fake.clone(), 0, 4);
        let payload: Vec<u8> = (0..32u8).collect();
        for i in 0..8 {
            f.write_at(
                i * 4,
                Bytes::copy_from_slice(&payload[i as usize * 4..][..4]),
            )
            .await
            .unwrap();
        }
        f.flush().await.unwrap();
        assert_eq!(&fake.data.lock().await[..], &payload[..]);
        assert_eq!(fake.unstable_writes.load(Ordering::SeqCst), 8);
        assert!(fake.max_concurrent_writes.load(Ordering::SeqCst) >= 2);
        // 32 bytes with a 16-byte threshold: one automatic COMMIT plus the flush.
        assert_eq!(fake.commits.load(Ordering::SeqCst), 2);
        assert_eq!(fake.stable_writes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stable_replies_skip_commit() {
        let fake = Arc::new(Fake {
            report_stable: true,
            ..Default::default()
        });
        let f = file(fake.clone(), 0, 2);
        f.write_at(0, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        f.flush().await.unwrap();
        assert_eq!(fake.commits.load(Ordering::SeqCst), 0);
        assert_eq!(&fake.data.lock().await[..], b"abcdefgh");
    }

    #[tokio::test]
    async fn verifier_change_resends_uncommitted_data_stable() {
        let fake = Arc::new(Fake {
            commit_verifier_bump: true, // COMMIT reports a different verifier: "server rebooted"
            ..Default::default()
        });
        let f = file(fake.clone(), 0, 4);
        f.write_at(0, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        f.flush().await.unwrap();
        assert_eq!(fake.stable_writes.load(Ordering::SeqCst), 2);
        assert_eq!(&fake.data.lock().await[..], b"abcdefgh");
    }

    #[tokio::test]
    async fn write_failure_is_reported_on_flush() {
        let fake = Arc::new(Fake {
            fail_unstable_at: Some(4),
            ..Default::default()
        });
        let f = file(fake, 0, 4);
        f.write_at(0, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        assert!(matches!(f.flush().await, Err(NfsError::Rpc(_))));
        assert!(f.flush().await.is_ok());
    }

    #[tokio::test]
    async fn read_after_write_sees_queued_data() {
        let fake = Arc::new(Fake::default());
        let f = file(fake.clone(), 2, 4);
        f.write_at(0, Bytes::from_static(b"abcdefghij"))
            .await
            .unwrap();
        assert_eq!(&f.read_at(4, 6).await.unwrap()[..], b"efghij");
        f.flush().await.unwrap();
        assert_eq!(fake.commits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn writeback_zero_is_synchronous_and_stable() {
        let fake = Arc::new(Fake::default());
        let f = file(fake.clone(), 0, 0);
        f.write_at(0, Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        assert_eq!(fake.stable_writes.load(Ordering::SeqCst), 1);
        f.flush().await.unwrap();
        assert_eq!(fake.commits.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn commit_range_covers_all_chunks() {
        let chunks = vec![
            (8, Bytes::from_static(b"abcd")),
            (0, Bytes::from_static(b"ab")),
        ];
        assert_eq!(commit_range(&chunks), (0, 12));
        assert_eq!(commit_range(&[]), (0, 0));
    }
}
