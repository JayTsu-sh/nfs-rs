//! NFSv4.1 session and slot table management (RFC 5661 §2.10).
//!
//! A session is established via EXCHANGE_ID → CREATE_SESSION → RECLAIM_COMPLETE.
//! Every subsequent COMPOUND must include a SEQUENCE operation that references a
//! slot in the session's slot table. The slot table bounds concurrent requests
//! and provides exactly-once semantics.

use std::collections::VecDeque;
use std::sync::{Arc, LazyLock};
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::{Buf, Bytes};
use tokio::sync::Semaphore;
use tracing::info;

use super::compound::{ChannelAttrsArgs, CompoundBuilder, CompoundResponse};
use super::mount::{delay_with_jitter_ms, grace_with_jitter_ms, DELAY_RETRY_MAX};
use crate::error::{NfsError, Result};
use crate::rpc;
use crate::rpc::auth::Auth;

/// 进程级 verifier，RFC 5661 §18.35.4 要求 verifier 在客户端重启时变化。
/// 同一进程内所有 mount 实例共享同一 verifier，仅在进程重启时改变。
static PROCESS_VERIFIER: LazyLock<[u8; 8]> = LazyLock::new(rand::random);

/// 客户端身份标识，在一个 mount 实例的生命周期内保持不变。
///
/// RFC 5661 §18.35.4：co_ownerid + verifier 唯一标识一个客户端实例。
/// - 不同 mount 实例使用不同的 `owner_id`，避免 EXCHANGE_ID 互相销毁 session
/// - 同一 mount 实例的 re-establishment 复用相同的 owner_id + verifier，
///   服务端会返回已有的 client_id（Non-Update on Existing Client ID）
/// - verifier 使用进程级常量，仅进程重启时变化（通知服务端回收旧状态）
#[derive(Clone, Debug)]
pub(crate) struct ClientIdentity {
    /// 唯一的 co_ownerid，格式："nfs-rs-{random_hex}"
    pub owner_id: String,
    /// 进程级 verifier，所有 mount 实例共享，仅进程重启时变化
    pub verifier: [u8; 8],
}

impl ClientIdentity {
    /// 创建一个新的唯一客户端身份（owner_id 唯一，verifier 进程共享）
    pub fn new() -> Self {
        let unique_id = rand::random::<u64>();
        Self {
            owner_id: format!("nfs-rs-{unique_id:016x}"),
            verifier: *PROCESS_VERIFIER,
        }
    }
}

/// NFSv4.1 session state.
pub(crate) struct Session {
    /// 16-byte session ID from CREATE_SESSION.
    session_id: [u8; 16],
    /// Client ID from EXCHANGE_ID.
    client_id: u64,
    /// Slot table for concurrent request management.
    slot_table: SlotTable,
}

/// Negotiated channel attributes from CREATE_SESSION (used for logging).
struct ChannelAttrs {
    max_request_size: u32,
    #[allow(dead_code)]
    max_response_size: u32,
    max_ops: u32,
    max_requests: u32,
}

/// Slot table that bounds concurrent in-flight requests.
///
/// Uses `Semaphore` for backpressure and `std::sync::Mutex<VecDeque>` for
/// correct slot assignment. We use std::sync::Mutex (not tokio::sync::Mutex)
/// because the free_pool lock must be accessible from Drop (synchronous),
/// and the critical section is trivially short (just push/pop).
struct SlotTable {
    slots: Vec<Slot>,
    /// FIFO pool of free slot indices. Uses std::sync::Mutex for Drop safety.
    free_pool: std::sync::Mutex<VecDeque<u32>>,
    /// Semaphore for bounded backpressure.
    semaphore: Semaphore,
}

/// A single slot in the session's slot table.
struct Slot {
    sequence_id: AtomicU32,
}

/// An acquired slot, held for the duration of a COMPOUND call.
/// On drop, the slot is returned to the free pool and the semaphore permit is released.
pub(crate) struct AcquiredSlot<'a> {
    pub slot_id: u32,
    pub sequence_id: u32,
    table: &'a SlotTable,
    slot: &'a Slot,
    released: bool,
}

impl SlotTable {
    fn new(num_slots: u32) -> Self {
        let num = num_slots.max(1) as usize;
        let slots: Vec<Slot> = (0..num)
            .map(|_| Slot {
                sequence_id: AtomicU32::new(1), // RFC 5661: initial sequence ID is 1
            })
            .collect();
        let free_pool: VecDeque<u32> = (0..num as u32).collect();
        Self {
            semaphore: Semaphore::new(num),
            free_pool: std::sync::Mutex::new(free_pool),
            slots,
        }
    }

    /// Acquire the next available slot. Blocks if all slots are in use.
    async fn acquire(&self) -> Result<AcquiredSlot<'_>> {
        // Semaphore ensures we don't exceed the slot count.
        let _permit = self.semaphore.acquire().await
            .map_err(|_| NfsError::Rpc("session slot table closed".to_string()))?;
        // Pop a free slot index — guaranteed to succeed because the semaphore guards it.
        let slot_id = {
            let mut pool = self.free_pool.lock()
                .map_err(|_| NfsError::Rpc("slot pool mutex poisoned".to_string()))?;
            pool.pop_front()
                .ok_or_else(|| NfsError::Rpc("slot pool empty (should not happen)".to_string()))?
        };
        let slot = &self.slots[slot_id as usize];
        let seq_id = slot.sequence_id.load(Ordering::Acquire);
        // Forget the permit — we manage release via the free_pool + semaphore.add_permits
        _permit.forget();
        Ok(AcquiredSlot {
            slot_id,
            sequence_id: seq_id,
            table: self,
            slot,
            released: false,
        })
    }

    /// Return a slot to the free pool.
    fn release(&self, slot_id: u32) {
        if let Ok(mut pool) = self.free_pool.lock() {
            pool.push_back(slot_id);
        }
        self.semaphore.add_permits(1);
    }
}

impl AcquiredSlot<'_> {
    /// Advance the slot's sequence ID after a successful COMPOUND response.
    pub fn advance(&self) {
        self.slot.sequence_id.fetch_add(1, Ordering::Release);
    }

    /// Re-read the slot's current sequence ID from the atomic.
    /// May differ from `self.sequence_id` if `advance()` has been called.
    /// Used to re-encode retried COMPOUNDs with the correct sequence ID.
    pub fn current_sequence_id(&self) -> u32 {
        self.slot.sequence_id.load(Ordering::Acquire)
    }
}

impl Drop for AcquiredSlot<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.table.release(self.slot_id);
        }
    }
}

impl Session {
    /// Session ID (16 bytes).
    pub fn id(&self) -> &[u8; 16] {
        &self.session_id
    }

    /// Client ID from EXCHANGE_ID.
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// Acquire a slot for a COMPOUND call.
    pub async fn acquire_slot(&self) -> Result<AcquiredSlot<'_>> {
        self.slot_table.acquire().await
    }

    /// Highest slot ID to report in SEQUENCE.
    pub fn highest_slot_id(&self) -> u32 {
        (self.slot_table.slots.len() as u32).saturating_sub(1)
    }

    /// Establish a new session: EXCHANGE_ID → CREATE_SESSION → RECLAIM_COMPLETE.
    ///
    /// This sends 3 separate COMPOUND calls (each with only the session-setup op,
    /// since SEQUENCE cannot be used before the session exists).
    ///
    /// `client_identity` 包含 co_ownerid 和 verifier，在 mount 生命周期内保持不变。
    /// RFC 5661 §18.35.4：同一 co_ownerid + 同一 verifier 表示同一客户端实例，
    /// 服务端会返回已有的 client_id 而不是销毁旧 session。
    /// 不同 mount 实例必须使用不同的 co_ownerid，否则后者会导致服务端销毁前者的 session。
    pub async fn establish(
        rpc: &rpc::Client,
        auth: &Auth,
        client_identity: &ClientIdentity,
    ) -> Result<Self> {
        // ─── Step 1: EXCHANGE_ID ─────────────────────────────────────────
        let verifier = &client_identity.verifier;
        let owner_id = client_identity.owner_id.as_bytes();

        let builder = CompoundBuilder::new("exchange_id")
            .exchange_id(
                verifier,
                owner_id,
                0x00020000, // EXCHGID4_FLAG_USE_PNFS_MDS — RFC 5661 §18.35.3: USE_NON_PNFS 和 USE_PNFS_MDS 互斥，只能选一个
                "nfs-rs",
                "nfs-rs NFSv4.1 client",
            );

        let resp = send_compound_no_session(rpc, auth, builder).await?;
        resp.check_status()?;
        let op = resp.op_ok(0)?;
        let mut data = op.data.clone();

        // Decode EXCHANGE_ID4resok
        if data.remaining() < 16 {
            return Err(NfsError::Xdr("EXCHANGE_ID result too short".to_string()));
        }
        let client_id = data.get_u64();
        let create_seq_id = data.get_u32();
        let _flags = data.get_u32();
        info!(client_id, create_seq_id, "EXCHANGE_ID successful");

        // ─── Step 2: CREATE_SESSION ──────────────────────────────────────
        let fore_attrs = ChannelAttrsArgs {
            headerpadsize: 0,
            maxrequestsize: 1048576,  // 1 MiB
            maxresponsesize: 1048576,
            maxresponsesize_cached: 4096,
            maxoperations: 16,
            maxrequests: 64, // match Linux client default (NFS4_DEF_SLOT_TABLE_SIZE)
        };
        let back_attrs = ChannelAttrsArgs {
            headerpadsize: 0,
            maxrequestsize: 4096,
            maxresponsesize: 4096,
            maxresponsesize_cached: 4096,
            maxoperations: 2,
            maxrequests: 1,
        };

        let builder = CompoundBuilder::new("create_session")
            .create_session(
                client_id,
                create_seq_id,
                0x00000002, // CREATE_SESSION4_FLAG_CONN_BACK_CHAN
                &fore_attrs,
                &back_attrs,
                super::callback::CB_PROGRAM,
            );

        let resp = send_compound_no_session(rpc, auth, builder).await?;
        resp.check_status()?;
        let op = resp.op_ok(0)?;
        let mut data = op.data.clone();

        // Decode CREATE_SESSION4resok
        if data.remaining() < 24 {
            return Err(NfsError::Xdr("CREATE_SESSION result too short".to_string()));
        }
        let mut session_id = [0u8; 16];
        data.copy_to_slice(&mut session_id);
        let _csr_sequence = data.get_u32();
        let _csr_flags = data.get_u32();
        // Decode fore channel attrs
        let fore_channel = decode_channel_attrs(&mut data)?;
        // Skip back channel attrs
        let _back_channel = decode_channel_attrs(&mut data)?;

        let num_slots = fore_channel.max_requests;
        info!(
            session_id = hex::encode(session_id),
            num_slots,
            max_ops = fore_channel.max_ops,
            max_req_size = fore_channel.max_request_size,
            "CREATE_SESSION successful"
        );

        let session = Session {
            session_id,
            client_id,
            slot_table: SlotTable::new(num_slots),
        };

        // ─── Step 3: RECLAIM_COMPLETE ────────────────────────────────────
        // 服务端可能处于 grace period，需要更长超时和 DELAY 重试。
        {
            let slot = session.acquire_slot().await?;
            for attempt in 0..=DELAY_RETRY_MAX {
                let builder = CompoundBuilder::new("reclaim_complete")
                    .sequence(
                        &session.session_id,
                        slot.current_sequence_id(),
                        slot.slot_id,
                        session.highest_slot_id(),
                        false,
                    )
                    .reclaim_complete(false);

                // grace period 期间服务端可能需要较长时间处理
                let timeout = std::time::Duration::from_secs(30);
                let resp = send_compound(rpc, auth, builder, timeout).await?;
                // RFC 5661 §2.10.6.1.3.1: SEQUENCE 已成功处理时必须递增 sequence_id
                if resp.op_ok(0).is_ok() {
                    slot.advance();
                }
                match resp.check_status() {
                    Ok(()) => {
                        resp.op_ok(1)?; // RECLAIM_COMPLETE
                        drop(slot);
                        info!("RECLAIM_COMPLETE successful, session ready");
                        return Ok(session);
                    }
                    Err(NfsError::Nfs4(super::fastxdr::nfsstat4::NFS4ERR_DELAY)) if attempt < DELAY_RETRY_MAX => {
                        let delay_ms = delay_with_jitter_ms(attempt);
                        tracing::warn!(attempt, delay_ms, "RECLAIM_COMPLETE got NFS4ERR_DELAY, retrying with jitter");
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    Err(NfsError::Nfs4(super::fastxdr::nfsstat4::NFS4ERR_GRACE)) if attempt < DELAY_RETRY_MAX => {
                        // RFC 5661 §8.4.2.1: server is in grace period, wait and retry
                        let delay_ms = grace_with_jitter_ms(attempt);
                        tracing::warn!(attempt, delay_ms, "RECLAIM_COMPLETE got NFS4ERR_GRACE, waiting for server grace period");
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            drop(slot);
            return Err(NfsError::Rpc("RECLAIM_COMPLETE NFS4ERR_DELAY/GRACE retry exhausted".to_string()));
        }
    }
}

/// Send a COMPOUND without session SEQUENCE (used during session establishment).
async fn send_compound_no_session(
    rpc: &rpc::Client,
    auth: &Auth,
    builder: CompoundBuilder,
) -> Result<CompoundResponse> {
    send_compound(rpc, auth, builder, std::time::Duration::from_secs(10)).await
}

/// Send a COMPOUND with custom timeout.
async fn send_compound(
    rpc: &rpc::Client,
    auth: &Auth,
    builder: CompoundBuilder,
    timeout: std::time::Duration,
) -> Result<CompoundResponse> {
    let mut buf = Vec::new();
    builder.encode_with_header(auth, &mut buf);
    let response_bytes = rpc.call(buf, 2, timeout).await?;
    CompoundResponse::decode(response_bytes)
}

fn decode_channel_attrs(data: &mut Bytes) -> Result<ChannelAttrs> {
    if data.remaining() < 24 {
        return Err(NfsError::Xdr("channel_attrs truncated".to_string()));
    }
    let _headerpadsize = data.get_u32();
    let max_request_size = data.get_u32();
    let max_response_size = data.get_u32();
    let _max_response_cached = data.get_u32();
    let max_ops = data.get_u32();
    let max_requests = data.get_u32();
    // ca_rdma_ird<1>
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("ca_rdma_ird length truncated".to_string()));
    }
    let n = data.get_u32() as usize;
    let skip = n * 4;
    if data.remaining() < skip {
        return Err(NfsError::Xdr("ca_rdma_ird data truncated".to_string()));
    }
    data.advance(skip);
    Ok(ChannelAttrs {
        max_request_size,
        max_response_size,
        max_ops,
        max_requests,
    })
}

/// A holder that allows atomic session replacement for recovery.
///
/// Normal operations take a read lock to clone the current `Arc<Session>`,
/// then immediately release the lock before doing any I/O.
/// Recovery operations take a write lock to replace the session.
pub(crate) struct SessionHolder {
    inner: tokio::sync::RwLock<Arc<Session>>,
}

impl SessionHolder {
    pub fn new(session: Session) -> Self {
        Self {
            inner: tokio::sync::RwLock::new(Arc::new(session)),
        }
    }

    /// Get a clone of the current session (shared read lock, immediately released).
    pub async fn get(&self) -> Arc<Session> {
        self.inner.read().await.clone()
    }

    /// Replace the session (exclusive write lock, used during recovery).
    pub async fn replace(&self, new_session: Session) {
        let mut guard = self.inner.write().await;
        *guard = Arc::new(new_session);
    }
}

/// Simple hex encoding for session IDs (avoids adding hex crate dependency).
mod hex {
    pub fn encode(bytes: [u8; 16]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slot_table_single_slot() {
        let table = SlotTable::new(1);
        let slot = table.acquire().await.unwrap();
        assert_eq!(slot.slot_id, 0);
        assert_eq!(slot.sequence_id, 1); // initial sequence ID is 1
    }

    #[tokio::test]
    async fn slot_advance_increments_sequence() {
        let table = SlotTable::new(1);
        {
            let slot = table.acquire().await.unwrap();
            assert_eq!(slot.sequence_id, 1);
            slot.advance();
        }
        // Wait for drop to release
        tokio::task::yield_now().await;
        {
            let slot = table.acquire().await.unwrap();
            assert_eq!(slot.sequence_id, 2);
        }
    }

    #[tokio::test]
    async fn slot_table_multiple_slots_unique() {
        let table = SlotTable::new(4);
        let s0 = table.acquire().await.unwrap();
        let s1 = table.acquire().await.unwrap();
        let s2 = table.acquire().await.unwrap();
        // All slot IDs must be distinct
        assert_ne!(s0.slot_id, s1.slot_id);
        assert_ne!(s1.slot_id, s2.slot_id);
        assert_ne!(s0.slot_id, s2.slot_id);
        assert!(s0.slot_id < 4);
        assert!(s1.slot_id < 4);
        assert!(s2.slot_id < 4);
    }

    #[tokio::test]
    async fn slot_table_zero_becomes_one() {
        let table = SlotTable::new(0);
        let slot = table.acquire().await.unwrap();
        assert_eq!(slot.slot_id, 0);
    }

    #[tokio::test]
    async fn slot_release_on_drop() {
        let table = SlotTable::new(1);
        {
            let _slot = table.acquire().await.unwrap();
            // slot is held
        }
        // After drop, should be able to acquire again
        tokio::task::yield_now().await;
        let slot2 = table.acquire().await.unwrap();
        assert_eq!(slot2.slot_id, 0);
    }

    #[test]
    fn hex_encode_zeros() {
        assert_eq!(hex::encode([0u8; 16]), "00000000000000000000000000000000");
    }

    #[test]
    fn hex_encode_values() {
        let mut bytes = [0u8; 16];
        bytes[0] = 0xAB;
        bytes[15] = 0xCD;
        let s = hex::encode(bytes);
        assert!(s.starts_with("ab"));
        assert!(s.ends_with("cd"));
        assert_eq!(s.len(), 32);
    }

    #[test]
    fn decode_channel_attrs_basic() {
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(&0u32.to_be_bytes());       // headerpadsize
        buf.extend_from_slice(&1048576u32.to_be_bytes());  // maxrequestsize
        buf.extend_from_slice(&1048576u32.to_be_bytes());  // maxresponsesize
        buf.extend_from_slice(&4096u32.to_be_bytes());     // maxresponsesize_cached
        buf.extend_from_slice(&16u32.to_be_bytes());       // maxoperations
        buf.extend_from_slice(&4u32.to_be_bytes());        // maxrequests
        buf.extend_from_slice(&0u32.to_be_bytes());        // ca_rdma_ird count = 0
        let mut bytes = buf.freeze();
        let attrs = decode_channel_attrs(&mut bytes).unwrap();
        assert_eq!(attrs.max_request_size, 1048576);
        assert_eq!(attrs.max_response_size, 1048576);
        assert_eq!(attrs.max_ops, 16);
        assert_eq!(attrs.max_requests, 4);
    }

    #[test]
    fn decode_channel_attrs_truncated() {
        let buf = Bytes::from(vec![0u8; 10]); // too short
        let mut b = buf;
        assert!(decode_channel_attrs(&mut b).is_err());
    }

    #[tokio::test]
    async fn current_sequence_id_reflects_advance() {
        let table = SlotTable::new(1);
        let slot = table.acquire().await.unwrap();
        assert_eq!(slot.sequence_id, 1);
        assert_eq!(slot.current_sequence_id(), 1);
        slot.advance();
        // After advance, current_sequence_id() returns the incremented value
        assert_eq!(slot.current_sequence_id(), 2);
        // The snapshot field is unchanged (still 1, as captured at acquisition time)
        assert_eq!(slot.sequence_id, 1);
    }
}
