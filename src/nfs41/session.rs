//! NFSv4.1 session and slot table management (RFC 5661 §2.10).
//!
//! A session is established via EXCHANGE_ID → CREATE_SESSION → RECLAIM_COMPLETE.
//! Every subsequent COMPOUND must include a SEQUENCE operation that references a
//! slot in the session's slot table. The slot table bounds concurrent requests
//! and provides exactly-once semantics.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use bytes::{Buf, Bytes};
use tokio::sync::{Notify, Semaphore};
use tracing::info;

use super::compound::{ChannelAttrsArgs, CompoundBuilder, CompoundResponse};
use super::mount::{DELAY_RETRY_MAX, delay_with_jitter_ms, grace_with_jitter_ms};
use crate::error::{NfsError, Result};
use crate::rpc;
use crate::rpc::auth::Auth;

/// 进程级 verifier，RFC 5661 §18.35.4 要求 verifier 在客户端重启时变化。
/// 同一进程内所有 mount 实例共享同一 verifier，仅在进程重启时改变。
static PROCESS_VERIFIER: LazyLock<[u8; 8]> = LazyLock::new(rand::random);

/// RFC 5661 §18.35.3：客户端声明 / 服务端确认其为 pNFS 元数据服务器。
/// USE_NON_PNFS 和 USE_PNFS_MDS 互斥，只能选一个。
pub(crate) const EXCHGID4_FLAG_USE_PNFS_MDS: u32 = 0x0002_0000;

/// RFC 8881 §13.1：客户端与 pNFS 数据服务器建立 client-id 时声明 DS 用途。
pub(crate) const EXCHGID4_FLAG_USE_PNFS_DS: u32 = 0x0004_0000;

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
    /// Monotonic local generation assigned when published by SessionHolder.
    generation: u64,
    /// 16-byte session ID from CREATE_SESSION.
    session_id: [u8; 16],
    /// Client ID from EXCHANGE_ID.
    client_id: u64,
    /// Slot table for concurrent request management.
    slot_table: SlotTable,
    /// Fore-channel request limit negotiated by CREATE_SESSION.
    max_request_size: u32,
    max_response_size: u32,
    /// Maximum complete reply the server agreed to cache for a slot replay.
    max_cached_response_size: u32,
    max_operations: u32,
    /// Backchannel callback slots negotiated by CREATE_SESSION.
    backchannel_max_requests: u32,
    /// Backchannel request and operation bounds negotiated by CREATE_SESSION.
    backchannel_max_request_size: u32,
    backchannel_max_operations: u32,
    /// 服务端在 EXCHANGE_ID eir_flags 中确认自己是 pNFS MDS（RFC 5661 §18.35.3）。
    /// 为 false 时整个 mount 禁用 pNFS（与 Linux 客户端行为一致），跳过所有 LAYOUTGET。
    pnfs_mds: bool,
}

/// Negotiated channel attributes from CREATE_SESSION (used for logging).
struct ChannelAttrs {
    header_pad_size: u32,
    max_request_size: u32,
    #[allow(dead_code)]
    max_response_size: u32,
    max_cached_response_size: u32,
    max_ops: u32,
    max_requests: u32,
}

pub(crate) struct SequenceResult {
    pub highest_slot_id: u32,
    pub target_highest_slot_id: u32,
    pub status_flags: u32,
}

pub(crate) fn validate_sequence_result(
    op: &super::compound::OpResponse,
    expected_session_id: &[u8; 16],
    expected_sequence_id: u32,
    expected_slot_id: u32,
) -> Result<SequenceResult> {
    if op.opcode != super::compound::OpNum::Sequence as u32
        || op.status != crate::nfs4::fastxdr::nfsstat4::NFS4_OK
    {
        return Err(NfsError::Xdr(
            "response does not contain a successful SEQUENCE result".to_string(),
        ));
    }
    let mut data = op.data.clone();
    if data.remaining() < 36 {
        return Err(NfsError::Xdr("SEQUENCE result truncated".to_string()));
    }
    let mut session_id = [0u8; 16];
    data.copy_to_slice(&mut session_id);
    let sequence_id = data.get_u32();
    let slot_id = data.get_u32();
    let highest_slot_id = data.get_u32();
    let target_highest_slot_id = data.get_u32();
    let status_flags = data.get_u32();
    if session_id != *expected_session_id
        || sequence_id != expected_sequence_id
        || slot_id != expected_slot_id
    {
        return Err(NfsError::Xdr(format!(
            "SEQUENCE identity mismatch: expected seq={expected_sequence_id} slot={expected_slot_id}"
        )));
    }
    Ok(SequenceResult {
        highest_slot_id,
        target_highest_slot_id,
        status_flags,
    })
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
    /// Permanently retained ambiguous slots in this session generation.
    fenced_slots: AtomicU32,
    /// Server-requested upper bound for new requests. In-flight slots above
    /// this value remain valid until their authoritative reply is received.
    target_highest_slot_id: AtomicU32,
    availability_changed: Notify,
    active_or_fenced_slots: AtomicU64,
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
    disposition: SlotDisposition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotDisposition {
    Release,
    Fence,
}

impl SlotTable {
    fn new(num_slots: u32) -> Self {
        let num = num_slots as usize;
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
            fenced_slots: AtomicU32::new(0),
            target_highest_slot_id: AtomicU32::new(num_slots.saturating_sub(1)),
            availability_changed: Notify::new(),
            active_or_fenced_slots: AtomicU64::new(0),
        }
    }

    /// Acquire the next available slot. Blocks if all slots are in use.
    async fn acquire(&self) -> Result<AcquiredSlot<'_>> {
        loop {
            let notified = self.availability_changed.notified();
            let permit = self
                .semaphore
                .acquire()
                .await
                .map_err(|_| NfsError::Rpc("session slot table closed".to_string()))?;
            let target = self.target_highest_slot_id.load(Ordering::Acquire);
            let slot_id = {
                let mut pool = self
                    .free_pool
                    .lock()
                    .map_err(|_| NfsError::Rpc("slot pool mutex poisoned".to_string()))?;
                pool.iter()
                    .position(|slot_id| *slot_id <= target)
                    .and_then(|position| pool.remove(position))
            };
            let Some(slot_id) = slot_id else {
                drop(permit);
                notified.await;
                continue;
            };
            permit.forget();
            return self.acquired(slot_id);
        }
    }

    fn acquired(&self, slot_id: u32) -> Result<AcquiredSlot<'_>> {
        let mask = 1u64
            .checked_shl(slot_id)
            .ok_or_else(|| NfsError::Rpc(format!("slot ID {slot_id} exceeds bitmap bound")))?;
        self.active_or_fenced_slots.fetch_or(mask, Ordering::AcqRel);
        let slot = &self.slots[slot_id as usize];
        let seq_id = slot.sequence_id.load(Ordering::Acquire);
        Ok(AcquiredSlot {
            slot_id,
            sequence_id: seq_id,
            table: self,
            slot,
            disposition: SlotDisposition::Release,
        })
    }

    fn update_target_highest_slot_id(&self, target: u32) -> Result<()> {
        let maximum = u32::try_from(self.slots.len())
            .map_err(|_| NfsError::Rpc("slot table size exceeds u32".to_string()))?
            .checked_sub(1)
            .ok_or_else(|| {
                NfsError::Rpc("session negotiated zero fore-channel slots".to_string())
            })?;
        if target > maximum {
            return Err(NfsError::Xdr(format!(
                "SEQUENCE target_highest_slotid {target} exceeds negotiated maximum {maximum}"
            )));
        }
        self.target_highest_slot_id.store(target, Ordering::Release);
        self.availability_changed.notify_waiters();
        Ok(())
    }

    /// Return a slot to the free pool.
    fn release(&self, slot_id: u32) {
        if let Some(mask) = 1u64.checked_shl(slot_id) {
            self.active_or_fenced_slots
                .fetch_and(!mask, Ordering::AcqRel);
        }
        if let Ok(mut pool) = self.free_pool.lock() {
            pool.push_back(slot_id);
        }
        self.semaphore.add_permits(1);
        self.availability_changed.notify_one();
    }

    fn highest_slot_id(&self) -> u32 {
        let active = self.active_or_fenced_slots.load(Ordering::Acquire);
        let highest_active = if active == 0 {
            0
        } else {
            63 - active.leading_zeros()
        };
        highest_active.max(self.target_highest_slot_id.load(Ordering::Acquire))
    }
}

impl AcquiredSlot<'_> {
    /// Fence this slot if the request future is cancelled or exits without an
    /// authoritative result. The permit is intentionally retained until the
    /// session is replaced, preventing a different request from reusing an
    /// ambiguous sequence ID.
    pub fn fence_on_drop(&mut self) {
        self.disposition = SlotDisposition::Fence;
    }

    /// Mark the logical request terminal so Drop can return the slot.
    pub fn resolve(&mut self) {
        self.disposition = SlotDisposition::Release;
    }

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
        if self.disposition == SlotDisposition::Release {
            self.table.release(self.slot_id);
        } else {
            let fenced = self.table.fenced_slots.fetch_add(1, Ordering::AcqRel) + 1;
            if fenced as usize >= self.table.slots.len() {
                self.table.semaphore.close();
            }
        }
    }
}

impl Session {
    /// Session ID (16 bytes).
    pub fn id(&self) -> &[u8; 16] {
        &self.session_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Client ID from EXCHANGE_ID.
    pub fn client_id(&self) -> u64 {
        self.client_id
    }

    /// 服务端是否为 pNFS MDS（EXCHANGE_ID eir_flags 含 USE_PNFS_MDS）。
    pub fn pnfs_mds(&self) -> bool {
        self.pnfs_mds
    }

    /// Acquire a slot for a COMPOUND call.
    pub async fn acquire_slot(&self) -> Result<AcquiredSlot<'_>> {
        self.slot_table.acquire().await
    }

    /// Highest slot ID to report in SEQUENCE.
    pub fn highest_slot_id(&self) -> u32 {
        self.slot_table.highest_slot_id()
    }

    pub fn update_sequence_slot_limits(&self, highest: u32, target: u32) -> Result<()> {
        let maximum = self.max_requests().checked_sub(1).ok_or_else(|| {
            NfsError::Rpc("session negotiated zero fore-channel slots".to_string())
        })?;
        if highest > maximum || target > maximum {
            return Err(NfsError::Xdr(format!(
                "SEQUENCE slot limits highest={highest} target={target} exceed negotiated maximum {maximum}"
            )));
        }
        self.slot_table
            .update_target_highest_slot_id(target.min(highest))
    }

    /// Maximum encoded RPC request size accepted by the fore channel.
    pub fn max_request_size(&self) -> u32 {
        self.max_request_size
    }

    pub fn max_response_size(&self) -> u32 {
        self.max_response_size
    }

    pub fn max_operations(&self) -> u32 {
        self.max_operations
    }

    pub fn max_cached_response_size(&self) -> u32 {
        self.max_cached_response_size
    }

    pub fn max_requests(&self) -> u32 {
        u32::try_from(self.slot_table.slots.len()).unwrap_or(u32::MAX)
    }

    pub fn effective_highest_slot_id(&self) -> u32 {
        self.slot_table.highest_slot_id()
    }

    pub fn backchannel_max_requests(&self) -> u32 {
        self.backchannel_max_requests
    }

    pub fn backchannel_max_request_size(&self) -> u32 {
        self.backchannel_max_request_size
    }

    pub fn backchannel_max_operations(&self) -> u32 {
        self.backchannel_max_operations
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
        let (client_id, create_seq_id, eir_flags) =
            exchange_id_step(rpc, auth, client_identity, EXCHGID4_FLAG_USE_PNFS_MDS).await?;
        // RFC 5661 §18.35.3：服务端通过 eir_flags 表明 pNFS 角色；
        // 无 USE_PNFS_MDS 即整个 mount 禁用 pNFS（与 Linux 客户端一致）
        let pnfs_mds = eir_flags & EXCHGID4_FLAG_USE_PNFS_MDS != 0;
        info!(client_id, create_seq_id, pnfs_mds, "EXCHANGE_ID successful");

        // ─── Step 2: CREATE_SESSION ──────────────────────────────────────
        let (session_id, fore_channel, back_channel) = create_session_step(
            rpc,
            auth,
            client_id,
            create_seq_id,
            0x00000002, // CREATE_SESSION4_FLAG_CONN_BACK_CHAN
        )
        .await?;

        let session = Session {
            generation: 1,
            session_id,
            client_id,
            slot_table: SlotTable::new(fore_channel.max_requests),
            max_request_size: fore_channel.max_request_size,
            max_response_size: fore_channel.max_response_size,
            max_cached_response_size: fore_channel.max_cached_response_size,
            max_operations: fore_channel.max_ops,
            backchannel_max_requests: back_channel.max_requests,
            backchannel_max_request_size: back_channel.max_request_size,
            backchannel_max_operations: back_channel.max_ops,
            pnfs_mds,
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
                    )
                    .reclaim_complete(false);

                // grace period 期间服务端可能需要较长时间处理
                let timeout = std::time::Duration::from_secs(30);
                let resp = send_compound_on_session(rpc, auth, builder, timeout, &session).await?;
                // RFC 5661 §2.10.6.1.3.1: SEQUENCE 已成功处理时必须递增 sequence_id
                if let Ok(sequence_op) = resp.op_ok(0) {
                    let sequence = validate_sequence_result(
                        sequence_op,
                        &session.session_id,
                        slot.current_sequence_id(),
                        slot.slot_id,
                    )?;
                    session.update_sequence_slot_limits(
                        sequence.highest_slot_id,
                        sequence.target_highest_slot_id,
                    )?;
                    slot.advance();
                }
                match resp.check_status() {
                    Ok(()) => {
                        resp.op_ok(1)?; // RECLAIM_COMPLETE
                        drop(slot);
                        info!("RECLAIM_COMPLETE successful, session ready");
                        return Ok(session);
                    }
                    Err(NfsError::Nfs4(crate::nfs4::fastxdr::nfsstat4::NFS4ERR_DELAY))
                        if attempt < DELAY_RETRY_MAX =>
                    {
                        let delay_ms = delay_with_jitter_ms(attempt);
                        tracing::warn!(
                            attempt,
                            delay_ms,
                            "RECLAIM_COMPLETE got NFS4ERR_DELAY, retrying with jitter"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    Err(NfsError::Nfs4(crate::nfs4::fastxdr::nfsstat4::NFS4ERR_GRACE))
                        if attempt < DELAY_RETRY_MAX =>
                    {
                        // RFC 5661 §8.4.2.1: server is in grace period, wait and retry
                        let delay_ms = grace_with_jitter_ms(attempt);
                        tracing::warn!(
                            attempt,
                            delay_ms,
                            "RECLAIM_COMPLETE got NFS4ERR_GRACE, waiting for server grace period"
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            drop(slot);
            Err(NfsError::Rpc(
                "RECLAIM_COMPLETE NFS4ERR_DELAY/GRACE retry exhausted".to_string(),
            ))
        }
    }

    /// 与 pNFS 数据服务器建立 session（RFC 8881 §13.1）。
    ///
    /// 与 [`Session::establish`] 的差异：
    /// - EXCHANGE_ID 声明 `USE_PNFS_DS`（而非 MDS）
    /// - CREATE_SESSION 不请求 backchannel（DS 不发 layout/delegation 召回）
    /// - 跳过 RECLAIM_COMPLETE（DS 上无可 reclaim 的状态，Linux 客户端同样跳过）
    pub async fn establish_ds(
        rpc: &rpc::Client,
        auth: &Auth,
        client_identity: &ClientIdentity,
    ) -> Result<Self> {
        let (client_id, create_seq_id, eir_flags) =
            exchange_id_step(rpc, auth, client_identity, EXCHGID4_FLAG_USE_PNFS_DS).await?;
        info!(
            client_id,
            create_seq_id, eir_flags, "DS EXCHANGE_ID successful"
        );

        let (session_id, fore_channel, _back_channel) =
            create_session_step(rpc, auth, client_id, create_seq_id, 0).await?;

        Ok(Session {
            generation: 1,
            session_id,
            client_id,
            slot_table: SlotTable::new(fore_channel.max_requests),
            max_request_size: fore_channel.max_request_size,
            max_response_size: fore_channel.max_response_size,
            max_cached_response_size: fore_channel.max_cached_response_size,
            max_operations: fore_channel.max_ops,
            backchannel_max_requests: 1,
            backchannel_max_request_size: 4096,
            backchannel_max_operations: 2,
            // DS session 不参与 layout 获取，此标志仅对 MDS session 有意义
            pnfs_mds: false,
        })
    }
}

/// EXCHANGE_ID：返回 (client_id, create_seq_id, eir_flags)。
async fn exchange_id_step(
    rpc: &rpc::Client,
    auth: &Auth,
    client_identity: &ClientIdentity,
    flags: u32,
) -> Result<(u64, u32, u32)> {
    let verifier = &client_identity.verifier;
    let owner_id = client_identity.owner_id.as_bytes();

    let builder = CompoundBuilder::new("exchange_id").exchange_id(
        verifier,
        owner_id,
        flags,
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
    let eir_flags = data.get_u32();
    Ok((client_id, create_seq_id, eir_flags))
}

/// CREATE_SESSION：返回 session ID 和协商出的前向通道属性。
async fn create_session_step(
    rpc: &rpc::Client,
    auth: &Auth,
    client_id: u64,
    create_seq_id: u32,
    csa_flags: u32,
) -> Result<([u8; 16], ChannelAttrs, ChannelAttrs)> {
    let fore_attrs = ChannelAttrsArgs {
        headerpadsize: 0,
        maxrequestsize: 1048576, // 1 MiB
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

    let builder = CompoundBuilder::new("create_session").create_session(
        client_id,
        create_seq_id,
        csa_flags,
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
    let back_channel = decode_channel_attrs(&mut data)?;
    validate_channel_attrs("fore", &fore_channel, &fore_attrs)?;
    validate_channel_attrs("back", &back_channel, &back_attrs)?;

    let num_slots = fore_channel.max_requests;
    info!(
        session_id = hex::encode(session_id),
        num_slots,
        max_ops = fore_channel.max_ops,
        max_req_size = fore_channel.max_request_size,
        "CREATE_SESSION successful"
    );
    Ok((session_id, fore_channel, back_channel))
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
    let response_bytes = rpc.call(buf, super::BOOTSTRAP_REPLAY, timeout).await?;
    CompoundResponse::decode(response_bytes)
}

async fn send_compound_on_session(
    rpc: &rpc::Client,
    auth: &Auth,
    builder: CompoundBuilder,
    timeout: std::time::Duration,
    session: &Session,
) -> Result<CompoundResponse> {
    builder.enforce_max_operations(session.max_operations())?;
    let requested_ops = builder.op_count();
    let mut buf = Vec::new();
    builder.encode_with_header(auth, &mut buf);
    let request_size = buf
        .len()
        .checked_add(8)
        .ok_or_else(|| NfsError::Rpc("session request size overflow".to_string()))?;
    if request_size > session.max_request_size() as usize {
        return Err(NfsError::Rpc(format!(
            "session request size {request_size} exceeds negotiated maximum {}",
            session.max_request_size()
        )));
    }
    let response_bytes = rpc.call(buf, super::BOOTSTRAP_REPLAY, timeout).await?;
    let response_size = response_bytes
        .len()
        .checked_add(24)
        .ok_or_else(|| NfsError::Rpc("session response size overflow".to_string()))?;
    if response_size > session.max_response_size() as usize {
        return Err(NfsError::Rpc(format!(
            "session response size {response_size} exceeds negotiated maximum {}",
            session.max_response_size()
        )));
    }
    let response = CompoundResponse::decode(response_bytes)?;
    if response.results.len() > requested_ops
        || response.results.len() > session.max_operations() as usize
    {
        return Err(NfsError::Xdr(
            "session response operation count exceeds negotiated/requested bound".to_string(),
        ));
    }
    Ok(response)
}

fn decode_channel_attrs(data: &mut Bytes) -> Result<ChannelAttrs> {
    if data.remaining() < 24 {
        return Err(NfsError::Xdr("channel_attrs truncated".to_string()));
    }
    let header_pad_size = data.get_u32();
    let max_request_size = data.get_u32();
    let max_response_size = data.get_u32();
    let max_cached_response_size = data.get_u32();
    let max_ops = data.get_u32();
    let max_requests = data.get_u32();
    // ca_rdma_ird<1>
    if data.remaining() < 4 {
        return Err(NfsError::Xdr("ca_rdma_ird length truncated".to_string()));
    }
    let n = data.get_u32();
    if n > 1 {
        return Err(NfsError::Xdr(format!(
            "ca_rdma_ird count {n} exceeds protocol bound 1"
        )));
    }
    let skip = usize::try_from(n)
        .ok()
        .and_then(|count| count.checked_mul(4))
        .ok_or_else(|| NfsError::Xdr("ca_rdma_ird length overflow".to_string()))?;
    if data.remaining() < skip {
        return Err(NfsError::Xdr("ca_rdma_ird data truncated".to_string()));
    }
    data.advance(skip);
    Ok(ChannelAttrs {
        header_pad_size,
        max_request_size,
        max_response_size,
        max_cached_response_size,
        max_ops,
        max_requests,
    })
}

fn validate_channel_attrs(
    name: &str,
    attrs: &ChannelAttrs,
    offered: &ChannelAttrsArgs,
) -> Result<()> {
    let values = [
        (
            "headerpadsize",
            attrs.header_pad_size,
            offered.headerpadsize,
        ),
        (
            "maxrequestsize",
            attrs.max_request_size,
            offered.maxrequestsize,
        ),
        (
            "maxresponsesize",
            attrs.max_response_size,
            offered.maxresponsesize,
        ),
        (
            "maxresponsesize_cached",
            attrs.max_cached_response_size,
            offered.maxresponsesize_cached,
        ),
        ("maxoperations", attrs.max_ops, offered.maxoperations),
        ("maxrequests", attrs.max_requests, offered.maxrequests),
    ];
    for (field, value, maximum) in values {
        if (field != "headerpadsize" && field != "maxresponsesize_cached" && value == 0)
            || value > maximum
        {
            return Err(NfsError::Rpc(format!(
                "invalid {name}-channel {field} {value}; offered range is 1..={maximum}"
            )));
        }
    }
    if attrs.max_cached_response_size > attrs.max_response_size {
        return Err(NfsError::Rpc(format!(
            "invalid {name}-channel cached response maximum {} exceeds response maximum {}",
            attrs.max_cached_response_size, attrs.max_response_size
        )));
    }
    Ok(())
}

/// A holder that allows atomic session replacement for recovery.
///
/// Normal operations take a read lock to clone the current `Arc<Session>`,
/// then immediately release the lock before doing any I/O.
/// Recovery operations take a write lock to replace the session.
pub(crate) struct SessionHolder {
    inner: tokio::sync::RwLock<Arc<Session>>,
    pnfs_mds: AtomicBool,
}

impl SessionHolder {
    pub fn new(session: Session) -> Self {
        let mut session = session;
        session.generation = 1;
        let pnfs_mds = session.pnfs_mds();
        Self {
            inner: tokio::sync::RwLock::new(Arc::new(session)),
            pnfs_mds: AtomicBool::new(pnfs_mds),
        }
    }

    /// Return the pNFS MDS capability from the currently published session.
    pub fn pnfs_mds(&self) -> bool {
        self.pnfs_mds.load(Ordering::Acquire)
    }

    /// Get a clone of the current session (shared read lock, immediately released).
    pub async fn get(&self) -> Arc<Session> {
        self.inner.read().await.clone()
    }

    /// Replace the session (exclusive write lock, used during recovery).
    #[cfg(test)]
    pub async fn replace_if_current(&self, expected: u64, mut new_session: Session) -> bool {
        let mut guard = self.inner.write().await;
        if guard.generation != expected {
            return false;
        }
        new_session.generation = expected.saturating_add(1);
        self.pnfs_mds
            .store(new_session.pnfs_mds(), Ordering::Release);
        *guard = Arc::new(new_session);
        true
    }

    /// Publish a replacement session and its callback identity under the same
    /// holder write lock, so no caller can observe the new fore-channel session
    /// while the backchannel still accepts the old session ID.
    pub async fn replace_with_callback_if_current(
        &self,
        expected: u64,
        mut new_session: Session,
        callback: &super::callback::CallbackState,
    ) -> Result<bool> {
        let mut guard = self.inner.write().await;
        if guard.generation != expected {
            return Ok(false);
        }
        new_session.generation = expected.saturating_add(1);
        callback.update_session(
            new_session.session_id,
            new_session.generation,
            new_session.backchannel_max_requests,
            new_session.backchannel_max_request_size,
            new_session.backchannel_max_operations,
        )?;
        self.pnfs_mds
            .store(new_session.pnfs_mds(), Ordering::Release);
        *guard = Arc::new(new_session);
        Ok(true)
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

    fn test_session(marker: u8) -> Session {
        Session {
            generation: 0,
            session_id: [marker; 16],
            client_id: marker as u64,
            slot_table: SlotTable::new(1),
            max_request_size: 4096,
            max_response_size: 4096,
            max_cached_response_size: 512,
            max_operations: 16,
            backchannel_max_requests: 1,
            backchannel_max_request_size: 4096,
            backchannel_max_operations: 2,
            pnfs_mds: false,
        }
    }

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

    fn offered_channel(maximum: u32) -> ChannelAttrsArgs {
        ChannelAttrsArgs {
            headerpadsize: 0,
            maxrequestsize: maximum,
            maxresponsesize: maximum,
            maxresponsesize_cached: maximum,
            maxoperations: maximum,
            maxrequests: maximum,
        }
    }

    fn channel(value: u32) -> ChannelAttrs {
        ChannelAttrs {
            header_pad_size: 0,
            max_request_size: value,
            max_response_size: value,
            max_cached_response_size: value,
            max_ops: value,
            max_requests: value,
        }
    }

    #[test]
    fn channel_limits_reject_zero_and_above_offer() {
        let offered = offered_channel(64);
        assert!(validate_channel_attrs("fore", &channel(0), &offered).is_err());
        assert!(validate_channel_attrs("fore", &channel(65), &offered).is_err());
        assert!(validate_channel_attrs("fore", &channel(1), &offered).is_ok());
        assert!(validate_channel_attrs("fore", &channel(63), &offered).is_ok());
        assert!(validate_channel_attrs("fore", &channel(64), &offered).is_ok());

        let mut invalid = channel(1);
        invalid.max_request_size = 65;
        assert!(validate_channel_attrs("fore", &invalid, &offered).is_err());
        invalid = channel(1);
        invalid.max_response_size = 65;
        assert!(validate_channel_attrs("fore", &invalid, &offered).is_err());
        invalid = channel(1);
        invalid.max_cached_response_size = 65;
        assert!(validate_channel_attrs("fore", &invalid, &offered).is_err());
        let mut no_reply_cache = channel(1);
        no_reply_cache.max_cached_response_size = 0;
        assert!(validate_channel_attrs("fore", &no_reply_cache, &offered).is_ok());
        invalid = channel(1);
        invalid.max_ops = 65;
        assert!(validate_channel_attrs("fore", &invalid, &offered).is_err());
        invalid = channel(1);
        invalid.max_requests = 65;
        assert!(validate_channel_attrs("fore", &invalid, &offered).is_err());
    }

    #[tokio::test]
    async fn target_shrink_preserves_in_flight_slots_and_growth_wakes_waiter() {
        let table = Arc::new(SlotTable::new(4));
        let low = table.acquire().await.unwrap();
        let high_one = table.acquire().await.unwrap();
        let high_two = table.acquire().await.unwrap();
        assert_eq!((low.slot_id, high_one.slot_id, high_two.slot_id), (0, 1, 2));

        table.update_target_highest_slot_id(0).unwrap();
        assert_eq!(
            table.highest_slot_id(),
            2,
            "in-flight high slot must be reported"
        );
        drop(low);
        let replacement = table.acquire().await.unwrap();
        assert_eq!(replacement.slot_id, 0);

        let waiting_table = Arc::clone(&table);
        let waiter = tokio::spawn(async move { waiting_table.acquire().await.map(|s| s.slot_id) });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !waiter.is_finished(),
            "slot 3 must be withheld after shrink"
        );

        table.update_target_highest_slot_id(3).unwrap();
        let grown_slot = tokio::time::timeout(std::time::Duration::from_millis(100), waiter)
            .await
            .expect("growth should wake waiter")
            .expect("waiter task should complete")
            .expect("slot acquisition should succeed");
        assert_eq!(grown_slot, 3);
        drop((replacement, high_one, high_two));
    }

    #[tokio::test]
    async fn highest_slot_falls_to_target_after_high_request_completes() {
        let table = SlotTable::new(4);
        let low = table.acquire().await.unwrap();
        let high = table.acquire().await.unwrap();
        table.update_target_highest_slot_id(0).unwrap();
        assert_eq!(table.highest_slot_id(), 1);
        drop(high);
        assert_eq!(table.highest_slot_id(), 0);
        drop(low);
    }

    #[test]
    fn target_above_negotiated_slot_count_is_rejected() {
        let table = SlotTable::new(4);
        assert!(table.update_target_highest_slot_id(3).is_ok());
        assert!(table.update_target_highest_slot_id(4).is_err());
    }

    #[test]
    fn sequence_enforced_and_target_limits_cannot_exceed_negotiated_table() {
        let session = test_session(1);
        assert!(session.update_sequence_slot_limits(0, 0).is_ok());
        assert!(session.update_sequence_slot_limits(1, 0).is_err());
        assert!(session.update_sequence_slot_limits(0, 1).is_err());
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

    #[tokio::test]
    async fn ambiguous_slot_is_not_reused() {
        let table = SlotTable::new(1);
        {
            let mut slot = table.acquire().await.unwrap();
            slot.fence_on_drop();
        }
        let acquire =
            tokio::time::timeout(std::time::Duration::from_millis(20), table.acquire()).await;
        assert!(
            !matches!(acquire, Ok(Ok(_))),
            "fenced slot must retain its permit"
        );
    }

    #[tokio::test]
    async fn resolved_slot_is_released_after_being_armed() {
        let table = SlotTable::new(1);
        {
            let mut slot = table.acquire().await.unwrap();
            slot.fence_on_drop();
            slot.resolve();
        }
        let slot = tokio::time::timeout(std::time::Duration::from_millis(100), table.acquire())
            .await
            .expect("resolved slot should be available")
            .expect("slot acquisition should succeed");
        assert_eq!(slot.slot_id, 0);
        assert_eq!(slot.sequence_id, 1);
    }

    #[tokio::test]
    async fn cancellation_keeps_armed_slot_fenced() {
        let table = Arc::new(SlotTable::new(1));
        let acquired = Arc::new(tokio::sync::Notify::new());
        let table_for_task = Arc::clone(&table);
        let acquired_for_task = Arc::clone(&acquired);
        let task = tokio::spawn(async move {
            let mut slot = table_for_task.acquire().await.unwrap();
            slot.fence_on_drop();
            acquired_for_task.notify_one();
            std::future::pending::<()>().await;
        });
        acquired.notified().await;
        task.abort();
        let _ = task.await;

        let acquire =
            tokio::time::timeout(std::time::Duration::from_millis(20), table.acquire()).await;
        assert!(
            !matches!(acquire, Ok(Ok(_))),
            "cancellation must not release an ambiguous slot"
        );
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
        buf.extend_from_slice(&0u32.to_be_bytes()); // headerpadsize
        buf.extend_from_slice(&1048576u32.to_be_bytes()); // maxrequestsize
        buf.extend_from_slice(&1048576u32.to_be_bytes()); // maxresponsesize
        buf.extend_from_slice(&4096u32.to_be_bytes()); // maxresponsesize_cached
        buf.extend_from_slice(&16u32.to_be_bytes()); // maxoperations
        buf.extend_from_slice(&4u32.to_be_bytes()); // maxrequests
        buf.extend_from_slice(&0u32.to_be_bytes()); // ca_rdma_ird count = 0
        let mut bytes = buf.freeze();
        let attrs = decode_channel_attrs(&mut bytes).unwrap();
        assert_eq!(attrs.max_request_size, 1048576);
        assert_eq!(attrs.max_response_size, 1048576);
        assert_eq!(attrs.max_cached_response_size, 4096);
        assert_eq!(attrs.max_ops, 16);
        assert_eq!(attrs.max_requests, 4);
    }

    #[test]
    fn decode_channel_attrs_truncated() {
        let buf = Bytes::from(vec![0u8; 10]); // too short
        let mut b = buf;
        assert!(decode_channel_attrs(&mut b).is_err());
    }

    #[test]
    fn decode_channel_attrs_rejects_rdma_array_above_protocol_bound() {
        let mut buf = bytes::BytesMut::new();
        for _ in 0..6 {
            buf.extend_from_slice(&1u32.to_be_bytes());
        }
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let mut encoded = buf.freeze();
        assert!(decode_channel_attrs(&mut encoded).is_err());
    }

    fn sequence_op(
        session_id: [u8; 16],
        sequence_id: u32,
        slot_id: u32,
        target: u32,
    ) -> super::super::compound::OpResponse {
        let mut data = Vec::new();
        data.extend_from_slice(&session_id);
        data.extend_from_slice(&sequence_id.to_be_bytes());
        data.extend_from_slice(&slot_id.to_be_bytes());
        data.extend_from_slice(&3u32.to_be_bytes());
        data.extend_from_slice(&target.to_be_bytes());
        data.extend_from_slice(&0u32.to_be_bytes());
        super::super::compound::OpResponse {
            opcode: super::super::compound::OpNum::Sequence as u32,
            status: crate::nfs4::fastxdr::nfsstat4::NFS4_OK,
            data: Bytes::from(data),
        }
    }

    #[test]
    fn sequence_identity_and_target_are_validated_before_advance() {
        let expected = [9; 16];
        let valid = sequence_op(expected, 7, 2, 1);
        let decoded = validate_sequence_result(&valid, &expected, 7, 2).unwrap();
        assert_eq!(decoded.target_highest_slot_id, 1);
        assert!(validate_sequence_result(&valid, &[8; 16], 7, 2).is_err());
        assert!(validate_sequence_result(&valid, &expected, 8, 2).is_err());
        assert!(validate_sequence_result(&valid, &expected, 7, 3).is_err());
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

    #[tokio::test]
    async fn session_holder_generations_are_monotonic_and_stale_safe() {
        let holder = SessionHolder::new(test_session(1));
        assert!(!holder.pnfs_mds());
        assert_eq!(holder.get().await.generation(), 1);
        let mut pnfs_session = test_session(2);
        pnfs_session.pnfs_mds = true;
        assert!(holder.replace_if_current(1, pnfs_session).await);
        assert!(holder.pnfs_mds());
        assert_eq!(holder.get().await.generation(), 2);
        assert!(!holder.replace_if_current(1, test_session(3)).await);
        assert!(holder.pnfs_mds());
        let active = holder.get().await;
        assert_eq!(active.generation(), 2);
        assert_eq!(active.id(), &[2; 16]);
    }

    #[tokio::test]
    async fn concurrent_replacements_publish_once() {
        let holder = Arc::new(SessionHolder::new(test_session(1)));
        let mut tasks = Vec::new();
        for marker in 2..=65 {
            let holder = Arc::clone(&holder);
            tasks.push(tokio::spawn(async move {
                holder.replace_if_current(1, test_session(marker)).await
            }));
        }
        let mut published = 0;
        for task in tasks {
            if task.await.unwrap() {
                published += 1;
            }
        }
        assert_eq!(published, 1);
        assert_eq!(holder.get().await.generation(), 2);
    }
}
