//! NFSv4.1 COMPOUND request builder and response decoder.
//!
//! NFSv4.1 uses a single RPC procedure (COMPOUND, procedure 1) that carries
//! a sequence of operations. Each operation has an opcode (nfs_opnum4) followed
//! by operation-specific arguments.
//!
//! # Wire format (COMPOUND4args)
//! ```text
//! tag: utf8str_cs (variable)
//! minorversion: uint32 (1 for v4.1)
//! argarray_len: uint32
//! argarray[0]: { opcode: uint32, op-specific args... }
//! argarray[1]: { opcode: uint32, op-specific args... }
//! ...
//! ```

use bytes::{Buf, Bytes};

use super::{NFS4_COMPOUND_PROC, NFS4_PROGRAM, NFS4_VERSION, NFS41_MINOR_VERSION};
use crate::error::{NfsError, Result};
use crate::nfs3::rpc_header;
use crate::nfs4::fastxdr::*;
use crate::rpc::auth::Auth;

// ─── Protocol safety limits for server-provided lengths ──────────────────────
// These prevent unbounded allocation from malicious/buggy server responses.
const MAX_COMPOUND_OPS: usize = 256;
const MAX_BITMAP_WORDS: usize = 16;
const MAX_ENTRY4_PER_PAGE: usize = 65536;
const MAX_LAYOUT_SEGMENTS: usize = 1024;

// NFSv4.1 operation numbers (nfs_opnum4, RFC 5661 §16)
#[repr(u32)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum OpNum {
    Access = 3,
    Close = 4,
    Commit = 5,
    Create = 6,
    GetAttr = 9,
    GetFh = 10,
    Link = 11,
    Lookup = 15,
    Lookupp = 16,
    Open = 18,
    PutFh = 22,
    PutRootFh = 24,
    Read = 25,
    ReadDir = 26,
    ReadLink = 27,
    Remove = 28,
    Rename = 29,
    RestoreFh = 31,
    SaveFh = 32,
    SetAttr = 34,
    Write = 38,
    OpenAttr = 20,
    Lock = 12,
    Lockt = 13,
    Locku = 14,
    LayoutGet = 50,
    LayoutCommit = 49,
    LayoutReturn = 51,
    GetDeviceInfo = 47,
    ExchangeId = 42,
    CreateSession = 43,
    DestroySession = 44,
    BindConnToSession = 41,
    Sequence = 53,
    ReclaimComplete = 58,
    DestroyClientId = 57,
    DelegReturn = 8,
    TestStateId = 55,
    FreeStateId = 56,
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum OperationClass {
    ReadOnly,
    SessionControl,
    ReplaySensitive,
}

impl OpNum {
    pub(crate) const fn class(self) -> OperationClass {
        match self {
            Self::Access
            | Self::GetAttr
            | Self::GetFh
            | Self::GetDeviceInfo
            | Self::Lockt
            | Self::Lookup
            | Self::Lookupp
            | Self::PutFh
            | Self::PutRootFh
            | Self::Read
            | Self::ReadDir
            | Self::ReadLink
            | Self::RestoreFh
            | Self::SaveFh
            | Self::TestStateId => OperationClass::ReadOnly,
            Self::BindConnToSession
            | Self::CreateSession
            | Self::DestroyClientId
            | Self::DestroySession
            | Self::ExchangeId
            | Self::ReclaimComplete
            | Self::Sequence => OperationClass::SessionControl,
            Self::Close
            | Self::Commit
            | Self::Create
            | Self::DelegReturn
            | Self::FreeStateId
            | Self::LayoutCommit
            | Self::LayoutGet
            | Self::LayoutReturn
            | Self::Link
            | Self::Lock
            | Self::Locku
            | Self::Open
            | Self::OpenAttr
            | Self::Remove
            | Self::Rename
            | Self::SetAttr
            | Self::Write => OperationClass::ReplaySensitive,
        }
    }
}

impl From<OperationClass> for crate::error::OperationClass {
    fn from(value: OperationClass) -> Self {
        match value {
            OperationClass::ReadOnly => Self::ReadOnly,
            OperationClass::SessionControl => Self::SessionControl,
            OperationClass::ReplaySensitive => Self::ReplaySensitive,
        }
    }
}

// ─── XDR encoding helpers (same pattern as nfs3) ─────────────────────────────

fn xdr_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn xdr_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn xdr_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn xdr_bool(buf: &mut Vec<u8>, v: bool) {
    xdr_u32(buf, if v { 1 } else { 0 });
}

fn xdr_fixed_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(data);
    let pad = (4 - data.len() % 4) % 4;
    for _ in 0..pad {
        buf.push(0);
    }
}

fn xdr_var_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    xdr_u32(buf, data.len() as u32);
    xdr_fixed_bytes(buf, data);
}

fn xdr_string(buf: &mut Vec<u8>, s: &str) {
    xdr_var_bytes(buf, s.as_bytes());
}

fn xdr_bitmap(buf: &mut Vec<u8>, bitmap: &[u32]) {
    xdr_u32(buf, bitmap.len() as u32);
    for &word in bitmap {
        xdr_u32(buf, word);
    }
}

// ─── Compound builder ────────────────────────────────────────────────────────

/// Builder for COMPOUND4args requests.
///
/// Operations are appended via chainable methods. The builder is consumed
/// by `encode()` to produce the wire-format bytes.
pub(crate) struct CompoundBuilder {
    tag: String,
    ops: Vec<EncodedOp>,
    required_generation: Option<u64>,
}

/// A single encoded operation (opcode + pre-serialized args).
struct EncodedOp {
    opcode: OpNum,
    args: Vec<u8>,
}

const SEQUENCE_CACHE_THIS_OFFSET: usize = 28;
// Minimum accepted RPC reply (AUTH_NONE verifier) plus COMPOUND4res, a successful
// SEQUENCE result, and the opcode/status pair for each remaining operation.
const MIN_RPC_REPLY_ENVELOPE_SIZE: usize = 24;
const MIN_COMPOUND_REPLY_ENVELOPE_SIZE: usize = 12;
const SEQUENCE_SUCCESS_REPLY_SIZE: usize = 44;
const MIN_OPERATION_REPLY_SIZE: usize = 8;

#[allow(dead_code)] // Builder offers full NFSv4.1 op set; not all ops used yet
impl CompoundBuilder {
    pub fn new(tag: &str) -> Self {
        Self {
            tag: tag.to_string(),
            ops: Vec::new(),
            required_generation: None,
        }
    }

    /// Number of operations currently in the builder.
    pub fn op_count(&self) -> usize {
        self.ops.len()
    }

    pub(crate) fn enforce_max_operations(&self, maximum: u32) -> Result<()> {
        let count = u32::try_from(self.ops.len())
            .map_err(|_| NfsError::Rpc("COMPOUND operation count exceeds u32".to_string()))?;
        if count == 0 || count > maximum {
            return Err(NfsError::Rpc(format!(
                "COMPOUND contains {count} operations; channel maximum is {maximum}"
            )));
        }
        Ok(())
    }

    pub(crate) fn operation_class(&self) -> crate::error::OperationClass {
        self.ops
            .iter()
            .skip(1)
            .map(|op| op.opcode.class())
            .max()
            .unwrap_or(OperationClass::SessionControl)
            .into()
    }

    pub(crate) fn require_generation(mut self, generation: u64) -> Self {
        if generation != 0 {
            self.required_generation = Some(generation);
        }
        self
    }

    pub(crate) fn required_generation(&self) -> Option<u64> {
        self.required_generation
    }

    // ─── Session ops ─────────────────────────────────────────────────────

    pub fn sequence(
        mut self,
        session_id: &[u8; 16],
        sequence_id: u32,
        slot_id: u32,
        highest_slot_id: u32,
    ) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(session_id); // fixed 16 bytes, no length prefix
        xdr_u32(&mut args, sequence_id);
        xdr_u32(&mut args, slot_id);
        xdr_u32(&mut args, highest_slot_id);
        xdr_bool(&mut args, false);
        self.ops.push(EncodedOp {
            opcode: OpNum::Sequence,
            args,
        });
        self
    }

    /// Apply the reply-cache policy after all COMPOUND operations have been added.
    ///
    /// RFC 5661 §2.10.6.1.3 requires replay-sensitive requests to retain enough
    /// reply state for exactly-once handling. Reject a channel that cannot hold
    /// even the smallest successful reply for this COMPOUND. Larger,
    /// operation-dependent replies remain subject to NFS4ERR_REP_TOO_BIG_TO_CACHE.
    pub fn apply_sequence_cache_policy(mut self, max_cached_response_size: u32) -> Result<Self> {
        let class = self
            .ops
            .iter()
            .skip(1)
            .map(|op| op.opcode.class())
            .max()
            .unwrap_or(OperationClass::SessionControl);
        let cachethis = class == OperationClass::ReplaySensitive;
        let required_minimum = self.minimum_cached_response_size();
        if cachethis && max_cached_response_size < required_minimum {
            return Err(NfsError::Rpc(format!(
                "replay-sensitive COMPOUND requires at least {} cached response bytes, server negotiated {}",
                required_minimum, max_cached_response_size
            )));
        }
        let sequence = self
            .ops
            .first_mut()
            .ok_or_else(|| NfsError::Rpc("COMPOUND has no SEQUENCE operation".to_string()))?;
        if sequence.opcode as u32 != OpNum::Sequence as u32
            || sequence.args.len() < SEQUENCE_CACHE_THIS_OFFSET + 4
        {
            return Err(NfsError::Rpc(
                "COMPOUND first operation is not a valid SEQUENCE".to_string(),
            ));
        }
        sequence.args[SEQUENCE_CACHE_THIS_OFFSET..SEQUENCE_CACHE_THIS_OFFSET + 4]
            .copy_from_slice(&(cachethis as u32).to_be_bytes());
        Ok(self)
    }

    fn minimum_cached_response_size(&self) -> u32 {
        let padded_tag_len = self.tag.len().saturating_add(3) & !3;
        let remaining_ops = self.ops.len().saturating_sub(1);
        let size = MIN_RPC_REPLY_ENVELOPE_SIZE
            .saturating_add(MIN_COMPOUND_REPLY_ENVELOPE_SIZE)
            .saturating_add(padded_tag_len)
            .saturating_add(SEQUENCE_SUCCESS_REPLY_SIZE)
            .saturating_add(remaining_ops.saturating_mul(MIN_OPERATION_REPLY_SIZE));
        u32::try_from(size).unwrap_or(u32::MAX)
    }

    pub fn exchange_id(
        mut self,
        co_verifier: &[u8; 8],
        co_ownerid: &[u8],
        flags: u32,
        impl_domain: &str,
        impl_name: &str,
    ) -> Self {
        let mut args = Vec::new();
        // client_owner4: verifier4 + opaque co_ownerid<>
        args.extend_from_slice(co_verifier);
        xdr_var_bytes(&mut args, co_ownerid);
        // flags
        xdr_u32(&mut args, flags);
        // state_protect4_a: SP4_NONE = 0
        xdr_u32(&mut args, 0);
        // nfs_impl_id4<1>: array of 1 element
        xdr_u32(&mut args, 1);
        xdr_string(&mut args, impl_domain);
        xdr_string(&mut args, impl_name);
        // nfstime4 date (0, 0)
        xdr_i64(&mut args, 0);
        xdr_u32(&mut args, 0);
        self.ops.push(EncodedOp {
            opcode: OpNum::ExchangeId,
            args,
        });
        self
    }

    pub fn create_session(
        mut self,
        client_id: u64,
        sequence_id: u32,
        flags: u32,
        fore_attrs: &ChannelAttrsArgs,
        back_attrs: &ChannelAttrsArgs,
        cb_program: u32,
    ) -> Self {
        let mut args = Vec::new();
        xdr_u64(&mut args, client_id);
        xdr_u32(&mut args, sequence_id);
        xdr_u32(&mut args, flags);
        fore_attrs.encode(&mut args);
        back_attrs.encode(&mut args);
        xdr_u32(&mut args, cb_program);
        // callback_sec_parms4: array of 1 AUTH_NONE entry
        xdr_u32(&mut args, 1); // array length
        xdr_u32(&mut args, 0); // AUTH_NONE flavor
        self.ops.push(EncodedOp {
            opcode: OpNum::CreateSession,
            args,
        });
        self
    }

    pub fn destroy_session(mut self, session_id: &[u8; 16]) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(session_id);
        self.ops.push(EncodedOp {
            opcode: OpNum::DestroySession,
            args,
        });
        self
    }

    /// BIND_CONN_TO_SESSION (RFC 5661 §18.34)
    /// dir: CDFC4_FORE=1, CDFC4_BACK=2, CDFC4_FORE_OR_BOTH=3
    pub fn bind_conn_to_session(
        mut self,
        session_id: &[u8; 16],
        dir: u32,
        use_conn_in_rdma_mode: bool,
    ) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(session_id);
        xdr_u32(&mut args, dir); // channel direction
        xdr_bool(&mut args, use_conn_in_rdma_mode);
        self.ops.push(EncodedOp {
            opcode: OpNum::BindConnToSession,
            args,
        });
        self
    }

    pub fn reclaim_complete(mut self, one_fs: bool) -> Self {
        let mut args = Vec::new();
        xdr_bool(&mut args, one_fs);
        self.ops.push(EncodedOp {
            opcode: OpNum::ReclaimComplete,
            args,
        });
        self
    }

    pub fn destroy_client_id(mut self, client_id: u64) -> Self {
        let mut args = Vec::new();
        xdr_u64(&mut args, client_id);
        self.ops.push(EncodedOp {
            opcode: OpNum::DestroyClientId,
            args,
        });
        self
    }

    // ─── File handle ops ─────────────────────────────────────────────────

    pub fn putrootfh(mut self) -> Self {
        self.ops.push(EncodedOp {
            opcode: OpNum::PutRootFh,
            args: Vec::new(),
        });
        self
    }

    pub fn putfh(mut self, fh: &[u8]) -> Self {
        let mut args = Vec::new();
        xdr_var_bytes(&mut args, fh);
        self.ops.push(EncodedOp {
            opcode: OpNum::PutFh,
            args,
        });
        self
    }

    pub fn getfh(mut self) -> Self {
        self.ops.push(EncodedOp {
            opcode: OpNum::GetFh,
            args: Vec::new(),
        });
        self
    }

    pub fn savefh(mut self) -> Self {
        self.ops.push(EncodedOp {
            opcode: OpNum::SaveFh,
            args: Vec::new(),
        });
        self
    }

    pub fn restorefh(mut self) -> Self {
        self.ops.push(EncodedOp {
            opcode: OpNum::RestoreFh,
            args: Vec::new(),
        });
        self
    }

    /// OPENATTR: open the named attribute directory for the current file handle.
    /// create_dir: if true, create the attribute directory if it doesn't exist.
    pub fn openattr(mut self, create_dir: bool) -> Self {
        let mut args = Vec::new();
        xdr_bool(&mut args, create_dir);
        self.ops.push(EncodedOp {
            opcode: OpNum::OpenAttr,
            args,
        });
        self
    }

    // ─── File operations ─────────────────────────────────────────────────

    pub fn lookup(mut self, name: &str) -> Self {
        let mut args = Vec::new();
        xdr_string(&mut args, name);
        self.ops.push(EncodedOp {
            opcode: OpNum::Lookup,
            args,
        });
        self
    }

    pub fn lookupp(mut self) -> Self {
        self.ops.push(EncodedOp {
            opcode: OpNum::Lookupp,
            args: Vec::new(),
        });
        self
    }

    pub fn getattr(mut self, bitmap: &[u32]) -> Self {
        let mut args = Vec::new();
        xdr_bitmap(&mut args, bitmap);
        self.ops.push(EncodedOp {
            opcode: OpNum::GetAttr,
            args,
        });
        self
    }

    pub fn setattr(mut self, stateid: &[u8; 16], attrmask: &[u32], attr_vals: &[u8]) -> Self {
        let mut args = Vec::new();
        // stateid4: seqid (4 bytes) + other (12 bytes)
        args.extend_from_slice(stateid);
        // fattr4: bitmap + opaque attr_vals
        xdr_bitmap(&mut args, attrmask);
        xdr_var_bytes(&mut args, attr_vals);
        self.ops.push(EncodedOp {
            opcode: OpNum::SetAttr,
            args,
        });
        self
    }

    pub fn access(mut self, access_mask: u32) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, access_mask);
        self.ops.push(EncodedOp {
            opcode: OpNum::Access,
            args,
        });
        self
    }

    pub fn read(mut self, stateid: &[u8; 16], offset: u64, count: u32) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        xdr_u64(&mut args, offset);
        xdr_u32(&mut args, count);
        self.ops.push(EncodedOp {
            opcode: OpNum::Read,
            args,
        });
        self
    }

    pub fn write(mut self, stateid: &[u8; 16], offset: u64, stable: u32, data: &[u8]) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        xdr_u64(&mut args, offset);
        xdr_u32(&mut args, stable); // stable_how4
        xdr_var_bytes(&mut args, data);
        self.ops.push(EncodedOp {
            opcode: OpNum::Write,
            args,
        });
        self
    }

    /// Encode WRITE header only (stateid, offset, stable, data_length) without the
    /// actual data bytes. The caller must send the data separately via
    /// `rpc::Client::call_with_data()` for zero-copy writes.
    pub fn write_header(
        mut self,
        stateid: &[u8; 16],
        offset: u64,
        stable: u32,
        data_len: u32,
    ) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        xdr_u64(&mut args, offset);
        xdr_u32(&mut args, stable); // stable_how4
        // XDR opaque length prefix only — actual data sent out-of-band
        xdr_u32(&mut args, data_len);
        self.ops.push(EncodedOp {
            opcode: OpNum::Write,
            args,
        });
        self
    }

    pub fn commit(mut self, offset: u64, count: u32) -> Self {
        let mut args = Vec::new();
        xdr_u64(&mut args, offset);
        xdr_u32(&mut args, count);
        self.ops.push(EncodedOp {
            opcode: OpNum::Commit,
            args,
        });
        self
    }

    pub fn readdir(
        mut self,
        cookie: u64,
        cookieverf: &[u8; 8],
        dircount: u32,
        maxcount: u32,
        attr_request: &[u32],
    ) -> Self {
        let mut args = Vec::new();
        xdr_u64(&mut args, cookie);
        args.extend_from_slice(cookieverf);
        xdr_u32(&mut args, dircount);
        xdr_u32(&mut args, maxcount);
        xdr_bitmap(&mut args, attr_request);
        self.ops.push(EncodedOp {
            opcode: OpNum::ReadDir,
            args,
        });
        self
    }

    pub fn readlink(mut self) -> Self {
        self.ops.push(EncodedOp {
            opcode: OpNum::ReadLink,
            args: Vec::new(),
        });
        self
    }

    pub fn open(mut self, open_args: &OpenArgs) -> Self {
        let mut args = Vec::new();
        open_args.encode(&mut args);
        self.ops.push(EncodedOp {
            opcode: OpNum::Open,
            args,
        });
        self
    }

    pub fn close(mut self, seqid: u32, stateid: &[u8; 16]) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, seqid);
        args.extend_from_slice(stateid);
        self.ops.push(EncodedOp {
            opcode: OpNum::Close,
            args,
        });
        self
    }

    /// CREATE for directories, sockets, FIFOs, etc.
    /// For NF4LNK use `create_symlink` instead.
    pub fn create(mut self, objtype: u32, name: &str, attrmask: &[u32], attr_vals: &[u8]) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, objtype); // nfs_ftype4
        // CREATE4args: objtype + objname + createattrs
        // For NF4BLK/NF4CHR, specdata would go here (not implemented yet)
        xdr_string(&mut args, name);
        xdr_bitmap(&mut args, attrmask);
        xdr_var_bytes(&mut args, attr_vals);
        self.ops.push(EncodedOp {
            opcode: OpNum::Create,
            args,
        });
        self
    }

    /// CREATE for symlinks (NF4LNK): requires the link target path.
    pub fn create_symlink(
        mut self,
        name: &str,
        link_target: &str,
        attrmask: &[u32],
        attr_vals: &[u8],
    ) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, 5); // NF4LNK = 5
        // For NF4LNK, linkdata (utf8str) comes before objname
        xdr_string(&mut args, link_target);
        xdr_string(&mut args, name);
        xdr_bitmap(&mut args, attrmask);
        xdr_var_bytes(&mut args, attr_vals);
        self.ops.push(EncodedOp {
            opcode: OpNum::Create,
            args,
        });
        self
    }

    pub fn remove(mut self, name: &str) -> Self {
        let mut args = Vec::new();
        xdr_string(&mut args, name);
        self.ops.push(EncodedOp {
            opcode: OpNum::Remove,
            args,
        });
        self
    }

    pub fn rename(mut self, oldname: &str, newname: &str) -> Self {
        let mut args = Vec::new();
        xdr_string(&mut args, oldname);
        xdr_string(&mut args, newname);
        self.ops.push(EncodedOp {
            opcode: OpNum::Rename,
            args,
        });
        self
    }

    pub fn link(mut self, newname: &str) -> Self {
        let mut args = Vec::new();
        xdr_string(&mut args, newname);
        self.ops.push(EncodedOp {
            opcode: OpNum::Link,
            args,
        });
        self
    }

    // ─── pNFS layout operations ────────────────────────────────────────────

    /// LAYOUTGET: request a layout for parallel data access.
    /// layout_type: 1=LAYOUT4_NFSV4_1_FILES, 2=LAYOUT4_OSD2_OBJECTS, 3=LAYOUT4_BLOCK_VOLUME
    /// iomode: 1=LAYOUTIOMODE4_READ, 2=LAYOUTIOMODE4_RW
    #[allow(clippy::too_many_arguments)]
    pub fn layoutget(
        mut self,
        signal_layout_avail: bool,
        layout_type: u32,
        iomode: u32,
        offset: u64,
        length: u64,
        min_length: u64,
        stateid: &[u8; 16],
        max_count: u32,
    ) -> Self {
        let mut args = Vec::new();
        xdr_bool(&mut args, signal_layout_avail);
        xdr_u32(&mut args, layout_type);
        xdr_u32(&mut args, iomode);
        xdr_u64(&mut args, offset);
        xdr_u64(&mut args, length);
        xdr_u64(&mut args, min_length);
        args.extend_from_slice(stateid);
        xdr_u32(&mut args, max_count);
        self.ops.push(EncodedOp {
            opcode: OpNum::LayoutGet,
            args,
        });
        self
    }

    /// LAYOUTCOMMIT: commit data written through a layout.
    pub fn layoutcommit(
        mut self,
        offset: u64,
        length: u64,
        reclaim: bool,
        stateid: &[u8; 16],
        last_write_offset: Option<u64>,
        layout_type: u32,
    ) -> Self {
        let mut args = Vec::new();
        xdr_u64(&mut args, offset);
        xdr_u64(&mut args, length);
        xdr_bool(&mut args, reclaim);
        args.extend_from_slice(stateid);
        // newoffset4: bool + optional offset
        if let Some(off) = last_write_offset {
            xdr_bool(&mut args, true);
            xdr_u64(&mut args, off);
        } else {
            xdr_bool(&mut args, false);
        }
        // time_modify: SET_TO_SERVER_TIME = 0
        xdr_u32(&mut args, 0);
        xdr_u32(&mut args, layout_type);
        // layoutupdate4: opaque body (empty for files layout)
        xdr_u32(&mut args, 0);
        self.ops.push(EncodedOp {
            opcode: OpNum::LayoutCommit,
            args,
        });
        self
    }

    /// LAYOUTRETURN: return a layout to the metadata server.
    /// return_type: 1=LAYOUTRETURN4_FILE, 2=LAYOUTRETURN4_FSID, 3=LAYOUTRETURN4_ALL
    #[allow(clippy::too_many_arguments)]
    pub fn layoutreturn(
        mut self,
        reclaim: bool,
        layout_type: u32,
        iomode: u32,
        return_type: u32,
        offset: u64,
        length: u64,
        stateid: &[u8; 16],
    ) -> Self {
        let mut args = Vec::new();
        xdr_bool(&mut args, reclaim);
        xdr_u32(&mut args, layout_type);
        xdr_u32(&mut args, iomode);
        xdr_u32(&mut args, return_type);
        if return_type == 1 {
            // LAYOUTRETURN4_FILE
            xdr_u64(&mut args, offset);
            xdr_u64(&mut args, length);
            args.extend_from_slice(stateid);
            // lrf_body (opaque, empty for files layout)
            xdr_u32(&mut args, 0);
        }
        self.ops.push(EncodedOp {
            opcode: OpNum::LayoutReturn,
            args,
        });
        self
    }

    /// GETDEVICEINFO: get info about a data server device.
    pub fn getdeviceinfo(mut self, device_id: &[u8; 16], layout_type: u32, max_count: u32) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(device_id); // deviceid4 = 16 bytes
        xdr_u32(&mut args, layout_type);
        xdr_u32(&mut args, max_count);
        // notify_types bitmap (empty)
        xdr_u32(&mut args, 0);
        self.ops.push(EncodedOp {
            opcode: OpNum::GetDeviceInfo,
            args,
        });
        self
    }

    // ─── Lock operations ──────────────────────────────────────────────────

    /// LOCK: acquire a byte-range lock.
    /// lock_type: 1=READ_LT, 2=WRITE_LT, 3=READW_LT, 4=WRITEW_LT
    #[allow(clippy::too_many_arguments)]
    pub fn lock(
        mut self,
        lock_type: u32,
        reclaim: bool,
        offset: u64,
        length: u64,
        new_lock_owner: bool,
        open_stateid: &[u8; 16],
        lock_seqid: u32,
        open_seqid: u32,
        lock_owner: &[u8],
        client_id: u64,
    ) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, lock_type);
        xdr_bool(&mut args, reclaim);
        xdr_u64(&mut args, offset);
        xdr_u64(&mut args, length);
        if new_lock_owner {
            xdr_bool(&mut args, true); // locker = TRUE (new lock owner)
            // open_to_lock_owner4
            xdr_u32(&mut args, open_seqid);
            args.extend_from_slice(open_stateid); // open_stateid4
            xdr_u32(&mut args, lock_seqid);
            // lock_owner4
            xdr_u64(&mut args, client_id);
            xdr_var_bytes(&mut args, lock_owner);
        } else {
            xdr_bool(&mut args, false); // locker = FALSE (existing lock owner)
            // exist_lock_owner4
            args.extend_from_slice(open_stateid); // lock_stateid4
            xdr_u32(&mut args, lock_seqid);
        }
        self.ops.push(EncodedOp {
            opcode: OpNum::Lock,
            args,
        });
        self
    }

    /// LOCKT: test for a byte-range lock (does not acquire).
    pub fn lockt(
        mut self,
        lock_type: u32,
        offset: u64,
        length: u64,
        lock_owner: &[u8],
        client_id: u64,
    ) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, lock_type);
        xdr_u64(&mut args, offset);
        xdr_u64(&mut args, length);
        // lock_owner4
        xdr_u64(&mut args, client_id);
        xdr_var_bytes(&mut args, lock_owner);
        self.ops.push(EncodedOp {
            opcode: OpNum::Lockt,
            args,
        });
        self
    }

    /// LOCKU: release a byte-range lock.
    pub fn locku(
        mut self,
        lock_type: u32,
        seqid: u32,
        lock_stateid: &[u8; 16],
        offset: u64,
        length: u64,
    ) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, lock_type);
        xdr_u32(&mut args, seqid);
        args.extend_from_slice(lock_stateid);
        xdr_u64(&mut args, offset);
        xdr_u64(&mut args, length);
        self.ops.push(EncodedOp {
            opcode: OpNum::Locku,
            args,
        });
        self
    }

    /// TEST_STATEID: check if stateids are still valid.
    pub fn test_stateid(mut self, stateids: &[[u8; 16]]) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, stateids.len() as u32);
        for sid in stateids {
            args.extend_from_slice(sid);
        }
        self.ops.push(EncodedOp {
            opcode: OpNum::TestStateId,
            args,
        });
        self
    }

    /// DELEGRETURN: return a delegation to the server (RFC 5661 §18.14).
    pub fn delegreturn(mut self, stateid: &[u8; 16]) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid); // stateid4 = 16 bytes
        self.ops.push(EncodedOp {
            opcode: OpNum::DelegReturn,
            args,
        });
        self
    }

    /// FREE_STATEID: free a stateid that is no longer needed.
    pub fn free_stateid(mut self, stateid: &[u8; 16]) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        self.ops.push(EncodedOp {
            opcode: OpNum::FreeStateId,
            args,
        });
        self
    }

    // ─── Encode to wire format ───────────────────────────────────────────

    /// Encode the COMPOUND4args into a buffer, prefixed with the RPC header.
    pub fn encode_with_header(self, auth: &Auth, buf: &mut Vec<u8>) {
        // RPC header: program=100003, version=4, procedure=1 (COMPOUND)
        rpc_header(NFS4_PROGRAM, NFS4_VERSION, NFS4_COMPOUND_PROC, auth).encode(buf);
        self.encode_body(buf);
    }

    /// Encode just the COMPOUND4args body (without RPC header).
    fn encode_body(self, buf: &mut Vec<u8>) {
        // tag (utf8str_cs)
        xdr_string(buf, &self.tag);
        // minorversion
        xdr_u32(buf, NFS41_MINOR_VERSION);
        // argarray length
        xdr_u32(buf, self.ops.len() as u32);
        // argarray entries
        for op in self.ops {
            xdr_u32(buf, op.opcode as u32);
            buf.extend_from_slice(&op.args);
        }
    }
}

// ─── Argument helper types ───────────────────────────────────────────────────

/// Channel attributes for CREATE_SESSION.
pub(crate) struct ChannelAttrsArgs {
    pub headerpadsize: u32,
    pub maxrequestsize: u32,
    pub maxresponsesize: u32,
    pub maxresponsesize_cached: u32,
    pub maxoperations: u32,
    pub maxrequests: u32,
}

impl ChannelAttrsArgs {
    fn encode(&self, buf: &mut Vec<u8>) {
        xdr_u32(buf, self.headerpadsize);
        xdr_u32(buf, self.maxrequestsize);
        xdr_u32(buf, self.maxresponsesize);
        xdr_u32(buf, self.maxresponsesize_cached);
        xdr_u32(buf, self.maxoperations);
        xdr_u32(buf, self.maxrequests);
        // ca_rdma_ird<1>: empty array
        xdr_u32(buf, 0);
    }
}

/// OPEN4_SHARE_ACCESS_WANT_NO_DELEG (RFC 8881 §18.16)：告知服务器不要授予 delegation。
/// 本客户端面向迁移/批量传输负载（每个文件只写一遍），delegation 没有缓存收益，
/// 只会引入 CB_RECALL / NFS4ERR_DELAY 停顿，因此所有 OPEN 一律拒绝。
const OPEN4_SHARE_ACCESS_WANT_NO_DELEG: u32 = 0x0400;
const OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG: u32 = 0x0200;

/// OPEN arguments.
pub(crate) struct OpenArgs {
    pub seqid: u32,
    pub share_access: u32,
    pub share_deny: u32,
    pub client_id: u64,
    pub owner: Bytes,
    pub create: bool,
    pub create_attrs_mask: Vec<u32>,
    pub create_attrs_vals: Vec<u8>,
    pub claim_file: String,
    pub want_no_delegation: bool,
}

impl OpenArgs {
    fn encode(&self, buf: &mut Vec<u8>) {
        xdr_u32(buf, self.seqid);
        // 统一附加 WANT_NO_DELEG：合规服务器（knfsd/ONTAP）将不再授予 delegation
        let share_access = if self.want_no_delegation {
            self.share_access | OPEN4_SHARE_ACCESS_WANT_NO_DELEG
        } else {
            self.share_access | OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG
        };
        xdr_u32(buf, share_access);
        xdr_u32(buf, self.share_deny);
        // open_owner4
        xdr_u64(buf, self.client_id);
        xdr_var_bytes(buf, &self.owner);
        // openflag4
        if self.create {
            xdr_u32(buf, 1); // OPEN4_CREATE
            xdr_u32(buf, 0); // UNCHECKED4
            // fattr4: createattrs
            xdr_bitmap(buf, &self.create_attrs_mask);
            xdr_var_bytes(buf, &self.create_attrs_vals);
        } else {
            xdr_u32(buf, 0); // OPEN4_NOCREATE
        }
        // open_claim4: CLAIM_NULL = 0
        xdr_u32(buf, 0);
        xdr_string(buf, &self.claim_file);
    }
}

// ─── Compound response decoder ───────────────────────────────────────────────

/// Decoded COMPOUND4res: overall status + per-operation results.
pub(crate) struct CompoundResponse {
    #[cfg_attr(not(test), allow(dead_code))]
    pub tag: String,
    pub status: nfsstat4,
    pub results: Vec<OpResponse>,
    /// Local session generation that carried this response (zero before publication).
    pub session_generation: u64,
}

/// A single operation result: opcode + status + remaining bytes for the caller to decode.
pub(crate) struct OpResponse {
    pub opcode: u32,
    pub status: nfsstat4,
    /// Raw bytes of the operation-specific result (after status).
    /// For void results this is empty.
    pub data: Bytes,
}

impl CompoundResponse {
    /// Decode a COMPOUND4res from the RPC response payload.
    pub fn decode(mut buf: Bytes) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(NfsError::Xdr("COMPOUND response too short".to_string()));
        }
        // status (nfsstat4)
        let status_val = buf.get_u32();
        let status = decode_nfsstat4(status_val)?;
        // tag (utf8str_cs)
        let tag = decode_string(&mut buf)?;
        // resarray length
        if buf.remaining() < 4 {
            return Err(NfsError::Xdr(
                "COMPOUND response missing resarray length".to_string(),
            ));
        }
        let num_results = buf.get_u32() as usize;
        if num_results > MAX_COMPOUND_OPS {
            return Err(NfsError::Xdr(format!(
                "COMPOUND response has {} ops, max {}",
                num_results, MAX_COMPOUND_OPS
            )));
        }
        let mut results = Vec::with_capacity(num_results);
        for _ in 0..num_results {
            if buf.remaining() < 8 {
                return Err(NfsError::Xdr(
                    "COMPOUND response truncated at op header".to_string(),
                ));
            }
            let opcode = buf.get_u32();
            let op_status_val = buf.get_u32();
            let op_status = decode_nfsstat4(op_status_val)?;
            // The remaining data for this op depends on the opcode and status.
            // We pass the entire remaining buffer; the caller must know how much to consume.
            // For efficiency, we snapshot the position and let typed decoders advance it.
            results.push(OpResponse {
                opcode,
                status: op_status,
                data: buf.clone(), // shallow clone of Bytes (zero-copy ref count)
            });
            // Skip past the op-specific data by trying to decode it
            skip_op_result(opcode, op_status_val, &mut buf)?;
        }
        Ok(CompoundResponse {
            tag,
            status,
            results,
            session_generation: 0,
        })
    }

    /// Check overall COMPOUND status and return error if not OK.
    pub fn check_status(&self) -> Result<()> {
        if matches!(self.status, nfsstat4::NFS4_OK) {
            Ok(())
        } else {
            Err(NfsError::Nfs4(self.status))
        }
    }

    /// Get the result for the nth operation (0-based), checking its status.
    pub fn op_ok(&self, index: usize) -> Result<&OpResponse> {
        let op = self.results.get(index).ok_or_else(|| {
            NfsError::Xdr(format!("COMPOUND response missing op at index {}", index))
        })?;
        if !matches!(op.status, nfsstat4::NFS4_OK) {
            return Err(NfsError::Nfs4(op.status));
        }
        Ok(op)
    }
}

// ─── Decoding helpers ────────────────────────────────────────────────────────

fn decode_nfsstat4(val: u32) -> Result<nfsstat4> {
    let be = val.to_be_bytes();
    nfsstat4::try_from(&mut Bytes::copy_from_slice(&be))
        .map_err(|e| NfsError::Xdr(format!("invalid nfsstat4 {}: {}", val, e)))
}

fn decode_string(buf: &mut Bytes) -> Result<String> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("string length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("string data truncated".to_string()));
    }
    let s = String::from_utf8(buf.slice(..len).to_vec())
        .map_err(|e| NfsError::Xdr(format!("invalid UTF-8 in string: {}", e)))?;
    buf.advance(padded);
    Ok(s)
}

fn skip_var_bytes(buf: &mut Bytes) -> Result<()> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("opaque length truncated".to_string()));
    }
    let len = buf.get_u32() as usize;
    let padded = (len + 3) & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("opaque data truncated".to_string()));
    }
    buf.advance(padded);
    Ok(())
}

fn skip_bitmap(buf: &mut Bytes) -> Result<()> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("bitmap length truncated".to_string()));
    }
    let n = buf.get_u32() as usize;
    if n > MAX_BITMAP_WORDS {
        return Err(NfsError::Xdr(format!(
            "bitmap has {} words, max {}",
            n, MAX_BITMAP_WORDS
        )));
    }
    let bytes_needed = n * 4;
    if buf.remaining() < bytes_needed {
        return Err(NfsError::Xdr("bitmap data truncated".to_string()));
    }
    buf.advance(bytes_needed);
    Ok(())
}

fn skip_fattr4(buf: &mut Bytes) -> Result<()> {
    skip_bitmap(buf)?;
    skip_var_bytes(buf)
}

fn skip_change_info(buf: &mut Bytes) -> Result<()> {
    // bool atomic + changeid4 before + changeid4 after = 4 + 8 + 8 = 20
    if buf.remaining() < 20 {
        return Err(NfsError::Xdr("change_info truncated".to_string()));
    }
    buf.advance(20);
    Ok(())
}

fn skip_stateid4(buf: &mut Bytes) -> Result<()> {
    // seqid (4) + other (12) = 16
    if buf.remaining() < 16 {
        return Err(NfsError::Xdr("stateid4 truncated".to_string()));
    }
    buf.advance(16);
    Ok(())
}

/// Skip past the operation-specific result data in the COMPOUND response buffer.
/// This is needed so we can index into results by operation position.
fn skip_op_result(opcode: u32, status: u32, buf: &mut Bytes) -> Result<()> {
    // If status != NFS4_OK, most ops have no additional data (void default arm).
    // Exceptions: SETATTR always returns attrsset bitmap.
    if status != 0 {
        // SETATTR4res always has status + bitmap, even on error
        if opcode == OpNum::SetAttr as u32 {
            skip_bitmap(buf)?;
        }
        // LOCK4denied: offset(8) + length(8) + locktype(4) + lock_owner4(clientid(8) + owner<>)
        if opcode == OpNum::Lock as u32
            && status == 10012 /* NFS4ERR_DENIED */
            && buf.remaining() >= 28
        {
            buf.advance(28); // offset + length + locktype + clientid
            skip_var_bytes(buf)?; // owner
        }
        return Ok(());
    }

    match opcode {
        // Void results (no data on success)
        op if op == OpNum::PutRootFh as u32 => {}
        op if op == OpNum::PutFh as u32 => {}
        op if op == OpNum::SaveFh as u32 => {}
        op if op == OpNum::RestoreFh as u32 => {}
        op if op == OpNum::Lookup as u32 => {}
        op if op == OpNum::Lookupp as u32 => {}
        op if op == OpNum::ReclaimComplete as u32 => {}
        op if op == OpNum::DestroySession as u32 => {}
        op if op == OpNum::DestroyClientId as u32 => {}
        op if op == OpNum::DelegReturn as u32 => {}

        // SEQUENCE4resok: sessionid(16) + sequenceid(4) + slotid(4) + highest_slotid(4) + target_highest_slotid(4) + status_flags(4) = 36
        op if op == OpNum::Sequence as u32 => {
            if buf.remaining() < 36 {
                return Err(NfsError::Xdr("SEQUENCE result truncated".to_string()));
            }
            buf.advance(36);
        }

        // GETFH4resok: nfs_fh4 (variable)
        op if op == OpNum::GetFh as u32 => {
            skip_var_bytes(buf)?;
        }

        // GETATTR4resok: fattr4 (bitmap + opaque)
        op if op == OpNum::GetAttr as u32 => {
            skip_fattr4(buf)?;
        }

        // SETATTR4res: status(already consumed) + bitmap
        op if op == OpNum::SetAttr as u32 => {
            skip_bitmap(buf)?;
        }

        // ACCESS4resok: supported(4) + access(4) = 8
        op if op == OpNum::Access as u32 => {
            if buf.remaining() < 8 {
                return Err(NfsError::Xdr("ACCESS result truncated".to_string()));
            }
            buf.advance(8);
        }

        // READ4resok: eof(4) + data<> (variable)
        op if op == OpNum::Read as u32 => {
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("READ result truncated".to_string()));
            }
            buf.advance(4); // eof
            skip_var_bytes(buf)?; // data
        }

        // WRITE4resok: count(4) + committed(4) + writeverf(8) = 16
        op if op == OpNum::Write as u32 => {
            if buf.remaining() < 16 {
                return Err(NfsError::Xdr("WRITE result truncated".to_string()));
            }
            buf.advance(16);
        }

        // COMMIT4resok: writeverf(8)
        op if op == OpNum::Commit as u32 => {
            if buf.remaining() < 8 {
                return Err(NfsError::Xdr("COMMIT result truncated".to_string()));
            }
            buf.advance(8);
        }

        // READDIR4resok: cookieverf(8) + dirlist4 (entries + eof)
        op if op == OpNum::ReadDir as u32 => {
            if buf.remaining() < 8 {
                return Err(NfsError::Xdr("READDIR result truncated".to_string()));
            }
            buf.advance(8); // cookieverf
            // dirlist4: linked list of entry4 followed by eof bool
            skip_entry4_list(buf)?;
        }

        // READLINK4resok: link (utf8string)
        op if op == OpNum::ReadLink as u32 => {
            skip_var_bytes(buf)?;
        }

        // OPENATTR4res OK: void (changes current fh to the named attribute directory)
        op if op == OpNum::OpenAttr as u32 => {}

        // OPEN4resok: stateid(16) + change_info(20) + rflags(4) + bitmap + open_delegation4
        op if op == OpNum::Open as u32 => {
            skip_stateid4(buf)?;
            skip_change_info(buf)?;
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("OPEN rflags truncated".to_string()));
            }
            buf.advance(4); // rflags
            skip_bitmap(buf)?; // attrset
            skip_open_delegation(buf)?;
        }

        // CLOSE4res OK: stateid4
        op if op == OpNum::Close as u32 => {
            skip_stateid4(buf)?;
        }

        // LOCK4resok: lock_stateid (stateid4 = 16 bytes)
        op if op == OpNum::Lock as u32 => {
            skip_stateid4(buf)?;
        }

        // LOCKT4res OK: void (lock not held)
        op if op == OpNum::Lockt as u32 => {}

        // LOCKU4res OK: lock_stateid (stateid4 = 16 bytes)
        op if op == OpNum::Locku as u32 => {
            skip_stateid4(buf)?;
        }

        // TEST_STATEID4resok: array of status codes
        op if op == OpNum::TestStateId as u32 => {
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("TEST_STATEID result truncated".to_string()));
            }
            let n = buf.get_u32() as usize;
            let bytes_needed = n
                .checked_mul(4)
                .ok_or_else(|| NfsError::Xdr("TEST_STATEID count overflow".to_string()))?;
            if buf.remaining() < bytes_needed {
                return Err(NfsError::Xdr("TEST_STATEID statuses truncated".to_string()));
            }
            buf.advance(bytes_needed);
        }

        // FREE_STATEID4res OK: void
        op if op == OpNum::FreeStateId as u32 => {}

        // CREATE4resok: change_info + bitmap
        op if op == OpNum::Create as u32 => {
            skip_change_info(buf)?;
            skip_bitmap(buf)?;
        }

        // REMOVE4resok: change_info
        op if op == OpNum::Remove as u32 => {
            skip_change_info(buf)?;
        }

        // RENAME4resok: source_cinfo + target_cinfo
        op if op == OpNum::Rename as u32 => {
            skip_change_info(buf)?;
            skip_change_info(buf)?;
        }

        // LINK4resok: change_info
        op if op == OpNum::Link as u32 => {
            skip_change_info(buf)?;
        }

        // EXCHANGE_ID4resok: complex structure
        op if op == OpNum::ExchangeId as u32 => {
            skip_exchange_id_result(buf)?;
        }

        // CREATE_SESSION4resok: sessionid(16) + sequence(4) + flags(4) + 2x channel_attrs4
        op if op == OpNum::CreateSession as u32 => {
            skip_create_session_result(buf)?;
        }

        // LAYOUTGET4resok: complex — return_on_close(4) + stateid(16) + layout_content array
        op if op == OpNum::LayoutGet as u32 => {
            if buf.remaining() < 20 {
                return Err(NfsError::Xdr("LAYOUTGET result truncated".to_string()));
            }
            buf.advance(4); // return_on_close
            skip_stateid4(buf)?; // stateid
            // layout4<>: array of layout segments
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr(
                    "LAYOUTGET segments len truncated".to_string(),
                ));
            }
            let n = buf.get_u32() as usize;
            if n > MAX_LAYOUT_SEGMENTS {
                return Err(NfsError::Xdr(format!(
                    "LAYOUTGET has {} segments, max {}",
                    n, MAX_LAYOUT_SEGMENTS
                )));
            }
            for _ in 0..n {
                // layout4: offset(8) + length(8) + iomode(4) + layout_type(4) + layout_content(var)
                if buf.remaining() < 24 {
                    return Err(NfsError::Xdr("layout4 segment truncated".to_string()));
                }
                buf.advance(24);
                skip_var_bytes(buf)?; // layout_content opaque
            }
        }

        // LAYOUTCOMMIT4resok: newsize4 (bool + optional uint64)
        op if op == OpNum::LayoutCommit as u32 => {
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("LAYOUTCOMMIT result truncated".to_string()));
            }
            let has_newsize = buf.get_u32();
            if has_newsize != 0 && buf.remaining() >= 8 {
                buf.advance(8);
            }
        }

        // LAYOUTRETURN4res: depends on return_type
        op if op == OpNum::LayoutReturn as u32 => {
            // layoutreturn_stateid: bool + optional stateid
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("LAYOUTRETURN result truncated".to_string()));
            }
            let has_stateid = buf.get_u32();
            if has_stateid != 0 {
                skip_stateid4(buf)?;
            }
        }

        // GETDEVICEINFO4resok: device_addr + notification bitmap
        op if op == OpNum::GetDeviceInfo as u32 => {
            // device_addr4: layout_type(4) + da_addr_body(var)
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr(
                    "GETDEVICEINFO layout_type truncated".to_string(),
                ));
            }
            buf.advance(4);
            skip_var_bytes(buf)?; // da_addr_body
            skip_bitmap(buf)?; // notification bitmap
        }

        // BIND_CONN_TO_SESSION4resok: sessionid(16) + dir(4) + use_conn_in_rdma_mode(4) = 24 bytes
        op if op == OpNum::BindConnToSession as u32 => {
            if buf.remaining() < 24 {
                return Err(NfsError::Xdr(
                    "BIND_CONN_TO_SESSION result truncated".to_string(),
                ));
            }
            buf.advance(24);
        }

        // Unknown op: we can't skip it safely
        _ => {
            return Err(NfsError::Xdr(format!(
                "unknown op {} in COMPOUND response, cannot skip",
                opcode
            )));
        }
    }
    Ok(())
}

fn skip_entry4_list(buf: &mut Bytes) -> Result<()> {
    let mut count = 0usize;
    loop {
        if buf.remaining() < 4 {
            return Err(NfsError::Xdr("entry4 list truncated".to_string()));
        }
        let has_entry = buf.get_u32();
        if has_entry == 0 {
            // End of list, read eof bool
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("dirlist4 eof truncated".to_string()));
            }
            buf.advance(4); // eof
            return Ok(());
        }
        count += 1;
        if count > MAX_ENTRY4_PER_PAGE {
            return Err(NfsError::Xdr(format!(
                "entry4 list exceeds max {}",
                MAX_ENTRY4_PER_PAGE
            )));
        }
        // entry4: cookie(8) + name(var) + attrs(fattr4) + nextentry (pointer, handled by loop)
        if buf.remaining() < 8 {
            return Err(NfsError::Xdr("entry4 cookie truncated".to_string()));
        }
        buf.advance(8); // cookie
        skip_var_bytes(buf)?; // name
        skip_fattr4(buf)?; // attrs
    }
}

fn skip_open_delegation(buf: &mut Bytes) -> Result<()> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("open_delegation type truncated".to_string()));
    }
    let deleg_type = buf.get_u32();
    match deleg_type {
        0 => {} // OPEN_DELEGATE_NONE
        1 => {
            // OPEN_DELEGATE_READ: stateid(16) + recall(4) + nfsace4(variable)
            skip_stateid4(buf)?;
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("delegation recall truncated".to_string()));
            }
            buf.advance(4); // recall
            let _ace = crate::nfs4::acl::decode_nfsace4(buf)?;
        }
        2 => {
            // OPEN_DELEGATE_WRITE: stateid(16) + recall(4) + space_limit(12) + nfsace4
            skip_stateid4(buf)?;
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("delegation recall truncated".to_string()));
            }
            buf.advance(4); // recall
            skip_space_limit(buf)?;
            let _ace = crate::nfs4::acl::decode_nfsace4(buf)?;
        }
        3 => {
            // OPEN_DELEGATE_NONE_EXT: why_no_delegation4 union
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr("why_no_deleg truncated".to_string()));
            }
            let why = buf.get_u32();
            match why {
                1 | 2 => {
                    // WND4_CONTENTION or WND4_RESOURCE: bool
                    if buf.remaining() < 4 {
                        return Err(NfsError::Xdr("why_no_deleg bool truncated".to_string()));
                    }
                    buf.advance(4);
                }
                _ => {} // other cases are void
            }
        }
        _ => {
            return Err(NfsError::Xdr(format!(
                "unknown delegation type {}",
                deleg_type
            )));
        }
    }
    Ok(())
}

fn skip_space_limit(buf: &mut Bytes) -> Result<()> {
    // limit_by4 union
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("space_limit type truncated".to_string()));
    }
    let limit_by = buf.get_u32();
    match limit_by {
        1 => {
            // NFS_LIMIT_SIZE: filesize (uint64)
            if buf.remaining() < 8 {
                return Err(NfsError::Xdr("space_limit size truncated".to_string()));
            }
            buf.advance(8);
        }
        2 => {
            // NFS_LIMIT_BLOCKS: num_blocks(4) + bytes_per_block(4) = 8
            if buf.remaining() < 8 {
                return Err(NfsError::Xdr("space_limit blocks truncated".to_string()));
            }
            buf.advance(8);
        }
        _ => return Err(NfsError::Xdr(format!("unknown limit_by {}", limit_by))),
    }
    Ok(())
}

fn skip_exchange_id_result(buf: &mut Bytes) -> Result<()> {
    // clientid(8) + sequenceid(4) + flags(4) = 16
    if buf.remaining() < 16 {
        return Err(NfsError::Xdr("EXCHANGE_ID result truncated".to_string()));
    }
    buf.advance(16);
    // state_protect4_r
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("state_protect4_r type truncated".to_string()));
    }
    let sp = buf.get_u32();
    match sp {
        0 => {} // SP4_NONE
        1 => {
            // SP4_MACH_CRED: 2x bitmap
            skip_bitmap(buf)?;
            skip_bitmap(buf)?;
        }
        2 => {
            // SP4_SSV: ssv_prot_info4
            // spi_ops (2x bitmap) + spi_hash_alg(4) + spi_encr_alg(4) +
            // spi_ssv_len(4) + spi_window(4) + gss_handles<>
            skip_bitmap(buf)?; // spi_ops (enforce)
            skip_bitmap(buf)?; // spi_ops (allow)
            if buf.remaining() < 16 {
                return Err(NfsError::Xdr("SP4_SSV fields truncated".to_string()));
            }
            buf.advance(16); // hash_alg + encr_alg + ssv_len + window
            // gss_handles: opaque<>[]
            if buf.remaining() < 4 {
                return Err(NfsError::Xdr(
                    "SP4_SSV gss_handles len truncated".to_string(),
                ));
            }
            let nh = buf.get_u32() as usize;
            if nh > 64 {
                return Err(NfsError::Xdr(format!(
                    "SP4_SSV has {} gss_handles, max 64",
                    nh
                )));
            }
            for _ in 0..nh {
                skip_var_bytes(buf)?;
            }
        }
        _ => {
            return Err(NfsError::Xdr(format!(
                "unsupported state_protect type {}",
                sp
            )));
        }
    }
    // server_owner4: minor_id(8) + major_id(var)
    if buf.remaining() < 8 {
        return Err(NfsError::Xdr("server_owner minor_id truncated".to_string()));
    }
    buf.advance(8);
    skip_var_bytes(buf)?;
    // server_scope (opaque<>)
    skip_var_bytes(buf)?;
    // nfs_impl_id4<1> (RFC: at most 1 element)
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("impl_id array len truncated".to_string()));
    }
    let n = buf.get_u32() as usize;
    if n > 1 {
        return Err(NfsError::Xdr(format!(
            "nfs_impl_id4 has {} elements, max 1",
            n
        )));
    }
    for _ in 0..n {
        skip_var_bytes(buf)?; // domain
        skip_var_bytes(buf)?; // name
        // nfstime4: seconds(8) + nseconds(4) = 12
        if buf.remaining() < 12 {
            return Err(NfsError::Xdr("impl_id time truncated".to_string()));
        }
        buf.advance(12);
    }
    Ok(())
}

fn skip_create_session_result(buf: &mut Bytes) -> Result<()> {
    // sessionid(16) + sequence(4) + flags(4) = 24
    if buf.remaining() < 24 {
        return Err(NfsError::Xdr("CREATE_SESSION result truncated".to_string()));
    }
    buf.advance(24);
    // 2x channel_attrs4: each has 6 x u32 + ca_rdma_ird<1>
    for _ in 0..2 {
        if buf.remaining() < 24 {
            return Err(NfsError::Xdr("channel_attrs truncated".to_string()));
        }
        buf.advance(24); // 6 x u32
        // ca_rdma_ird<1>
        if buf.remaining() < 4 {
            return Err(NfsError::Xdr("ca_rdma_ird length truncated".to_string()));
        }
        let n = buf.get_u32() as usize;
        let bytes_needed = n
            .checked_mul(4)
            .ok_or_else(|| NfsError::Xdr("ca_rdma_ird count overflow".to_string()))?;
        if buf.remaining() < bytes_needed {
            return Err(NfsError::Xdr("ca_rdma_ird data truncated".to_string()));
        }
        buf.advance(bytes_needed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── XDR encoding helper tests ─────────────────────────────────────

    #[test]
    fn xdr_u32_encode() {
        let mut buf = Vec::new();
        xdr_u32(&mut buf, 0x12345678);
        assert_eq!(buf, vec![0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn xdr_u64_encode() {
        let mut buf = Vec::new();
        xdr_u64(&mut buf, 0x0102030405060708);
        assert_eq!(buf, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn xdr_bool_encode() {
        let mut buf = Vec::new();
        xdr_bool(&mut buf, true);
        xdr_bool(&mut buf, false);
        assert_eq!(buf, vec![0, 0, 0, 1, 0, 0, 0, 0]);
    }

    #[test]
    fn xdr_var_bytes_with_padding() {
        let mut buf = Vec::new();
        xdr_var_bytes(&mut buf, &[0xAA, 0xBB, 0xCC]); // 3 bytes → needs 1 byte padding
        assert_eq!(buf.len(), 4 + 4); // length(4) + data(3) + pad(1) = 8
        assert_eq!(&buf[0..4], &3u32.to_be_bytes());
        assert_eq!(&buf[4..7], &[0xAA, 0xBB, 0xCC]);
        assert_eq!(buf[7], 0); // padding
    }

    #[test]
    fn xdr_var_bytes_aligned() {
        let mut buf = Vec::new();
        xdr_var_bytes(&mut buf, &[1, 2, 3, 4]); // 4 bytes → no padding
        assert_eq!(buf.len(), 8); // length(4) + data(4)
    }

    #[test]
    fn xdr_var_bytes_empty() {
        let mut buf = Vec::new();
        xdr_var_bytes(&mut buf, &[]);
        assert_eq!(buf, vec![0, 0, 0, 0]); // just the length
    }

    #[test]
    fn xdr_string_encode() {
        let mut buf = Vec::new();
        xdr_string(&mut buf, "hi");
        assert_eq!(buf.len(), 8); // length(4) + "hi"(2) + pad(2)
        assert_eq!(&buf[0..4], &2u32.to_be_bytes());
        assert_eq!(&buf[4..6], b"hi");
    }

    #[test]
    fn xdr_bitmap_encode() {
        let mut buf = Vec::new();
        xdr_bitmap(&mut buf, &[0x1234, 0x5678]);
        assert_eq!(buf.len(), 12); // count(4) + word0(4) + word1(4)
        assert_eq!(&buf[0..4], &2u32.to_be_bytes());
        assert_eq!(&buf[4..8], &0x1234u32.to_be_bytes());
        assert_eq!(&buf[8..12], &0x5678u32.to_be_bytes());
    }

    // ─── CompoundBuilder tests ───────────────────────────────────────

    #[test]
    fn compound_builder_empty() {
        let builder = CompoundBuilder::new("empty");
        assert_eq!(builder.op_count(), 0);
        let mut buf = Vec::new();
        builder.encode_body(&mut buf);
        // tag: len(4) = 5, "empty"(5) + pad(3) = 8 → total tag = 12
        // then minorversion(4) + opcount(4) = 8
        // total header = 20
        assert_eq!(&buf[0..4], &5u32.to_be_bytes()); // tag length
        assert_eq!(&buf[4..9], b"empty");
        // op count = 0
        let opcount_offset = 12 + 4; // tag(12) + minorversion(4) = 16
        assert_eq!(
            &buf[opcount_offset..opcount_offset + 4],
            &0u32.to_be_bytes()
        );
    }

    #[test]
    fn compound_builder_putfh_encode() {
        let fh = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let builder = CompoundBuilder::new("t").putfh(&fh);
        assert_eq!(builder.op_count(), 1);
        let mut buf = Vec::new();
        builder.encode_body(&mut buf);
        // Find PUTFH opcode (22) after header
        // header: tag len(4) + "t\0\0\0"(4) + minor(4) + opcount(4) = 16
        assert_eq!(&buf[16..20], &22u32.to_be_bytes()); // PUTFH opcode
        // fh: length(4) + data(4) = 8
        assert_eq!(&buf[20..24], &4u32.to_be_bytes()); // fh length
        assert_eq!(&buf[24..28], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn compound_builder_lookup_encode() {
        let builder = CompoundBuilder::new("t").lookup("test.txt");
        let mut buf = Vec::new();
        builder.encode_body(&mut buf);
        // After header (16 bytes): opcode(4) + string_len(4) + "test.txt"(8) = 16
        assert_eq!(&buf[16..20], &15u32.to_be_bytes()); // LOOKUP opcode = 15
        assert_eq!(&buf[20..24], &8u32.to_be_bytes()); // name length
        assert_eq!(&buf[24..32], b"test.txt");
    }

    #[test]
    fn compound_builder_read_encode() {
        let stateid = [0u8; 16];
        let builder = CompoundBuilder::new("t").read(&stateid, 1024, 4096);
        let mut buf = Vec::new();
        builder.encode_body(&mut buf);
        assert_eq!(&buf[16..20], &25u32.to_be_bytes()); // READ opcode = 25
        // stateid (16) + offset (8) + count (4) = 28
        assert_eq!(&buf[36..44], &1024u64.to_be_bytes()); // offset
        assert_eq!(&buf[44..48], &4096u32.to_be_bytes()); // count
    }

    #[test]
    fn compound_builder_chained_ops() {
        let builder = CompoundBuilder::new("c")
            .putrootfh()
            .lookup("dir1")
            .lookup("dir2")
            .lookup("file")
            .getfh()
            .getattr(&[0, 0]);
        assert_eq!(builder.op_count(), 6);
    }

    #[test]
    fn compound_builder_write_encode() {
        let stateid = [0xABu8; 16];
        let data = [1u8, 2, 3, 4, 5];
        let builder = CompoundBuilder::new("t").write(&stateid, 0, 2, &data);
        let mut buf = Vec::new();
        builder.encode_body(&mut buf);
        assert_eq!(&buf[16..20], &38u32.to_be_bytes()); // WRITE opcode = 38
    }

    #[test]
    fn open_delegation_preferences_match_linux_wire_values() {
        let encode_access = |want_no_delegation| {
            let args = OpenArgs {
                seqid: 0,
                share_access: 3,
                share_deny: 0,
                client_id: 1,
                owner: Bytes::new(),
                create: false,
                create_attrs_mask: Vec::new(),
                create_attrs_vals: Vec::new(),
                claim_file: "f".to_string(),
                want_no_delegation,
            };
            let mut encoded = Vec::new();
            args.encode(&mut encoded);
            u32::from_be_bytes(encoded[4..8].try_into().unwrap())
        };
        assert_eq!(encode_access(true), 0x0403);
        assert_eq!(encode_access(false), 0x0203);
    }

    #[test]
    fn compound_builder_remove_encode() {
        let builder = CompoundBuilder::new("t").remove("oldfile");
        let mut buf = Vec::new();
        builder.encode_body(&mut buf);
        assert_eq!(&buf[16..20], &28u32.to_be_bytes()); // REMOVE opcode = 28
    }

    #[test]
    fn compound_builder_rename_encode() {
        let builder = CompoundBuilder::new("t").rename("old", "new");
        assert_eq!(builder.op_count(), 1);
    }

    #[test]
    fn compound_builder_create_symlink_encode() {
        let builder = CompoundBuilder::new("t").create_symlink("link", "/target/path", &[], &[]);
        assert_eq!(builder.op_count(), 1);
        let mut buf = Vec::new();
        builder.encode_body(&mut buf);
        assert_eq!(&buf[16..20], &6u32.to_be_bytes()); // CREATE opcode = 6
        // NF4LNK = 5
        assert_eq!(&buf[20..24], &5u32.to_be_bytes());
    }

    // ─── CompoundResponse decode tests ───────────────────────────────

    #[test]
    fn compound_response_decode_simple_ok() {
        // Build a minimal COMPOUND4res: status=0 + tag="" + 1 op (PUTROOTFH OK)
        let mut buf = Vec::new();
        xdr_u32(&mut buf, 0); // status NFS4_OK
        xdr_string(&mut buf, ""); // tag
        xdr_u32(&mut buf, 1); // 1 result op
        xdr_u32(&mut buf, OpNum::PutRootFh as u32); // opcode
        xdr_u32(&mut buf, 0); // NFS4_OK (void result)

        let resp = CompoundResponse::decode(Bytes::from(buf)).unwrap();
        assert!(matches!(resp.status, nfsstat4::NFS4_OK));
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].opcode, OpNum::PutRootFh as u32);
    }

    #[test]
    fn compound_response_decode_with_sequence() {
        let mut buf = Vec::new();
        xdr_u32(&mut buf, 0); // NFS4_OK
        xdr_string(&mut buf, "test");
        xdr_u32(&mut buf, 2); // 2 ops

        // SEQUENCE result
        xdr_u32(&mut buf, OpNum::Sequence as u32);
        xdr_u32(&mut buf, 0); // NFS4_OK
        buf.extend_from_slice(&[0u8; 16]); // session_id
        xdr_u32(&mut buf, 1); // sequence_id
        xdr_u32(&mut buf, 0); // slot_id
        xdr_u32(&mut buf, 3); // highest_slot_id
        xdr_u32(&mut buf, 3); // target_highest_slot_id
        xdr_u32(&mut buf, 0); // status_flags

        // PUTROOTFH result
        xdr_u32(&mut buf, OpNum::PutRootFh as u32);
        xdr_u32(&mut buf, 0);

        let resp = CompoundResponse::decode(Bytes::from(buf)).unwrap();
        assert_eq!(resp.tag, "test");
        assert_eq!(resp.results.len(), 2);
        resp.op_ok(0).unwrap(); // SEQUENCE
        resp.op_ok(1).unwrap(); // PUTROOTFH
    }

    #[test]
    fn compound_response_decode_error_status() {
        let mut buf = Vec::new();
        xdr_u32(&mut buf, 2); // NFS4ERR_NOENT
        xdr_string(&mut buf, "");
        xdr_u32(&mut buf, 0); // 0 ops

        let resp = CompoundResponse::decode(Bytes::from(buf)).unwrap();
        assert!(matches!(resp.status, nfsstat4::NFS4ERR_NOENT));
        assert!(resp.check_status().is_err());
    }

    #[test]
    fn compound_response_op_ok_out_of_bounds() {
        let mut buf = Vec::new();
        xdr_u32(&mut buf, 0);
        xdr_string(&mut buf, "");
        xdr_u32(&mut buf, 0); // 0 ops

        let resp = CompoundResponse::decode(Bytes::from(buf)).unwrap();
        assert!(resp.op_ok(0).is_err()); // no ops
    }

    #[test]
    fn compound_response_decode_getfh_ok() {
        let mut buf = Vec::new();
        xdr_u32(&mut buf, 0); // NFS4_OK
        xdr_string(&mut buf, "");
        xdr_u32(&mut buf, 1); // 1 op
        // GETFH result
        xdr_u32(&mut buf, OpNum::GetFh as u32);
        xdr_u32(&mut buf, 0); // NFS4_OK
        xdr_var_bytes(&mut buf, &[0xDE, 0xAD]); // fh

        let resp = CompoundResponse::decode(Bytes::from(buf)).unwrap();
        let op = resp.op_ok(0).unwrap();
        let mut data = op.data.clone();
        // Should have the fh opaque
        let len = data.get_u32() as usize;
        assert_eq!(len, 2);
    }

    #[test]
    fn compound_response_decode_truncated() {
        let buf = vec![0u8; 2]; // too short
        assert!(CompoundResponse::decode(Bytes::from(buf)).is_err());
    }

    #[test]
    fn compound_builder_basic_encode() {
        let builder = CompoundBuilder::new("test")
            .putrootfh()
            .lookup("mydir")
            .getfh()
            .getattr(&[0x0018_00bb, 0x0030_0b73]);

        assert_eq!(builder.op_count(), 4);

        let mut buf = Vec::new();
        builder.encode_body(&mut buf);

        // Verify tag
        assert_eq!(&buf[0..4], &4u32.to_be_bytes()); // tag length = 4
        assert_eq!(&buf[4..8], b"test");
        // Verify minor version = 1
        assert_eq!(&buf[8..12], &1u32.to_be_bytes());
        // Verify op count = 4
        assert_eq!(&buf[12..16], &4u32.to_be_bytes());
        // First op = PUTROOTFH (24)
        assert_eq!(&buf[16..20], &24u32.to_be_bytes());
    }

    #[test]
    fn compound_builder_sequence_encode() {
        let session_id = [1u8; 16];
        let builder = CompoundBuilder::new("seq").sequence(&session_id, 42, 0, 0);

        let mut buf = Vec::new();
        builder.encode_body(&mut buf);

        // Skip tag (4+4=8) + minor_version (4) + op_count (4) = 20 bytes header
        // Then SEQUENCE opcode (4) = 24
        let op_start = 16; // after "seq\0" tag + minorversion + opcount
        assert_eq!(
            &buf[op_start..op_start + 4],
            &(OpNum::Sequence as u32).to_be_bytes()
        );
        // session_id starts at op_start + 4
        assert_eq!(&buf[op_start + 4..op_start + 20], &session_id);
        // sequence_id = 42 at op_start + 20
        assert_eq!(&buf[op_start + 20..op_start + 24], &42u32.to_be_bytes());
    }

    #[test]
    fn negotiated_operation_limit_is_enforced_at_boundaries() {
        let session_id = [0u8; 16];
        let one = CompoundBuilder::new("one").sequence(&session_id, 1, 0, 0);
        assert!(one.enforce_max_operations(0).is_err());
        assert!(one.enforce_max_operations(1).is_ok());

        let two = CompoundBuilder::new("two")
            .sequence(&session_id, 1, 0, 0)
            .putrootfh();
        assert!(two.enforce_max_operations(1).is_err());
        assert!(two.enforce_max_operations(2).is_ok());
        assert!(two.enforce_max_operations(3).is_ok());
    }

    #[test]
    fn operation_classes_cover_supported_opnums() {
        let read_only = [
            OpNum::Access,
            OpNum::GetAttr,
            OpNum::GetFh,
            OpNum::GetDeviceInfo,
            OpNum::Lockt,
            OpNum::Lookup,
            OpNum::Lookupp,
            OpNum::PutFh,
            OpNum::PutRootFh,
            OpNum::Read,
            OpNum::ReadDir,
            OpNum::ReadLink,
            OpNum::RestoreFh,
            OpNum::SaveFh,
            OpNum::TestStateId,
        ];
        let control = [
            OpNum::BindConnToSession,
            OpNum::CreateSession,
            OpNum::DestroyClientId,
            OpNum::DestroySession,
            OpNum::ExchangeId,
            OpNum::ReclaimComplete,
            OpNum::Sequence,
        ];
        let replay_sensitive = [
            OpNum::Close,
            OpNum::Commit,
            OpNum::Create,
            OpNum::DelegReturn,
            OpNum::FreeStateId,
            OpNum::LayoutCommit,
            OpNum::LayoutGet,
            OpNum::LayoutReturn,
            OpNum::Link,
            OpNum::Lock,
            OpNum::Locku,
            OpNum::Open,
            OpNum::OpenAttr,
            OpNum::Remove,
            OpNum::Rename,
            OpNum::SetAttr,
            OpNum::Write,
        ];
        assert!(
            read_only
                .iter()
                .all(|op| op.class() == OperationClass::ReadOnly)
        );
        assert!(
            control
                .iter()
                .all(|op| op.class() == OperationClass::SessionControl)
        );
        assert!(
            replay_sensitive
                .iter()
                .all(|op| op.class() == OperationClass::ReplaySensitive)
        );
        assert_eq!(read_only.len() + control.len() + replay_sensitive.len(), 39);
    }

    #[test]
    fn cache_policy_is_false_for_read_only_compound() {
        let session_id = [1u8; 16];
        let builder = CompoundBuilder::new("read")
            .sequence(&session_id, 1, 0, 0)
            .putrootfh()
            .lookup("file")
            .getattr(&[1])
            .apply_sequence_cache_policy(0)
            .unwrap();
        assert_eq!(
            &builder.ops[0].args[SEQUENCE_CACHE_THIS_OFFSET..SEQUENCE_CACHE_THIS_OFFSET + 4],
            &0u32.to_be_bytes()
        );
    }

    #[test]
    fn cache_policy_is_true_for_mixed_modifying_compound() {
        let session_id = [1u8; 16];
        let stateid = [0u8; 16];
        let builder = CompoundBuilder::new("write")
            .sequence(&session_id, 1, 0, 0)
            .putfh(b"fh")
            .write_header(&stateid, 0, 2, 1_048_576)
            .getattr(&[1])
            .apply_sequence_cache_policy(2128)
            .unwrap();
        assert_eq!(
            &builder.ops[0].args[SEQUENCE_CACHE_THIS_OFFSET..SEQUENCE_CACHE_THIS_OFFSET + 4],
            &1u32.to_be_bytes()
        );
        assert_eq!(builder.ops[2].args.len(), 32);
        assert_eq!(&builder.ops[2].args[28..32], &1_048_576u32.to_be_bytes());
    }

    #[test]
    fn cache_policy_checks_negotiated_capacity_boundary_before_encode() {
        let session_id = [1u8; 16];
        let builder_at_limit = CompoundBuilder::new("remove")
            .sequence(&session_id, 1, 0, 0)
            .putfh(b"fh")
            .remove("file");
        let required = builder_at_limit.minimum_cached_response_size();
        assert!(
            builder_at_limit
                .apply_sequence_cache_policy(required)
                .is_ok()
        );

        let builder_below_limit = CompoundBuilder::new("remove")
            .sequence(&session_id, 1, 0, 0)
            .putfh(b"fh")
            .remove("file");
        let error = match builder_below_limit.apply_sequence_cache_policy(required - 1) {
            Ok(_) => panic!("modifying compound requires cached reply capacity"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains(&format!("requires at least {required}"))
        );
    }

    #[test]
    fn cache_policy_requires_sequence_first() {
        let result = CompoundBuilder::new("invalid")
            .putrootfh()
            .apply_sequence_cache_policy(2128);
        let error = match result {
            Ok(_) => panic!("missing SEQUENCE must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not a valid SEQUENCE"));
    }

    #[test]
    fn required_generation_is_preserved_by_builder_chaining() {
        let builder = CompoundBuilder::new("generation-fence")
            .require_generation(7)
            .putrootfh()
            .getattr(&[1]);
        assert_eq!(builder.required_generation(), Some(7));
    }

    #[test]
    fn anonymous_stateid_generation_is_not_fenced() {
        let builder = CompoundBuilder::new("anonymous")
            .require_generation(0)
            .putrootfh();
        assert_eq!(builder.required_generation(), None);
    }
}
