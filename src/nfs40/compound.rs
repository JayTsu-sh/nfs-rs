use bytes::{Buf, Bytes};

use crate::nfs4::compound::{
    CompoundBuilder as CommonCompoundBuilder, OP_GETFH, OP_PUTFH, check_status, response_ops,
    take_opaque, take_u32, xdr_opaque, xdr_u32,
};
use crate::rpc::auth::Auth;
use crate::{NfsError, Result};

const OP_SETCLIENTID: u32 = 35;
const OP_SETCLIENTID_CONFIRM: u32 = 36;
const OP_SETATTR: u32 = 34;
const OP_CLOSE: u32 = 4;
const OP_CREATE: u32 = 6;
const OP_ACCESS: u32 = 3;
const OP_COMMIT: u32 = 5;
const OP_GETATTR: u32 = 9;
const OP_LINK: u32 = 11;
const OP_LOCK: u32 = 12;
const OP_LOCKT: u32 = 13;
const OP_LOCKU: u32 = 14;
const OP_RELEASE_LOCKOWNER: u32 = 39;
const OP_OPEN: u32 = 18;
const OP_OPEN_CONFIRM: u32 = 20;
const OP_READ: u32 = 25;
const OP_READDIR: u32 = 26;
const OP_REMOVE: u32 = 28;
const OP_RENAME: u32 = 29;
const OP_RENEW: u32 = 30;
const OP_SAVEFH: u32 = 32;
const OP_READLINK: u32 = 27;
const OP_WRITE: u32 = 38;
const NFS4ERR_DENIED: u32 = 10010;
const MAX_LOCK_OWNER_LEN: usize = 1024;
pub(crate) const OPEN4_RESULT_CONFIRM: u32 = 0x0000_0002;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OpenResult {
    pub stateid: [u8; 16],
    pub confirm_required: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct OpenArgs<'a> {
    pub seqid: u32,
    pub share_access: u32,
    pub client_id: u64,
    pub owner: &'a [u8],
    pub filename: &'a str,
    pub create: bool,
}

pub(crate) struct NewLockArgs<'a> {
    pub lock_type: u32,
    pub reclaim: bool,
    pub offset: u64,
    pub length: u64,
    pub open_seqid: u32,
    pub open_stateid: &'a [u8; 16],
    pub lock_seqid: u32,
    pub client_id: u64,
    pub owner: &'a [u8],
}

pub(crate) struct OpenReclaimArgs<'a> {
    pub seqid: u32,
    pub share_access: u32,
    pub client_id: u64,
    pub owner: &'a [u8],
}

pub(crate) struct CompoundBuilder {
    inner: CommonCompoundBuilder,
}

pub(crate) struct SetClientIdArgs<'a> {
    pub verifier: [u8; 8],
    pub owner: &'a [u8],
    pub callback: CallbackAddress<'a>,
}

pub(crate) struct CallbackAddress<'a> {
    program: u32,
    netid: &'a str,
    addr: &'a str,
    ident: u32,
}

impl CallbackAddress<'static> {
    pub(crate) const DISABLED: Self = Self {
        program: 0,
        netid: "",
        addr: "",
        ident: 0,
    };
}

impl<'a> CallbackAddress<'a> {
    pub(crate) fn tcp(addr: &'a str, ident: u32) -> Self {
        Self {
            program: super::callback::CB_PROGRAM,
            netid: "tcp",
            addr,
            ident,
        }
    }
}

impl CompoundBuilder {
    pub(crate) fn new(tag: &str) -> Self {
        Self {
            inner: CommonCompoundBuilder::new(tag, 0),
        }
    }

    pub(crate) fn setclientid(mut self, identity: SetClientIdArgs<'_>) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(&identity.verifier);
        xdr_opaque(&mut args, identity.owner);
        xdr_u32(&mut args, identity.callback.program);
        xdr_opaque(&mut args, identity.callback.netid.as_bytes());
        xdr_opaque(&mut args, identity.callback.addr.as_bytes());
        xdr_u32(&mut args, identity.callback.ident);
        self.inner = self.inner.operation(OP_SETCLIENTID, args);
        self
    }

    pub(crate) fn setclientid_confirm(mut self, client_id: u64, verifier: [u8; 8]) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(&client_id.to_be_bytes());
        args.extend_from_slice(&verifier);
        self.inner = self.inner.operation(OP_SETCLIENTID_CONFIRM, args);
        self
    }

    pub(crate) fn putrootfh(mut self) -> Self {
        self.inner = self.inner.putrootfh();
        self
    }

    pub(crate) fn putfh(mut self, fh: &[u8]) -> Self {
        self.inner = self.inner.putfh(fh);
        self
    }

    pub(crate) fn open(mut self, open: OpenArgs<'_>) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, open.seqid);
        xdr_u32(&mut args, open.share_access);
        xdr_u32(&mut args, 0); // OPEN4_SHARE_DENY_NONE
        args.extend_from_slice(&open.client_id.to_be_bytes());
        xdr_opaque(&mut args, open.owner);
        if open.create {
            xdr_u32(&mut args, 1); // OPEN4_CREATE
            xdr_u32(&mut args, 0); // UNCHECKED4
            xdr_u32(&mut args, 0); // empty createattrs bitmap
            xdr_opaque(&mut args, &[]);
        } else {
            xdr_u32(&mut args, 0); // OPEN4_NOCREATE
        }
        xdr_u32(&mut args, 0); // CLAIM_NULL
        xdr_opaque(&mut args, open.filename.as_bytes());
        self.inner = self.inner.operation(OP_OPEN, args);
        self
    }

    pub(crate) fn open_reclaim(mut self, open: OpenReclaimArgs<'_>) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, open.seqid);
        xdr_u32(&mut args, open.share_access);
        xdr_u32(&mut args, 0); // OPEN4_SHARE_DENY_NONE
        args.extend_from_slice(&open.client_id.to_be_bytes());
        xdr_opaque(&mut args, open.owner);
        xdr_u32(&mut args, 0); // OPEN4_NOCREATE
        xdr_u32(&mut args, 1); // CLAIM_PREVIOUS
        xdr_u32(&mut args, 0); // OPEN_DELEGATE_NONE
        self.inner = self.inner.operation(OP_OPEN, args);
        self
    }

    pub(crate) fn open_confirm(mut self, stateid: &[u8; 16], seqid: u32) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        xdr_u32(&mut args, seqid);
        self.inner = self.inner.operation(OP_OPEN_CONFIRM, args);
        self
    }

    pub(crate) fn read(mut self, stateid: &[u8; 16], offset: u64, count: u32) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        args.extend_from_slice(&offset.to_be_bytes());
        xdr_u32(&mut args, count);
        self.inner = self.inner.operation(OP_READ, args);
        self
    }

    pub(crate) fn lock_new(mut self, lock: NewLockArgs<'_>) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, lock.lock_type);
        xdr_u32(&mut args, u32::from(lock.reclaim));
        args.extend_from_slice(&lock.offset.to_be_bytes());
        args.extend_from_slice(&lock.length.to_be_bytes());
        xdr_u32(&mut args, 1);
        xdr_u32(&mut args, lock.open_seqid);
        args.extend_from_slice(lock.open_stateid);
        xdr_u32(&mut args, lock.lock_seqid);
        args.extend_from_slice(&lock.client_id.to_be_bytes());
        xdr_opaque(&mut args, lock.owner);
        self.inner = self.inner.operation(OP_LOCK, args);
        self
    }

    pub(crate) fn lockt(
        mut self,
        lock_type: u32,
        offset: u64,
        length: u64,
        client_id: u64,
        owner: &[u8],
    ) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, lock_type);
        args.extend_from_slice(&offset.to_be_bytes());
        args.extend_from_slice(&length.to_be_bytes());
        args.extend_from_slice(&client_id.to_be_bytes());
        xdr_opaque(&mut args, owner);
        self.inner = self.inner.operation(OP_LOCKT, args);
        self
    }

    pub(crate) fn locku(
        mut self,
        lock_type: u32,
        seqid: u32,
        stateid: &[u8; 16],
        offset: u64,
        length: u64,
    ) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, lock_type);
        xdr_u32(&mut args, seqid);
        args.extend_from_slice(stateid);
        args.extend_from_slice(&offset.to_be_bytes());
        args.extend_from_slice(&length.to_be_bytes());
        self.inner = self.inner.operation(OP_LOCKU, args);
        self
    }

    pub(crate) fn release_lockowner(mut self, client_id: u64, owner: &[u8]) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(&client_id.to_be_bytes());
        xdr_opaque(&mut args, owner);
        self.inner = self.inner.operation(OP_RELEASE_LOCKOWNER, args);
        self
    }

    pub(crate) fn renew(mut self, client_id: u64) -> Self {
        self.inner = self
            .inner
            .operation(OP_RENEW, client_id.to_be_bytes().to_vec());
        self
    }

    pub(crate) fn write_header(
        mut self,
        stateid: &[u8; 16],
        offset: u64,
        stable: u32,
        data_len: u32,
    ) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        args.extend_from_slice(&offset.to_be_bytes());
        xdr_u32(&mut args, stable);
        xdr_u32(&mut args, data_len);
        self.inner = self.inner.operation(OP_WRITE, args);
        self
    }

    pub(crate) fn commit(mut self, offset: u64, count: u32) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(&offset.to_be_bytes());
        xdr_u32(&mut args, count);
        self.inner = self.inner.operation(OP_COMMIT, args);
        self
    }

    pub(crate) fn close(mut self, seqid: u32, stateid: &[u8; 16]) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, seqid);
        args.extend_from_slice(stateid);
        self.inner = self.inner.operation(OP_CLOSE, args);
        self
    }

    pub(crate) fn lookup(mut self, name: &str) -> Self {
        self.inner = self.inner.lookup(name);
        self
    }

    pub(crate) fn getfh(mut self) -> Self {
        self.inner = self.inner.getfh();
        self
    }

    pub(crate) fn getattr(mut self, bitmap: &[u32]) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, bitmap.len() as u32);
        for word in bitmap {
            xdr_u32(&mut args, *word);
        }
        self.inner = self.inner.operation(OP_GETATTR, args);
        self
    }

    pub(crate) fn access(mut self, requested: u32) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, requested);
        self.inner = self.inner.operation(OP_ACCESS, args);
        self
    }

    pub(crate) fn setattr(mut self, stateid: &[u8; 16], bitmap: &[u32], values: &[u8]) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(stateid);
        xdr_u32(&mut args, bitmap.len() as u32);
        for word in bitmap {
            xdr_u32(&mut args, *word);
        }
        xdr_opaque(&mut args, values);
        self.inner = self.inner.operation(OP_SETATTR, args);
        self
    }

    pub(crate) fn create_directory(mut self, name: &str) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, 2); // NF4DIR
        xdr_opaque(&mut args, name.as_bytes());
        xdr_u32(&mut args, 0); // empty createattrs bitmap
        xdr_opaque(&mut args, &[]);
        self.inner = self.inner.operation(OP_CREATE, args);
        self
    }

    pub(crate) fn create_symlink(mut self, name: &str, target: &str) -> Self {
        let mut args = Vec::new();
        xdr_u32(&mut args, 5); // NF4LNK
        xdr_opaque(&mut args, target.as_bytes());
        xdr_opaque(&mut args, name.as_bytes());
        xdr_u32(&mut args, 0);
        xdr_opaque(&mut args, &[]);
        self.inner = self.inner.operation(OP_CREATE, args);
        self
    }

    pub(crate) fn savefh(mut self) -> Self {
        self.inner = self.inner.operation(OP_SAVEFH, Vec::new());
        self
    }

    pub(crate) fn rename(mut self, from: &str, to: &str) -> Self {
        let mut args = Vec::new();
        xdr_opaque(&mut args, from.as_bytes());
        xdr_opaque(&mut args, to.as_bytes());
        self.inner = self.inner.operation(OP_RENAME, args);
        self
    }

    pub(crate) fn link(mut self, name: &str) -> Self {
        let mut args = Vec::new();
        xdr_opaque(&mut args, name.as_bytes());
        self.inner = self.inner.operation(OP_LINK, args);
        self
    }

    pub(crate) fn readlink(mut self) -> Self {
        self.inner = self.inner.operation(OP_READLINK, Vec::new());
        self
    }

    pub(crate) fn readdir(
        mut self,
        cookie: u64,
        verifier: &[u8; 8],
        dircount: u32,
        maxcount: u32,
        bitmap: &[u32],
    ) -> Self {
        let mut args = Vec::new();
        args.extend_from_slice(&cookie.to_be_bytes());
        args.extend_from_slice(verifier);
        xdr_u32(&mut args, dircount);
        xdr_u32(&mut args, maxcount);
        xdr_u32(&mut args, bitmap.len() as u32);
        for word in bitmap {
            xdr_u32(&mut args, *word);
        }
        self.inner = self.inner.operation(OP_READDIR, args);
        self
    }

    pub(crate) fn remove(mut self, name: &str) -> Self {
        let mut args = Vec::new();
        xdr_opaque(&mut args, name.as_bytes());
        self.inner = self.inner.operation(OP_REMOVE, args);
        self
    }

    #[cfg(test)]
    pub(crate) fn encode_body(self) -> Vec<u8> {
        self.inner.encode_body()
    }

    pub(crate) fn encode_with_header(self, auth: &Auth) -> Vec<u8> {
        self.inner.encode_with_header(auth)
    }
}

pub(crate) fn decode_setclientid_response(buf: Bytes) -> Result<(u64, [u8; 8])> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 1 || take_u32(&mut buf, "SETCLIENTID opcode")? != OP_SETCLIENTID {
        return Err(NfsError::Xdr(
            "SETCLIENTID response operation mismatch".to_string(),
        ));
    }
    check_status(take_u32(&mut buf, "SETCLIENTID status")?)?;
    if buf.remaining() < 16 {
        return Err(NfsError::Xdr("SETCLIENTID result truncated".to_string()));
    }
    let client_id = buf.get_u64();
    let mut verifier = [0; 8];
    buf.copy_to_slice(&mut verifier);
    Ok((client_id, verifier))
}

pub(crate) fn decode_confirm_response(buf: Bytes) -> Result<()> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 1 || take_u32(&mut buf, "SETCLIENTID_CONFIRM opcode")? != OP_SETCLIENTID_CONFIRM {
        return Err(NfsError::Xdr(
            "SETCLIENTID_CONFIRM response operation mismatch".to_string(),
        ));
    }
    check_status(take_u32(&mut buf, "SETCLIENTID_CONFIRM status")?)
}

fn expect_op(buf: &mut Bytes, expected: u32, name: &str) -> Result<()> {
    let actual = take_u32(buf, &format!("{name} opcode"))?;
    if actual != expected {
        return Err(NfsError::Xdr(format!(
            "{name} opcode {actual} does not match {expected}"
        )));
    }
    check_status(take_u32(buf, &format!("{name} status"))?)
}

fn take_stateid(buf: &mut Bytes, what: &str) -> Result<[u8; 16]> {
    if buf.remaining() < 16 {
        return Err(NfsError::Xdr(format!("{what} stateid truncated")));
    }
    let mut stateid = [0; 16];
    buf.copy_to_slice(&mut stateid);
    Ok(stateid)
}

fn skip_bitmap(buf: &mut Bytes, what: &str) -> Result<()> {
    let words = take_u32(buf, &format!("{what} bitmap length"))? as usize;
    let bytes = words
        .checked_mul(4)
        .ok_or_else(|| NfsError::Xdr(format!("{what} bitmap overflow")))?;
    if buf.remaining() < bytes {
        return Err(NfsError::Xdr(format!("{what} bitmap truncated")));
    }
    buf.advance(bytes);
    Ok(())
}

fn skip_ace(buf: &mut Bytes) -> Result<()> {
    for what in ["ACE type", "ACE flags", "ACE mask"] {
        let _ = take_u32(buf, what)?;
    }
    let _ = take_opaque(buf, "ACE who")?;
    Ok(())
}

fn skip_delegation(buf: &mut Bytes) -> Result<()> {
    match take_u32(buf, "delegation type")? {
        0 => Ok(()),
        1 => {
            let _ = take_stateid(buf, "read delegation")?;
            let _ = take_u32(buf, "recall")?;
            skip_ace(buf)
        }
        2 => {
            let _ = take_stateid(buf, "write delegation")?;
            let _ = take_u32(buf, "recall")?;
            match take_u32(buf, "space limit")? {
                1 => {
                    if buf.remaining() < 8 {
                        return Err(NfsError::Xdr("delegation size truncated".into()));
                    }
                    buf.advance(8);
                }
                2 => {
                    if buf.remaining() < 16 {
                        return Err(NfsError::Xdr("delegation blocks truncated".into()));
                    }
                    buf.advance(16);
                }
                value => {
                    return Err(NfsError::Xdr(format!(
                        "invalid delegation space limit {value}"
                    )));
                }
            }
            skip_ace(buf)
        }
        value => Err(NfsError::Xdr(format!(
            "unsupported delegation type {value}"
        ))),
    }
}

pub(crate) fn decode_open_response(buf: Bytes) -> Result<(OpenResult, Bytes)> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 3 {
        return Err(NfsError::Xdr(format!(
            "OPEN response has {count} operations, expected 3"
        )));
    }
    expect_op(&mut buf, OP_PUTFH, "PUTFH")?;
    expect_op(&mut buf, OP_OPEN, "OPEN")?;
    let stateid = take_stateid(&mut buf, "OPEN")?;
    if buf.remaining() < 20 {
        return Err(NfsError::Xdr("OPEN change_info truncated".into()));
    }
    buf.advance(20);
    let flags = take_u32(&mut buf, "OPEN result flags")?;
    skip_bitmap(&mut buf, "OPEN attrset")?;
    skip_delegation(&mut buf)?;
    expect_op(&mut buf, OP_GETFH, "GETFH")?;
    let fh = take_opaque(&mut buf, "GETFH filehandle")?;
    Ok((
        OpenResult {
            stateid,
            confirm_required: flags & OPEN4_RESULT_CONFIRM != 0,
        },
        fh,
    ))
}

pub(crate) fn open_succeeded_before_compound_failure(mut buf: Bytes) -> bool {
    let Ok(overall) = take_u32(&mut buf, "COMPOUND status") else {
        return false;
    };
    if overall == 0 || take_opaque(&mut buf, "COMPOUND tag").is_err() {
        return false;
    }
    let Ok(count) = take_u32(&mut buf, "COMPOUND result count") else {
        return false;
    };
    count >= 2
        && take_u32(&mut buf, "PUTFH opcode").ok() == Some(OP_PUTFH)
        && take_u32(&mut buf, "PUTFH status").ok() == Some(0)
        && take_u32(&mut buf, "OPEN opcode").ok() == Some(OP_OPEN)
        && take_u32(&mut buf, "OPEN status").ok() == Some(0)
}

pub(crate) fn create_succeeded_before_compound_failure(mut buf: Bytes) -> bool {
    let Ok(overall) = take_u32(&mut buf, "COMPOUND status") else {
        return false;
    };
    if overall == 0 || take_opaque(&mut buf, "COMPOUND tag").is_err() {
        return false;
    }
    let Ok(count) = take_u32(&mut buf, "COMPOUND result count") else {
        return false;
    };
    count >= 2
        && take_u32(&mut buf, "PUTFH opcode").ok() == Some(OP_PUTFH)
        && take_u32(&mut buf, "PUTFH status").ok() == Some(0)
        && take_u32(&mut buf, "CREATE opcode").ok() == Some(OP_CREATE)
        && take_u32(&mut buf, "CREATE status").ok() == Some(0)
}

pub(crate) fn decode_getattr_response(buf: Bytes) -> Result<Bytes> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "GETATTR response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_GETATTR, "GETATTR")?;
    Ok(ops)
}

pub(crate) fn decode_access_response(buf: Bytes) -> Result<(u32, u32)> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "ACCESS response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_ACCESS, "ACCESS")?;
    let supported = take_u32(&mut ops, "ACCESS supported")?;
    let access = take_u32(&mut ops, "ACCESS granted")?;
    if access & !supported != 0 {
        return Err(NfsError::Xdr(
            "ACCESS granted bits exceed supported bits".to_string(),
        ));
    }
    Ok((supported, access))
}

pub(crate) fn decode_setattr_response(buf: Bytes, requested: &[u32]) -> Result<()> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "SETATTR response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_SETATTR, "SETATTR")?;
    let words = take_u32(&mut ops, "SETATTR attrsset length")? as usize;
    for index in 0..words {
        let set = take_u32(&mut ops, "SETATTR attrsset word")?;
        if set & !requested.get(index).copied().unwrap_or(0) != 0 {
            return Err(NfsError::Xdr(
                "SETATTR response reports an unrequested attribute".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn decode_lookup_getattr_response(buf: Bytes) -> Result<(Bytes, Bytes)> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 4 {
        return Err(NfsError::Xdr(format!(
            "LOOKUP response has {count} operations, expected 4"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, crate::nfs4::compound::OP_LOOKUP, "LOOKUP")?;
    expect_op(&mut ops, OP_GETFH, "GETFH")?;
    let fh = take_opaque(&mut ops, "GETFH filehandle")?;
    expect_op(&mut ops, OP_GETATTR, "GETATTR")?;
    Ok((fh, ops))
}

pub(crate) fn decode_create_response(buf: Bytes) -> Result<(Bytes, Bytes)> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 4 {
        return Err(NfsError::Xdr(format!(
            "CREATE response has {count} operations, expected 4"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_CREATE, "CREATE")?;
    if ops.remaining() < 20 {
        return Err(NfsError::Xdr("CREATE change_info truncated".to_string()));
    }
    ops.advance(20);
    skip_bitmap(&mut ops, "CREATE attrset")?;
    expect_op(&mut ops, OP_GETFH, "GETFH")?;
    let fh = take_opaque(&mut ops, "GETFH filehandle")?;
    expect_op(&mut ops, OP_GETATTR, "GETATTR")?;
    Ok((fh, ops))
}

pub(crate) fn decode_remove_response(buf: Bytes) -> Result<()> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "REMOVE response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_REMOVE, "REMOVE")?;
    if ops.remaining() < 20 {
        return Err(NfsError::Xdr("REMOVE change_info truncated".to_string()));
    }
    Ok(())
}

pub(crate) fn decode_readlink_response(buf: Bytes) -> Result<String> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "READLINK response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_READLINK, "READLINK")?;
    let link = take_opaque(&mut ops, "READLINK target")?;
    String::from_utf8(link.to_vec())
        .map_err(|error| NfsError::Xdr(format!("READLINK target is not UTF-8: {error}")))
}

pub(crate) fn decode_readdir_response(buf: Bytes) -> Result<Bytes> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "READDIR response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_READDIR, "READDIR")?;
    Ok(ops)
}

pub(crate) fn decode_rename_response(buf: Bytes) -> Result<()> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 4 {
        return Err(NfsError::Xdr(format!(
            "RENAME response has {count} operations, expected 4"
        )));
    }
    for (opcode, name) in [
        (OP_PUTFH, "PUTFH"),
        (OP_SAVEFH, "SAVEFH"),
        (OP_PUTFH, "PUTFH"),
        (OP_RENAME, "RENAME"),
    ] {
        expect_op(&mut ops, opcode, name)?;
    }
    if ops.remaining() < 40 {
        return Err(NfsError::Xdr("RENAME change_info truncated".to_string()));
    }
    Ok(())
}

pub(crate) fn decode_link_response(buf: Bytes) -> Result<()> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 4 {
        return Err(NfsError::Xdr(format!(
            "LINK response has {count} operations, expected 4"
        )));
    }
    for (opcode, name) in [
        (OP_PUTFH, "PUTFH"),
        (OP_SAVEFH, "SAVEFH"),
        (OP_PUTFH, "PUTFH"),
        (OP_LINK, "LINK"),
    ] {
        expect_op(&mut ops, opcode, name)?;
    }
    if ops.remaining() < 20 {
        return Err(NfsError::Xdr("LINK change_info truncated".to_string()));
    }
    Ok(())
}

pub(crate) fn decode_lock_response(buf: Bytes, opcode: u32, name: &str) -> Result<[u8; 16]> {
    decode_lock_denied_result(buf.clone(), opcode, name)?;
    decode_stateid_response(buf, opcode, name)
}

pub(crate) fn decode_lockt_response(buf: Bytes) -> Result<()> {
    decode_lock_denied_result(buf.clone(), OP_LOCKT, "LOCKT")?;
    let (count, mut ops) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "LOCKT response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut ops, OP_PUTFH, "PUTFH")?;
    expect_op(&mut ops, OP_LOCKT, "LOCKT")
}

fn decode_lock_denied_result(mut buf: Bytes, opcode: u32, name: &str) -> Result<()> {
    let overall = take_u32(&mut buf, "COMPOUND status")?;
    if overall != NFS4ERR_DENIED {
        return Ok(());
    }
    let _tag = take_opaque(&mut buf, "COMPOUND tag")?;
    let count = take_u32(&mut buf, "COMPOUND result count")?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "{name} denied response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut buf, OP_PUTFH, "PUTFH")?;
    let actual = take_u32(&mut buf, &format!("{name} opcode"))?;
    if actual != opcode {
        return Err(NfsError::Xdr(format!(
            "expected {name} opcode {opcode}, got {actual}"
        )));
    }
    let status = take_u32(&mut buf, &format!("{name} status"))?;
    if status != NFS4ERR_DENIED {
        return Err(NfsError::Xdr(format!(
            "{name} status {status} does not match denied COMPOUND status"
        )));
    }
    if buf.remaining() < 28 {
        return Err(NfsError::Xdr(format!("{name} denied range truncated")));
    }
    let offset = buf.get_u64();
    let length = buf.get_u64();
    let lock_type = buf.get_u32();
    buf.advance(8); // conflicting owner clientid
    let owner = take_opaque(&mut buf, &format!("{name} denied owner"))?;
    if owner.len() > MAX_LOCK_OWNER_LEN {
        return Err(NfsError::Xdr(format!(
            "{name} denied owner exceeds {MAX_LOCK_OWNER_LEN} bytes"
        )));
    }
    Err(NfsError::LockDenied {
        lock_type,
        offset,
        length,
        owner,
    })
}

pub(crate) fn decode_release_lockowner_response(buf: Bytes) -> Result<()> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 1 {
        return Err(NfsError::Xdr(format!(
            "RELEASE_LOCKOWNER response has {count} operations, expected 1"
        )));
    }
    expect_op(&mut ops, OP_RELEASE_LOCKOWNER, "RELEASE_LOCKOWNER")
}

pub(crate) fn decode_renew_response(buf: Bytes) -> Result<()> {
    let (count, mut ops) = response_ops(buf)?;
    if count != 1 {
        return Err(NfsError::Xdr(format!(
            "RENEW response has {count} operations, expected 1"
        )));
    }
    expect_op(&mut ops, OP_RENEW, "RENEW")
}

pub(crate) fn decode_stateid_response(buf: Bytes, opcode: u32, name: &str) -> Result<[u8; 16]> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "{name} response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut buf, OP_PUTFH, "PUTFH")?;
    expect_op(&mut buf, opcode, name)?;
    take_stateid(&mut buf, name)
}

pub(crate) fn decode_read_response(buf: Bytes) -> Result<Bytes> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "READ response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut buf, OP_PUTFH, "PUTFH")?;
    expect_op(&mut buf, OP_READ, "READ")?;
    let _eof = take_u32(&mut buf, "READ eof")?;
    take_opaque(&mut buf, "READ data")
}

pub(crate) fn decode_write_response(buf: Bytes) -> Result<(u32, u32, [u8; 8])> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "WRITE response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut buf, OP_PUTFH, "PUTFH")?;
    expect_op(&mut buf, OP_WRITE, "WRITE")?;
    let count = take_u32(&mut buf, "WRITE count")?;
    let committed = take_u32(&mut buf, "WRITE committed")?;
    if committed > 2 {
        return Err(NfsError::Xdr(format!(
            "WRITE committed value {committed} is outside stable_how4"
        )));
    }
    if buf.remaining() < 8 {
        return Err(NfsError::Xdr("WRITE verifier truncated".into()));
    }
    let mut verifier = [0; 8];
    buf.copy_to_slice(&mut verifier);
    Ok((count, committed, verifier))
}

pub(crate) fn decode_commit_response(buf: Bytes) -> Result<[u8; 8]> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 2 {
        return Err(NfsError::Xdr(format!(
            "COMMIT response has {count} operations, expected 2"
        )));
    }
    expect_op(&mut buf, OP_PUTFH, "PUTFH")?;
    expect_op(&mut buf, OP_COMMIT, "COMMIT")?;
    if buf.remaining() < 8 {
        return Err(NfsError::Xdr("COMMIT verifier truncated".into()));
    }
    let mut verifier = [0; 8];
    buf.copy_to_slice(&mut verifier);
    Ok(verifier)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_op_response(opcode: u32, status: u32, data: &[u8]) -> Bytes {
        let mut wire = Vec::new();
        wire.extend_from_slice(&status.to_be_bytes());
        xdr_opaque(&mut wire, b"test");
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(&opcode.to_be_bytes());
        wire.extend_from_slice(&status.to_be_bytes());
        wire.extend_from_slice(data);
        Bytes::from(wire)
    }

    #[test]
    fn create_success_before_later_compound_failure_is_detected() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&10006u32.to_be_bytes());
        xdr_opaque(&mut wire, b"mkdir");
        wire.extend_from_slice(&2u32.to_be_bytes());
        for opcode in [OP_PUTFH, OP_CREATE] {
            wire.extend_from_slice(&opcode.to_be_bytes());
            wire.extend_from_slice(&0u32.to_be_bytes());
        }
        assert!(create_succeeded_before_compound_failure(Bytes::from(wire)));
    }

    #[test]
    fn setclientid_compound_matches_rfc7530_wire_shape() {
        let request = CompoundBuilder::new("identity")
            .setclientid(SetClientIdArgs {
                verifier: [0x11; 8],
                owner: b"nfs-rs-client",
                callback: CallbackAddress {
                    program: 0x4000_0000,
                    netid: "tcp",
                    addr: "0.0.0.0.0.0",
                    ident: 7,
                },
            })
            .encode_body();

        let mut expected = Vec::new();
        xdr_opaque(&mut expected, b"identity");
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(&35u32.to_be_bytes());
        expected.extend_from_slice(&[0x11; 8]);
        xdr_opaque(&mut expected, b"nfs-rs-client");
        expected.extend_from_slice(&0x4000_0000u32.to_be_bytes());
        xdr_opaque(&mut expected, b"tcp");
        xdr_opaque(&mut expected, b"0.0.0.0.0.0");
        expected.extend_from_slice(&7u32.to_be_bytes());
        assert_eq!(request, expected);
    }

    #[test]
    fn setclientid_confirm_compound_matches_rfc7530_wire_shape() {
        let request = CompoundBuilder::new("confirm")
            .setclientid_confirm(0x0102_0304_0506_0708, [0xaa; 8])
            .encode_body();

        let mut expected = Vec::new();
        xdr_opaque(&mut expected, b"confirm");
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&1u32.to_be_bytes());
        expected.extend_from_slice(&36u32.to_be_bytes());
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        expected.extend_from_slice(&[0xaa; 8]);
        assert_eq!(request, expected);
    }

    #[test]
    fn setclientid_protocol_failure_is_preserved() {
        let response = single_op_response(OP_SETCLIENTID, 13, &[]);
        assert!(matches!(
            decode_setclientid_response(response),
            Err(crate::NfsError::Nfs4(_))
        ));
    }

    #[test]
    fn confirm_rejects_truncated_operation_status() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&0u32.to_be_bytes());
        xdr_opaque(&mut wire, b"confirm");
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(&OP_SETCLIENTID_CONFIRM.to_be_bytes());
        assert!(matches!(
            decode_confirm_response(Bytes::from(wire)),
            Err(crate::NfsError::Xdr(_))
        ));
    }

    #[test]
    fn open_decoder_preserves_full_stateid_and_confirm_flag() {
        let mut open = Vec::new();
        open.extend_from_slice(&[0x5a; 16]);
        open.extend_from_slice(&[0; 20]); // change_info4
        open.extend_from_slice(&OPEN4_RESULT_CONFIRM.to_be_bytes());
        open.extend_from_slice(&0u32.to_be_bytes()); // attrset
        open.extend_from_slice(&0u32.to_be_bytes()); // OPEN_DELEGATE_NONE
        let mut fh = Vec::new();
        xdr_opaque(&mut fh, b"file-fh");
        let response = compound_response(
            "open",
            &[(OP_PUTFH, &[]), (OP_OPEN, &open), (OP_GETFH, &fh)],
        );
        let (opened, actual_fh) = decode_open_response(response).unwrap();
        assert_eq!(opened.stateid, [0x5a; 16]);
        assert!(opened.confirm_required);
        assert_eq!(actual_fh, Bytes::from_static(b"file-fh"));
    }

    #[test]
    fn read_write_and_commit_validate_result_shapes() {
        let mut read = Vec::new();
        read.extend_from_slice(&1u32.to_be_bytes());
        xdr_opaque(&mut read, b"payload");
        assert_eq!(
            decode_read_response(compound_response(
                "read",
                &[(OP_PUTFH, &[]), (OP_READ, &read)]
            ))
            .unwrap(),
            Bytes::from_static(b"payload")
        );

        let write = [
            7u32.to_be_bytes().as_slice(),
            2u32.to_be_bytes().as_slice(),
            [0x33; 8].as_slice(),
        ]
        .concat();
        assert_eq!(
            decode_write_response(compound_response(
                "write",
                &[(OP_PUTFH, &[]), (OP_WRITE, &write)]
            ))
            .unwrap(),
            (7, 2, [0x33; 8])
        );
        assert_eq!(
            decode_commit_response(compound_response(
                "commit",
                &[(OP_PUTFH, &[]), (OP_COMMIT, &[0x44; 8])]
            ))
            .unwrap(),
            [0x44; 8]
        );
    }

    #[test]
    fn access_decoder_rejects_grants_outside_supported_mask() {
        let valid = [0x3fu32.to_be_bytes(), 0x05u32.to_be_bytes()].concat();
        assert_eq!(
            decode_access_response(compound_response(
                "access",
                &[(OP_PUTFH, &[]), (OP_ACCESS, &valid)]
            ))
            .unwrap(),
            (0x3f, 0x05)
        );

        let invalid = [0x01u32.to_be_bytes(), 0x03u32.to_be_bytes()].concat();
        assert!(matches!(
            decode_access_response(compound_response(
                "access",
                &[(OP_PUTFH, &[]), (OP_ACCESS, &invalid)]
            )),
            Err(NfsError::Xdr(_))
        ));
    }

    #[test]
    fn namespace_opcodes_match_rfc7530_registry() {
        let request = CompoundBuilder::new("namespace")
            .putfh(b"source")
            .savefh()
            .putfh(b"target")
            .link("hardlink")
            .encode_body();
        let mut expected = Vec::new();
        xdr_opaque(&mut expected, b"namespace");
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&4u32.to_be_bytes());
        expected.extend_from_slice(&22u32.to_be_bytes());
        xdr_opaque(&mut expected, b"source");
        expected.extend_from_slice(&32u32.to_be_bytes());
        expected.extend_from_slice(&22u32.to_be_bytes());
        xdr_opaque(&mut expected, b"target");
        expected.extend_from_slice(&11u32.to_be_bytes());
        xdr_opaque(&mut expected, b"hardlink");
        assert_eq!(request, expected);
    }

    #[test]
    fn namespace_and_metadata_result_decoders_accept_rfc7530_shapes() {
        let getattr = [0u32.to_be_bytes(), 0u32.to_be_bytes()].concat();
        assert_eq!(
            decode_getattr_response(compound_response(
                "getattr",
                &[(OP_PUTFH, &[]), (OP_GETATTR, &getattr)]
            ))
            .unwrap(),
            Bytes::from(getattr)
        );

        let attrsset = [1u32.to_be_bytes(), (1u32 << 4).to_be_bytes()].concat();
        decode_setattr_response(
            compound_response("setattr", &[(OP_PUTFH, &[]), (OP_SETATTR, &attrsset)]),
            &[1 << 4],
        )
        .unwrap();

        let change = [0u8; 20];
        decode_remove_response(compound_response(
            "remove",
            &[(OP_PUTFH, &[]), (OP_REMOVE, &change)],
        ))
        .unwrap();
        decode_link_response(compound_response(
            "link",
            &[
                (OP_PUTFH, &[]),
                (OP_SAVEFH, &[]),
                (OP_PUTFH, &[]),
                (OP_LINK, &change),
            ],
        ))
        .unwrap();
        let rename_changes = [0u8; 40];
        decode_rename_response(compound_response(
            "rename",
            &[
                (OP_PUTFH, &[]),
                (OP_SAVEFH, &[]),
                (OP_PUTFH, &[]),
                (OP_RENAME, &rename_changes),
            ],
        ))
        .unwrap();

        let mut target = Vec::new();
        xdr_opaque(&mut target, b"target");
        assert_eq!(
            decode_readlink_response(compound_response(
                "readlink",
                &[(OP_PUTFH, &[]), (OP_READLINK, &target)]
            ))
            .unwrap(),
            "target"
        );

        let directory = [0x5au8; 16];
        assert_eq!(
            decode_readdir_response(compound_response(
                "readdir",
                &[(OP_PUTFH, &[]), (OP_READDIR, &directory)]
            ))
            .unwrap(),
            Bytes::copy_from_slice(&directory)
        );

        let mut create = vec![0; 20];
        create.extend_from_slice(&0u32.to_be_bytes());
        let mut fh = Vec::new();
        xdr_opaque(&mut fh, b"created");
        let getattr = [0u8; 8];
        let (actual_fh, actual_attrs) = decode_create_response(compound_response(
            "create",
            &[
                (OP_PUTFH, &[]),
                (OP_CREATE, &create),
                (OP_GETFH, &fh),
                (OP_GETATTR, &getattr),
            ],
        ))
        .unwrap();
        assert_eq!(actual_fh, Bytes::from_static(b"created"));
        assert_eq!(actual_attrs, Bytes::copy_from_slice(&getattr));
    }

    fn operation_arguments(body: &[u8], index: usize) -> (u32, Bytes) {
        let mut data = Bytes::copy_from_slice(body);
        let _tag = take_opaque(&mut data, "tag").unwrap();
        assert_eq!(data.get_u32(), 0);
        let count = data.get_u32() as usize;
        assert!(index < count);
        assert_eq!(index, 0, "test helper only selects the first operation");
        (data.get_u32(), data)
    }

    #[test]
    fn metadata_operation_arguments_match_rfc7530_vectors() {
        let cases = [
            (
                CompoundBuilder::new("access").access(0x21).encode_body(),
                OP_ACCESS,
                0x21u32.to_be_bytes().to_vec(),
            ),
            (
                CompoundBuilder::new("getattr")
                    .getattr(&[0x12, 0x34])
                    .encode_body(),
                OP_GETATTR,
                [
                    2u32.to_be_bytes().as_slice(),
                    0x12u32.to_be_bytes().as_slice(),
                    0x34u32.to_be_bytes().as_slice(),
                ]
                .concat(),
            ),
            (
                CompoundBuilder::new("remove").remove("file").encode_body(),
                OP_REMOVE,
                [4u32.to_be_bytes().as_slice(), b"file"].concat(),
            ),
            (
                CompoundBuilder::new("readlink").readlink().encode_body(),
                OP_READLINK,
                Vec::new(),
            ),
        ];
        for (body, expected_opcode, expected_args) in cases {
            let (opcode, args) = operation_arguments(&body, 0);
            assert_eq!(opcode, expected_opcode);
            assert_eq!(args, Bytes::from(expected_args));
        }

        let setattr = CompoundBuilder::new("setattr")
            .setattr(&[0x11; 16], &[1 << 4], &[0x22; 8])
            .encode_body();
        let (opcode, args) = operation_arguments(&setattr, 0);
        assert_eq!(opcode, OP_SETATTR);
        assert_eq!(&args[..16], &[0x11; 16]);
        assert_eq!(
            &args[16..],
            &[
                1u32.to_be_bytes().as_slice(),
                (1u32 << 4).to_be_bytes().as_slice(),
                8u32.to_be_bytes().as_slice(),
                &[0x22; 8]
            ]
            .concat()
        );
    }

    #[test]
    fn namespace_operation_arguments_match_rfc7530_vectors() {
        let cases = [
            (
                CompoundBuilder::new("mkdir")
                    .create_directory("dir")
                    .encode_body(),
                OP_CREATE,
                2,
            ),
            (
                CompoundBuilder::new("symlink")
                    .create_symlink("name", "target")
                    .encode_body(),
                OP_CREATE,
                5,
            ),
            (
                CompoundBuilder::new("rename")
                    .rename("old", "new")
                    .encode_body(),
                OP_RENAME,
                3,
            ),
            (
                CompoundBuilder::new("link").link("name").encode_body(),
                OP_LINK,
                4,
            ),
            (
                CompoundBuilder::new("readdir")
                    .readdir(7, b"verifier", 8192, 32768, &[1 << 20])
                    .encode_body(),
                OP_READDIR,
                0,
            ),
        ];
        for (body, expected_opcode, expected_first) in cases {
            let (opcode, mut args) = operation_arguments(&body, 0);
            assert_eq!(opcode, expected_opcode);
            assert_eq!(args.get_u32(), expected_first);
        }
    }

    #[test]
    fn open_create_arguments_use_unchecked4_and_claim_null() {
        let body = CompoundBuilder::new("create")
            .open(OpenArgs {
                seqid: 3,
                share_access: 3,
                client_id: 9,
                owner: b"owner",
                filename: "file",
                create: true,
            })
            .encode_body();
        let (opcode, args) = operation_arguments(&body, 0);
        assert_eq!(opcode, OP_OPEN);
        assert!(args.windows(16).any(|wire| {
            wire == [
                1u32.to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
            ]
            .concat()
        }));
        assert!(args.ends_with(&[4u32.to_be_bytes().as_slice(), b"file"].concat()));
    }

    #[test]
    fn lock_operation_arguments_match_rfc7530_vectors() {
        let new = CompoundBuilder::new("lock")
            .lock_new(NewLockArgs {
                lock_type: 2,
                reclaim: false,
                offset: 7,
                length: 11,
                open_seqid: 3,
                open_stateid: &[0x41; 16],
                lock_seqid: 0,
                client_id: 9,
                owner: b"owner",
            })
            .encode_body();
        let (opcode, args) = operation_arguments(&new, 0);
        assert_eq!(opcode, OP_LOCK);
        assert_eq!(
            args,
            [
                2u32.to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
                7u64.to_be_bytes().as_slice(),
                11u64.to_be_bytes().as_slice(),
                1u32.to_be_bytes().as_slice(),
                3u32.to_be_bytes().as_slice(),
                &[0x41; 16],
                0u32.to_be_bytes().as_slice(),
                9u64.to_be_bytes().as_slice(),
                5u32.to_be_bytes().as_slice(),
                b"owner",
                &[0, 0, 0],
            ]
            .concat()
        );

        let test = CompoundBuilder::new("lockt")
            .lockt(2, 19, 23, 9, b"tester")
            .encode_body();
        let (opcode, args) = operation_arguments(&test, 0);
        assert_eq!(opcode, OP_LOCKT);
        assert_eq!(
            args,
            [
                2u32.to_be_bytes().as_slice(),
                19u64.to_be_bytes().as_slice(),
                23u64.to_be_bytes().as_slice(),
                9u64.to_be_bytes().as_slice(),
                6u32.to_be_bytes().as_slice(),
                b"tester",
                &[0, 0],
            ]
            .concat()
        );
        let unlock = CompoundBuilder::new("locku")
            .locku(2, 5, &[0x43; 16], 29, 31)
            .encode_body();
        let (opcode, args) = operation_arguments(&unlock, 0);
        assert_eq!(opcode, OP_LOCKU);
        assert_eq!(
            args,
            [
                2u32.to_be_bytes().as_slice(),
                5u32.to_be_bytes().as_slice(),
                &[0x43; 16],
                29u64.to_be_bytes().as_slice(),
                31u64.to_be_bytes().as_slice(),
            ]
            .concat()
        );
        let release = CompoundBuilder::new("release")
            .release_lockowner(9, b"owner")
            .encode_body();
        assert_eq!(operation_arguments(&release, 0).0, OP_RELEASE_LOCKOWNER);
    }

    #[test]
    fn lock_and_locku_results_preserve_full_stateid() {
        assert_eq!(
            decode_lock_response(
                compound_response("lock", &[(OP_PUTFH, &[]), (OP_LOCK, &[0x51; 16])]),
                OP_LOCK,
                "LOCK"
            )
            .unwrap(),
            [0x51; 16]
        );
        assert_eq!(
            decode_lock_response(
                compound_response("locku", &[(OP_PUTFH, &[]), (OP_LOCKU, &[0x52; 16])]),
                OP_LOCKU,
                "LOCKU"
            )
            .unwrap(),
            [0x52; 16]
        );
    }

    #[test]
    fn lock_denied_result_is_validated_before_reporting_conflict() {
        let mut denied = Vec::new();
        denied.extend_from_slice(&7u64.to_be_bytes());
        denied.extend_from_slice(&11u64.to_be_bytes());
        denied.extend_from_slice(&2u32.to_be_bytes());
        denied.extend_from_slice(&9u64.to_be_bytes());
        xdr_opaque(&mut denied, b"conflicting-owner");

        let response = failed_compound_response(
            "lock",
            10010,
            &[(OP_PUTFH, 0, &[]), (OP_LOCK, 10010, &denied)],
        );
        assert!(matches!(
            decode_lock_response(response, OP_LOCK, "LOCK"),
            Err(NfsError::LockDenied {
                lock_type: 2,
                offset: 7,
                length: 11,
                ..
            })
        ));

        let truncated = failed_compound_response(
            "lock",
            10010,
            &[(OP_PUTFH, 0, &[]), (OP_LOCK, 10010, &denied[..27])],
        );
        assert!(matches!(
            decode_lock_response(truncated, OP_LOCK, "LOCK"),
            Err(NfsError::Xdr(_))
        ));

        let lockt = failed_compound_response(
            "lockt",
            10010,
            &[(OP_PUTFH, 0, &[]), (OP_LOCKT, 10010, &denied)],
        );
        assert!(matches!(
            decode_lockt_response(lockt),
            Err(NfsError::LockDenied {
                lock_type: 2,
                offset: 7,
                length: 11,
                ..
            })
        ));
    }

    #[test]
    fn lock_result_decoders_reject_truncated_success_arms() {
        for (opcode, name) in [(OP_LOCK, "LOCK"), (OP_LOCKU, "LOCKU")] {
            let response = compound_response(name, &[(OP_PUTFH, &[]), (opcode, &[0x51; 15])]);
            assert!(matches!(
                decode_lock_response(response, opcode, name),
                Err(NfsError::Xdr(_))
            ));
        }

        let mut truncated =
            compound_response("lockt", &[(OP_PUTFH, &[]), (OP_LOCKT, &[])]).to_vec();
        truncated.truncate(truncated.len() - 1);
        assert!(matches!(
            decode_lockt_response(Bytes::from(truncated)),
            Err(NfsError::Xdr(_))
        ));
    }

    #[test]
    fn release_lockowner_result_is_bounded() {
        decode_release_lockowner_response(compound_response(
            "release-lockowner",
            &[(OP_RELEASE_LOCKOWNER, &[])],
        ))
        .unwrap();
        assert!(
            decode_release_lockowner_response(compound_response(
                "release-lockowner",
                &[(OP_RELEASE_LOCKOWNER, &[]), (OP_PUTFH, &[])],
            ))
            .is_err()
        );
    }

    #[test]
    fn renew_arguments_match_rfc7530_vector() {
        let body = CompoundBuilder::new("renew")
            .renew(0x0102_0304_0506_0708)
            .encode_body();
        let (opcode, args) = operation_arguments(&body, 0);
        assert_eq!(opcode, OP_RENEW);
        assert_eq!(args.as_ref(), &0x0102_0304_0506_0708u64.to_be_bytes());
    }

    #[test]
    fn renew_result_requires_one_complete_void_operation() {
        decode_renew_response(compound_response("renew", &[(OP_RENEW, &[])])).unwrap();
        assert!(
            decode_renew_response(compound_response(
                "renew",
                &[(OP_RENEW, &[]), (OP_PUTFH, &[])],
            ))
            .is_err()
        );

        let mut truncated = compound_response("renew", &[(OP_RENEW, &[])]).to_vec();
        truncated.pop();
        assert!(decode_renew_response(Bytes::from(truncated)).is_err());
    }

    #[test]
    fn reclaim_open_and_lock_match_rfc7530_vectors() {
        let open = CompoundBuilder::new("reclaim-open")
            .open_reclaim(OpenReclaimArgs {
                seqid: 4,
                share_access: 3,
                client_id: 9,
                owner: b"owner",
            })
            .encode_body();
        let (opcode, args) = operation_arguments(&open, 0);
        assert_eq!(opcode, OP_OPEN);
        assert_eq!(
            args,
            [
                4u32.to_be_bytes().as_slice(),
                3u32.to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
                9u64.to_be_bytes().as_slice(),
                5u32.to_be_bytes().as_slice(),
                b"owner",
                &[0, 0, 0],
                0u32.to_be_bytes().as_slice(),
                1u32.to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
            ]
            .concat()
        );

        let lock = CompoundBuilder::new("reclaim-lock")
            .lock_new(NewLockArgs {
                lock_type: 2,
                reclaim: true,
                offset: 7,
                length: 11,
                open_seqid: 5,
                open_stateid: &[0x41; 16],
                lock_seqid: 0,
                client_id: 9,
                owner: b"locker",
            })
            .encode_body();
        let (_, args) = operation_arguments(&lock, 0);
        assert_eq!(&args[4..8], &1u32.to_be_bytes());
        assert_eq!(&args[24..28], &1u32.to_be_bytes());
    }

    fn compound_response(tag: &str, ops: &[(u32, &[u8])]) -> Bytes {
        let mut wire = Vec::new();
        wire.extend_from_slice(&0u32.to_be_bytes());
        xdr_opaque(&mut wire, tag.as_bytes());
        wire.extend_from_slice(&(ops.len() as u32).to_be_bytes());
        for (opcode, data) in ops {
            wire.extend_from_slice(&opcode.to_be_bytes());
            wire.extend_from_slice(&0u32.to_be_bytes());
            wire.extend_from_slice(data);
        }
        Bytes::from(wire)
    }

    fn failed_compound_response(tag: &str, status: u32, ops: &[(u32, u32, &[u8])]) -> Bytes {
        let mut wire = Vec::new();
        wire.extend_from_slice(&status.to_be_bytes());
        xdr_opaque(&mut wire, tag.as_bytes());
        wire.extend_from_slice(&(ops.len() as u32).to_be_bytes());
        for (opcode, op_status, data) in ops {
            wire.extend_from_slice(&opcode.to_be_bytes());
            wire.extend_from_slice(&op_status.to_be_bytes());
            wire.extend_from_slice(data);
        }
        Bytes::from(wire)
    }
}
