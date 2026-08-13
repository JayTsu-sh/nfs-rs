use bytes::{Buf, Bytes};

use crate::nfs4::compound::{
    CompoundBuilder as CommonCompoundBuilder, OP_GETFH, OP_PUTFH, check_status, response_ops,
    take_opaque, take_u32, xdr_opaque, xdr_u32,
};
use crate::rpc::auth::Auth;
use crate::{NfsError, Result};

const OP_SETCLIENTID: u32 = 35;
const OP_SETCLIENTID_CONFIRM: u32 = 36;
const OP_CLOSE: u32 = 4;
const OP_COMMIT: u32 = 5;
const OP_OPEN: u32 = 18;
const OP_OPEN_CONFIRM: u32 = 20;
const OP_READ: u32 = 25;
const OP_WRITE: u32 = 38;
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
        xdr_u32(&mut args, 0); // OPEN4_NOCREATE
        xdr_u32(&mut args, 0); // CLAIM_NULL
        xdr_opaque(&mut args, open.filename.as_bytes());
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

pub(crate) fn decode_lookup_response(buf: Bytes) -> Result<Bytes> {
    let (count, mut buf) = response_ops(buf)?;
    if count != 3 {
        return Err(NfsError::Xdr(format!(
            "LOOKUP response has {count} operations, expected 3"
        )));
    }
    expect_op(&mut buf, OP_PUTFH, "PUTFH")?;
    expect_op(&mut buf, crate::nfs4::compound::OP_LOOKUP, "LOOKUP")?;
    expect_op(&mut buf, OP_GETFH, "GETFH")?;
    take_opaque(&mut buf, "GETFH filehandle")
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
}
