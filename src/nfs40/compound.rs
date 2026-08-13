use bytes::{Buf, Bytes};

use crate::nfs4::compound::{
    CompoundBuilder as CommonCompoundBuilder, check_status, response_ops, take_u32, xdr_opaque,
    xdr_u32,
};
use crate::rpc::auth::Auth;
use crate::{NfsError, Result};

const OP_SETCLIENTID: u32 = 35;
const OP_SETCLIENTID_CONFIRM: u32 = 36;

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
}
