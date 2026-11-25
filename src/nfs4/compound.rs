//! Minor-version-independent NFSv4 COMPOUND wire primitives.

use bytes::{Buf, Bytes};

use crate::error::{NfsError, Result};
use crate::nfs3::rpc_header;
use crate::nfs4::fastxdr::nfsstat4;
use crate::rpc::auth::Auth;

pub(crate) const OP_GETFH: u32 = 10;
pub(crate) const OP_LOOKUP: u32 = 15;
pub(crate) const OP_PUTFH: u32 = 22;
pub(crate) const OP_PUTROOTFH: u32 = 24;

pub(crate) fn xdr_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn xdr_opaque(buf: &mut Vec<u8>, value: &[u8]) {
    xdr_u32(buf, value.len() as u32);
    buf.extend_from_slice(value);
    buf.resize(buf.len() + (4 - value.len() % 4) % 4, 0);
}

/// Serialized fixed fields with borrowed XDR strings inserted during final encoding.
pub(crate) struct WireArgs<'a> {
    pub(crate) fixed: Vec<u8>,
    names: Option<NamedFields<'a>>,
}

struct NamedFields<'a> {
    offset: usize,
    first: &'a [u8],
    second: Option<&'a [u8]>,
}

impl From<Vec<u8>> for WireArgs<'_> {
    fn from(fixed: Vec<u8>) -> Self {
        Self { fixed, names: None }
    }
}

impl<'a> WireArgs<'a> {
    pub(crate) fn with_names(
        fixed: Vec<u8>,
        offset: usize,
        first: &'a [u8],
        second: Option<&'a [u8]>,
    ) -> Self {
        Self {
            fixed,
            names: Some(NamedFields {
                offset,
                first,
                second,
            }),
        }
    }

    pub(crate) fn wire_len(&self) -> usize {
        self.fixed.len()
            + self.names.as_ref().map_or(0, |names| {
                opaque_len(names.first) + names.second.map_or(0, opaque_len)
            })
    }

    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        if let Some(names) = &self.names {
            buf.extend_from_slice(&self.fixed[..names.offset]);
            xdr_opaque(buf, names.first);
            if let Some(second) = names.second {
                xdr_opaque(buf, second);
            }
            buf.extend_from_slice(&self.fixed[names.offset..]);
        } else {
            buf.extend_from_slice(&self.fixed);
        }
    }
}

pub(crate) fn opaque_len(value: &[u8]) -> usize {
    4 + value.len().next_multiple_of(4)
}

pub(crate) struct CompoundBuilder<'a> {
    tag: String,
    minor_version: u32,
    ops: Vec<(u32, WireArgs<'a>)>,
}

impl<'a> CompoundBuilder<'a> {
    pub(crate) fn new(tag: &str, minor_version: u32) -> Self {
        Self {
            tag: tag.to_string(),
            minor_version,
            ops: Vec::new(),
        }
    }

    pub(crate) fn operation(mut self, opcode: u32, args: impl Into<WireArgs<'a>>) -> Self {
        self.ops.push((opcode, args.into()));
        self
    }

    pub(crate) fn putrootfh(self) -> Self {
        self.operation(OP_PUTROOTFH, Vec::new())
    }

    pub(crate) fn putfh(self, fh: &[u8]) -> Self {
        let mut args = Vec::new();
        xdr_opaque(&mut args, fh);
        self.operation(OP_PUTFH, args)
    }

    pub(crate) fn lookup(self, name: &'a [u8]) -> Self {
        self.operation(OP_LOOKUP, WireArgs::with_names(Vec::new(), 0, name, None))
    }

    pub(crate) fn getfh(self) -> Self {
        self.operation(OP_GETFH, Vec::new())
    }

    #[cfg(test)]
    pub(crate) fn encode_body(self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_body_into(&mut buf);
        buf
    }

    fn encode_body_into(self, buf: &mut Vec<u8>) {
        buf.reserve(
            opaque_len(self.tag.as_bytes())
                + 8
                + self
                    .ops
                    .iter()
                    .map(|(_, args)| 4 + args.wire_len())
                    .sum::<usize>(),
        );
        xdr_opaque(buf, self.tag.as_bytes());
        xdr_u32(buf, self.minor_version);
        xdr_u32(buf, self.ops.len() as u32);
        for (opcode, args) in self.ops {
            xdr_u32(buf, opcode);
            args.encode(buf);
        }
    }

    pub(crate) fn encode_with_header(self, auth: &Auth) -> Vec<u8> {
        let mut buf = Vec::new();
        rpc_header(100003, 4, 1, auth).encode(&mut buf);
        self.encode_body_into(&mut buf);
        buf
    }
}

pub(crate) fn take_u32(buf: &mut Bytes, what: &str) -> Result<u32> {
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr(format!("{what} truncated")));
    }
    Ok(buf.get_u32())
}

pub(crate) fn take_opaque(buf: &mut Bytes, what: &str) -> Result<Bytes> {
    let len = take_u32(buf, &format!("{what} length"))? as usize;
    let padded = len
        .checked_add(3)
        .map(|value| value & !3)
        .ok_or_else(|| NfsError::Xdr(format!("{what} length overflow")))?;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr(format!("{what} data truncated")));
    }
    let value = buf.slice(..len);
    buf.advance(padded);
    Ok(value)
}

pub(crate) fn check_status(status: u32) -> Result<()> {
    if status == 0 {
        return Ok(());
    }
    Err(status_error(status)?)
}

/// Convert a raw non-zero NFSv4 status into its typed error.
pub(crate) fn status_error(status: u32) -> Result<NfsError> {
    if status == 0 {
        return Err(NfsError::Xdr("unexpected success NFSv4 status".into()));
    }
    let mut wire = Bytes::copy_from_slice(&status.to_be_bytes());
    let status = nfsstat4::try_from(&mut wire)
        .map_err(|error| NfsError::Xdr(format!("invalid NFSv4 status: {error}")))?;
    Ok(NfsError::Nfs4(status))
}

pub(crate) fn response_ops(mut buf: Bytes) -> Result<(u32, Bytes)> {
    check_status(take_u32(&mut buf, "COMPOUND status")?)?;
    let _tag = take_opaque(&mut buf, "COMPOUND tag")?;
    let count = take_u32(&mut buf, "COMPOUND result count")?;
    Ok((count, buf))
}

pub(crate) fn decode_navigation_response(buf: Bytes, component_count: usize) -> Result<Bytes> {
    let (count, mut buf) = response_ops(buf)?;
    let expected = component_count + 2;
    if count as usize != expected {
        return Err(NfsError::Xdr(format!(
            "navigation response has {count} operations, expected {expected}"
        )));
    }
    let expected_ops = std::iter::once(OP_PUTROOTFH)
        .chain(std::iter::repeat_n(OP_LOOKUP, component_count))
        .chain(std::iter::once(OP_GETFH));
    for expected_opcode in expected_ops {
        let opcode = take_u32(&mut buf, "navigation opcode")?;
        if opcode != expected_opcode {
            return Err(NfsError::Xdr(format!(
                "navigation opcode {opcode} does not match {expected_opcode}"
            )));
        }
        check_status(take_u32(&mut buf, "navigation operation status")?)?;
    }
    take_opaque(&mut buf, "GETFH filehandle")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minor_zero_navigation_has_canonical_common_opcodes() {
        let actual = CompoundBuilder::new("navigate", 0)
            .putrootfh()
            .lookup(b"export")
            .getfh()
            .encode_body();
        let mut expected = Vec::new();
        xdr_opaque(&mut expected, b"navigate");
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&3u32.to_be_bytes());
        expected.extend_from_slice(&OP_PUTROOTFH.to_be_bytes());
        expected.extend_from_slice(&OP_LOOKUP.to_be_bytes());
        xdr_opaque(&mut expected, b"export");
        expected.extend_from_slice(&OP_GETFH.to_be_bytes());
        assert_eq!(actual, expected);
    }

    #[test]
    fn navigation_decoder_rejects_truncated_getfh() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&0u32.to_be_bytes());
        xdr_opaque(&mut wire, b"navigate");
        wire.extend_from_slice(&2u32.to_be_bytes());
        wire.extend_from_slice(&OP_PUTROOTFH.to_be_bytes());
        wire.extend_from_slice(&0u32.to_be_bytes());
        wire.extend_from_slice(&OP_GETFH.to_be_bytes());
        wire.extend_from_slice(&0u32.to_be_bytes());
        wire.extend_from_slice(&8u32.to_be_bytes());
        assert!(matches!(
            decode_navigation_response(Bytes::from(wire), 0),
            Err(NfsError::Xdr(_))
        ));
    }
}
