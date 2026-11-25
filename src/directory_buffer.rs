use crate::error::{NfsError, Result};
use bytes::{Buf, Bytes};
use std::io;
use std::sync::{Arc, LazyLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// A name can outlive its iterator and keep the complete RPC response alive.
// Account for retained replies across scans, including simultaneous mounts.
const RETAINED_BYTES: usize = 64 * 1024 * 1024;
const GRANULARITY: usize = 4096;
static PAGES: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(RETAINED_BYTES / GRANULARITY)));

struct DirectoryBuffer {
    data: Bytes,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for DirectoryBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// Admit an RPC reply before exposing any view. Fail promptly if caller-held
/// views exhaust the budget: waiting for those views could deadlock a scan.
pub(crate) fn retain(data: Bytes, reply_len: usize) -> Result<Bytes> {
    retain_with_budget(data, reply_len, &PAGES)
}

fn retain_with_budget(data: Bytes, reply_len: usize, budget: &Arc<Semaphore>) -> Result<Bytes> {
    // Include stripped RPC headers and reserve for geometric buffer growth
    // during fragment assembly. Charge once per backing allocation.
    let units = data
        .len()
        .max(reply_len)
        .saturating_add(4096)
        .saturating_mul(2)
        .div_ceil(GRANULARITY);
    let units = u32::try_from(units)
        .map_err(|_| NfsError::Xdr("directory response size overflow".into()))?;
    let permit = Arc::clone(budget)
        .try_acquire_many_owned(units)
        .map_err(|_| {
            NfsError::Io(io::Error::new(
                io::ErrorKind::WouldBlock,
                "directory response buffer budget exhausted; release retained directory entries",
            ))
        })?;
    Ok(Bytes::from_owner(DirectoryBuffer {
        data,
        _permit: permit,
    }))
}

/// Decode an XDR component without imposing a text encoding (RFC 7530 §12.8,
/// RFC 8881 §14.4). Length and padding are validated before slicing.
pub(crate) fn decode_name(data: &mut Bytes) -> Result<Bytes> {
    let name = decode_opaque(data)?;
    crate::path_resolution::validate_name(&name)
        .map_err(|_| NfsError::Xdr("invalid READDIR entry name".into()))?;
    Ok(name)
}

pub(crate) fn decode_opaque(data: &mut Bytes) -> Result<Bytes> {
    if data.remaining() < 4 {
        return Err(NfsError::Xdr(
            "directory entry field length truncated".into(),
        ));
    }
    let len = data.get_u32() as usize;
    let padded = len
        .checked_add(3)
        .ok_or_else(|| NfsError::Xdr("directory entry field length overflow".into()))?
        & !3;
    if data.remaining() < padded {
        return Err(NfsError::Xdr("directory entry field truncated".into()));
    }
    let name = data.split_to(len);
    data.advance(padded - len);
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_encoding_names_share_the_response_allocation() {
        let reply =
            Bytes::from_static(b"\0\0\0\x04\xd6\xd0\xce\xc4\0\0\0\x06\xe4\xb8\xad\xe6\x96\x87\0\0");
        let mut page = retain(reply.clone(), reply.len()).unwrap();
        let first = decode_name(&mut page).unwrap();
        let second = decode_name(&mut page).unwrap();
        assert_eq!(first.as_ref(), b"\xd6\xd0\xce\xc4");
        assert_eq!(second.as_ref(), "中文".as_bytes());
        assert_eq!(first.as_ptr(), reply[4..].as_ptr());
        assert_eq!(second.as_ptr(), reply[12..].as_ptr());
        assert!(page.is_empty());
    }

    #[test]
    fn name_with_truncated_padding_is_an_error() {
        let mut data = Bytes::from_static(b"\0\0\0\x01x");
        assert!(matches!(decode_name(&mut data), Err(NfsError::Xdr(_))));
    }

    #[test]
    fn a_retained_name_holds_the_page_credit_until_the_last_view_drops() {
        let budget = Arc::new(Semaphore::new(3));
        let reply = Bytes::from_static(b"\0\0\0\x01x\0\0\0");
        let mut page = retain_with_budget(reply.clone(), reply.len(), &budget).unwrap();
        let name = decode_name(&mut page).unwrap();
        drop(page);
        let Err(error) = retain_with_budget(reply.clone(), reply.len(), &budget) else {
            panic!("a view must retain the page's reservation");
        };
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        drop(name);
        assert!(retain_with_budget(reply.clone(), reply.len(), &budget).is_ok());
    }
}
