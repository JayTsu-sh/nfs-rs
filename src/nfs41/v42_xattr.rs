//! RFC 8276 xattrs, distinct from the OPENATTR namespace.
use super::mount::Mount41;
use super::v42_ops::{decode_bool, decode_u64, opaque, result_data, word};
use crate::{NfsError, Result, XattrSetMode};
use bytes::Bytes;
use std::collections::HashSet;

pub(super) fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.contains('\0') || name.len() > u32::MAX as usize {
        return Err(NfsError::InvalidInput(
            "xattr name must be nonempty text without NUL".into(),
        ));
    }
    Ok(())
}

pub(super) fn decode_list(buf: &mut Bytes) -> Result<(u64, Vec<String>, bool)> {
    let cookie = decode_u64(buf)?;
    let count = word(buf)? as usize;
    if count > 65536 || count > buf.len().saturating_sub(4) / 4 {
        return Err(NfsError::Xdr(
            "LISTXATTRS excessive/truncated name array".into(),
        ));
    }
    let mut names = Vec::with_capacity(count);
    for _ in 0..count {
        let raw = opaque(buf)?;
        let name = std::str::from_utf8(&raw)
            .map_err(|_| NfsError::Xdr("LISTXATTRS key is not UTF-8".into()))?;
        if name.is_empty() || name.contains('\0') {
            return Err(NfsError::Xdr("LISTXATTRS invalid key".into()));
        }
        names.push(name.to_owned());
    }
    Ok((cookie, names, decode_bool(buf)?))
}

impl Mount41 {
    async fn require_xattrs(&self, fh: &Bytes) -> Result<()> {
        if !self.v42_attributes(fh.clone()).await?.xattr_support {
            return Err(NfsError::Unsupported(
                "filesystem does not advertise RFC 8276 xattr_support".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn v42_getxattr(&self, fh: Bytes, name: &str) -> Result<Bytes> {
        validate_name(name)?;
        self.require_xattrs(&fh).await?;
        let response = self
            .compound("getxattr42", |b| b.putfh(&fh).getxattr(name))
            .await?;
        opaque(&mut result_data(&response, 2, 72)?)
    }

    pub(super) async fn v42_setxattr(
        &self,
        fh: Bytes,
        name: &str,
        value: Bytes,
        mode: XattrSetMode,
    ) -> Result<()> {
        validate_name(name)?;
        if value.len() > crate::mount::MAX_IO_SIZE as usize {
            return Err(NfsError::InvalidInput(
                "xattr exceeds client payload ceiling".into(),
            ));
        }
        self.require_xattrs(&fh).await?;
        let response = self
            .compound("setxattr42", |b| b.putfh(&fh).setxattr(name, &value, mode))
            .await?;
        result_data(&response, 2, 73).map_err(|e| self.mutation_error("setxattr", e))?;
        Ok(())
    }

    pub(super) async fn v42_listxattrs(&self, fh: Bytes) -> Result<Vec<String>> {
        self.require_xattrs(&fh).await?;
        // maxcount includes all LISTXATTRS4resok XDR overhead (RFC 8276 §8.4.3).
        let maximum = self.rsize.min(4096);
        if maximum < 16 {
            return Err(NfsError::Rpc("channel too small for LISTXATTRS".into()));
        }
        let mut cookie = 0;
        let mut seen = HashSet::from([0]);
        let mut names = Vec::new();
        loop {
            let reply = self
                .compound("listxattrs42", |b| b.putfh(&fh).listxattrs(cookie, maximum))
                .await?;
            let mut data = result_data(&reply, 2, 74)?;
            let before = data.len();
            let (next, page, eof) = decode_list(&mut data)?;
            if before - data.len() > maximum as usize
                || names.len().saturating_add(page.len()) > 65536
            {
                return Err(NfsError::Xdr(
                    "LISTXATTRS exceeded reply/entry bound".into(),
                ));
            }
            if !eof && (page.is_empty() || !seen.insert(next)) {
                return Err(NfsError::Xdr(
                    "LISTXATTRS made no progress or repeated cookie".into(),
                ));
            }
            names.extend(page);
            if eof {
                return Ok(names);
            }
            cookie = next;
        }
    }

    pub(super) async fn v42_removexattr(&self, fh: Bytes, name: &str) -> Result<()> {
        validate_name(name)?;
        self.require_xattrs(&fh).await?;
        let reply = self
            .compound("removexattr42", |b| b.putfh(&fh).removexattr(name))
            .await?;
        result_data(&reply, 2, 75).map_err(|e| self.mutation_error("removexattr", e))?;
        Ok(())
    }
}
