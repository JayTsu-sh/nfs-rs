//! NFSv4.1 Named Attributes (xattr) implementation.
//!
//! NFSv4.1 exposes extended attributes as files in a per-file "named attribute
//! directory" accessed via OPENATTR. Each attribute is a regular file that can
//! be read/written/removed with standard operations.
//!
//! - getxattr:    PUTFH + OPENATTR + LOOKUP(name) + GETFH  →  PUTFH + READ
//! - setxattr:    PUTFH + OPENATTR + OPEN(name, CREATE) + GETFH  →  WRITE + CLOSE
//! - listxattr:   PUTFH + OPENATTR + READDIR
//! - removexattr: PUTFH + OPENATTR + REMOVE(name)

use bytes::{Buf, Bytes};

use super::compound::OpenArgs;
use super::mount::{Mount41, decode_fh};
use crate::error::{NfsError, Result};

/// Maximum xattr value size we'll read in one request (1 MiB).
const MAX_XATTR_SIZE: u32 = 1024 * 1024;

impl Mount41 {
    /// Get the file handle for the named attribute directory of a file.
    /// Returns the attr dir fh, or an error if the file has no named attributes.
    async fn open_attr_dir(&self, fh: &Bytes, create: bool) -> Result<Bytes> {
        let resp = self
            .compound("openattr", |b| b.putfh(fh).openattr(create).getfh())
            .await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // OPENATTR
        let getfh = resp.op_ok(3)?;
        let mut data = getfh.data.clone();
        decode_fh(&mut data)
    }

    pub(crate) async fn getxattr(&self, fh: Bytes, name: &str) -> Result<Bytes> {
        // Step 1: open attr dir + lookup the named attribute → get its fh
        let resp = self
            .compound("getxattr-lookup", |b| {
                b.putfh(&fh).openattr(false).lookup(name).getfh()
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // OPENATTR
        resp.op_ok(3)?; // LOOKUP
        let getfh = resp.op_ok(4)?;
        let mut data = getfh.data.clone();
        let attr_fh = decode_fh(&mut data)?;

        // Step 2: read the xattr value using anonymous stateid
        let stateid = [0u8; 16];
        let resp = self
            .compound_data("getxattr-read", MAX_XATTR_SIZE as usize, |b| {
                b.putfh(&attr_fh).read(&stateid, 0, MAX_XATTR_SIZE)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let read_op = resp.op_ok(2)?;
        let mut rdata = read_op.data.clone();
        if rdata.remaining() < 4 {
            return Err(NfsError::Xdr("xattr READ result too short".to_string()));
        }
        let _eof = rdata.get_u32();
        if rdata.remaining() < 4 {
            return Err(NfsError::Xdr("xattr READ data length missing".to_string()));
        }
        let data_len = rdata.get_u32() as usize;
        if rdata.remaining() < data_len {
            return Err(NfsError::Xdr("xattr READ data truncated".to_string()));
        }
        Ok(rdata.slice(..data_len))
    }

    pub(crate) async fn setxattr(&self, fh: Bytes, name: &str, value: Bytes) -> Result<()> {
        // Step 1: open attr dir + OPEN(name, CREATE) → get stateid + attr fh
        let open_args = OpenArgs {
            seqid: 0,
            share_access: 0x00000003, // OPEN4_SHARE_ACCESS_BOTH
            share_deny: 0,
            client_id: self.session_holder.get().await.client_id(),
            owner: Bytes::from_static(b"nfs-rs-xattr"),
            create: true,
            create_attrs_mask: vec![],
            create_attrs_vals: vec![],
            claim_file: name.to_string(),
            want_no_delegation: true,
        };
        let resp = self
            .compound("setxattr-open", |b| {
                b.putfh(&fh).openattr(true).open(&open_args).getfh()
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // OPENATTR
        let open_op = resp.op_ok(3)?; // OPEN
        let mut open_data = open_op.data.clone();
        let stateid = super::mount::extract_stateid(&mut open_data)?;
        let getfh = resp.op_ok(4)?;
        let mut fh_data = getfh.data.clone();
        let attr_fh = decode_fh(&mut fh_data)?;

        // Step 2: write the value + close
        let data_len = value.len() as u32;
        let write_result = self
            .compound_write("setxattr-write", value, |b| {
                b.putfh(&attr_fh)
                    .write_header(&stateid, 0, 2 /* FILE_SYNC4 */, data_len)
            })
            .await;

        // Step 3: always close, regardless of write result
        let _ = self
            .compound("setxattr-close", |b| b.putfh(&attr_fh).close(0, &stateid))
            .await;

        write_result?;
        Ok(())
    }

    pub(crate) async fn listxattr(&self, fh: Bytes) -> Result<Vec<String>> {
        // Open the named attribute directory
        let attr_dir_fh = match self.open_attr_dir(&fh, false).await {
            Ok(fh) => fh,
            Err(NfsError::Nfs4(status))
                if status as u32 == 10023 /* NFS4ERR_NOENT: no attr dir */ =>
            {
                return Ok(vec![]);
            }
            Err(e) => return Err(e),
        };

        // READDIR on the attr directory to list all named attributes
        let mut names = Vec::new();
        let mut cookie = 0u64;
        let cookieverf = [0u8; 8];
        // Request only the name (no attrs needed) — use empty bitmap
        let bitmap: [u32; 0] = [];

        loop {
            let resp = self
                .compound("listxattr-readdir", |b| {
                    b.putfh(&attr_dir_fh)
                        .readdir(cookie, &cookieverf, 4096, 32768, &bitmap)
                })
                .await?;
            resp.op_ok(1)?; // PUTFH
            let readdir_op = resp.op_ok(2)?;
            let mut data = readdir_op.data.clone();

            // READDIR4resok: cookieverf(8) + dirlist4(entries + eof)
            if data.remaining() < 8 {
                return Err(NfsError::Xdr(
                    "xattr READDIR cookieverf truncated".to_string(),
                ));
            }
            data.advance(8); // cookieverf

            // Parse entry4 linked list
            let mut last_cookie = cookie;
            loop {
                if data.remaining() < 4 {
                    return Err(NfsError::Xdr(
                        "xattr READDIR entry flag truncated".to_string(),
                    ));
                }
                let has_entry = data.get_u32();
                if has_entry == 0 {
                    break;
                }

                // entry4: cookie(8) + name(utf8str) + attrs(fattr4)
                if data.remaining() < 8 {
                    return Err(NfsError::Xdr("xattr READDIR cookie truncated".to_string()));
                }
                last_cookie = data.get_u64();
                let name = super::mount::decode_string_from_bytes(&mut data)?;
                // Skip fattr4 (bitmap + attr_vals)
                skip_fattr4(&mut data)?;
                // Skip "." and ".." entries
                if name != "." && name != ".." {
                    names.push(name);
                }
            }

            // eof flag
            if data.remaining() < 4 {
                return Err(NfsError::Xdr("xattr READDIR eof truncated".to_string()));
            }
            let eof = data.get_u32() != 0;
            if eof || last_cookie == cookie {
                break;
            }
            cookie = last_cookie;
        }

        Ok(names)
    }

    pub(crate) async fn removexattr(&self, fh: Bytes, name: &str) -> Result<()> {
        let resp = self
            .compound("removexattr", |b| b.putfh(&fh).openattr(false).remove(name))
            .await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // OPENATTR
        resp.op_ok(3)?; // REMOVE
        Ok(())
    }
}

/// Skip over fattr4 in a READDIR response (bitmap + attr_vals opaque).
fn skip_fattr4(buf: &mut Bytes) -> Result<()> {
    // bitmap: length + words
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr("fattr4 bitmap length truncated".to_string()));
    }
    let bitmap_len = buf.get_u32() as usize;
    let bitmap_bytes = bitmap_len
        .checked_mul(4)
        .ok_or_else(|| NfsError::Xdr("fattr4 bitmap overflow".to_string()))?;
    if buf.remaining() < bitmap_bytes {
        return Err(NfsError::Xdr("fattr4 bitmap truncated".to_string()));
    }
    buf.advance(bitmap_bytes);
    // attr_vals opaque
    if buf.remaining() < 4 {
        return Err(NfsError::Xdr(
            "fattr4 attr_vals length truncated".to_string(),
        ));
    }
    let vals_len = buf.get_u32() as usize;
    let padded = (vals_len + 3) & !3;
    if buf.remaining() < padded {
        return Err(NfsError::Xdr("fattr4 attr_vals truncated".to_string()));
    }
    buf.advance(padded);
    Ok(())
}
