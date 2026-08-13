use bytes::{Buf, Bytes};
use futures::stream::TryStreamExt as _;

use super::mount::{Mount41, decode_string_from_bytes};
use crate::error::{NfsError, Result};
use crate::mount;
use crate::nfs4::attrs::{decode_getattr_response, standard_getattr_bitmap};

impl Mount41 {
    pub(crate) async fn readdir(&self, dir_fh: Bytes) -> mount::ReaddirStream<'_> {
        let this = self;
        Box::pin(
            futures::stream::try_unfold(Some((dir_fh, 0u64, [0u8; 8])), move |state| async move {
                let Some((fh, cookie, verf)) = state else {
                    return Ok::<_, NfsError>(None);
                };
                let (entries, last_cookie, new_verf, eof) =
                    this.readdir_page(&fh, cookie, &verf).await?;
                // H7: detect no-progress to avoid infinite loop
                if entries.is_empty() && !eof && last_cookie == cookie {
                    return Ok(None);
                }
                let next = if eof {
                    None
                } else {
                    Some((fh, last_cookie, new_verf))
                };
                let page = futures::stream::iter(entries.into_iter().map(Ok::<_, NfsError>));
                Ok(Some((page, next)))
            })
            .try_flatten(),
        )
    }

    pub(crate) async fn readdir_path(&self, dir_path: &str) -> Result<mount::ReaddirStream<'_>> {
        let obj = self.lookup_path(dir_path).await?;
        Ok(self.readdir(obj.fh).await)
    }

    pub(crate) async fn readdirplus(&self, dir_fh: Bytes) -> mount::ReaddirplusStream<'_> {
        let this = self;
        Box::pin(
            futures::stream::try_unfold(Some((dir_fh, 0u64, [0u8; 8])), move |state| async move {
                let Some((fh, cookie, verf)) = state else {
                    return Ok::<_, NfsError>(None);
                };
                let (entries, last_cookie, new_verf, eof) =
                    this.readdirplus_page(&fh, cookie, &verf).await?;
                // H7: detect no-progress to avoid infinite loop
                if entries.is_empty() && !eof && last_cookie == cookie {
                    return Ok(None);
                }
                let next = if eof {
                    None
                } else {
                    Some((fh, last_cookie, new_verf))
                };
                let page = futures::stream::iter(entries.into_iter().map(Ok::<_, NfsError>));
                Ok(Some((page, next)))
            })
            .try_flatten(),
        )
    }

    pub(crate) async fn readdirplus_path(
        &self,
        dir_path: &str,
    ) -> Result<mount::ReaddirplusStream<'_>> {
        let obj = self.lookup_path(dir_path).await?;
        Ok(self.readdirplus(obj.fh).await)
    }

    async fn readdir_page(
        &self,
        fh: &Bytes,
        cookie: u64,
        cookieverf: &[u8; 8],
    ) -> Result<(Vec<mount::ReaddirEntry>, u64, [u8; 8], bool)> {
        // Request fileid attribute for each entry (NFSv4.1: attr #20 = word 0, bit 20)
        let attr_request = [1u32 << 20];

        let resp = self
            .compound("readdir", |b| {
                b.putfh(fh)
                    .readdir(cookie, cookieverf, 8192, 32768, &attr_request)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let readdir_op = resp.op_ok(2)?;
        let mut data = readdir_op.data.clone();

        // READDIR4resok: cookieverf(8) + dirlist4
        if data.remaining() < 8 {
            return Err(NfsError::Xdr("READDIR cookieverf truncated".to_string()));
        }
        let mut new_verf = [0u8; 8];
        data.copy_to_slice(&mut new_verf);

        // dirlist4: linked list of entry4, then eof
        let mut entries = Vec::new();
        let mut last_cookie = cookie;
        loop {
            if data.remaining() < 4 {
                return Err(NfsError::Xdr(
                    "dirlist4 value_follows truncated".to_string(),
                ));
            }
            let has_entry = data.get_u32();
            if has_entry == 0 {
                break;
            }
            // entry4: cookie(8) + name(var) + attrs(fattr4)
            if data.remaining() < 8 {
                return Err(NfsError::Xdr("entry4 cookie truncated".to_string()));
            }
            let entry_cookie = data.get_u64();
            last_cookie = entry_cookie; // track for pagination
            let name = decode_string_from_bytes(&mut data)?;
            // Decode attrs to extract fileid (NFSv4.1: attr #20)
            let attr = decode_entry_fattr4(&mut data).ok();
            let fileid = attr.as_ref().map(|a| a.fileid).unwrap_or(entry_cookie);

            entries.push(mount::ReaddirEntry {
                fileid,
                file_name: name,
            });
        }

        // eof
        if data.remaining() < 4 {
            return Err(NfsError::Xdr("dirlist4 eof truncated".to_string()));
        }
        let eof = data.get_u32() != 0;

        Ok((entries, last_cookie, new_verf, eof))
    }

    async fn readdirplus_page(
        &self,
        fh: &Bytes,
        cookie: u64,
        cookieverf: &[u8; 8],
    ) -> Result<(Vec<mount::ReaddirplusEntry>, u64, [u8; 8], bool)> {
        let attr_request = standard_getattr_bitmap();

        let resp = self
            .compound("readdirplus", |b| {
                b.putfh(fh)
                    .readdir(cookie, cookieverf, 8192, 32768, &attr_request)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let readdir_op = resp.op_ok(2)?;
        let mut data = readdir_op.data.clone();

        if data.remaining() < 8 {
            return Err(NfsError::Xdr("READDIR cookieverf truncated".to_string()));
        }
        let mut new_verf = [0u8; 8];
        data.copy_to_slice(&mut new_verf);

        let mut entries = Vec::new();
        let mut last_cookie = cookie;
        loop {
            if data.remaining() < 4 {
                return Err(NfsError::Xdr(
                    "dirlist4 value_follows truncated".to_string(),
                ));
            }
            let has_entry = data.get_u32();
            if has_entry == 0 {
                break;
            }
            if data.remaining() < 8 {
                return Err(NfsError::Xdr("entry4 cookie truncated".to_string()));
            }
            let entry_cookie = data.get_u64();
            last_cookie = entry_cookie;
            let name = decode_string_from_bytes(&mut data)?;
            let attr = match decode_entry_fattr4(&mut data) {
                Ok(a) => Some(a),
                Err(e) => {
                    tracing::warn!(
                        "readdirplus: failed to decode attrs for entry '{}': {}",
                        name,
                        e
                    );
                    None
                }
            };
            let fileid = attr.as_ref().map(|a| a.fileid).unwrap_or(entry_cookie);
            // NFSv4.1 FATTR4_FILEHANDLE (attr 19) provides per-entry file handles
            // when requested in the READDIR attr bitmap.
            let handle = attr
                .as_ref()
                .map(|a| a.filehandle.clone())
                .unwrap_or_default();
            entries.push(mount::ReaddirplusEntry {
                fileid,
                file_name: name,
                attr,
                handle,
            });
        }

        if data.remaining() < 4 {
            return Err(NfsError::Xdr("dirlist4 eof truncated".to_string()));
        }
        let eof = data.get_u32() != 0;

        Ok((entries, last_cookie, new_verf, eof))
    }
}

fn decode_entry_fattr4(data: &mut Bytes) -> Result<mount::Attr> {
    decode_getattr_response(data)
}
