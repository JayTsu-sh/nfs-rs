use bytes::{Buf, Bytes};
use futures::stream::TryStreamExt as _;

use super::mount::{Mount41, decode_string_from_bytes};
use crate::error::{NfsError, Result};
use crate::mount;
use crate::nfs4::attrs::{decode_getattr_response, standard_getattr_bitmap};

impl Mount41 {
    async fn directory_limits(&self, tag: &str) -> Result<(u32, u32)> {
        // RPC reply + COMPOUND status/tag/count + SEQUENCE + PUTFH + READDIR status.
        let overhead = 24 + 4 + 4 + ((tag.len() as u32 + 3) & !3) + 4 + 44 + 8 + 8;
        let session = self.session_holder.get().await;
        let maxcount = self
            .maxcount
            .min(session.max_response_size().saturating_sub(overhead));
        if maxcount < 16 {
            return Err(NfsError::InvalidInput(
                "READDIR maxcount cannot hold an empty page".into(),
            ));
        }
        Ok((self.dircount.min(maxcount), maxcount))
    }

    pub(crate) async fn readdir(&self, dir_fh: Bytes) -> mount::ReaddirStream<'_> {
        let this = self;
        Box::pin(
            futures::stream::try_unfold(
                Some((dir_fh, mount::DirectoryCursor::default())),
                move |state| async move {
                    let Some((fh, mut cursor)) = state else {
                        return Ok::<_, NfsError>(None);
                    };
                    let (entries, last_cookie, new_verf, eof) = this
                        .readdir_page(&fh, cursor.cookie, &cursor.verifier)
                        .await?;
                    let next = if cursor.advance(last_cookie, new_verf, entries.len(), eof)? {
                        Some((fh, cursor))
                    } else {
                        None
                    };
                    let page = futures::stream::iter(entries.into_iter().map(Ok::<_, NfsError>));
                    Ok(Some((page, next)))
                },
            )
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
            futures::stream::try_unfold(
                Some((dir_fh, mount::DirectoryCursor::default())),
                move |state| async move {
                    let Some((fh, mut cursor)) = state else {
                        return Ok::<_, NfsError>(None);
                    };
                    let (entries, last_cookie, new_verf, eof) = this
                        .readdirplus_page(&fh, cursor.cookie, &cursor.verifier)
                        .await?;
                    let next = if cursor.advance(last_cookie, new_verf, entries.len(), eof)? {
                        Some((fh, cursor))
                    } else {
                        None
                    };
                    let page = futures::stream::iter(entries.into_iter().map(Ok::<_, NfsError>));
                    Ok(Some((page, next)))
                },
            )
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

        let (dircount, maxcount) = self.directory_limits("readdir").await?;
        let resp = self
            .compound("readdir", |b| {
                b.putfh(fh)
                    .readdir(cookie, cookieverf, dircount, maxcount, &attr_request)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let readdir_op = resp.op_ok(2)?;
        if readdir_op.data.len() > maxcount as usize {
            return Err(NfsError::Xdr(
                "READDIR reply exceeds requested maxcount".into(),
            ));
        }
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

        let (dircount, maxcount) = self.directory_limits("readdirplus").await?;
        let resp = self
            .compound("readdirplus", |b| {
                b.putfh(fh)
                    .readdir(cookie, cookieverf, dircount, maxcount, &attr_request)
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        let readdir_op = resp.op_ok(2)?;
        if readdir_op.data.len() > maxcount as usize {
            return Err(NfsError::Xdr(
                "READDIR reply exceeds requested maxcount".into(),
            ));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs41::test_support::directory_page as page;
    use crate::nfs41::test_support::{ok, putfh, serve};

    #[tokio::test]
    async fn directory_empty_non_eof_is_an_error() {
        let (mount, server) =
            serve(|_, _| vec![ok(22, vec![]), ok(26, page(None, false, [1; 8]))]).await;
        for plus in [false, true] {
            let error = if plus {
                mount.readdirplus(Bytes::new()).await.try_next().await.err()
            } else {
                mount.readdir(Bytes::new()).await.try_next().await.err()
            };
            assert!(
                error.is_some(),
                "non-EOF empty page must not look like complete traversal"
            );
        }
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn directory_repeated_cookie_is_an_error() {
        let (mount, server) =
            serve(|_, _| vec![ok(22, vec![]), ok(26, page(Some(5), false, [1; 8]))]).await;
        let mut entries = mount.readdir(Bytes::new()).await;
        assert!(entries.try_next().await.unwrap().is_some());
        assert!(entries.try_next().await.is_err());
        drop(entries);
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn directory_pagination_echoes_verifier_and_configuration() {
        let mut calls = 0;
        let (mut mount, server) = serve(move |_, mut args| {
            putfh(&mut args);
            assert_eq!(args.get_u32(), 26);
            assert_eq!(args.get_u64(), if calls == 0 { 0 } else { 5 });
            assert_eq!(&args.split_to(8)[..], &[if calls == 0 { 0 } else { 9 }; 8]);
            assert_eq!(args.get_u32(), 64);
            assert_eq!(args.get_u32(), 128);
            calls += 1;
            vec![
                ok(22, vec![]),
                ok(
                    26,
                    page(if calls == 1 { Some(5) } else { None }, calls == 2, [9; 8]),
                ),
            ]
        })
        .await;
        // These fields are the MountArgs URL values; the wire assertion catches
        // implementations that silently substitute hard-coded page sizes.
        mount.dircount = 64;
        mount.maxcount = 128;
        assert_eq!(
            mount
                .readdir(Bytes::new())
                .await
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            1
        );
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }
}
