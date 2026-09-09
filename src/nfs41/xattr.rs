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

use bytes::{Buf, Bytes, BytesMut};
use futures::TryStreamExt;
use std::sync::Arc;

use super::compound::OpenArgs;
use super::mount::{Mount41, decode_fh};
use crate::error::{
    FileCloseFailure, NfsError, OperationClass, OperationOutcome, OperationOutcomeError,
    RecoveryAction, RequestContext, Result,
};
use crate::nfs4::fastxdr::nfsstat4;

fn xattr_uncertain(error: NfsError, completed: u64) -> NfsError {
    NfsError::OperationOutcome(Box::new(
        OperationOutcomeError::new(
            OperationOutcome::Uncertain,
            OperationClass::ReplaySensitive,
            RecoveryAction::VerifyThenResume,
            RequestContext {
                operation: "setxattr".into(),
                protocol: crate::NFSVersion::NFSv4p1,
                request_id: None,
            },
            error,
        )
        .with_completed_bytes(completed),
    ))
}

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

        // RFC 5661 §18.22: a short READ does not imply EOF. The negotiated
        // read size limits each request, not the complete named attribute value.
        let mut value = BytesMut::new();
        loop {
            let count = self.rsize;
            let offset = value.len() as u64;
            let resp = self
                .compound_data("getxattr-read", count as usize, |b| {
                    b.putfh(&attr_fh).read(&[0; 16], offset, count)
                })
                .await?;
            resp.op_ok(1)?;
            let mut data = resp.op_ok(2)?.data.clone();
            if data.remaining() < 8 {
                return Err(NfsError::Xdr("xattr READ result truncated".into()));
            }
            let eof = data.get_u32() != 0;
            let len = data.get_u32() as usize;
            if len > count as usize || data.remaining() < len {
                return Err(NfsError::Xdr(
                    "xattr READ returned an invalid length".into(),
                ));
            }
            value.extend_from_slice(&data[..len]);
            if eof {
                return Ok(value.freeze());
            }
            if len == 0 {
                return Err(NfsError::Rpc(
                    "xattr READ made no progress before EOF".into(),
                ));
            }
        }
    }

    pub(crate) async fn setxattr(&self, fh: Bytes, name: &str, value: Bytes) -> Result<()> {
        // UNCHECKED4 only truncates an existing attribute when SIZE=0 is explicit.
        // RFC 5661 §18.16.3. Empty values must also execute this OPEN.
        // Step 1: open attr dir + OPEN(name, CREATE) → get stateid + attr fh
        let open_args = OpenArgs {
            seqid: 0,
            share_access: 0x00000003, // OPEN4_SHARE_ACCESS_BOTH
            share_deny: 0,
            client_id: self.session_holder.get().await.client_id(),
            owner: Bytes::from_static(b"nfs-rs-xattr"),
            create: true,
            create_attrs_mask: vec![1 << 4],
            create_attrs_vals: 0u64.to_be_bytes().to_vec(),
            claim_file: name.to_string(),
            want_no_delegation: true,
        };
        let resp = self
            .compound_preserving_progress("setxattr-open", 3, |b| {
                b.putfh(&fh).openattr(true).open(&open_args).getfh()
            })
            .await?;
        resp.op_ok(1)?; // PUTFH
        resp.op_ok(2)?; // OPENATTR
        let open_op = resp.op_ok(3)?; // OPEN
        // OPEN has already created/truncated the value, even when GETFH fails.
        let (stateid, attr_fh) = (|| {
            let mut open_data = open_op.data.clone();
            let stateid = super::mount::extract_stateid(&mut open_data)?;
            let getfh = resp.op_ok(4)?;
            let mut fh_data = getfh.data.clone();
            Ok((stateid, decode_fh(&mut fh_data)?))
        })()
        .map_err(|error| xattr_uncertain(error, 0))?;

        let generation = resp.session_generation;
        let mut completed = 0usize;
        let write_result: Result<()> = async {
            while completed < value.len() {
                let end = completed + (value.len() - completed).min(self.wsize as usize);
                let chunk = value.slice(completed..end);
                let count = chunk.len() as u32;
                let response = self
                    .compound_write("setxattr-write", chunk, |b| {
                        b.require_generation(generation)
                            .putfh(&attr_fh)
                            .write_header(&stateid, completed as u64, 2, count)
                    })
                    .await?;
                response.op_ok(1)?;
                let mut data = response.op_ok(2)?.data.clone();
                if data.remaining() < 16 {
                    return Err(NfsError::Xdr("xattr WRITE result truncated".into()));
                }
                let written = data.get_u32() as usize;
                let committed = data.get_u32();
                if written == 0 || written > count as usize || committed != 2 {
                    return Err(NfsError::Rpc(
                        "xattr WRITE returned invalid count or stability".into(),
                    ));
                }
                completed += written;
            }
            Ok(())
        }
        .await;
        // Preserve cleanup failures as well as the primary write failure.
        let close_result = self
            .compound("setxattr-close", |b| {
                b.require_generation(generation)
                    .putfh(&attr_fh)
                    .close(0, &stateid)
            })
            .await;
        let mut failures = Vec::new();
        if let Err(error) = write_result {
            failures.push(FileCloseFailure {
                operation: "write",
                error: Arc::new(error),
            });
        }
        if let Err(error) = close_result {
            failures.push(FileCloseFailure {
                operation: "close",
                error: Arc::new(error),
            });
        }
        if failures.is_empty() {
            Ok(())
        } else {
            // OPEN has already truncated/created the value. Even a definite
            // later failure cannot be advertised as leaving the old value intact.
            Err(xattr_uncertain(
                NfsError::FileClose(failures),
                completed as u64,
            ))
        }
    }

    pub(crate) async fn listxattr(&self, fh: Bytes) -> Result<Vec<String>> {
        let attr_dir_fh = match self.open_attr_dir(&fh, false).await {
            Ok(fh) => fh,
            Err(NfsError::Nfs4(nfsstat4::NFS4ERR_NOENT)) => return Ok(vec![]),
            Err(error) => return Err(error),
        };
        let mut entries = self.readdir(attr_dir_fh).await;
        let mut names = Vec::new();
        while let Some(entry) = entries.try_next().await? {
            if entry.file_name != "." && entry.file_name != ".." {
                names.push(entry.file_name);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nfs4::compound::{take_opaque, xdr_u32};
    use crate::nfs41::test_support::{Reply, ok, opaque, putfh, serve};
    use std::sync::{Arc, Mutex};

    async fn attribute_server(
        initial: Vec<u8>,
        close_error: bool,
        short: usize,
    ) -> (Mount41, tokio::task::JoinHandle<()>, Arc<Mutex<Vec<u8>>>) {
        let stored = Arc::new(Mutex::new(initial));
        let contents = stored.clone();
        let (mount, server) = serve(move |tag, mut args| {
            putfh(&mut args);
            match tag {
                "setxattr-open" => {
                    assert_eq!(args.get_u32(), 20);
                    args.advance(4); // createdir
                    assert_eq!(args.get_u32(), 18);
                    args.advance(20); // seqid, share access/deny, clientid
                    take_opaque(&mut args, "owner").unwrap();
                    assert_eq!(args.get_u32(), 1); // CREATE
                    assert_eq!(args.get_u32(), 0); // UNCHECKED
                    let words = args.get_u32();
                    let mask = if words > 0 { args.get_u32() } else { 0 };
                    for _ in 1..words {
                        args.advance(4);
                    }
                    let mut attrs = take_opaque(&mut args, "attrs").unwrap();
                    if mask & (1 << 4) != 0 {
                        assert_eq!(attrs.get_u64(), 0);
                        contents.lock().unwrap().clear();
                    }
                    let mut open = vec![0; 48];
                    open[..16].fill(7);
                    vec![
                        ok(22, vec![]),
                        ok(20, vec![]),
                        ok(18, open),
                        ok(10, opaque(b"attr")),
                    ]
                }
                "getxattr-lookup" => vec![
                    ok(22, vec![]),
                    ok(20, vec![]),
                    ok(15, vec![]),
                    ok(10, opaque(b"attr")),
                ],
                "setxattr-write" => {
                    assert_eq!(args.get_u32(), 38);
                    args.advance(16);
                    let offset = args.get_u64() as usize;
                    assert_eq!(args.get_u32(), 2); // FILE_SYNC
                    let payload = take_opaque(&mut args, "write").unwrap();
                    let count = payload.len().min(short); // legal short write
                    let mut value = contents.lock().unwrap();
                    let end = offset + count;
                    if value.len() < end {
                        value.resize(end, 0);
                    }
                    value[offset..end].copy_from_slice(&payload[..count]);
                    let mut reply = Vec::new();
                    xdr_u32(&mut reply, count as u32);
                    xdr_u32(&mut reply, 2);
                    reply.extend([1; 8]);
                    vec![ok(22, vec![]), ok(38, reply)]
                }
                "getxattr-read" => {
                    assert_eq!(args.get_u32(), 25);
                    args.advance(16);
                    let offset = args.get_u64() as usize;
                    let count = args.get_u32() as usize;
                    let value = contents.lock().unwrap();
                    let start = offset.min(value.len());
                    let end = (start + count.min(short)).min(value.len());
                    let mut reply = Vec::new();
                    xdr_u32(&mut reply, u32::from(end == value.len()));
                    reply.extend(opaque(&value[start..end]));
                    vec![ok(22, vec![]), ok(25, reply)]
                }
                "setxattr-close" => vec![
                    ok(22, vec![]),
                    if close_error {
                        Reply {
                            opcode: 4,
                            status: 5,
                            data: vec![],
                        }
                    } else {
                        ok(4, vec![7; 16])
                    },
                ],
                _ => panic!("unexpected compound {tag}"),
            }
        })
        .await;
        (mount, server, stored)
    }

    #[tokio::test]
    async fn xattr_short_io_and_replacement_preserve_exact_value() {
        let (mount, server, stored) = attribute_server(b"old-long-value".to_vec(), false, 2).await;
        for value in [b"abcdef".as_slice(), b"xy", b""] {
            mount
                .setxattr(
                    Bytes::from_static(b"file"),
                    "key",
                    Bytes::copy_from_slice(value),
                )
                .await
                .unwrap();
            assert_eq!(stored.lock().unwrap().as_slice(), value);
            assert_eq!(
                mount
                    .getxattr(Bytes::from_static(b"file"), "key")
                    .await
                    .unwrap()
                    .as_ref(),
                value
            );
        }
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn xattr_get_continues_short_non_eof_reads() {
        let (mount, server, _) = attribute_server(b"abcdef".to_vec(), false, 2).await;
        assert_eq!(
            mount.getxattr(Bytes::new(), "key").await.unwrap().as_ref(),
            b"abcdef"
        );
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn xattr_open_progress_preserves_uncertainty_without_retry() {
        // A failed OPEN leaves the value intact; failed GETFH follows truncation.
        // DELAY after OPEN must not cause the whole mutation to be retried.
        for (failed_opcode, status) in [(18, 13), (10, 5), (10, 10008)] {
            let stored = Arc::new(Mutex::new(b"old-value".to_vec()));
            let contents = stored.clone();
            let mut calls = 0;
            let (mount, server) = serve(move |tag, mut args| {
                calls += 1;
                assert_eq!(calls, 1, "must not replay a successful truncating OPEN");
                assert_eq!(tag, "setxattr-open");
                putfh(&mut args);
                assert_eq!(args.get_u32(), 20);
                args.advance(4);
                assert_eq!(args.get_u32(), 18);
                args.advance(20);
                take_opaque(&mut args, "owner").unwrap();
                assert_eq!(args.get_u32(), 1); // CREATE
                assert_eq!(args.get_u32(), 0); // UNCHECKED
                assert_eq!(args.get_u32(), 1); // bitmap length
                assert_eq!(args.get_u32(), 1 << 4); // SIZE
                assert_eq!(take_opaque(&mut args, "attrs").unwrap().get_u64(), 0);
                let mut replies = vec![ok(22, vec![]), ok(20, vec![])];
                if failed_opcode == 10 {
                    contents.lock().unwrap().clear();
                    replies.push(ok(18, vec![0; 48]));
                }
                replies.push(Reply {
                    opcode: failed_opcode,
                    status,
                    data: vec![],
                });
                replies
            })
            .await;
            let error = mount
                .setxattr(Bytes::new(), "key", Bytes::from_static(b"new"))
                .await
                .unwrap_err();
            if failed_opcode == 18 {
                assert!(matches!(error, NfsError::Nfs4(nfsstat4::NFS4ERR_ACCESS)));
                assert_eq!(stored.lock().unwrap().as_slice(), b"old-value");
            } else {
                let outcome = error.operation_outcome().unwrap();
                assert_eq!(outcome.outcome, OperationOutcome::Uncertain);
                assert_eq!(outcome.recovery, RecoveryAction::VerifyThenResume);
                assert_eq!(
                    outcome.transmission,
                    crate::error::RequestTransmission::Sent
                );
                assert_eq!(outcome.completed_bytes, Some(0));
                assert_eq!(outcome.context().operation, "setxattr");
                assert!(
                    matches!(outcome.source.as_ref(), NfsError::Nfs4(code) if *code as u32 == status)
                );
                assert!(stored.lock().unwrap().is_empty());
            }
            mount.rpc.shutdown().await;
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn xattr_close_failure_is_returned() {
        let (mount, server, _) = attribute_server(vec![], true, 2).await;
        assert!(
            mount
                .setxattr(Bytes::new(), "key", Bytes::from_static(b"xy"))
                .await
                .is_err()
        );
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }
    #[tokio::test]
    async fn xattr_values_above_one_mib_roundtrip_with_short_io() {
        let initial: Vec<u8> = (0..1024 * 1024 + 1).map(|i| (i % 251) as u8).collect();
        let (mut mount, server, stored) = attribute_server(initial.clone(), false, 1536).await;
        mount.rsize = 2048;
        mount.wsize = 3072;
        assert_eq!(
            mount.getxattr(Bytes::new(), "key").await.unwrap().as_ref(),
            initial.as_slice()
        );
        let replacement = Bytes::from(initial.into_iter().rev().collect::<Vec<_>>());
        mount
            .setxattr(Bytes::new(), "key", replacement.clone())
            .await
            .unwrap();
        assert_eq!(stored.lock().unwrap().as_slice(), replacement.as_ref());
        assert_eq!(
            mount.getxattr(Bytes::new(), "key").await.unwrap(),
            replacement
        );
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn xattr_listing_handles_noent_and_preserves_page_verifier() {
        let (mount, server) = serve(|_, _| {
            vec![
                ok(22, vec![]),
                Reply {
                    opcode: 20,
                    status: 2,
                    data: vec![],
                },
            ]
        })
        .await;
        assert!(mount.listxattr(Bytes::new()).await.unwrap().is_empty());
        mount.rpc.shutdown().await;
        server.await.unwrap();

        let mut pages = 0;
        let (mount, server) = serve(move |tag, mut args| {
            if tag == "openattr" {
                return vec![ok(22, vec![]), ok(20, vec![]), ok(10, opaque(b"attrs"))];
            }
            assert_eq!(tag, "readdir");
            putfh(&mut args);
            assert_eq!(args.get_u32(), 26);
            assert_eq!(args.get_u64(), if pages == 0 { 0 } else { 5 });
            assert_eq!(&args.split_to(8)[..], &[if pages == 0 { 0 } else { 7 }; 8]);
            pages += 1;
            vec![
                ok(22, vec![]),
                ok(
                    26,
                    crate::nfs41::test_support::directory_page(
                        if pages == 1 { Some(5) } else { None },
                        pages == 2,
                        [7; 8],
                    ),
                ),
            ]
        })
        .await;
        assert_eq!(mount.listxattr(Bytes::new()).await.unwrap(), ["entry"]);
        mount.rpc.shutdown().await;
        server.await.unwrap();
    }
    #[tokio::test]
    async fn xattr_zero_progress_preserves_write_and_close_errors() {
        let (mount, server, _) = attribute_server(b"old".to_vec(), false, 0).await;
        assert!(mount.getxattr(Bytes::new(), "key").await.is_err());
        mount.rpc.shutdown().await;
        server.await.unwrap();
        for close_error in [false, true] {
            let (mount, server, stored) = attribute_server(b"old".to_vec(), close_error, 0).await;
            let error = mount
                .setxattr(Bytes::new(), "key", Bytes::from_static(b"new"))
                .await
                .unwrap_err();
            let outcome = error.operation_outcome().unwrap();
            assert_eq!(outcome.outcome, OperationOutcome::Uncertain);
            assert_eq!(outcome.completed_bytes, Some(0));
            let NfsError::FileClose(failures) = outcome.source.as_ref() else {
                panic!("phase failures missing");
            };
            assert_eq!(failures.len(), if close_error { 2 } else { 1 });
            assert_eq!(failures[0].operation, "write");
            if close_error {
                assert_eq!(failures[1].operation, "close");
            }
            assert!(
                stored.lock().unwrap().is_empty(),
                "OPEN has already truncated the value"
            );
            mount.rpc.shutdown().await;
            server.await.unwrap();
        }
    }
}
