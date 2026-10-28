//! Native NFSACL v3 (RPC program 100227), independent of Linux xattr encoding.
//!
//! Wire layout follows Linux v6.12 fs/nfsd/nfs3acl.c and
//! fs/nfs_common/nfsacl.c. Each ACL is a count followed by an XDR array
//! of tag/id/permission triples; default entries carry tag bit 0x1000.
//! This Sun extension has no NFS RFC; see
//! <https://github.com/torvalds/linux/blob/v6.12/fs/nfsd/nfs3acl.c> and
//! <https://github.com/torvalds/linux/blob/v6.12/fs/nfs_common/nfsacl.c>.
use super::{METADATA_TIMEOUT, NFS_REPLAY, rpc_header, xdr_u32, xdr_var_bytes};
use crate::error::classify_sent_nfs3_error;
use crate::{
    Auth, NfsError, OperationClass, PosixAcl, PosixAclEntry, RequestContext, Result, SocketAddr,
    rpc,
};
use bytes::{Buf, Bytes};
use std::io::{Error as IoError, ErrorKind};

const PROGRAM: u32 = rpc::NFSACL_PROG;
const VERSION: u32 = 3;
const FULL_MASK: u32 = 15;
const DEFAULT_TAG: u32 = 0x1000;
const MAX_ENTRIES: usize = 1024;

#[derive(Debug)]
pub(super) struct Client {
    rpc: rpc::Client,
    auth: Auth,
}

impl Client {
    pub(super) async fn discover(
        addr: SocketAddr,
        rpc: &rpc::Client,
        auth: &Auth,
        noresvport: bool,
    ) -> Option<Self> {
        // Linux normally multiplexes NFSACL on the NFS socket. Probe it there
        // first, then resolve a dedicated TCP port for other implementations.
        // Failed optional discovery must not prevent mounting ordinary NFS.
        let discovery = async {
            let direct = Self {
                rpc: rpc.clone(),
                auth: auth.clone(),
            };
            if direct.probe().await.is_ok() {
                return Some(direct);
            }
            let mut mapper_addr = addr;
            mapper_addr.set_port(rpc::PORTMAP_PORT);
            let port = rpc::portmap(
                &vec![mapper_addr],
                PROGRAM,
                VERSION,
                auth,
                super::MOUNT_RETRIES,
                noresvport,
            )
            .await
            .ok()?;
            if port == addr.port() {
                return None;
            }
            let mut acl_addr = addr;
            acl_addr.set_port(port);
            let mux = rpc::StreamMux::connect(acl_addr, noresvport).await.ok()?;
            let client = Self {
                rpc: rpc::Client::new(mux, None),
                auth: auth.clone(),
            };
            client.probe().await.ok()?;
            Some(client)
        };
        match tokio::time::timeout(METADATA_TIMEOUT, discovery).await {
            Ok(result) => result,
            Err(_) => {
                tracing::debug!("optional NFSACL v3 discovery timed out");
                None
            }
        }
    }

    async fn probe(&self) -> Result<()> {
        let mut request = Vec::new();
        rpc_header(PROGRAM, VERSION, 0, &self.auth).encode(&mut request);
        let reply = self
            .rpc
            .call(request, rpc::ReplayPolicy::ONE_ATTEMPT, METADATA_TIMEOUT)
            .await?;
        if !reply.is_empty() {
            return Err(NfsError::Xdr("NFSACL NULL returned unexpected data".into()));
        }
        Ok(())
    }

    fn request(&self, procedure: u32, fh: &Bytes) -> Result<Vec<u8>> {
        if fh.is_empty() || fh.len() > 64 {
            return Err(NfsError::InvalidInput(
                "NFSACL v3 file handle must contain 1..=64 bytes".into(),
            ));
        }
        let mut request = Vec::new();
        rpc_header(PROGRAM, VERSION, procedure, &self.auth).encode(&mut request);
        xdr_var_bytes(&mut request, fh);
        xdr_u32(&mut request, FULL_MASK);
        Ok(request)
    }

    async fn call(&self, request: Vec<u8>, write: bool) -> Result<Bytes> {
        let class = if write {
            OperationClass::ReplaySensitive
        } else {
            OperationClass::ReadOnly
        };
        let replay = if write {
            rpc::ReplayPolicy::ONE_ATTEMPT
        } else {
            NFS_REPLAY
        };
        self.rpc
            .call(request, replay, METADATA_TIMEOUT)
            .await
            .map_err(|error| classify_sent_nfs3_error(class, context(write), error))
    }

    pub(super) async fn get(&self, fh: Bytes) -> Result<PosixAcl> {
        let mut reply = self.call(self.request(1, &fh)?, false).await?;
        decode_get(&mut reply).map_err(|error| {
            classify_sent_nfs3_error(OperationClass::ReadOnly, context(false), error)
        })
    }

    pub(super) async fn set(&self, fh: Bytes, acl: &PosixAcl) -> Result<()> {
        let mut request = self.request(2, &fh)?;
        encode_acl(&mut request, &acl.access, false)?;
        encode_acl(&mut request, &acl.default, true)?;
        let mut reply = self.call(request, true).await?;
        let result = (|| {
            decode_status(&mut reply)?;
            skip_attrs(&mut reply)?;
            ensure_empty(&reply)
        })();
        result.map_err(|error| {
            classify_sent_nfs3_error(OperationClass::ReplaySensitive, context(true), error)
        })
    }
}

fn context(write: bool) -> RequestContext {
    RequestContext {
        operation: if write {
            "set_posix_acl"
        } else {
            "get_posix_acl"
        }
        .into(),
        protocol: crate::NFSVersion::NFSv3,
        request_id: None,
    }
}

fn read_u32(bytes: &mut Bytes) -> Result<u32> {
    if bytes.remaining() < 4 {
        return Err(NfsError::Xdr("truncated NFSACL v3 response".into()));
    }
    Ok(bytes.get_u32())
}

fn skip_attrs(bytes: &mut Bytes) -> Result<()> {
    match read_u32(bytes)? {
        0 => Ok(()),
        1 if bytes.remaining() >= 84 => {
            bytes.advance(84);
            Ok(())
        }
        1 => Err(NfsError::Xdr("truncated NFSACL post_op_attr".into())),
        _ => Err(NfsError::Xdr("invalid NFSACL post_op_attr boolean".into())),
    }
}

fn decode_status(bytes: &mut Bytes) -> Result<()> {
    let status = super::nfsstat3::try_from(bytes).map_err(|error| {
        // Keep the concrete generated decoder error available through get_ref().
        // InvalidData is still classified as a non-authoritative reply, so GET
        // remains SafeToRetry and SET remains Uncertain at the operation boundary.
        NfsError::Io(IoError::new(ErrorKind::InvalidData, error))
    })?;
    match status {
        super::nfsstat3::NFS3_OK => Ok(()),
        super::nfsstat3::NFS3ERR_NOTSUPP => Err(NfsError::Unsupported(
            "NFSACL v3 is unsupported for this object".into(),
        )),
        error => Err(NfsError::Nfs3(error)),
    }
}

fn ensure_empty(bytes: &Bytes) -> Result<()> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(NfsError::Xdr("trailing NFSACL response data".into()))
    }
}

fn decode_get(bytes: &mut Bytes) -> Result<PosixAcl> {
    decode_status(bytes)?;
    skip_attrs(bytes)?;
    if read_u32(bytes)? != FULL_MASK {
        return Err(NfsError::Xdr(
            "NFSACL GETACL response does not contain the complete requested ACL mask".into(),
        ));
    }
    let acl = PosixAcl {
        access: decode_acl(bytes, false)?,
        default: decode_acl(bytes, true)?,
    };
    ensure_empty(bytes)?;
    Ok(acl)
}

fn valid_entry(entry: &PosixAclEntry) -> bool {
    matches!(entry.tag, 1 | 2 | 4 | 8 | 16 | 32) && entry.permissions <= 7
}

fn decode_acl(bytes: &mut Bytes, default: bool) -> Result<Vec<PosixAclEntry>> {
    let count = read_u32(bytes)? as usize;
    let array_count = read_u32(bytes)? as usize;
    if count != array_count || count > MAX_ENTRIES || count > bytes.remaining() / 12 {
        return Err(NfsError::Xdr(
            "NFSACL invalid or truncated ACL count/array".into(),
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let raw_tag = read_u32(bytes)?;
        if (raw_tag & DEFAULT_TAG != 0) != default {
            return Err(NfsError::Xdr(
                "NFSACL access/default entry flag mismatch".into(),
            ));
        }
        let entry = PosixAclEntry {
            tag: raw_tag & !DEFAULT_TAG,
            id: read_u32(bytes)?,
            permissions: read_u32(bytes)?,
        };
        if !valid_entry(&entry) {
            return Err(NfsError::Xdr(
                "NFSACL invalid ACL tag or permissions".into(),
            ));
        }
        entries.push(entry);
    }
    Ok(entries)
}

fn encode_acl(out: &mut Vec<u8>, entries: &[PosixAclEntry], default: bool) -> Result<()> {
    if entries.len() > MAX_ENTRIES || entries.iter().any(|entry| !valid_entry(entry)) {
        return Err(NfsError::InvalidInput(
            "NFSACL invalid ACL entries or entry limit exceeded".into(),
        ));
    }
    xdr_u32(out, entries.len() as u32);
    xdr_u32(out, entries.len() as u32);
    for entry in entries {
        xdr_u32(out, entry.tag | if default { DEFAULT_TAG } else { 0 });
        xdr_u32(out, entry.id);
        xdr_u32(out, entry.permissions);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    type StatusDecodeError = <super::super::nfsstat3 as TryFrom<Bytes>>::Error;

    fn words(values: &[u32]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_be_bytes())
            .collect()
    }

    fn entry(tag: u32, id: u32, permissions: u32) -> PosixAclEntry {
        PosixAclEntry {
            tag,
            id,
            permissions,
        }
    }

    fn acl_reply() -> Vec<u8> {
        // Linux NFSACL GETACL body: status, absent post_op_attr, full mask,
        // then access/default counts and XDR arrays. These fixed vectors are
        // independent of the encoder under test.
        words(&[
            0, 0, 15, 5, 5, 1, 1001, 6, 2, 42421, 7, 4, 1002, 4, 16, 0, 4, 32, 0, 0, 3, 3, 0x1001,
            1001, 7, 0x1004, 1002, 5, 0x1020, 0, 0,
        ])
    }

    #[test]
    fn getacl_vector_preserves_raw_masked_entries_and_default_acl() {
        let mut reply = Bytes::from(acl_reply());
        let acl = decode_get(&mut reply).unwrap();
        assert_eq!(
            acl,
            PosixAcl {
                access: vec![
                    entry(1, 1001, 6),
                    entry(2, 42421, 7),
                    entry(4, 1002, 4),
                    entry(16, 0, 4),
                    entry(32, 0, 0),
                ],
                default: vec![entry(1, 1001, 7), entry(4, 1002, 5), entry(32, 0, 0)],
            }
        );
        assert!(reply.is_empty());

        // NFSACL's synthesized mask on a minimum ACL must remain observable;
        // this transport must not turn the four wire entries into three.
        let mut minimum = Bytes::from(words(&[
            0, 0, 15, 4, 4, 1, 1001, 7, 4, 1002, 5, 16, 0, 5, 32, 0, 0, 0, 0,
        ]));
        let minimum = decode_get(&mut minimum).unwrap();
        assert_eq!(
            minimum.access,
            vec![
                entry(1, 1001, 7),
                entry(4, 1002, 5),
                entry(16, 0, 5),
                entry(32, 0, 0),
            ]
        );
        assert!(minimum.default.is_empty());
    }

    #[test]
    fn setacl_vectors_preserve_raw_permissions_defaults_and_explicit_clear() {
        let access = [
            entry(1, 1001, 6),
            entry(2, 42421, 7),
            entry(4, 1002, 4),
            entry(8, 42422, 7),
            entry(16, 0, 4),
            entry(32, 0, 0),
        ];
        let mut encoded = Vec::new();
        encode_acl(&mut encoded, &access, false).unwrap();
        assert_eq!(
            encoded,
            words(&[
                6, 6, 1, 1001, 6, 2, 42421, 7, 4, 1002, 4, 8, 42422, 7, 16, 0, 4, 32, 0, 0,
            ])
        );

        let default = [entry(1, 1001, 7), entry(4, 1002, 5), entry(32, 0, 0)];
        encoded.clear();
        encode_acl(&mut encoded, &default, true).unwrap();
        assert_eq!(
            encoded,
            words(&[3, 3, 0x1001, 1001, 7, 0x1004, 1002, 5, 0x1020, 0, 0,])
        );

        encoded.clear();
        encode_acl(&mut encoded, &[], true).unwrap();
        assert_eq!(encoded, [0; 8]); // Both ACL count and XDR array length clear.
        let mut empty = Bytes::from(words(&[0, 0, 15, 0, 0, 0, 0]));
        assert_eq!(decode_get(&mut empty).unwrap(), PosixAcl::default());
    }

    #[test]
    fn getacl_rejects_partial_masks_counts_invalid_entries_and_truncation() {
        let mut cases = Vec::new();
        for (label, offset, value) in [
            ("partial response mask", 8, 3u32),
            ("unknown response mask bit", 8, 31),
            ("count/array mismatch", 12, 6),
            ("unknown access tag", 20, 3),
            ("default bit in access ACL", 20, 0x1001),
            ("permission outside rwx", 28, 8),
            ("missing default bit", 88, 1),
        ] {
            let mut reply = acl_reply();
            reply[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
            cases.push((label, reply));
        }
        cases.push(("entry limit", words(&[0, 0, 15, 1025, 1025])));
        let mut trailing = acl_reply();
        trailing.extend(words(&[0]));
        cases.push(("trailing data", trailing));
        for (label, reply) in cases {
            assert!(
                matches!(decode_get(&mut Bytes::from(reply)), Err(NfsError::Xdr(_))),
                "{label}"
            );
        }

        // Every prefix of a complete response is malformed; in particular this
        // covers split count fields and truncation inside either entry array.
        let complete = acl_reply();
        // Short status fields have a retained typed error, covered separately.
        for len in 4..complete.len() {
            assert!(
                matches!(
                    decode_get(&mut Bytes::copy_from_slice(&complete[..len])),
                    Err(NfsError::Xdr(_))
                ),
                "truncated at byte {len}"
            );
        }
    }

    #[test]
    fn post_op_attributes_consume_exactly_the_present_or_absent_frame() {
        let mut absent = Bytes::from(words(&[0, 0xaabbccdd]));
        skip_attrs(&mut absent).unwrap();
        assert_eq!(absent.as_ref(), words(&[0xaabbccdd]));

        let attributes: Vec<u8> = (0..84).collect();
        let mut framed = words(&[1]);
        framed.extend(&attributes);
        framed.extend(words(&[0xaabbccdd]));
        let mut present = Bytes::from(framed);
        skip_attrs(&mut present).unwrap();
        assert_eq!(present.as_ref(), words(&[0xaabbccdd]));

        let mut reply = words(&[0, 1]);
        reply.extend(&attributes);
        reply.extend(&acl_reply()[8..]);
        let actual = decode_get(&mut Bytes::from(reply)).unwrap();
        let expected = decode_get(&mut Bytes::from(acl_reply())).unwrap();
        assert_eq!(actual, expected);

        for reply in [vec![], vec![0; 3], words(&[2]), words(&[u32::MAX])] {
            assert!(matches!(
                skip_attrs(&mut Bytes::from(reply)),
                Err(NfsError::Xdr(_))
            ));
        }
        for len in 0..84 {
            let mut reply = words(&[1]);
            reply.extend(&attributes[..len]);
            assert!(matches!(
                skip_attrs(&mut Bytes::from(reply)),
                Err(NfsError::Xdr(_))
            ));
        }
    }

    #[test]
    fn encoder_rejects_invalid_entries_and_enforces_the_wire_entry_limit() {
        for entries in [
            vec![entry(2, 42, 8)],
            vec![entry(0x1001, 42, 7)],
            vec![entry(3, 42, 7)],
            vec![entry(1, 0, 7); 1025],
        ] {
            for default in [false, true] {
                let mut encoded = vec![0xaa];
                assert!(matches!(
                    encode_acl(&mut encoded, &entries, default),
                    Err(NfsError::InvalidInput(_))
                ));
                assert_eq!(encoded, [0xaa]); // Reject before appending partial ACLs.
            }
        }
        let limit = vec![entry(2, 42, 7); 1024];
        let mut encoded = Vec::new();
        encode_acl(&mut encoded, &limit, false).unwrap();
        assert_eq!(&encoded[..8], words(&[1024, 1024]));
        assert_eq!(encoded.len(), 8 + 1024 * 12);
        assert_eq!(decode_acl(&mut Bytes::from(encoded), false).unwrap(), limit);
    }

    #[test]
    fn status_vectors_distinguish_definite_failure_and_uncertain_set_reply() {
        let mut success = Bytes::from(words(&[0, 0xaabbccdd]));
        decode_status(&mut success).unwrap();
        assert_eq!(success.as_ref(), words(&[0xaabbccdd]));

        let unsupported = decode_status(&mut Bytes::from(words(&[10004]))).unwrap_err();
        let unsupported =
            classify_sent_nfs3_error(OperationClass::ReplaySensitive, context(true), unsupported);
        assert!(matches!(unsupported, NfsError::Unsupported(_)));
        assert!(unsupported.operation_outcome().is_none());

        let denied = decode_status(&mut Bytes::from(words(&[13]))).unwrap_err();
        let denied =
            classify_sent_nfs3_error(OperationClass::ReplaySensitive, context(true), denied);
        assert!(matches!(
            denied,
            NfsError::Nfs3(crate::Nfs3ErrorCode::NFS3ERR_ACCES)
        ));
        assert!(denied.operation_outcome().is_none());

        // An accepted SET status followed by a malformed attribute frame does
        // not prove that the remote write failed: use the production classifier.
        let mut reply = Bytes::from(words(&[0, 1]));
        decode_status(&mut reply).unwrap();
        let malformed = skip_attrs(&mut reply).unwrap_err();
        let error =
            classify_sent_nfs3_error(OperationClass::ReplaySensitive, context(true), malformed);
        let outcome = error.operation_outcome().unwrap();
        assert_eq!(outcome.outcome, crate::OperationOutcome::Uncertain);
        assert_eq!(outcome.recovery, crate::RecoveryAction::VerifyThenResume);
        assert_eq!(outcome.context(), &context(true));

        let malformed = decode_get(&mut Bytes::from(vec![0; 3])).unwrap_err();
        let error = classify_sent_nfs3_error(OperationClass::ReadOnly, context(false), malformed);
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::SafeToRetry
        );
    }

    #[test]
    fn malformed_status_retains_decoder_source_and_native_operation_disposition() {
        let mut cases = (0..4)
            .map(|len| (vec![0; len], StatusDecodeError::InvalidLength))
            .collect::<Vec<_>>();
        cases.push((words(&[u32::MAX]), StatusDecodeError::UnknownVariant(-1)));
        for (reply, expected) in cases {
            for write in [false, true] {
                let source = decode_status(&mut Bytes::copy_from_slice(&reply)).unwrap_err();
                let class = if write {
                    OperationClass::ReplaySensitive
                } else {
                    OperationClass::ReadOnly
                };
                let error = classify_sent_nfs3_error(class, context(write), source);
                let outcome = error.operation_outcome().unwrap();
                assert_eq!(outcome.operation_class, class);
                assert_eq!(outcome.transmission, crate::RequestTransmission::Sent);
                assert_eq!(outcome.context(), &context(write));
                assert_eq!(
                    (outcome.outcome, outcome.recovery),
                    if write {
                        (
                            crate::OperationOutcome::Uncertain,
                            crate::RecoveryAction::VerifyThenResume,
                        )
                    } else {
                        (
                            crate::OperationOutcome::SafeToRetry,
                            crate::RecoveryAction::Retry,
                        )
                    }
                );
                let NfsError::Io(io) = &*outcome.source else {
                    panic!("expected a retained typed decoder source: {error:?}");
                };
                assert_eq!(io.kind(), ErrorKind::InvalidData);
                assert_eq!(
                    io.get_ref()
                        .and_then(|source| source.downcast_ref::<StatusDecodeError>()),
                    Some(&expected)
                );
            }
        }
    }
}
