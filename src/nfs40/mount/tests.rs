//! Test-only module. The inner `cfg` also tells the panic scan in
//! `tests/reliability_coverage.rs` to skip this file as production code.
#![cfg(test)]

use super::*;
use crate::nfs4::compound::xdr_opaque;
use crate::nfs40::recovery::{MAX_CONCURRENT_RECLAIMS, retry_settled};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};

/// Answers a v4.0 mount on `stream`: SETCLIENTID, SETCLIENTID_CONFIRM, the root navigation
/// and the mount-parameter GETATTR, whose attribute mask and values are `bitmap` / `values`.
async fn serve_mount(stream: &mut TcpStream, bitmap: u32, values: &[u8]) -> std::io::Result<()> {
    let identity = read_record(stream).await?;
    assert!(
        identity
            .windows(4)
            .any(|value| value == 35u32.to_be_bytes())
    );
    let mut identity_data = Vec::new();
    identity_data.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
    identity_data.extend_from_slice(&[0x77; 8]);
    reply(
        stream,
        &identity,
        &compound_result("identity", &[(35, &identity_data)]),
    )
    .await?;

    let confirm = read_record(stream).await?;
    let identity_tuple = [
        0x0102_0304_0506_0708u64.to_be_bytes().as_slice(),
        [0x77; 8].as_slice(),
    ]
    .concat();
    assert!(confirm.windows(16).any(|value| value == identity_tuple));
    reply(stream, &confirm, &compound_result("confirm", &[(36, &[])])).await?;

    let navigation = read_record(stream).await?;
    let minor_and_ops = [0u32.to_be_bytes(), 3u32.to_be_bytes()].concat();
    assert!(navigation.windows(8).any(|value| value == minor_and_ops));
    let mut fh = Vec::new();
    xdr_opaque(&mut fh, b"scripted-fh");
    reply(
        stream,
        &navigation,
        &compound_result("navigate", &[(24, &[]), (15, &[]), (10, &fh)]),
    )
    .await?;
    let lease_time = read_record(stream).await?;
    let mut attrs = Vec::new();
    attrs.extend_from_slice(&1u32.to_be_bytes());
    attrs.extend_from_slice(&bitmap.to_be_bytes());
    xdr_opaque(&mut attrs, values);
    reply(
        stream,
        &lease_time,
        &compound_result("lease-time", &[(22, &[]), (9, &attrs)]),
    )
    .await
}

async fn read_record(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let marker = stream.read_u32().await?;
    let mut record = vec![0; (marker & 0x7fff_ffff) as usize];
    stream.read_exact(&mut record).await?;
    Ok(record)
}

fn compound_result(tag: &str, ops: &[(u32, &[u8])]) -> Vec<u8> {
    compound_result_with_status(0, tag, ops)
}

fn compound_result_with_status(overall: u32, tag: &str, ops: &[(u32, &[u8])]) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend_from_slice(&overall.to_be_bytes());
    xdr_opaque(&mut result, tag.as_bytes());
    result.extend_from_slice(&(ops.len() as u32).to_be_bytes());
    for (opcode, data) in ops {
        result.extend_from_slice(&opcode.to_be_bytes());
        result.extend_from_slice(&0u32.to_be_bytes());
        result.extend_from_slice(data);
    }
    result
}

/// A reply whose overall status contradicts a fully successful operation list.
fn contradictory_result(tag: &str, ops: &[(u32, &[u8])]) -> Vec<u8> {
    compound_result_with_status(10006, tag, ops)
}

fn denied_lock_result(tag: &str, opcode: u32) -> Vec<u8> {
    let mut denied = Vec::new();
    denied.extend_from_slice(&7u64.to_be_bytes());
    denied.extend_from_slice(&11u64.to_be_bytes());
    denied.extend_from_slice(&2u32.to_be_bytes());
    denied.extend_from_slice(&99u64.to_be_bytes());
    xdr_opaque(&mut denied, b"holder");
    let mut result = Vec::new();
    result.extend_from_slice(&10010u32.to_be_bytes());
    xdr_opaque(&mut result, tag.as_bytes());
    result.extend_from_slice(&2u32.to_be_bytes());
    result.extend_from_slice(&22u32.to_be_bytes());
    result.extend_from_slice(&0u32.to_be_bytes());
    result.extend_from_slice(&opcode.to_be_bytes());
    result.extend_from_slice(&10010u32.to_be_bytes());
    result.extend_from_slice(&denied);
    result
}

fn succeeded_lock_result(tag: &str, seqid: u32, stateid: [u8; 16]) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend_from_slice(&0u32.to_be_bytes());
    xdr_opaque(&mut result, tag.as_bytes());
    result.extend_from_slice(&2u32.to_be_bytes());
    result.extend_from_slice(&22u32.to_be_bytes());
    result.extend_from_slice(&0u32.to_be_bytes());
    result.extend_from_slice(&12u32.to_be_bytes());
    result.extend_from_slice(&0u32.to_be_bytes());
    result.extend_from_slice(&0u32.to_be_bytes());
    result.extend_from_slice(&seqid.to_be_bytes());
    result.extend_from_slice(&stateid);
    result
}

fn failed_lock_result(tag: &str, opcode: u32, status: u32) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend_from_slice(&status.to_be_bytes());
    xdr_opaque(&mut result, tag.as_bytes());
    result.extend_from_slice(&2u32.to_be_bytes());
    result.extend_from_slice(&22u32.to_be_bytes());
    result.extend_from_slice(&0u32.to_be_bytes());
    result.extend_from_slice(&opcode.to_be_bytes());
    result.extend_from_slice(&status.to_be_bytes());
    result
}

/// A COMPOUND that failed at PUTFH: the following operation never ran.
fn putfh_failure_result(tag: &str, status: u32) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend_from_slice(&status.to_be_bytes());
    xdr_opaque(&mut result, tag.as_bytes());
    result.extend_from_slice(&1u32.to_be_bytes());
    result.extend_from_slice(&22u32.to_be_bytes());
    result.extend_from_slice(&status.to_be_bytes());
    result
}

fn close_result(stateid: [u8; 16]) -> Vec<u8> {
    compound_result("close", &[(22, &[]), (4, &stateid)])
}

fn read_result(data: &[u8]) -> Vec<u8> {
    let mut read = Vec::new();
    read.extend_from_slice(&1u32.to_be_bytes());
    xdr_opaque(&mut read, data);
    compound_result("read", &[(22, &[]), (25, &read)])
}

fn write_result(count: u32) -> Vec<u8> {
    let mut write = Vec::new();
    write.extend_from_slice(&count.to_be_bytes());
    write.extend_from_slice(&2u32.to_be_bytes());
    write.extend_from_slice(&[0x33; 8]);
    compound_result("write", &[(22, &[]), (38, &write)])
}

fn setattr_result() -> Vec<u8> {
    let mut attrs = Vec::new();
    attrs.extend_from_slice(&0u32.to_be_bytes());
    compound_result("setattr", &[(22, &[]), (34, &attrs)])
}

async fn register_stateid_lane(
    mount: &Mount40,
    owner: u64,
    stateid: [u8; 16],
    access: u32,
) -> Arc<tokio::sync::Mutex<OwnerLane>> {
    mount
        .state
        .register(OwnerLane {
            owner,
            next_seqid: 1,
            stateid,
            fh: Bytes::from_static(b"fh"),
            dir_fh: Bytes::from_static(b"dir"),
            name: "file".into(),
            access,
            write_verifier: None,
            io_fence: Arc::new(tokio::sync::RwLock::new(())),
            closing: false,
        })
        .await
}

#[test]
fn stateid_failures_are_classified_for_callers() {
    use crate::Nfs4ErrorCode;
    for status in [
        Nfs4ErrorCode::NFS4ERR_BAD_STATEID,
        Nfs4ErrorCode::NFS4ERR_STALE_STATEID,
        Nfs4ErrorCode::NFS4ERR_OLD_STATEID,
    ] {
        let error = NfsError::Nfs4(status);
        assert!(error.is_recoverable_state());
        assert_eq!(
            error.stateid_failure(),
            Some(crate::StateidFailure::Recoverable)
        );
    }
    let exhausted = NfsError::StateRecoveryExhausted(Box::new(crate::StateRecoveryError {
        operation: "read".into(),
        status: Nfs4ErrorCode::NFS4ERR_BAD_STATEID,
        source: Box::new(NfsError::Nfs4(Nfs4ErrorCode::NFS4ERR_BAD_STATEID)),
    }));
    assert_eq!(
        exhausted.stateid_failure(),
        Some(crate::StateidFailure::Exhausted)
    );
    assert_eq!(NfsError::InvalidInput("x".into()).stateid_failure(), None);
}

/// RFC 7530 §9.1.7: every owner-sequenced operation consumes its seqid except
/// for a fixed exception table. Pin the table so a constant or a status that
/// was added to the enum cannot silently change reclaim retry behaviour.
#[test]
fn seqid_consumption_matches_the_rfc7530_exception_table() {
    use crate::Nfs4ErrorCode;
    use crate::nfs40::recovery::{consumes_owner_seqid, status_consumes_seqid};

    for status in [
        Nfs4ErrorCode::NFS4ERR_BADXDR,
        Nfs4ErrorCode::NFS4ERR_BAD_SEQID,
        Nfs4ErrorCode::NFS4ERR_BAD_STATEID,
        Nfs4ErrorCode::NFS4ERR_MOVED,
        Nfs4ErrorCode::NFS4ERR_NOFILEHANDLE,
        Nfs4ErrorCode::NFS4ERR_STALE_CLIENTID,
        Nfs4ErrorCode::NFS4ERR_STALE_STATEID,
    ] {
        assert!(
            !status_consumes_seqid(status),
            "{status:?} is in the exception table and must not consume the seqid"
        );
    }
    // NFS4ERR_RESOURCE (10018) is in the RFC table but not modelled by the
    // generated enum, so it cannot be constructed here.
    for status in [
        Nfs4ErrorCode::NFS4ERR_SERVERFAULT,
        Nfs4ErrorCode::NFS4ERR_DELAY,
        Nfs4ErrorCode::NFS4ERR_GRACE,
        Nfs4ErrorCode::NFS4ERR_OLD_STATEID,
        Nfs4ErrorCode::NFS4ERR_LOCKED,
        Nfs4ErrorCode::NFS4ERR_EXPIRED,
    ] {
        assert!(
            status_consumes_seqid(status),
            "{status:?} must consume the seqid"
        );
    }
    assert!(consumes_owner_seqid(&Ok::<(), NfsError>(())));
    assert!(consumes_owner_seqid(&Err::<(), _>(NfsError::Nfs4(
        Nfs4ErrorCode::NFS4ERR_SERVERFAULT
    ))));
    assert!(!consumes_owner_seqid(&Err::<(), _>(NfsError::Nfs4(
        Nfs4ErrorCode::NFS4ERR_BAD_SEQID
    ))));
    // Io/Rpc failures have an unknown outcome, so the cursor must not move.
    assert!(!consumes_owner_seqid(&Err::<(), _>(NfsError::Io(
        std::io::Error::other("unknown outcome")
    ))));
}

#[tokio::test]
async fn close_with_bad_stateid_is_treated_as_released() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_stateid_lane(&mount, 207, [0xa7; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &failed_lock_result("close", 4, 10025)).await?;
        // §9.1.4.4: a seqid ahead of the server's also reports BAD_STATEID,
        // so one probe with the previous stateid seqid is sent first.
        let probe = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &probe,
            &failed_lock_result("close-probe", 4, 10025),
        )
        .await
    });
    mount
        .close(Bytes::from_static(b"fh"))
        .await
        .expect("BAD_STATEID on CLOSE means the server already released the state");
    assert!(mount.state.by_owner(207).await.is_none());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_bad_stateid_with_ahead_seqid_probes_previous_seqid_then_releases() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_stateid_lane(&mount, 216, [0xca; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &failed_lock_result("close", 4, 10025)).await?;
        // The open state exists with the previous stateid seqid, so the
        // probe closes it instead of leaking the server-side state.
        let probe = read_record(&mut stream).await?;
        let expected = [
            1u32.to_be_bytes().as_slice(),
            &[0xca, 0xca, 0xca, 0xc9],
            &[0xca; 12],
        ]
        .concat();
        assert!(
            probe.windows(expected.len()).any(|wire| wire == expected),
            "CLOSE probe did not carry the previous stateid seqid"
        );
        reply(&mut stream, &probe, &close_result([0xcb; 16])).await
    });
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    assert!(mount.state.by_owner(216).await.is_none());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_failure_does_not_wedge_the_lane() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 208, [0xa8; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &failed_lock_result("close", 4, 10006)).await?;
        // SERVERFAULT is outside the §9.1.7 exception list, so it consumed
        // seqid 1; the next CLOSE must send 2 or the server answers
        // BAD_SEQID forever.
        let retry = read_record(&mut stream).await?;
        let expected = [2u32.to_be_bytes().as_slice(), &[0xa8; 16]].concat();
        assert!(
            retry.windows(expected.len()).any(|wire| wire == expected),
            "CLOSE retry reused the seqid consumed by the failed attempt"
        );
        reply(&mut stream, &retry, &close_result([0xc8; 16])).await
    });
    assert!(mount.close(Bytes::from_static(b"fh")).await.is_err());
    assert!(!lane.lock().await.closing);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    assert!(mount.state.by_owner(208).await.is_none());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_putfh_failure_does_not_consume_the_owner_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 209, [0xa9; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &putfh_failure_result("close", 10006)).await?;
        let retry = read_record(&mut stream).await?;
        let expected = [1u32.to_be_bytes().as_slice(), &[0xa9; 16]].concat();
        assert!(
            retry.windows(expected.len()).any(|wire| wire == expected),
            "the retry must reuse seqid 1 that PUTFH never consumed"
        );
        reply(&mut stream, &retry, &close_result([0xc9; 16])).await
    });
    assert!(mount.close(Bytes::from_static(b"fh")).await.is_err());
    assert_eq!(lane.lock().await.next_seqid, 1);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn bad_stateid_read_reopens_once_and_retries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 201, [0xa1; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // Exactly one fresh OPEN (CLAIM_NULL) is allowed for one recovery.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xb2; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        reply(&mut stream, &retry, &read_result(b"data")).await
    });
    let data = mount.read(Bytes::from_static(b"fh"), 0, 4).await.unwrap();
    assert_eq!(data, Bytes::from_static(b"data"));
    let lane = lane.lock().await;
    assert_eq!(lane.stateid, [0xb2; 16]);
    assert_eq!(lane.next_seqid, 1);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn bad_stateid_reopen_falls_back_to_lane_seqid_on_bad_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 215, [0xab; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // The open-owner record survived, so seqid 0 is a BAD_SEQID.
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &failed_lock_result("open-recover", 18, 10026),
        )
        .await?;
        // The bounded fallback resends with the lane's next seqid.
        let open_retry = read_record(&mut stream).await?;
        reply(&mut stream, &open_retry, &open_result(0, [0xbc; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        reply(&mut stream, &retry, &read_result(b"data")).await
    });
    let data = mount.read(Bytes::from_static(b"fh"), 0, 4).await.unwrap();
    assert_eq!(data, Bytes::from_static(b"data"));
    assert_eq!(lane.lock().await.stateid, [0xbc; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn refresh_returns_the_delegation_before_closing_a_replacement_open() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_stateid_lane(&mount, 230, [0xd1; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // The refresh OPEN resolves to a different file and carries a delegation.
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &open_result_with_read_delegation([0xd2; 16], [0xdd; 16], b"other"),
        )
        .await?;
        // The delegation must be returned before the fresh state is closed.
        let delegreturn = read_record(&mut stream).await?;
        assert!(
            delegreturn.windows(16).any(|wire| wire == [0xdd; 16]),
            "a replacement open's delegation must be returned before CLOSE"
        );
        reply(
            &mut stream,
            &delegreturn,
            &compound_result("delegreturn", &[(22, &[]), (8, &[])]),
        )
        .await?;
        let close = read_record(&mut stream).await?;
        assert!(close.windows(16).any(|wire| wire == [0xd2; 16]));
        reply(&mut stream, &close, &close_result([0xd2; 16])).await
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    assert!(
        matches!(error, NfsError::LostOpenState(_)),
        "unexpected error: {error:?}"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn stale_stateid_returns_typed_state_lost_without_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 202, [0xa2; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10023)).await?;
        // Crash recovery (SETCLIENTID + reclaim) is driven by the lease
        // renewal, never from inside one I/O call: the server must see no
        // second request here.
        Ok::<(), std::io::Error>(())
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    assert!(
        matches!(error, NfsError::LostOpenState(_)),
        "unexpected error: {error:?}"
    );
    assert!(error.stateid_failure().is_none());
    assert_eq!(lane.lock().await.stateid, [0xa2; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn exhausted_state_recovery_returns_classified_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 204, [0xa4; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // A malformed OPEN reply has no authoritative status, so the lane
        // cannot be refreshed and the recovery is reported as exhausted.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, b"malformed-open-reply").await
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    assert_eq!(
        error.stateid_failure(),
        Some(crate::StateidFailure::Exhausted)
    );
    match error {
        NfsError::StateRecoveryExhausted(error) => {
            assert_eq!(error.operation, "read");
            assert_eq!(error.status, crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID);
            assert!(matches!(*error.source, NfsError::Xdr(_)));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    // A failed recovery leaves the lane untouched so a later attempt can retry.
    assert_eq!(lane.lock().await.stateid, [0xa4; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_open_not_found_is_propagated() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 217, [0xcc; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // The name no longer resolves: report the authoritative NOENT
        // instead of hiding it behind a state-recovery failure.
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &failed_lock_result("open-recover", 18, 2),
        )
        .await
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_NOENT)),
        "unexpected error: {error:?}"
    );
    assert!(error.stateid_failure().is_none());
    assert_eq!(lane.lock().await.stateid, [0xcc; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_open_grace_after_bad_seqid_stays_retryable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 218, [0xcd; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // The open-owner record survived (seqid 0 is a replay check), then
        // the server enters grace and refuses the CLAIM_NULL OPEN.
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &failed_lock_result("open-recover", 18, 10026),
        )
        .await?;
        let retry = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &retry,
            &failed_lock_result("open-grace", 18, 10013),
        )
        .await
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)),
        "grace must stay retryable, got {error:?}"
    );
    // GRACE is outside the §9.1.7 exception table: the failed OPEN consumed
    // its seqid, so a later recovery must not replay it.
    let lane = lane.lock().await;
    assert_eq!(lane.next_seqid, 2);
    assert_eq!(lane.stateid, [0xcd; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn write_recovers_bad_stateid_and_retries_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 205, [0xa5; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let write = read_record(&mut stream).await?;
        reply(&mut stream, &write, &failed_lock_result("write", 38, 10025)).await?;
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xb5; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        reply(&mut stream, &retry, &write_result(3)).await
    });
    let outcome = mount
        .write(Bytes::from_static(b"fh"), 0, Bytes::from_static(b"abc"))
        .await
        .unwrap();
    assert_eq!(outcome.count, 3);
    assert_eq!(lane.lock().await.stateid, [0xb5; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn setattr_size_recovers_bad_stateid_and_retries_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 206, [0xa6; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let setattr = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &setattr,
            &failed_lock_result("setattr", 34, 10025),
        )
        .await?;
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xb6; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        reply(&mut stream, &retry, &setattr_result()).await
    });
    mount
        .setattr(
            Bytes::from_static(b"fh"),
            None,
            None,
            None,
            None,
            Some(3),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(lane.lock().await.stateid, [0xb6; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn setattr_without_lane_keeps_the_anonymous_stateid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let setattr = read_record(&mut stream).await?;
        // Path-based metadata (chmod/truncate) has no open lane: RFC 7530
        // permits the anonymous stateid for these requests, so no recovery
        // is attempted.
        let expected = [0u8; 16];
        assert!(
            setattr.windows(16).any(|wire| wire == expected),
            "stateless SETATTR did not use the anonymous stateid"
        );
        reply(&mut stream, &setattr, &setattr_result()).await
    });
    mount
        .setattr(
            Bytes::from_static(b"fh"),
            None,
            None,
            None,
            None,
            Some(3),
            None,
            None,
        )
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_recovers_bad_open_stateid_and_retries_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_stateid_lane(&mount, 213, [0xa3; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(&mut stream, &lock, &failed_lock_result("lock", 12, 10025)).await?;
        // The open stateid is refreshed with exactly one fresh OPEN.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xb3; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        // BAD_STATEID belongs to the RFC 7530 §9.1.7 exception table, so
        // neither seqid is consumed: the retry keeps open_seqid 1 and
        // lock_seqid 0 and only swaps in the refreshed stateid.
        let expected = [
            2u32.to_be_bytes().as_slice(),
            0u32.to_be_bytes().as_slice(),
            0u64.to_be_bytes().as_slice(),
            1u64.to_be_bytes().as_slice(),
            1u32.to_be_bytes().as_slice(),
            1u32.to_be_bytes().as_slice(),
            &[0xb3; 16],
            0u32.to_be_bytes().as_slice(),
        ]
        .concat();
        assert!(
            retry.windows(expected.len()).any(|wire| wire == expected),
            "LOCK retry after BAD_STATEID did not reuse the refreshed stateid"
        );
        reply(
            &mut stream,
            &retry,
            &compound_result("lock-retry", &[(22, &[]), (12, &[0xb5; 16])]),
        )
        .await
    });
    let stateid = mount
        .lock(Bytes::from_static(b"fh"), 2, 0, 1)
        .await
        .unwrap();
    assert_eq!(stateid, Bytes::from_static(&[0xb5; 16]));
    let open_lane = mount.state.by_owner(213).await.unwrap();
    assert_eq!(open_lane.lock().await.stateid, [0xb3; 16]);
    assert!(mount.locks.by_stateid(&[0xb5; 16]).await.is_some());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_stateid_recovery_is_bounded_to_one_refresh() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_stateid_lane(&mount, 215, [0xa4; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(&mut stream, &lock, &failed_lock_result("lock", 12, 10025)).await?;
        // The first BAD_STATEID refreshes the open lane with one fresh OPEN.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xb4; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        // A second BAD_STATEID must end the bounded retry instead of
        // refreshing the lane again.
        reply(&mut stream, &retry, &failed_lock_result("lock", 12, 10025)).await
    });
    let error = mount
        .lock(Bytes::from_static(b"fh"), 2, 0, 1)
        .await
        .unwrap_err();
    assert_eq!(
        error.stateid_failure(),
        Some(crate::StateidFailure::Exhausted)
    );
    // The refreshed state stays on the lane so a later attempt can converge.
    let open_lane = mount.state.by_owner(215).await.unwrap();
    assert_eq!(open_lane.lock().await.stateid, [0xb4; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_putfh_failure_does_not_consume_the_open_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 216, [0xac; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(&mut stream, &lock, &putfh_failure_result("lock", 10006)).await
    });
    assert!(
        mount
            .lock(Bytes::from_static(b"fh"), 2, 0, 1)
            .await
            .is_err()
    );
    assert_eq!(
        lane.lock().await.next_seqid,
        1,
        "a PUTFH failure never ran LOCK and must not consume seqid 1"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_overall_status_contradiction_consumes_the_owner_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 217, [0xad; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        // PUTFH and CLOSE both succeeded, but the overall status claims failure:
        // the CLOSE still ran and consumed its seqid.
        reply(
            &mut stream,
            &close,
            &contradictory_result("close", &[(22, &[]), (4, &[0xb7; 16])]),
        )
        .await
    });
    assert!(mount.close(Bytes::from_static(b"fh")).await.is_err());
    assert_eq!(lane.lock().await.next_seqid, 2);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_probe_putfh_failure_keeps_the_first_consumption() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 218, [0xdb; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        // Overall BAD_STATEID but PUTFH and CLOSE both succeeded: the CLOSE ran
        // and consumed its seqid, so a BAD_STATEID probe is issued.
        reply(
            &mut stream,
            &close,
            &compound_result_with_status(10025, "close", &[(22, &[]), (4, &[0xdb; 16])]),
        )
        .await?;
        let probe = read_record(&mut stream).await?;
        // The probe never reached CLOSE and must not erase the first request's
        // consumption.
        reply(
            &mut stream,
            &probe,
            &putfh_failure_result("close-probe", 10006),
        )
        .await
    });
    assert!(mount.close(Bytes::from_static(b"fh")).await.is_err());
    assert_eq!(
        lane.lock().await.next_seqid,
        2,
        "an unexecuted probe must not discard the first CLOSE's consumption"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn open_confirm_putfh_failure_keeps_only_the_open_consumption() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let client = rpc::Client::new(mux, None);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(2, [0x21; 16], b"fh")).await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &putfh_failure_result("open_confirm", 10006),
        )
        .await
    });
    let dir = Bytes::from_static(b"dir");
    let spec = crate::nfs40::recovery::FreshOpenSpec {
        dir_fh: &dir,
        name: "file",
        owner_wire: b"owner",
        access: crate::OPEN_READ,
        client_id: 7,
    };
    let attempt = crate::nfs40::recovery::fresh_open_lane_with_seqid(
        &client,
        &Auth::new_null(),
        &spec,
        1,
        "recovery-open",
    )
    .await;
    assert!(attempt.result.is_err());
    assert_eq!(attempt.sent_seqid, 0);
    assert_eq!(
        attempt.next_seqid, 1,
        "OPEN consumed seqid 0, but a PUTFH-failed CONFIRM did not consume 1"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_old_stateid_retries_with_advanced_lock_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let open_lane = register_stateid_lane(&mount, 214, [0xaa; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(&mut stream, &lock, &failed_lock_result("lock", 12, 10024)).await?;
        // OLD_STATEID is not in the §9.1.7 exception table, so the rejected
        // LOCK consumes the open-owner seqid; the lane is then refreshed
        // with one fresh OPEN, and OLD_STATEID also advances the lock-owner
        // seqid (0 -> 1) on the retry.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xb3; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        let expected = [
            2u32.to_be_bytes().as_slice(),
            0u32.to_be_bytes().as_slice(),
            0u64.to_be_bytes().as_slice(),
            1u64.to_be_bytes().as_slice(),
            1u32.to_be_bytes().as_slice(),
            1u32.to_be_bytes().as_slice(),
            &[0xb3; 16],
            1u32.to_be_bytes().as_slice(),
        ]
        .concat();
        assert!(
            retry.windows(expected.len()).any(|wire| wire == expected),
            "LOCK retry after OLD_STATEID did not advance the lock seqid or swap the refreshed stateid"
        );
        reply(
            &mut stream,
            &retry,
            &compound_result("lock-retry", &[(22, &[]), (12, &[0xb6; 16])]),
        )
        .await
    });
    let stateid = mount
        .lock(Bytes::from_static(b"fh"), 2, 0, 1)
        .await
        .unwrap();
    assert_eq!(stateid, Bytes::from_static(&[0xb6; 16]));
    let lane = open_lane.lock().await;
    // The fresh OPEN re-syncs the stateid authoritatively and restarts the
    // open-owner seqid at 1 (the OPEN is sent with seqid 0 and succeeds);
    // the successful retry LOCK then consumes it once more (1 -> 2).
    assert_eq!(lane.stateid, [0xb3; 16]);
    assert_eq!(lane.next_seqid, 2);
    drop(lane);
    let lock_lane = mount.locks.by_stateid(&[0xb6; 16]).await.unwrap();
    assert_eq!(lock_lane.lock().await.next_seqid, 2);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn locku_bad_stateid_is_treated_as_released() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_lock(&mount, 212, [0xb4; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        reply(&mut stream, &locku, &failed_lock_result("locku", 14, 10025)).await?;
        // §9.1.4.4: a seqid ahead of the server's also reports BAD_STATEID,
        // so one probe with the previous stateid seqid is sent first.
        let probe = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &probe,
            &failed_lock_result("locku-probe", 14, 10025),
        )
        .await?;
        // The lock-owner record may outlive the lost lock state; release it.
        let release = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &release,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    mount
        .locku(
            Bytes::from_static(b"fh"),
            Bytes::from([0xb4; 16].as_slice()),
            2,
            0,
            1,
        )
        .await
        .expect("BAD_STATEID on LOCKU means the lock is already gone");
    assert!(mount.locks.by_stateid(&[0xb4; 16]).await.is_none());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn locku_bad_stateid_with_ahead_seqid_probes_previous_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_lock(&mount, 219, [0xd0; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        reply(&mut stream, &locku, &failed_lock_result("locku", 14, 10025)).await?;
        // The lock still exists under the previous stateid seqid: the probe
        // unlocks it instead of leaking the server-side lock.
        let probe = read_record(&mut stream).await?;
        let expected = [
            1u32.to_be_bytes().as_slice(),
            &[0xd0, 0xd0, 0xd0, 0xcf],
            &[0xd0; 12],
        ]
        .concat();
        assert!(
            probe.windows(expected.len()).any(|wire| wire == expected),
            "LOCKU probe did not carry the previous stateid seqid"
        );
        reply(
            &mut stream,
            &probe,
            &compound_result("locku-probe", &[(22, &[]), (14, &[0xd1; 16])]),
        )
        .await?;
        let release = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &release,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    mount
        .locku(
            Bytes::from_static(b"fh"),
            Bytes::from([0xd0; 16].as_slice()),
            2,
            0,
            1,
        )
        .await
        .unwrap();
    assert!(mount.locks.by_stateid(&[0xd0; 16]).await.is_none());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn rejected_reclaim_fallback_grace_records_consumed_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 85, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10033),
        )
        .await?;
        // The open-owner record survived, so the seqid-0 fallback is a
        // replay check (BAD_SEQID); the seqid-1 fallback then hits grace.
        let fallback_zero = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &fallback_zero,
            &failed_lock_result("fallback-open", 18, 10026),
        )
        .await?;
        let fallback_lane_seqid = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &fallback_lane_seqid,
            &failed_lock_result("fallback-open", 18, 10013),
        )
        .await?;
        // Second recovery pass on the same client id: the fallback OPEN moved
        // the owner cursor to 2, so the reclaim must carry 2 rather than replay
        // the already-consumed 1.
        let identity = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        let client_id = 19u64.to_be_bytes();
        let position = reclaim
            .windows(8)
            .rposition(|window| window == client_id)
            .expect("reclaim OPEN carries the client id");
        // OPEN encodes seqid, share_access, share_deny, then the client id.
        let seqid = u32::from_be_bytes(reclaim[position - 12..position - 8].try_into().unwrap());
        assert_eq!(
            seqid, 2,
            "the retry after a fallback OPEN must send the next unconsumed seqid"
        );
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10013),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-fallback-grace".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(matches!(
        error,
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)
    ));
    // GRACE consumed the seqid sent by the fallback OPEN (1), so the next
    // recovery must send 2 instead of replaying 1.
    let lane = mount.state.by_owner(85).await.unwrap();
    let lane = lane.lock().await;
    assert_eq!(lane.next_seqid, 2);
    assert_eq!(lane.stateid, [0x61; 16]);
    drop(lane);
    // The reclaim ledger must match the lane cursor: both owners were moved by
    // the fallback OPEN, so a retry cannot resend the consumed seqid 1.
    assert_eq!(mount.lease.open_reclaim_seqid(85), 2);
    mount.lease.clear_recovery_backoff();
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(matches!(
        error,
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)
    ));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn reclaim_lock_without_open_owner_drops_only_the_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_lock(&mount, 86, [0x86; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        // No open lane exists, so no reclaim request is sent for the lock.
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-orphan-lock".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    // The missing open-owner is a per-lane condition: only the orphan lock
    // is dropped, the mount still recovers.
    context.recover_or_lose().await.unwrap();
    assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
    assert!(mount.locks.snapshot().await.is_empty());
    server.await.unwrap().unwrap();
}

async fn reply(stream: &mut TcpStream, request: &[u8], payload: &[u8]) -> std::io::Result<()> {
    let xid = u32::from_be_bytes(request[0..4].try_into().unwrap());
    let mut response = Vec::new();
    response.extend_from_slice(&xid.to_be_bytes());
    response.extend_from_slice(&1u32.to_be_bytes());
    response.extend_from_slice(&0u32.to_be_bytes());
    response.extend_from_slice(&0u32.to_be_bytes());
    response.extend_from_slice(&0u32.to_be_bytes());
    response.extend_from_slice(&0u32.to_be_bytes());
    response.extend_from_slice(payload);
    stream
        .write_u32(0x8000_0000 | response.len() as u32)
        .await?;
    stream.write_all(&response).await
}

fn direct_mount(rpc: rpc::Client) -> Arc<Mount40> {
    direct_mount_with_acl(rpc, false)
}

fn direct_mount_with_acl(rpc: rpc::Client, acl_supported: bool) -> Arc<Mount40> {
    Arc::new(Mount40 {
        rpc,
        auth: Auth::new_null(),
        root_fh: Bytes::from_static(b"root"),
        client_id: Arc::new(AtomicU64::new(7)),
        issuer: 9,
        next_owner: AtomicU64::new(1),
        state: Arc::new(OpenState::default()),
        locks: Arc::new(LockState::default()),
        lease: LeaseState::ready(1, 60),
        _renewal: None,
        _callback: None,
        _callback_worker: None,
        callback_state: None,
        dircount: 8192,
        maxcount: 32768,
        rsize: 1_048_576,
        wsize: 1_048_576,
        acl_supported,
    })
}

#[tokio::test]
async fn capabilities_report_server_negotiated_acl_support() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let _ = listener.accept().await;
    });
    let mux = rpc::StreamMux::connect(addr, true).await.unwrap();
    let mount = direct_mount(rpc::Client::new(mux, None));

    assert!(!mount.capabilities().acl);
}

fn direct_mount_with_renewal(rpc: rpc::Client, interval: Duration) -> Arc<Mount40> {
    direct_mount_with_lease_renewal(rpc, interval, 60)
}

fn direct_mount_with_lease_renewal(
    rpc: rpc::Client,
    interval: Duration,
    lease_seconds: u32,
) -> Arc<Mount40> {
    direct_mount_with_recovery(rpc, interval, lease_seconds, None)
}

fn direct_mount_with_recovery(
    rpc: rpc::Client,
    interval: Duration,
    lease_seconds: u32,
    recovery: Option<RecoveryHandler>,
) -> Arc<Mount40> {
    let lease = LeaseState::ready(1, lease_seconds);
    let reconnect_lease = Arc::clone(&lease);
    rpc.set_reconnect_handler(move |_client, _generation| {
        let lease = Arc::clone(&reconnect_lease);
        async move {
            lease.mark_reconnecting();
            Ok(())
        }
    })
    .unwrap();
    let renewal = LeaseRenewal::start(
        rpc.clone(),
        Auth::new_null(),
        Arc::new(AtomicU64::new(7)),
        interval,
        Arc::clone(&lease),
        recovery,
    );
    Arc::new(Mount40 {
        rpc,
        auth: Auth::new_null(),
        root_fh: Bytes::from_static(b"root"),
        client_id: Arc::new(AtomicU64::new(7)),
        issuer: 9,
        next_owner: AtomicU64::new(1),
        state: Arc::new(OpenState::default()),
        locks: Arc::new(LockState::default()),
        lease,
        _renewal: Some(renewal),
        _callback: None,
        _callback_worker: None,
        callback_state: None,
        dircount: 8192,
        maxcount: 32768,
        rsize: 1_048_576,
        wsize: 1_048_576,
        acl_supported: false,
    })
}

fn open_result(flags: u32, stateid: [u8; 16], fh: &[u8]) -> Vec<u8> {
    let mut open = Vec::new();
    open.extend_from_slice(&stateid);
    open.extend_from_slice(&[0; 20]);
    open.extend_from_slice(&flags.to_be_bytes());
    open.extend_from_slice(&0u32.to_be_bytes());
    open.extend_from_slice(&0u32.to_be_bytes());
    let mut getfh = Vec::new();
    xdr_opaque(&mut getfh, fh);
    compound_result("open", &[(22, &[]), (18, &open), (10, &getfh)])
}

#[test]
fn delegation_getattr_decodes_exact_change_and_size() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&1u32.to_be_bytes());
    encoded.extend_from_slice(&0x18u32.to_be_bytes());
    encoded.extend_from_slice(&16u32.to_be_bytes());
    encoded.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
    encoded.extend_from_slice(&0x1112_1314_1516_1718u64.to_be_bytes());
    assert_eq!(
        decode_callback_attributes(Bytes::from(encoded)).unwrap(),
        (0x0102_0304_0506_0708, 0x1112_1314_1516_1718)
    );
}

#[test]
fn delegation_getattr_rejects_missing_required_attributes() {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&1u32.to_be_bytes());
    encoded.extend_from_slice(&0x10u32.to_be_bytes());
    encoded.extend_from_slice(&8u32.to_be_bytes());
    encoded.extend_from_slice(&7u64.to_be_bytes());
    assert!(decode_callback_attributes(Bytes::from(encoded)).is_err());
}

fn open_result_with_read_delegation(stateid: [u8; 16], delegation: [u8; 16], fh: &[u8]) -> Vec<u8> {
    let mut open = Vec::new();
    open.extend_from_slice(&stateid);
    open.extend_from_slice(&[0; 20]);
    open.extend_from_slice(&0u32.to_be_bytes());
    open.extend_from_slice(&0u32.to_be_bytes());
    open.extend_from_slice(&1u32.to_be_bytes());
    open.extend_from_slice(&delegation);
    open.extend_from_slice(&0u32.to_be_bytes());
    open.extend_from_slice(&0u32.to_be_bytes());
    open.extend_from_slice(&0u32.to_be_bytes());
    open.extend_from_slice(&0u32.to_be_bytes());
    xdr_opaque(&mut open, b"");
    let mut getfh = Vec::new();
    xdr_opaque(&mut getfh, fh);
    compound_result("open", &[(22, &[]), (18, &open), (10, &getfh)])
}

fn open_owner_seqid(request: &[u8], owner: &str) -> u32 {
    let needle = owner.as_bytes();
    let start = request
        .windows(needle.len())
        .position(|wire| wire == needle)
        .expect("open-owner wire not found in request");
    // OPEN4args: [opcode][seqid][share_access][share_deny][open_owner4], where
    // open_owner4 is [clientid:8][owner_len:4][owner...].
    let at = start - 24;
    u32::from_be_bytes(request[at..at + 4].try_into().unwrap())
}

async fn connected_direct_mount(listener: &TcpListener) -> Arc<Mount40> {
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    direct_mount(rpc::Client::new(mux, None))
}

async fn connected_direct_mount_with_acl(listener: &TcpListener) -> Arc<Mount40> {
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    direct_mount_with_acl(rpc::Client::new(mux, None), true)
}

#[tokio::test]
async fn public_health_reports_successful_background_renewal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount = direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let renew = read_record(&mut stream).await?;
        reply(&mut stream, &renew, &compound_result("renew", &[(30, &[])])).await
    });
    server.await.unwrap().unwrap();
    for _ in 0..20 {
        let health = mount.health();
        if health.lease_renewals == 1 {
            assert_eq!(health.lifecycle, crate::MountLifecycleState::Ready);
            assert_eq!(health.generation, 1);
            assert_eq!(health.lease_seconds, Some(60));
            assert_eq!(health.lease_healthy, Some(true));
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("successful RENEW was not reflected in public health");
}

#[tokio::test]
async fn renewal_reconnects_with_byte_identical_request_and_preserves_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount = direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await?;
        let original = read_record(&mut first).await?;
        socket2::SockRef::from(&first).set_linger(Some(Duration::ZERO))?;
        drop(first);

        let (mut second, _) = listener.accept().await?;
        let replay = read_record(&mut second).await?;
        assert_eq!(&original[4..], &replay[4..]);
        reply(
            &mut second,
            &replay,
            &compound_result("renew", &[(30, &[])]),
        )
        .await
    });
    server.await.unwrap().unwrap();
    for _ in 0..100 {
        let health = mount.health();
        if health.lease_renewals == 1 {
            assert_eq!(health.lifecycle, crate::MountLifecycleState::Ready);
            assert_eq!(health.generation, 1);
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("replayed RENEW did not restore healthy lease state");
}

#[tokio::test]
async fn grace_renewal_is_suspect_until_a_later_success() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount = direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
    let (grace_tx, grace_rx) = oneshot::channel();
    let (continue_tx, continue_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        let mut grace = Vec::new();
        grace.extend_from_slice(&10013u32.to_be_bytes());
        xdr_opaque(&mut grace, b"renew");
        grace.extend_from_slice(&1u32.to_be_bytes());
        grace.extend_from_slice(&30u32.to_be_bytes());
        grace.extend_from_slice(&10013u32.to_be_bytes());
        reply(&mut stream, &first, &grace).await?;
        let _ = grace_tx.send(());
        let _ = continue_rx.await;
        let second = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &second,
            &compound_result("renew", &[(30, &[])]),
        )
        .await
    });
    grace_rx.await.unwrap();
    for _ in 0..100 {
        if mount.health().lifecycle == crate::MountLifecycleState::Suspect {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::Suspect
    );
    assert_eq!(mount.health().generation, 1);
    let _ = continue_tx.send(());
    server.await.unwrap().unwrap();
    for _ in 0..100 {
        if mount.health().lease_renewals == 1 {
            assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("successful RENEW did not clear GRACE suspicion");
}

#[tokio::test]
async fn transient_renewal_failures_lose_state_after_the_lease_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount =
        direct_mount_with_lease_renewal(rpc::Client::new(mux, None), Duration::from_millis(20), 1);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        while let Ok(request) = read_record(&mut stream).await {
            if reply(
                &mut stream,
                &request,
                &failed_lock_result("renew", 30, 10013),
            )
            .await
            .is_err()
            {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if mount.health().lifecycle == crate::MountLifecycleState::LostState {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("transient renewal failures never expired the lease");
    assert_eq!(mount.health().generation, 2);
    mount.umount().await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn in_flight_renewal_is_fenced_at_deadline_but_still_settles() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount =
        direct_mount_with_lease_renewal(rpc::Client::new(mux, None), Duration::from_millis(10), 1);
    let (sent_tx, sent_rx) = oneshot::channel();
    let (settle_tx, settle_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let _ = sent_tx.send(());
        let _ = settle_rx.await;
        reply(
            &mut stream,
            &request,
            &compound_result("renew", &[(30, &[])]),
        )
        .await
    });
    sent_rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if mount.health().lifecycle == crate::MountLifecycleState::LostState {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("in-flight RENEW prevented conservative lease expiry");
    assert_eq!(mount.health().generation, 2);
    let _ = settle_tx.send(());
    mount.umount().await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn successful_stateid_activity_extends_an_in_flight_renew_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount =
        direct_mount_with_lease_renewal(rpc::Client::new(mux, None), Duration::from_millis(10), 1);
    register_scripted_open(&mount, 81, b"fh").await;
    let (renew_sent_tx, renew_sent_rx) = oneshot::channel();
    let (settle_tx, settle_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let renew = read_record(&mut stream).await?;
        let _ = renew_sent_tx.send(());
        let read = read_record(&mut stream).await?;
        let mut read_data = Vec::new();
        read_data.extend_from_slice(&1u32.to_be_bytes());
        xdr_opaque(&mut read_data, b"x");
        reply(
            &mut stream,
            &read,
            &compound_result("read", &[(22, &[]), (25, &read_data)]),
        )
        .await?;
        let _ = settle_rx.await;
        reply(&mut stream, &renew, &compound_result("renew", &[(30, &[])])).await
    });
    renew_sent_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        mount.read(Bytes::from_static(b"fh"), 0, 1).await.unwrap(),
        Bytes::from_static(b"x")
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_ne!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState,
        "successful foreground activity did not extend the lease deadline"
    );
    let _ = settle_tx.send(());
    mount.umount().await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn successful_stateless_activity_does_not_extend_the_lease_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount =
        direct_mount_with_lease_renewal(rpc::Client::new(mux, None), Duration::from_millis(10), 1);
    let (renew_sent_tx, renew_sent_rx) = oneshot::channel();
    let (settle_tx, settle_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let renew = read_record(&mut stream).await?;
        let _ = renew_sent_tx.send(());
        let null = read_record(&mut stream).await?;
        reply(&mut stream, &null, &[]).await?;
        let _ = settle_rx.await;
        reply(&mut stream, &renew, &compound_result("renew", &[(30, &[])])).await
    });
    renew_sent_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    mount.null().await.unwrap();
    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if mount.health().lifecycle == crate::MountLifecycleState::LostState {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stateless NULL incorrectly extended the lease deadline");
    let _ = settle_tx.send(());
    mount.umount().await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn expired_renewal_loses_generation_and_gates_stateful_work() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let mount = direct_mount_with_renewal(rpc::Client::new(mux, None), Duration::from_millis(10));
    register_scripted_open(&mount, 82, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let renew = read_record(&mut stream).await?;
        let mut result = Vec::new();
        result.extend_from_slice(&10011u32.to_be_bytes());
        xdr_opaque(&mut result, b"renew");
        result.extend_from_slice(&1u32.to_be_bytes());
        result.extend_from_slice(&30u32.to_be_bytes());
        result.extend_from_slice(&10011u32.to_be_bytes());
        reply(&mut stream, &renew, &result).await
    });
    server.await.unwrap().unwrap();
    for _ in 0..100 {
        let health = mount.health();
        if health.lease_healthy == Some(false) {
            assert_eq!(health.lifecycle, crate::MountLifecycleState::LostState);
            assert_eq!(health.generation, 2);
            let error = mount
                .read(Bytes::from_static(b"fh"), 0, 1)
                .await
                .unwrap_err();
            assert_eq!(
                error.operation_outcome().unwrap().recovery,
                crate::RecoveryAction::Reopen
            );
            let stale = crate::LockToken::new(
                Bytes::from_static(b"fh"),
                Bytes::from_static(&[0x31; 16]),
                2,
                0,
                1,
                mount.issuer,
                1,
            );
            assert!(mount.unlock_stateful(stale).await.is_err());
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("expired RENEW was not reflected in public health");
}

#[tokio::test]
async fn stale_clientid_renewal_runs_recovery_and_preserves_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let recoveries = Arc::new(AtomicU64::new(0));
    let recovery_count = Arc::clone(&recoveries);
    let recovery: RecoveryHandler = Arc::new(move || {
        let recovery_count = Arc::clone(&recovery_count);
        Box::pin(async move {
            recovery_count.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    });
    let mount = direct_mount_with_recovery(
        rpc::Client::new(mux, None),
        Duration::from_millis(10),
        60,
        Some(recovery),
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let renew = read_record(&mut stream).await?;
        reply(&mut stream, &renew, &failed_lock_result("renew", 30, 10022)).await
    });
    server.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if recoveries.load(Ordering::Acquire) == 1
                && mount.health().lifecycle == crate::MountLifecycleState::Ready
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(mount.health().generation, 1);
}

#[tokio::test]
async fn late_lock_result_cannot_publish_across_a_generation_fence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 83, b"fh").await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(
            &mut stream,
            &request,
            &compound_result("lock", &[(22, &[]), (12, &[0x8a; 16])]),
        )
        .await
    });
    let for_lock = Arc::clone(&mount);
    let lock = tokio::spawn(async move {
        for_lock
            .lock_stateful(Bytes::from_static(b"fh"), 2, 0, 1)
            .await
    });
    seen_rx.await.unwrap();
    mount.lease.mark_lost().await;
    let _ = reply_tx.send(());
    let error = lock.await.unwrap().unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().recovery,
        crate::RecoveryAction::Reopen
    );
    assert!(mount.locks.snapshot().await.is_empty());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn public_umount_stops_renewal_and_reports_closed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let accepted = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(stream);
        Ok::<(), std::io::Error>(())
    });
    mount.umount().await.unwrap();
    let health = mount.health();
    assert_eq!(health.lifecycle, crate::MountLifecycleState::Closed);
    assert_eq!(health.lease_healthy, Some(false));
    assert!(mount.read(Bytes::from_static(b"fh"), 0, 1).await.is_err());
    accepted.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_umount_keeps_a_sent_reclaim_settling() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let rpc = rpc::Client::new(mux, None);
    let auth = Auth::new_null();
    let client_id = Arc::new(AtomicU64::new(7));
    let lease = LeaseState::ready(1, 60);
    let state = Arc::new(OpenState::default());
    state
        .register(OwnerLane {
            owner: 75,
            next_seqid: 1,
            stateid: [0x75; 16],
            fh: Bytes::from_static(b"fh"),
            dir_fh: Bytes::from_static(b"dir"),
            name: "reclaim-file".into(),
            access: crate::OPEN_BOTH,
            write_verifier: None,
            io_fence: Arc::new(tokio::sync::RwLock::new(())),
            closing: false,
        })
        .await;
    let locks = Arc::new(LockState::default());
    let context = RecoveryContext {
        rpc: rpc.clone(),
        auth: auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5a; 8],
            owner: "shutdown-reclaim".into(),
        },
        client_id: Arc::clone(&client_id),
        issuer: 9,
        state: Arc::clone(&state),
        locks: Arc::clone(&locks),
        lease: Arc::clone(&lease),
        callback_addr: None,
    };
    let recovery: RecoveryHandler = Arc::new(move || {
        let context = context.clone();
        Box::pin(async move { context.recover_or_lose().await })
    });
    let renewal = LeaseRenewal::start(
        rpc.clone(),
        auth.clone(),
        Arc::clone(&client_id),
        Duration::from_millis(10),
        Arc::clone(&lease),
        Some(recovery),
    );
    let mount = Arc::new(Mount40 {
        rpc,
        auth,
        root_fh: Bytes::from_static(b"root"),
        client_id,
        issuer: 9,
        next_owner: AtomicU64::new(1),
        state,
        locks,
        lease,
        _renewal: Some(renewal),
        _callback: None,
        _callback_worker: None,
        callback_state: None,
        dircount: 8192,
        maxcount: 32768,
        rsize: 1_048_576,
        wsize: 1_048_576,
        acl_supported: false,
    });
    let (reclaim_seen_tx, reclaim_seen_rx) = oneshot::channel();
    let (release_reclaim_tx, release_reclaim_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let renew = read_record(&mut stream).await?;
        reply(&mut stream, &renew, &failed_lock_result("renew", 30, 10011)).await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&23u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7a; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        let _ = reclaim_seen_tx.send(());
        let _ = release_reclaim_rx.await;
        reply(&mut stream, &reclaim, &open_result(0, [0x85; 16], b"fh")).await
    });

    reclaim_seen_rx.await.unwrap();
    let for_umount = Arc::clone(&mount);
    let mut umount = tokio::spawn(async move { for_umount.umount().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut umount)
            .await
            .is_err(),
        "umount completed while the sent reclaim was unsettled"
    );
    umount.abort();
    let _ = umount.await;
    let _ = release_reclaim_tx.send(());
    server.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if mount.state.by_owner(75).await.unwrap().lock().await.stateid == [0x85; 16] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled umount abandoned the sent reclaim settlement");
    mount.umount().await.unwrap();
    assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Closed);
}

#[tokio::test]
async fn scripted_recovery_reclaims_open_before_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 71, b"fh").await;
    mount
        .locks
        .register(LockLane {
            owner: 72,
            open_owner: 71,
            owner_wire: Bytes::from_static(b"lock-owner"),
            next_seqid: 1,
            stateid: [0x72; 16],
            fh: Bytes::from_static(b"fh"),
            lock_type: 2,
            offset: 7,
            length: 11,
        })
        .await;
    let original_lock_token = crate::LockToken::new(
        Bytes::from_static(b"fh"),
        Bytes::from_static(&[0x72; 16]),
        2,
        7,
        11,
        mount.issuer,
        mount.lease.generation(),
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&17u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x77; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim_open = read_record(&mut stream).await?;
        assert!(
            reclaim_open
                .windows(4)
                .any(|wire| wire == 18u32.to_be_bytes())
        );
        reply(
            &mut stream,
            &reclaim_open,
            &open_result(0, [0x81; 16], b"fh"),
        )
        .await?;
        let reclaim_lock = read_record(&mut stream).await?;
        assert!(
            reclaim_lock
                .windows(8)
                .any(|wire| { wire == [2u32.to_be_bytes(), 1u32.to_be_bytes()].concat() })
        );
        reply(
            &mut stream,
            &reclaim_lock,
            &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x82; 16])]),
        )
        .await?;
        let locku = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0x83; 16])]),
        )
        .await?;
        let release = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &release,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    let identity = ClientIdentity {
        verifier: [0x55; 8],
        owner: "scripted-recovery".into(),
    };
    mount.lease.mark_recovering();
    RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity,
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    }
    .recover()
    .await
    .unwrap();
    assert_eq!(mount.client_id.load(Ordering::Acquire), 17);
    assert_eq!(
        mount.state.by_owner(71).await.unwrap().lock().await.stateid,
        [0x81; 16]
    );
    assert!(mount.locks.by_stateid(&[0x72; 16]).await.is_some());
    assert!(mount.locks.by_stateid(&[0x82; 16]).await.is_some());
    mount.lease.mark_ready();
    mount.unlock_stateful(original_lock_token).await.unwrap();
    assert!(mount.locks.by_stateid(&[0x72; 16]).await.is_none());
    assert!(mount.locks.by_stateid(&[0x82; 16]).await.is_none());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_reclaims_independent_open_and_lock_owners_concurrently() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 76, b"fh-a").await;
    register_scripted_open(&mount, 77, b"fh-b").await;
    register_scripted_lock(&mount, 76, [0x76; 16]).await;
    register_scripted_lock(&mount, 77, [0x77; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&29u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7b; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;

        // Reading both before replying proves that one owner's pending OPEN
        // does not prevent another independent owner from being reclaimed.
        let first = read_record(&mut stream).await?;
        let second = tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
            .await
            .expect("second owner reclaim was globally serialized")?;
        reply(&mut stream, &first, &open_result(0, [0x86; 16], b"fh-a")).await?;
        reply(&mut stream, &second, &open_result(0, [0x87; 16], b"fh-b")).await?;

        let first_lock = read_record(&mut stream).await?;
        let second_lock =
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .expect("second owner LOCK reclaim was globally serialized")?;
        reply(
            &mut stream,
            &first_lock,
            &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x96; 16])]),
        )
        .await?;
        reply(
            &mut stream,
            &second_lock,
            &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x97; 16])]),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5b; 8],
            owner: "concurrent-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover().await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_bounds_concurrent_open_reclaims() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    for owner in 100..117 {
        register_scripted_open(&mount, owner, b"fh").await;
    }
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let identity_data = [31u64.to_be_bytes().as_slice(), &[0x7c; 8]].concat();
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;

        let mut first_window = Vec::new();
        for _ in 0..16 {
            first_window.push(read_record(&mut stream).await?);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "recovery exceeded its 16-owner concurrency bound"
        );
        for request in first_window {
            reply(&mut stream, &request, &open_result(0, [0x88; 16], b"fh")).await?;
        }
        let final_request = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &final_request,
            &open_result(0, [0x89; 16], b"fh"),
        )
        .await
    });
    mount.lease.mark_recovering();
    RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5c; 8],
            owner: "bounded-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    }
    .recover()
    .await
    .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_bounds_concurrent_lock_owner_reclaims() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    for owner in 200..217u64 {
        register_scripted_open(&mount, owner, b"fh").await;
        register_scripted_lock(&mount, owner, [owner as u8; 16]).await;
    }
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let identity_data = [32u64.to_be_bytes().as_slice(), &[0x7d; 8]].concat();
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;

        let mut open_window = Vec::new();
        for _ in 0..MAX_CONCURRENT_RECLAIMS {
            open_window.push(read_record(&mut stream).await?);
        }
        for (index, request) in open_window.into_iter().enumerate() {
            reply(
                &mut stream,
                &request,
                &open_result(0, [index as u8; 16], b"fh"),
            )
            .await?;
        }
        let final_open = read_record(&mut stream).await?;
        reply(&mut stream, &final_open, &open_result(0, [0xf0; 16], b"fh")).await?;

        let mut lock_window = Vec::new();
        for _ in 0..MAX_CONCURRENT_RECLAIMS {
            lock_window.push(read_record(&mut stream).await?);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "recovery exceeded its lock-owner concurrency bound"
        );
        for (index, request) in lock_window.into_iter().enumerate() {
            reply(
                &mut stream,
                &request,
                &compound_result(
                    "reclaim-lock",
                    &[(22, &[]), (12, &[(index as u8).wrapping_add(1); 16])],
                ),
            )
            .await?;
        }
        let final_lock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &final_lock,
            &compound_result("reclaim-lock", &[(22, &[]), (12, &[0xf1; 16])]),
        )
        .await
    });
    mount.lease.mark_recovering();
    RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5d; 8],
            owner: "bounded-lock-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    }
    .recover()
    .await
    .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_serializes_locks_that_share_an_open_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 78, b"fh").await;
    for (owner, stateid) in [(780, [0x78; 16]), (781, [0x79; 16])] {
        mount
            .locks
            .register(LockLane {
                owner,
                open_owner: 78,
                owner_wire: Bytes::from(format!("same-open-{owner}")),
                next_seqid: 1,
                stateid,
                fh: Bytes::from_static(b"fh"),
                lock_type: 2,
                offset: owner - 780,
                length: 1,
            })
            .await;
    }
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&31u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7c; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0x88; 16], b"fh")).await?;

        let first = read_record(&mut stream).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), read_record(&mut stream))
                .await
                .is_err(),
            "same-owner LOCK reclaims were concurrently in flight"
        );
        reply(
            &mut stream,
            &first,
            &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x98; 16])]),
        )
        .await?;
        let second = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &second,
            &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x99; 16])]),
        )
        .await
    });
    RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5c; 8],
            owner: "same-owner-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    }
    .recover()
    .await
    .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn reclaim_rejected_by_fsal_falls_back_to_fresh_open_per_lane() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 73, b"fh").await;
    register_scripted_lock(&mount, 73, [0x74; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        // RFC 7530 §9.6.3.4.5: the FSAL rejects CLAIM_PREVIOUS, so the
        // client re-establishes the open with a normal CLAIM_NULL OPEN.
        let reclaim = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10033),
        )
        .await?;
        let fresh = read_record(&mut stream).await?;
        reply(&mut stream, &fresh, &open_result(0, [0x7a; 16], b"fh")).await?;
        let reclaim_lock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim_lock,
            &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x7b; 16])]),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-no-grace".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover_or_lose().await.unwrap();
    // A rejected reclaim no longer discards the mount: only that lane is
    // re-opened, and the generation (and lease identity) is preserved.
    assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
    assert_eq!(mount.health().generation, 1);
    let lane = mount.state.by_owner(73).await.unwrap();
    assert_eq!(lane.lock().await.stateid, [0x7a; 16]);
    let lock = mount.locks.by_stateid(&[0x7b; 16]).await.unwrap();
    assert_eq!(lock.lock().await.stateid, [0x7b; 16]);
    // The pre-recovery stateid remains a valid alias for the same lane.
    let aliased = mount.locks.by_stateid(&[0x74; 16]).await.unwrap();
    assert!(Arc::ptr_eq(&lock, &aliased));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn non_consuming_reclaim_rejection_keeps_the_reclaim_cursor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 72, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        // BAD_SEQID is in the §9.1.7 exception table, so the reclaim did not
        // consume seqid 0. The fallback OPEN therefore starts at seqid 0 too.
        let expected = [
            0u32.to_be_bytes().as_slice(),
            crate::OPEN_BOTH.to_be_bytes().as_slice(),
        ]
        .concat();
        assert!(
            reclaim.windows(expected.len()).any(|wire| wire == expected),
            "reclaim OPEN must carry seqid 0"
        );
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10026),
        )
        .await?;
        let fresh = read_record(&mut stream).await?;
        assert!(
            fresh.windows(expected.len()).any(|wire| wire == expected),
            "fresh OPEN must reuse seqid 0 that BAD_SEQID never consumed"
        );
        // End the recovery pass on a retryable status so the reclaim cursor is
        // not cleared by a successful recovery.
        reply(
            &mut stream,
            &fresh,
            &failed_lock_result("reclaim-fallback-open", 18, 10013),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-non-consuming-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::Reclaiming
    );
    assert_eq!(
        mount.lease.open_reclaim_seqid(72),
        1,
        "the reclaim did not consume, but the fallback OPEN did; the cursor must follow it"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn reclaim_putfh_failure_does_not_consume_the_reclaim_cursor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 71, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x71; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let expected = [
            0u32.to_be_bytes().as_slice(),
            crate::OPEN_BOTH.to_be_bytes().as_slice(),
        ]
        .concat();
        let reclaim = read_record(&mut stream).await?;
        assert!(
            reclaim.windows(expected.len()).any(|wire| wire == expected),
            "reclaim OPEN must carry seqid 0"
        );
        // SERVERFAULT is normally consuming, but a PUTFH failure means the
        // reclaim OPEN never ran, so seqid 0 must stay unconsumed.
        reply(
            &mut stream,
            &reclaim,
            &putfh_failure_result("reclaim-open", 10006),
        )
        .await?;
        let fresh = read_record(&mut stream).await?;
        assert!(
            fresh.windows(expected.len()).any(|wire| wire == expected),
            "fresh OPEN must reuse seqid 0 that PUTFH never consumed"
        );
        // End the recovery pass on a retryable status so the reclaim cursor is
        // not cleared by a successful recovery.
        reply(
            &mut stream,
            &fresh,
            &failed_lock_result("reclaim-fallback-open", 18, 10013),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x51; 8],
            owner: "scripted-putfh-failed-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        mount.lease.open_reclaim_seqid(71),
        1,
        "the PUTFH-failed reclaim did not consume, but the fallback OPEN did"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn reclaim_fallback_uses_the_reclaim_cursor_after_busy_retries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 90, b"fh").await;
    let lane = mount.state.by_owner(90).await.unwrap();
    // The lane did a lot of normal work before the state loss; its cursor is
    // unrelated to the reclaim session that starts at seqid 0.
    lane.lock().await.next_seqid = 7;
    let owner_wire = crate::nfs40::recovery::open_owner_wire(mount.issuer, 90);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        // First pass: the reclaim consumes seqid 0 but the server is busy.
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        assert_eq!(
            open_owner_seqid(&reclaim, &owner_wire),
            0,
            "the first reclaim of a session starts at seqid 0"
        );
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10013),
        )
        .await?;
        // Second pass: the retry uses the consumed cursor 1, but is rejected
        // without consuming it (§9.1.7 table, NFS4ERR_MOVED).
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        assert_eq!(
            open_owner_seqid(&reclaim, &owner_wire),
            1,
            "the busy retry must carry the next unconsumed seqid"
        );
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10019),
        )
        .await?;
        // The fallback first probes seqid 0 (owner released) and then retries
        // with the seqid the reclaim session actually consumed. The pre-loss
        // lane cursor (7) must not be used: the server would answer BAD_SEQID.
        let probe = read_record(&mut stream).await?;
        assert_eq!(open_owner_seqid(&probe, &owner_wire), 0);
        reply(
            &mut stream,
            &probe,
            &failed_lock_result("reclaim-fallback-open", 18, 10026),
        )
        .await?;
        let retry = read_record(&mut stream).await?;
        assert_eq!(
            open_owner_seqid(&retry, &owner_wire),
            1,
            "the fallback retry must use the reclaim ledger, not the stale lane cursor"
        );
        reply(&mut stream, &retry, &open_result(0, [0xf2; 16], b"fh")).await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-busy-fallback".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    // `recover` leaves the backoff/retry bookkeeping to its caller, so driving
    // it directly keeps both passes deterministic.
    let error = context.recover().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        mount.lease.open_reclaim_seqid(90),
        1,
        "the consumed GRACE reclaim must advance the session cursor"
    );
    context.recover().await.unwrap();
    let lane = mount.state.by_owner(90).await.unwrap();
    let guard = lane.lock().await;
    assert_eq!(guard.stateid, [0xf2; 16]);
    assert_eq!(guard.next_seqid, 2);
    assert_eq!(mount.lease.open_reclaim_seqid(90), 2);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn incompatible_reclaim_releases_its_state_and_delegation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 91, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        // The reclaim succeeds but the path now resolves to another file. The
        // returned state (and its delegation) must be released, not adopted.
        let reclaim = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim,
            &open_result_with_read_delegation([0xe2; 16], [0xed; 16], b"other"),
        )
        .await?;
        let delegreturn = read_record(&mut stream).await?;
        assert!(
            delegreturn.windows(16).any(|wire| wire == [0xed; 16]),
            "the incompatible reclaim must return the delegation it was granted"
        );
        reply(
            &mut stream,
            &delegreturn,
            &compound_result("delegreturn", &[(22, &[]), (8, &[])]),
        )
        .await?;
        let close = read_record(&mut stream).await?;
        let expected = [1u32.to_be_bytes().as_slice(), &[0xe2; 16]].concat();
        assert!(
            close.windows(expected.len()).any(|wire| wire == expected),
            "the reclaimed open must be closed with the next owner seqid"
        );
        reply(&mut stream, &close, &close_result([0xe2; 16])).await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-incompatible-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover().await.unwrap();
    assert!(
        mount.state.by_owner(91).await.is_none(),
        "a reclaim that returned unusable state must drop only that lane"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_grace_keeps_state_and_defers_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 76, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10013),
        )
        .await?;
        // The backoff must suppress a second recovery: no further request
        // may arrive on this connection.
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-grace".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)),
        "unexpected error: {error:?}"
    );
    // Grace is a retry signal: state and identity survive, but the mount
    // stays gated (Reclaiming) and stops extending the lease deadline: a
    // fresh OPEN during grace would wipe opens and locks that must be kept.
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::Reclaiming
    );
    assert_eq!(mount.health().generation, 1);
    assert!(!mount.state.snapshot().await.is_empty());
    assert!(mount.lease.active_recovery_backoff().is_some());
    // GRACE is outside the §9.1.7 exception table, so the retry must send the
    // next unconsumed open-owner seqid instead of replaying seqid 0.
    assert_eq!(mount.lease.open_reclaim_seqid(76), 1);
    let retry = context.recover_or_lose().await.unwrap_err();
    assert!(matches!(
        retry,
        NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)
    ));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_delay_keeps_state_and_defers_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 78, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        // NFS4ERR_DELAY is the other retryable status: the reclaim was not
        // executed, so the lane must survive exactly as it does for GRACE.
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10008),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-delay".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_DELAY)),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::Reclaiming
    );
    assert_eq!(mount.health().generation, 1);
    assert!(!mount.state.snapshot().await.is_empty());
    assert!(mount.lease.active_recovery_backoff().is_some());
    // DELAY is also outside the exception table.
    assert_eq!(mount.lease.open_reclaim_seqid(78), 1);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn retry_settled_stops_at_the_deadline_or_a_terminal_lifecycle() {
    // lease_seconds = 0 puts the deadline in the past.
    assert!(retry_settled(&LeaseState::ready(1, 0)));

    let active = LeaseState::ready(1, 60);
    active.mark_reclaiming();
    assert!(!retry_settled(&active));

    active.mark_ready();
    assert!(retry_settled(&active));

    let lost = LeaseState::ready(1, 60);
    lost.mark_lost().await;
    assert!(retry_settled(&lost));

    let closing = LeaseState::ready(1, 60);
    closing.mark_closing();
    assert!(retry_settled(&closing));

    let closed = LeaseState::ready(1, 60);
    closed.mark_closed();
    assert!(retry_settled(&closed));
}

#[tokio::test]
async fn concurrent_recoveries_are_serialized() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 89, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        for client_id in [19u64, 20] {
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&client_id.to_be_bytes());
            identity_data.extend_from_slice(&[0x79; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let reclaim = read_record(&mut stream).await?;
            if client_id == 19 {
                // The waiting caller must not issue its own SETCLIENTID
                // while this reclaim is still in flight.
                assert!(
                    tokio::time::timeout(Duration::from_millis(30), read_record(&mut stream))
                        .await
                        .is_err(),
                    "concurrent recovery issued a second SETCLIENTID"
                );
            }
            reply(&mut stream, &reclaim, &open_result(0, [0x63; 16], b"fh")).await?;
        }
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-serialized".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let second = context.clone();
    let (first, next) = tokio::join!(context.recover_or_lose(), second.recover_or_lose());
    first.unwrap();
    next.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn busy_recovery_is_retried_after_the_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 87, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        // First recovery hits grace; the client must come back on its own
        // once the backoff lapses and then reclaim the lane successfully.
        for client_id in [19u64, 20] {
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&client_id.to_be_bytes());
            identity_data.extend_from_slice(&[0x79; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            let reclaim = read_record(&mut stream).await?;
            if client_id == 19 {
                reply(
                    &mut stream,
                    &reclaim,
                    &failed_lock_result("reclaim-open", 18, 10013),
                )
                .await?;
            } else {
                reply(&mut stream, &reclaim, &open_result(0, [0x62; 16], b"fh")).await?;
            }
        }
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-grace-retry".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_GRACE)),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::Reclaiming
    );
    let retried = tokio::time::timeout(Duration::from_secs(10), async {
        while mount.health().lifecycle != crate::MountLifecycleState::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        retried.is_ok(),
        "GRACE backoff never retried recovery; mount stayed {:?}",
        mount.health().lifecycle
    );
    assert!(mount.lease.active_recovery_backoff().is_none());
    // mark_recovered resumes the lease but only RENEW counts as a renewal.
    assert_eq!(mount.health().lease_renewals, 0);
    let lane = mount.state.by_owner(87).await.unwrap();
    assert_eq!(lane.lock().await.stateid, [0x62; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn busy_lock_reclaim_retries_with_the_next_unconsumed_seqids() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 88, b"fh").await;
    register_scripted_lock(&mount, 88, [0x61; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        // Both recovery sessions get the same client id, so the reclaimed-owner
        // ledger survives and only the lock is retried.
        for pass in 0..2 {
            let identity = read_record(&mut stream).await?;
            let mut identity_data = Vec::new();
            identity_data.extend_from_slice(&19u64.to_be_bytes());
            identity_data.extend_from_slice(&[0x79; 8]);
            reply(
                &mut stream,
                &identity,
                &compound_result("identity", &[(35, &identity_data)]),
            )
            .await?;
            let confirm = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &confirm,
                &compound_result("confirm", &[(36, &[])]),
            )
            .await?;
            if pass == 0 {
                // OPEN reclaim succeeds and leaves the open owner at seqid 1.
                let open = read_record(&mut stream).await?;
                reply(&mut stream, &open, &open_result(0, [0x62; 16], b"fh")).await?;
                let lock = read_record(&mut stream).await?;
                let expected = [
                    1u32.to_be_bytes().as_slice(),
                    &[0x62; 16],
                    0u32.to_be_bytes().as_slice(),
                ]
                .concat();
                assert!(
                    lock.windows(expected.len()).any(|wire| wire == expected),
                    "first reclaim LOCK did not carry open_seqid=1 and lock_seqid=0"
                );
                // DELAY consumed both owner seqids.
                reply(
                    &mut stream,
                    &lock,
                    &failed_lock_result("reclaim-lock", 12, 10008),
                )
                .await?;
            } else {
                // The successful OPEN is not replayed; the retry uses the seqids
                // the DELAYed LOCK consumed.
                let lock = read_record(&mut stream).await?;
                let expected = [
                    2u32.to_be_bytes().as_slice(),
                    &[0x62; 16],
                    1u32.to_be_bytes().as_slice(),
                ]
                .concat();
                assert!(
                    lock.windows(expected.len()).any(|wire| wire == expected),
                    "lock retry replayed a consumed open/lock seqid"
                );
                reply(
                    &mut stream,
                    &lock,
                    &compound_result("reclaim-lock", &[(22, &[]), (12, &[0x63; 16])]),
                )
                .await?;
            }
        }
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-busy-lock-retry".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_DELAY)),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        mount
            .state
            .by_owner(88)
            .await
            .unwrap()
            .lock()
            .await
            .next_seqid,
        2
    );
    assert_eq!(mount.lease.lock_reclaim_seqid(88), 1);
    assert_eq!(mount.lease.open_reclaim_seqid(88), 1);
    // Reconnecting during the busy backoff and a successful RENEW must not
    // resume service or cancel the pending reclaim.
    mount.lease.mark_reconnecting();
    mount.lease.mark_renewed();
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::Reclaiming
    );
    let retried = tokio::time::timeout(Duration::from_secs(12), async {
        while mount.health().lifecycle != crate::MountLifecycleState::Ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        retried.is_ok(),
        "DELAY backoff never retried recovery; mount stayed {:?}",
        mount.health().lifecycle
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn reclaim_lock_putfh_failure_does_not_consume_either_cursor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 70, b"fh").await;
    register_scripted_lock(&mount, 70, [0x60; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0x64; 16], b"fh")).await?;
        let lock = read_record(&mut stream).await?;
        let expected = [
            1u32.to_be_bytes().as_slice(),
            &[0x64; 16],
            0u32.to_be_bytes().as_slice(),
        ]
        .concat();
        assert!(
            lock.windows(expected.len()).any(|wire| wire == expected),
            "reclaim LOCK must carry open_seqid=1 and lock_seqid=0"
        );
        // DELAY is retryable, but the PUTFH failure means LOCK never ran and
        // neither owner seqid was consumed.
        reply(
            &mut stream,
            &lock,
            &putfh_failure_result("reclaim-lock", 10008),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x58; 8],
            owner: "scripted-putfh-failed-lock".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    assert!(
        matches!(error, NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_DELAY)),
        "unexpected error: {error:?}"
    );
    assert_eq!(mount.lease.lock_reclaim_seqid(70), 0);
    assert_eq!(
        mount
            .state
            .by_owner(70)
            .await
            .unwrap()
            .lock()
            .await
            .next_seqid,
        1,
        "the successful reclaim OPEN consumed 0, but the LOCK never ran"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn stale_clientid_during_reclaim_is_not_treated_as_lane_loss() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 90, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let open = read_record(&mut stream).await?;
        // The server restarted again: the client id used for the reclaim is
        // stale, which invalidates every lane rather than just this one.
        reply(
            &mut stream,
            &open,
            &failed_lock_result("reclaim-open", 18, 10022),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-stale-clientid".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    assert!(context.recover_or_lose().await.is_err());
    // The lane must not be silently discarded while the mount is promoted to
    // Ready; a session-wide failure is terminal for this recovery.
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState
    );
    assert!(mount.state.snapshot().await.is_empty());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_success_after_lost_state_is_not_published() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 91, b"fh").await;
    let (reclaim_tx, mut reclaim_rx) = tokio::sync::mpsc::unbounded_channel();
    let (reply_tx, reply_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let open = read_record(&mut stream).await?;
        reclaim_tx.send(()).ok();
        // Hold the successful reclaim until the renewal deadline has crossed.
        let _ = reply_rx.await;
        reply(&mut stream, &open, &open_result(0, [0x62; 16], b"fh")).await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-lost-state".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let recovery = tokio::spawn(async move { context.recover_or_lose().await });
    reclaim_rx.recv().await.unwrap();
    // RENEW crossed the lease deadline while the reclaim was in flight.
    mount.lease.mark_lost().await;
    reply_tx.send(()).unwrap();
    assert!(recovery.await.unwrap().is_err());
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState,
        "a late successful reclaim must not revive a lost mount"
    );
    assert!(
        mount.state.snapshot().await.is_empty(),
        "a late successful reclaim must not leave usable lanes behind"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn queued_recovery_does_not_start_after_the_mount_lost_its_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 92, b"fh").await;
    let (reclaim_tx, mut reclaim_rx) = tokio::sync::mpsc::unbounded_channel();
    let (reply_tx, reply_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7a; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let open = read_record(&mut stream).await?;
        reclaim_tx.send(()).ok();
        let _ = reply_rx.await;
        reply(&mut stream, &open, &open_result(0, [0x63; 16], b"fh")).await
    });
    let first_context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5a; 8],
            owner: "scripted-queued-recovery".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let second_context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5a; 8],
            owner: "scripted-queued-recovery".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let first = tokio::spawn(async move { first_context.recover_or_lose().await });
    reclaim_rx.recv().await.unwrap();
    // This attempt passes the early LostState check while the first recovery
    // still holds the guard, so it must re-check once it acquires the guard.
    let second = tokio::spawn(async move { second_context.recover_or_lose().await });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    mount.lease.mark_lost().await;
    reply_tx.send(()).unwrap();
    assert!(first.await.unwrap().is_err());
    let error = second.await.unwrap().unwrap_err();
    assert!(
        matches!(error, NfsError::LostOpenState(_)),
        "a queued recovery must not run after the mount lost its state: {error:?}"
    );
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn reclaim_failure_drops_only_the_failed_lane() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 81, b"fh1").await;
    register_scripted_open(&mount, 82, b"fh2").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        // One lane's file is gone: its reclaim is rejected, and the fresh
        // OPEN fallback reports the same NOENT. The other lane reclaims
        // normally. Reclaims run concurrently, so answer by payload.
        for _ in 0..3 {
            let request = read_record(&mut stream).await?;
            if request.windows(3).any(|wire| wire == b"fh1") {
                reply(
                    &mut stream,
                    &request,
                    &failed_lock_result("reclaim-open", 18, 10033),
                )
                .await?;
            } else if request.windows(13).any(|wire| wire == b"scripted-file") {
                reply(
                    &mut stream,
                    &request,
                    &failed_lock_result("reclaim-fallback", 18, 2),
                )
                .await?;
            } else {
                reply(&mut stream, &request, &open_result(0, [0x83; 16], b"fh2")).await?;
            }
        }
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-partial".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover_or_lose().await.unwrap();
    assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
    // Exactly one lane was lost; the mount and its generation survive.
    let remaining = mount.state.snapshot().await;
    assert_eq!(remaining.len(), 1);
    assert_eq!(mount.health().generation, 1);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_skips_closing_lanes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 84, [0x84; 16], crate::OPEN_BOTH).await;
    lane.lock().await.closing = true;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-closing".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    // No reclaim request is sent for the closing lane, so recovery does not
    // wait on (or deadlock with) the CLOSE that owns the lane.
    context.recover_or_lose().await.unwrap();
    assert!(mount.state.by_owner(84).await.is_some());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn partial_reclaim_compound_outcome_is_unknown_and_keeps_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 79, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&37u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7d; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        let mut partial = open_result(0, [0x89; 16], b"fh");
        partial[..4].copy_from_slice(&10006u32.to_be_bytes());
        reply(&mut stream, &reclaim, &partial).await
    });
    let error = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5d; 8],
            owner: "partial-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    }
    .recover_or_lose()
    .await
    .unwrap_err();
    // The contradictory reclaim reply sends this lane through the fallback OPEN,
    // which then loses its reply. That outcome is unknown, not a definitive
    // rejection, so the mount keeps the lane and retries within the lease
    // window instead of discarding every open state.
    assert!(
        matches!(&error, crate::NfsError::Io(_)),
        "the fallback OPEN failure must stay an unknown outcome: {error:?}"
    );
    assert_ne!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState
    );
    assert_eq!(mount.health().generation, 1);
    assert!(
        !mount.state.snapshot().await.is_empty(),
        "an unknown outcome must keep the open lane for the retry"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn reclaim_reply_loss_is_retried_with_the_same_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 80, b"fh").await;
    let owner_wire = crate::nfs40::recovery::open_owner_wire(mount.issuer, 80);
    let (seqid_tx, seqid_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        // First pass: the reclaim may have executed, but its reply is lost (RST).
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&41u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7e; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        let first = open_owner_seqid(&reclaim, &owner_wire);
        socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
        drop(stream);

        // Second pass: the scheduled retry reconnects and must reuse the seqid.
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&42u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7f; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        let second = open_owner_seqid(&reclaim, &owner_wire);
        let _ = seqid_tx.send((first, second));
        reply(&mut stream, &reclaim, &open_result(0, [0x8a; 16], b"fh")).await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5e; 8],
            owner: "reply-loss-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let error = context.recover_or_lose().await.unwrap_err();
    // A lost reclaim reply may have executed on the server. Keep every lane and
    // let the scheduled retry resend the same seqid (an executed CLAIM_PREVIOUS
    // is answered with BAD_SEQID, which the per-lane fallback OPEN handles)
    // instead of declaring the whole mount lost after one dropped packet.
    assert!(
        matches!(&error, crate::NfsError::Io(_)),
        "a lost reclaim reply must surface as an unknown outcome: {error:?}"
    );
    assert_ne!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState
    );
    assert_eq!(mount.health().generation, 1);
    assert!(
        !mount.state.snapshot().await.is_empty(),
        "the ramp-up open lane must survive for the retry"
    );
    let (first, second) = tokio::time::timeout(Duration::from_secs(15), seqid_rx)
        .await
        .expect("the scheduled retry never reached the server")
        .expect("the retry dropped the seqid channel");
    assert_eq!(first, 0, "the first reclaim of a session starts at seqid 0");
    assert_eq!(
        second, first,
        "a reclaim whose reply was lost must be retried with the same seqid"
    );
    let lane = mount
        .state
        .by_owner(80)
        .await
        .expect("the recovered lane must stay published");
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while mount.health().lifecycle != crate::MountLifecycleState::Ready {
        assert!(
            std::time::Instant::now() < deadline,
            "the retry never resumed the mount after reclaiming the lane"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(lane.lock().await.stateid, [0x8a; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_open_after_send_finishes_owner_state_settlement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let (mut stream, _) = listener.accept().await.unwrap();
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let request = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        tokio::task::yield_now().await;
        reply(
            &mut stream,
            &request,
            &open_result(0, [0x51; 16], b"opened-fh"),
        )
        .await
    });
    let for_task = Arc::clone(&mount);
    let task = tokio::spawn(async move {
        for_task
            .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_BOTH)
            .await
    });
    seen_rx.await.unwrap();
    task.abort();
    let _ = task.await;
    server.await.unwrap().unwrap();
    for _ in 0..20 {
        if mount
            .state
            .for_fh(&Bytes::from_static(b"opened-fh"), crate::OPEN_BOTH)
            .await
            .is_some()
        {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("detached OPEN reply did not register owner state");
}

#[tokio::test]
async fn open_reply_loss_is_structured_as_uncertain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let _request = read_record(&mut stream).await?;
        socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
        drop(stream);
        Ok::<(), std::io::Error>(())
    });
    let error = mount
        .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_READ)
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn partial_open_compound_is_structured_as_uncertain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let mut partial = compound_result("open", &[(22, &[]), (18, &[])]);
        partial[..4].copy_from_slice(&10006u32.to_be_bytes()); // NFS4ERR_SERVERFAULT after OPEN
        reply(&mut stream, &request, &partial).await
    });
    let error = mount
        .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_READ)
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn conditional_open_confirm_uses_next_seqid_and_returned_stateid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &open_result(2, [0x61; 16], b"confirmed-fh"),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        let expected = [[0x61; 16].as_slice(), 1u32.to_be_bytes().as_slice()].concat();
        assert!(confirm.windows(expected.len()).any(|wire| wire == expected));
        let data = [0x62; 16];
        reply(
            &mut stream,
            &confirm,
            &compound_result("open_confirm", &[(22, &[]), (20, &data)]),
        )
        .await
    });
    let opened = mount
        .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_BOTH)
        .await
        .unwrap();
    let owner = decode_owner(&opened.into_parts().1.unwrap()).unwrap().1;
    let lane = mount.state.by_owner(owner).await.unwrap();
    assert_eq!(lane.lock().await.stateid, [0x62; 16]);
    assert_eq!(lane.lock().await.next_seqid, 2);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn disabled_retention_returns_an_unsolicited_delegation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &open_result_with_read_delegation([0x61; 16], [0x71; 16], b"delegated-fh"),
        )
        .await?;
        let delegreturn =
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .expect("unsolicited delegation was not returned")?;
        assert!(delegreturn.windows(16).any(|value| value == [0x71; 16]));
        reply(
            &mut stream,
            &delegreturn,
            &compound_result("delegreturn", &[(22, &[]), (8, &[])]),
        )
        .await
    });
    mount
        .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_READ)
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn disabled_retention_reports_unsolicited_delegreturn_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &open_result_with_read_delegation([0x61; 16], [0x71; 16], b"delegated-fh"),
        )
        .await?;
        let delegreturn = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &delegreturn,
            &failed_lock_result("delegreturn", 8, 10006),
        )
        .await?;
        let close = tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
            .await
            .map_err(|_| {
                std::io::Error::other("OPEN state was not released after DELEGRETURN")
            })??;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x62; 16])]),
        )
        .await
    });

    let error = mount
        .open_stateful(Bytes::from_static(b"root"), "file", crate::OPEN_READ)
        .await
        .expect_err("OPEN must not publish a delegation the client failed to return");
    assert!(matches!(error, NfsError::Nfs4(_)) || error.operation_outcome().is_some());
    // The caller never receives a handle, so the failed OPEN must release the
    // state itself instead of leaking it until the lease expires.
    assert!(mount.state.snapshot().await.is_empty());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scripted_mount_reconnect_preserves_confirmed_identity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mounted = Arc::new(Notify::new());
    let mounted_for_server = Arc::clone(&mounted);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await?;
        let values = [
            60u32.to_be_bytes().as_slice(),
            65536u64.to_be_bytes().as_slice(),
            32768u64.to_be_bytes().as_slice(),
        ]
        .concat();
        serve_mount(&mut first, (1 << 10) | (1 << 30) | (1 << 31), &values).await?;
        mounted_for_server.notified().await;
        socket2::SockRef::from(&first).set_linger(Some(Duration::ZERO))?;
        drop(first);

        let (mut second, _) = listener.accept().await?;
        let null = read_record(&mut second).await?;
        reply(&mut second, &null, &[]).await?;
        Ok::<(), std::io::Error>(())
    });

    let args = crate::MountArgs {
        versions: vec![NFSVersion::NFSv4p0],
        host: "127.0.0.1".to_string(),
        dirpath: "/export".to_string(),
        mountport: 0,
        nfsport: addr.port(),
        uid: 0,
        gid: 0,
        readdir_buffer: None,
        noresvport: true,
        retain_delegations: false,
        dialer: None,
    };
    let mount = mount_on_addr(&addr.into(), &args, Auth::new_null())
        .await
        .unwrap();
    assert_eq!(mount.get_max_read_size(), 65536);
    assert_eq!(mount.get_max_write_size(), 32768);
    // Without `readdir-buffer`, READDIR replies are sized from the published MAXREAD.
    assert_eq!((mount.dircount, mount.maxcount), (65536, 65536));
    assert_eq!(mount.root_fh, Bytes::from_static(b"scripted-fh"));
    mounted.notify_one();
    tokio::time::sleep(Duration::from_millis(10)).await;
    mount.null().await.unwrap();
    server.await.unwrap().unwrap();
    mount.umount().await.unwrap();
}

/// A v4.0 server that publishes no MAXREAD: READ falls back to the client ceiling, but
/// READDIR replies keep the 8 KiB default rather than growing to it (0.8.6); an explicit
/// `readdir-buffer` is used as given.
#[tokio::test]
async fn scripted_mount_without_maxread_keeps_the_default_directory_reply_size() {
    let lease_and_maxwrite = [
        60u32.to_be_bytes().as_slice(),
        32768u64.to_be_bytes().as_slice(),
    ]
    .concat();
    let default = crate::mount::DEFAULT_READDIR_BUFFER;
    for (readdir_buffer, expected) in [
        (None, (default, default)),
        (Some((2048, 4096)), (2048, 4096)),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let values = lease_and_maxwrite.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            serve_mount(&mut stream, (1 << 10) | (1 << 31), &values).await?;
            Ok::<TcpStream, std::io::Error>(stream)
        });
        let args = crate::MountArgs {
            versions: vec![NFSVersion::NFSv4p0],
            host: "127.0.0.1".to_string(),
            dirpath: "/export".to_string(),
            mountport: 0,
            nfsport: addr.port(),
            uid: 0,
            gid: 0,
            readdir_buffer,
            noresvport: true,
            retain_delegations: false,
            dialer: None,
        };
        let mount = mount_on_addr(&addr.into(), &args, Auth::new_null())
            .await
            .unwrap();
        assert_eq!(mount.get_max_read_size(), crate::mount::MAX_IO_SIZE);
        assert_eq!(mount.get_max_write_size(), 32768);
        assert_eq!((mount.dircount, mount.maxcount), expected);
        let _stream = server.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn retained_delegations_publish_callback_before_setclientid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        assert!(
            identity.windows(3).any(|value| value == b"tcp"),
            "SETCLIENTID did not publish a ready TCP callback listener"
        );
        let netid = identity
            .windows(3)
            .position(|value| value == b"tcp")
            .expect("TCP netid missing");
        let addr_len_offset = netid + 4;
        let addr_len = u32::from_be_bytes(
            identity[addr_len_offset..addr_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let universal =
            std::str::from_utf8(&identity[addr_len_offset + 4..addr_len_offset + 4 + addr_len])
                .unwrap();
        let fields: Vec<_> = universal.split('.').collect();
        assert_eq!(fields.len(), 6, "invalid callback universal address");
        let callback_addr = format!(
            "{}.{}.{}.{}:{}",
            fields[0],
            fields[1],
            fields[2],
            fields[3],
            fields[4].parse::<u16>().unwrap() * 256 + fields[5].parse::<u16>().unwrap()
        );
        TcpStream::connect(callback_addr).await?;
        Ok::<(), std::io::Error>(())
    });
    let args = crate::MountArgs {
        versions: vec![NFSVersion::NFSv4p0],
        host: "127.0.0.1".to_string(),
        dirpath: "/export".to_string(),
        mountport: 0,
        nfsport: addr.port(),
        uid: 0,
        gid: 0,
        readdir_buffer: Some((32 * 1024, 32 * 1024)),
        noresvport: true,
        retain_delegations: true,
        dialer: None,
    };

    let error = mount_on_addr(&addr.into(), &args, Auth::new_null())
        .await
        .unwrap_err();
    assert!(matches!(error, NfsError::Rpc(_) | NfsError::Io(_)));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scripted_public_io_survives_a_lost_callback_reply() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mux = rpc::StreamMux::connect(addr, true).await.unwrap();
    let rpc = rpc::Client::new(mux, None);
    let callback = CallbackService::bind_for(addr).await.unwrap();
    let callback_addr = callback.universal_addr().to_string();
    let callback_state = callback.state();
    let recalls = callback.take_recall_receiver().unwrap();
    let worker = CallbackWorker::start(
        recalls,
        rpc.clone(),
        Auth::new_null(),
        Arc::clone(&callback_state),
    );
    let mount = Arc::new(Mount40 {
        rpc,
        auth: Auth::new_null(),
        root_fh: Bytes::from_static(b"root"),
        client_id: Arc::new(AtomicU64::new(7)),
        issuer: 9,
        next_owner: AtomicU64::new(1),
        state: Arc::new(OpenState::default()),
        locks: Arc::new(LockState::default()),
        lease: LeaseState::ready(1, 60),
        _renewal: None,
        _callback: Some(callback),
        _callback_worker: Some(worker),
        callback_state: Some(callback_state),
        dircount: 8192,
        maxcount: 32768,
        rsize: 1_048_576,
        wsize: 1_048_576,
        acl_supported: false,
    });
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &open_result_with_read_delegation([0x61; 16], [0x71; 16], b"delegated-fh"),
        )
        .await?;
        let mut operations = Vec::new();
        for _ in 0..5 {
            let request = read_record(&mut stream).await?;
            let (tag, result) = if request.windows(11).any(|part| part == b"delegreturn") {
                (
                    "delegreturn",
                    compound_result("delegreturn", &[(22, &[]), (8, &[])]),
                )
            } else if request.windows(5).any(|part| part == b"write") {
                let data = [
                    4u32.to_be_bytes().as_slice(),
                    2u32.to_be_bytes().as_slice(),
                    [0x33; 8].as_slice(),
                ]
                .concat();
                ("write", compound_result("write", &[(22, &[]), (38, &data)]))
            } else if request.windows(6).any(|part| part == b"commit") {
                (
                    "commit",
                    compound_result("commit", &[(22, &[]), (5, &[0x33; 8])]),
                )
            } else if request.windows(4).any(|part| part == b"read") {
                let mut data = 1u32.to_be_bytes().to_vec();
                xdr_opaque(&mut data, b"data");
                ("read", compound_result("read", &[(22, &[]), (25, &data)]))
            } else {
                (
                    "close",
                    compound_result("close", &[(22, &[]), (4, &[0x62; 16])]),
                )
            };
            operations.push(tag);
            reply(&mut stream, &request, &result).await?;
        }
        Ok::<_, std::io::Error>(operations)
    });

    let opened = mount
        .open(Bytes::from_static(b"root"), "file", crate::OPEN_BOTH)
        .await
        .unwrap();
    let fields = callback_addr
        .split('.')
        .map(|field| field.parse::<u16>().unwrap())
        .collect::<Vec<_>>();
    let callback_socket = SocketAddr::from((
        [
            fields[0] as u8,
            fields[1] as u8,
            fields[2] as u8,
            fields[3] as u8,
        ],
        fields[4] * 256 + fields[5],
    ));
    let mut words = vec![
        77,
        0,
        2,
        super::super::callback::CB_PROGRAM,
        1,
        1,
        0,
        0,
        0,
        0,
    ];
    words.extend_from_slice(&[0, 0, 1, 1, 4]);
    words.extend_from_slice(&[0x7171_7171; 4]);
    words.extend_from_slice(&[0, 12]);
    words.extend(
        b"delegated-fh"
            .chunks_exact(4)
            .map(|word| u32::from_be_bytes(word.try_into().unwrap())),
    );
    let call = words
        .iter()
        .flat_map(|word| word.to_be_bytes())
        .collect::<Vec<_>>();
    let mut first = TcpStream::connect(callback_socket).await.unwrap();
    first
        .write_u32(0x8000_0000 | call.len() as u32)
        .await
        .unwrap();
    first.write_all(&call).await.unwrap();
    drop(first);
    let mut retry = TcpStream::connect(callback_socket).await.unwrap();
    retry
        .write_u32(0x8000_0000 | call.len() as u32)
        .await
        .unwrap();
    retry.write_all(&call).await.unwrap();
    let marker = retry.read_u32().await.unwrap();
    let mut reply_body = vec![0; (marker & 0x7fff_ffff) as usize];
    retry.read_exact(&mut reply_body).await.unwrap();
    assert_eq!(u32::from_be_bytes(reply_body[0..4].try_into().unwrap()), 77);
    for _ in 0..100 {
        if mount.callback_stats().await.returns_completed == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(mount.callback_stats().await.returns_completed, 1);
    assert_eq!(
        crate::write_all(&*mount, opened.fh.clone(), 0, Bytes::from_static(b"data"))
            .await
            .unwrap(),
        4
    );
    mount.commit(opened.fh.clone(), 0, 4).await.unwrap();
    assert_eq!(
        mount.read(opened.fh.clone(), 0, 4).await.unwrap(),
        Bytes::from_static(b"data")
    );
    mount.close(opened.fh).await.unwrap();
    assert_eq!(
        server.await.unwrap().unwrap(),
        ["delegreturn", "write", "commit", "read", "close"]
    );
}

#[tokio::test]
async fn public_getacl_uses_the_nfsv40_getattr_path() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let expected = mount::Acl {
        aces: vec![mount::NfsAce {
            ace_type: mount::AceType::AccessAllowed,
            flags: mount::AceFlags(0),
            access_mask: mount::AceMask(mount::AceMask::READ_DATA),
            who: "OWNER@".to_string(),
        }],
    };
    let expected_for_server = expected.clone();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let (bitmap, values) = crate::nfs4::acl::encode_setattr_acl(&expected_for_server);
        let mut getattr = Vec::new();
        getattr.extend_from_slice(&1u32.to_be_bytes());
        getattr.extend_from_slice(&bitmap[0].to_be_bytes());
        xdr_opaque(&mut getattr, &values);
        reply(
            &mut stream,
            &request,
            &compound_result("getacl", &[(22, &[]), (9, &getattr)]),
        )
        .await
    });
    assert_eq!(
        mount.getacl(Bytes::from_static(b"file")).await.unwrap(),
        expected
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn getacl_attrnotsupp_is_per_call_and_preserves_aclsupport() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount_with_acl(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let support_request = read_record(&mut stream).await?;
        let mut getattr = Vec::new();
        getattr.extend_from_slice(&1u32.to_be_bytes());
        getattr.extend_from_slice(&(1u32 << 13).to_be_bytes());
        xdr_opaque(&mut getattr, &mount::AclSupport::ALLOW.to_be_bytes());
        reply(
            &mut stream,
            &support_request,
            &compound_result("aclsupport", &[(22, &[]), (9, &getattr)]),
        )
        .await?;
        let request = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &request,
            &failed_lock_result("getacl", 9, 10032),
        )
        .await?;
        let support_request = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &support_request,
            &compound_result("aclsupport", &[(22, &[]), (9, &getattr)]),
        )
        .await?;
        let request = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &request,
            &failed_lock_result("getacl", 9, 10032),
        )
        .await?;
        Ok::<_, std::io::Error>(())
    });

    assert_eq!(
        mount.aclsupport(Bytes::from_static(b"file")).await.unwrap(),
        mount::AclSupport(mount::AclSupport::ALLOW)
    );
    let first = mount.getacl(Bytes::from_static(b"file")).await.unwrap_err();
    assert!(matches!(first, NfsError::Unsupported(_)));
    assert!(mount.capabilities().acl);
    assert_eq!(
        mount.aclsupport(Bytes::from_static(b"file")).await.unwrap(),
        mount::AclSupport(mount::AclSupport::ALLOW)
    );
    let second = mount
        .getacl(Bytes::from_static(b"other"))
        .await
        .unwrap_err();
    assert!(matches!(second, NfsError::Unsupported(_)));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn setacl_attrnotsupp_does_not_suppress_a_later_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount_with_acl(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &request,
            &failed_lock_result("setacl", 34, 10032),
        )
        .await?;
        let request = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &request,
            &failed_lock_result("setacl", 34, 10032),
        )
        .await?;
        Ok::<_, std::io::Error>(())
    });

    let first = mount
        .setacl(Bytes::from_static(b"file"), &mount::Acl::default())
        .await
        .unwrap_err();
    assert!(matches!(first, NfsError::Unsupported(_)));
    assert!(mount.capabilities().acl);
    let second = mount
        .setacl(Bytes::from_static(b"other"), &mount::Acl::default())
        .await
        .unwrap_err();
    assert!(matches!(second, NfsError::Unsupported(_)));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn public_setacl_and_aclsupport_use_nfsv40_attributes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let acl = mount::Acl {
        aces: vec![mount::NfsAce {
            ace_type: mount::AceType::AccessAllowed,
            flags: mount::AceFlags(0),
            access_mask: mount::AceMask(mount::AceMask::READ_DATA),
            who: "OWNER@".to_string(),
        }],
    };
    let (_, encoded_acl) = crate::nfs4::acl::encode_setattr_acl(&acl);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let setacl = read_record(&mut stream).await?;
        assert!(
            setacl
                .windows(encoded_acl.len())
                .any(|wire| wire == encoded_acl)
        );
        let attrsset = [1u32.to_be_bytes(), (1u32 << 12).to_be_bytes()].concat();
        reply(
            &mut stream,
            &setacl,
            &compound_result("setacl", &[(22, &[]), (34, &attrsset)]),
        )
        .await?;
        let support = read_record(&mut stream).await?;
        let mut getattr = Vec::new();
        getattr.extend_from_slice(&1u32.to_be_bytes());
        getattr.extend_from_slice(&(1u32 << 13).to_be_bytes());
        xdr_opaque(
            &mut getattr,
            &(mount::AclSupport::ALLOW | mount::AclSupport::DENY).to_be_bytes(),
        );
        reply(
            &mut stream,
            &support,
            &compound_result("aclsupport", &[(22, &[]), (9, &getattr)]),
        )
        .await
    });
    mount
        .setacl(Bytes::from_static(b"file"), &acl)
        .await
        .unwrap();
    assert_eq!(
        mount.aclsupport(Bytes::from_static(b"file")).await.unwrap(),
        mount::AclSupport(mount::AclSupport::ALLOW | mount::AclSupport::DENY)
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelling_after_setclientid_detaches_while_confirm_settles() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (identity_seen_tx, identity_seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        assert!(
            identity
                .windows(4)
                .any(|value| value == 35u32.to_be_bytes())
        );
        let _ = identity_seen_tx.send(());
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x77; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        Ok::<bool, std::io::Error>(confirm.windows(4).any(|value| value == 36u32.to_be_bytes()))
    });
    let args = crate::MountArgs {
        versions: vec![NFSVersion::NFSv4p0],
        host: "127.0.0.1".to_string(),
        dirpath: "/export".to_string(),
        mountport: 0,
        nfsport: addr.port(),
        uid: 0,
        gid: 0,
        readdir_buffer: Some((32 * 1024, 32 * 1024)),
        noresvport: true,
        retain_delegations: false,
        dialer: None,
    };
    let task =
        tokio::spawn(async move { mount_on_addr(&addr.into(), &args, Auth::new_null()).await });
    identity_seen_rx.await.unwrap();
    task.abort();
    let _ = task.await;
    assert!(
        server.await.unwrap().unwrap(),
        "sent SETCLIENTID was not settled with confirm"
    );
}

fn directory_page(entry_cookie: u64, include_eof: bool) -> Bytes {
    let mut data = Vec::new();
    data.extend_from_slice(b"verifier");
    data.extend_from_slice(&1u32.to_be_bytes());
    data.extend_from_slice(&entry_cookie.to_be_bytes());
    xdr_opaque(&mut data, b"file");
    data.extend_from_slice(&1u32.to_be_bytes());
    data.extend_from_slice(&(1u32 << 20).to_be_bytes());
    data.extend_from_slice(&8u32.to_be_bytes());
    data.extend_from_slice(&42u64.to_be_bytes());
    data.extend_from_slice(&0u32.to_be_bytes());
    if include_eof {
        data.extend_from_slice(&1u32.to_be_bytes());
    }
    Bytes::from(data)
}

fn readdir_result(verifier: [u8; 8], cookie: u64, name: &[u8], eof: bool) -> Vec<u8> {
    let mut page = Vec::new();
    page.extend_from_slice(&verifier);
    page.extend_from_slice(&1u32.to_be_bytes());
    page.extend_from_slice(&cookie.to_be_bytes());
    xdr_opaque(&mut page, name);
    page.extend_from_slice(&1u32.to_be_bytes());
    page.extend_from_slice(&(1u32 << 20).to_be_bytes());
    page.extend_from_slice(&8u32.to_be_bytes());
    page.extend_from_slice(&(cookie + 100).to_be_bytes());
    page.extend_from_slice(&0u32.to_be_bytes());
    page.extend_from_slice(&u32::from(eof).to_be_bytes());
    compound_result("readdir", &[(26 - 4, &[]), (26, &page)])
}

#[tokio::test]
async fn public_readdir_stream_propagates_cookie_and_verifier_across_pages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &first,
            &readdir_result(*b"firstver", 7, b"one", false),
        )
        .await?;
        let second = read_record(&mut stream).await?;
        let continuation = [7u64.to_be_bytes().as_slice(), b"firstver"].concat();
        assert!(
            second
                .windows(continuation.len())
                .any(|wire| wire == continuation)
        );
        reply(
            &mut stream,
            &second,
            &readdir_result(*b"secondve", 11, b"two", true),
        )
        .await
    });
    let entries = mount
        .readdir(Bytes::from_static(b"root"))
        .await
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.file_name.as_str())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    server.await.unwrap().unwrap();
}

fn failed_readdir_result(status: u32) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend_from_slice(&status.to_be_bytes());
    xdr_opaque(&mut result, b"readdir");
    result.extend_from_slice(&2u32.to_be_bytes());
    for (opcode, op_status) in [(22u32, 0u32), (26, status)] {
        result.extend_from_slice(&opcode.to_be_bytes());
        result.extend_from_slice(&op_status.to_be_bytes());
    }
    result
}

#[tokio::test]
async fn readdirplus_page_resumes_from_caller_position_and_types_stale_cookies() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let position = [7u64.to_be_bytes().as_slice(), b"firstver"].concat();
        assert!(request.windows(position.len()).any(|wire| wire == position));
        reply(
            &mut stream,
            &request,
            &readdir_result(*b"secondve", 11, b"two", false),
        )
        .await?;
        for status in [10003u32, 10027] {
            let request = read_record(&mut stream).await?;
            reply(&mut stream, &request, &failed_readdir_result(status)).await?;
        }
        Ok::<_, std::io::Error>(())
    });
    let position = mount::DirectoryCookie {
        cookie: 7,
        verifier: *b"firstver",
    };
    let page = mount
        .readdirplus_page(Bytes::from_static(b"root"), position)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].file_name, "two");
    assert_eq!(page.entries[0].fileid, 111);
    assert!(!page.eof);
    assert_eq!(
        page.next,
        mount::DirectoryCookie {
            cookie: 11,
            verifier: *b"secondve"
        }
    );
    for _ in 0..2 {
        let error = mount
            .readdirplus_page(Bytes::from_static(b"root"), page.next)
            .await
            .unwrap_err();
        assert!(error.is_bad_directory_cookie(), "{error:?}");
    }
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn readdirplus_page_rejects_page_ending_at_requested_cookie() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &request,
            &readdir_result(*b"verifier", 0, b"zero", false),
        )
        .await
    });
    let error = mount
        .readdirplus_page(
            Bytes::from_static(b"root"),
            mount::DirectoryCookie::default(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("did not advance cookie"),
        "{error}"
    );
    server.await.unwrap().unwrap();
}

fn empty_readdir_result(verifier: [u8; 8], eof: bool) -> Vec<u8> {
    let mut page = verifier.to_vec();
    page.extend_from_slice(&0u32.to_be_bytes());
    page.extend_from_slice(&u32::from(eof).to_be_bytes());
    compound_result("readdir", &[(26 - 4, &[]), (26, &page)])
}

#[tokio::test]
async fn readdirplus_page_rejects_empty_page_before_eof_and_keeps_position_at_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        for eof in [false, true] {
            let request = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &request,
                &empty_readdir_result(*b"newverif", eof),
            )
            .await?;
        }
        Ok::<_, std::io::Error>(())
    });
    let position = mount::DirectoryCookie {
        cookie: 7,
        verifier: *b"firstver",
    };
    let error = mount
        .readdirplus_page(Bytes::from_static(b"root"), position)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("made no progress"), "{error}");
    let page = mount
        .readdirplus_page(Bytes::from_static(b"root"), position)
        .await
        .unwrap();
    assert!(page.entries.is_empty() && page.eof);
    assert_eq!(page.next, position);
    server.await.unwrap().unwrap();
}

#[test]
fn readdir_page_decodes_cookie_name_and_fileid() {
    let mut data = directory_page(7, true);
    let (verifier, entries, eof) = decode_directory_page(&mut data, 0).unwrap();
    assert_eq!(&verifier, b"verifier");
    assert_eq!(entries.len(), 1);
    let entry = entries[0].as_ref().unwrap();
    assert_eq!(entry.cookie, 7);
    assert_eq!(entry.name, "file");
    assert_eq!(entry.attr.fileid, 42);
    assert!(eof);
}

#[test]
fn readdir_page_rejects_non_advancing_cookie() {
    let mut data = directory_page(7, true);
    let (_, entries, _) = decode_directory_page(&mut data, 7).unwrap();
    let error = entries[0].as_ref().unwrap_err();
    assert!(error.to_string().contains("did not advance cookie"));
}

#[test]
fn readdir_page_rejects_missing_eof_flag() {
    let mut data = directory_page(7, false);
    let (_, entries, _) = decode_directory_page(&mut data, 0).unwrap();
    let error = entries[1].as_ref().unwrap_err();
    assert!(error.to_string().contains("eof flag truncated"));
}

#[test]
fn readdir_page_enforces_negotiated_maxcount() {
    let error = validate_readdir_payload(Bytes::from_static(b"12345"), 4).unwrap_err();
    assert!(error.to_string().contains("exceeds requested maxcount"));
}

#[test]
fn malformed_later_entry_preserves_prior_entry_result() {
    let mut data = directory_page(7, true).to_vec();
    data.truncate(data.len() - 8);
    data.extend_from_slice(&1u32.to_be_bytes());
    data.extend_from_slice(&8u64.to_be_bytes());
    data.extend_from_slice(&99u32.to_be_bytes());
    let (_, entries, eof) = decode_directory_page(&mut Bytes::from(data), 0).unwrap();
    assert_eq!(entries[0].as_ref().unwrap().name, "file");
    assert!(entries[1].is_err());
    assert!(eof);
}

#[test]
fn metadata_results_reject_missing_required_attributes() {
    let error = require_fattrs(&[0], &[30], "FSINFO").unwrap_err();
    assert!(matches!(error, NfsError::Unsupported(_)));
}

#[tokio::test]
async fn guarded_setattr_is_explicitly_unsupported() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let error = mount
        .setattr(
            Bytes::from_static(b"fh"),
            Some(crate::Time::default()),
            Some(0o600),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, NfsError::Unsupported(_)));
}

#[tokio::test]
async fn remove_reply_loss_is_structured_as_uncertain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let _request = read_record(&mut stream).await?;
        socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
        drop(stream);
        Ok::<(), std::io::Error>(())
    });
    let error = mount
        .remove(Bytes::from_static(b"root"), "victim")
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_remove_after_send_is_settled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let change_info = [0u8; 20];
        reply(
            &mut stream,
            &request,
            &compound_result("remove", &[(22, &[]), (28, &change_info)]),
        )
        .await
    });
    let task_mount = Arc::clone(&mount);
    let task = tokio::spawn(async move {
        task_mount
            .remove(Bytes::from_static(b"root"), "victim")
            .await
    });
    seen_rx.await.unwrap();
    task.abort();
    let _ = task.await;
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn scripted_public_metadata_and_namespace_families_succeed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let access = read_record(&mut stream).await?;
        let access_data = [0x3fu32.to_be_bytes(), 0x21u32.to_be_bytes()].concat();
        reply(
            &mut stream,
            &access,
            &compound_result("access", &[(22, &[]), (3, &access_data)]),
        )
        .await?;

        let setattr = read_record(&mut stream).await?;
        let attrsset = [1u32.to_be_bytes(), (1u32 << 4).to_be_bytes()].concat();
        reply(
            &mut stream,
            &setattr,
            &compound_result("setattr", &[(22, &[]), (34, &attrsset)]),
        )
        .await?;

        let readlink = read_record(&mut stream).await?;
        let mut target = Vec::new();
        xdr_opaque(&mut target, b"target");
        reply(
            &mut stream,
            &readlink,
            &compound_result("readlink", &[(22, &[]), (27, &target)]),
        )
        .await?;

        let rename = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &rename,
            &compound_result("rename", &[(22, &[]), (32, &[]), (22, &[]), (29, &[0; 40])]),
        )
        .await?;

        let link = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &link,
            &compound_result("link", &[(22, &[]), (32, &[]), (22, &[]), (11, &[0; 20])]),
        )
        .await?;
        let linked_getattr = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &linked_getattr,
            &compound_result("getattr", &[(22, &[]), (9, &[0; 8])]),
        )
        .await?;

        let remove = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &remove,
            &compound_result("remove", &[(22, &[]), (28, &[0; 20])]),
        )
        .await
    });

    assert_eq!(
        mount.access(Bytes::from_static(b"fh"), 0x21).await.unwrap(),
        0x21
    );
    mount
        .setattr(
            Bytes::from_static(b"fh"),
            None,
            None,
            None,
            None,
            Some(7),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        mount.readlink(Bytes::from_static(b"fh")).await.unwrap(),
        "target"
    );
    mount
        .rename(
            Bytes::from_static(b"from"),
            "old",
            Bytes::from_static(b"to"),
            "new",
        )
        .await
        .unwrap();
    mount
        .link(
            Bytes::from_static(b"source"),
            Bytes::from_static(b"target"),
            "link",
        )
        .await
        .unwrap();
    mount
        .remove(Bytes::from_static(b"root"), "file")
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

fn fattr_result(bitmap: &[u32], values: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    result.extend_from_slice(&(bitmap.len() as u32).to_be_bytes());
    for word in bitmap {
        result.extend_from_slice(&word.to_be_bytes());
    }
    xdr_opaque(&mut result, values);
    result
}

fn bitmap4_value(bitmap: &[u32]) -> Vec<u8> {
    let mut value = Vec::new();
    value.extend_from_slice(&(bitmap.len() as u32).to_be_bytes());
    for word in bitmap {
        value.extend_from_slice(&word.to_be_bytes());
    }
    value
}

#[tokio::test]
async fn scripted_public_getattr_and_filesystem_metadata_succeed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let getattr = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &getattr,
            &compound_result("getattr", &[(22, &[]), (9, &[0; 8])]),
        )
        .await?;
        let fsinfo = read_record(&mut stream).await?;
        let bm = [
            (1 << 5) | (1 << 6) | (1 << 15) | (1 << 27) | (1 << 30) | (1 << 31),
            1 << 19,
        ];
        let values = [
            1u32.to_be_bytes().as_slice(),
            1u32.to_be_bytes().as_slice(),
            1u32.to_be_bytes().as_slice(),
            u64::MAX.to_be_bytes().as_slice(),
            65536u64.to_be_bytes().as_slice(),
            32768u64.to_be_bytes().as_slice(),
            0i64.to_be_bytes().as_slice(),
            1u32.to_be_bytes().as_slice(),
        ]
        .concat();
        let data = fattr_result(&bm, &values);
        reply(
            &mut stream,
            &fsinfo,
            &compound_result("fsinfo", &[(22, &[]), (9, &data)]),
        )
        .await?;
        let fsstat = read_record(&mut stream).await?;
        let bm = [
            (1 << 21) | (1 << 22) | (1 << 23),
            (1 << 10) | (1 << 11) | (1 << 12),
        ];
        let values = (1u64..=6).flat_map(u64::to_be_bytes).collect::<Vec<_>>();
        let data = fattr_result(&bm, &values);
        reply(
            &mut stream,
            &fsstat,
            &compound_result("fsstat", &[(22, &[]), (9, &data)]),
        )
        .await?;
        let pathconf = read_record(&mut stream).await?;
        let bm = [
            (1 << 0) | (1 << 8) | (1 << 16) | (1 << 17) | (1 << 18) | (1 << 28) | (1 << 29),
            1 << 2,
        ];
        let mut values = bitmap4_value(&bm);
        values.extend_from_slice(&7u64.to_be_bytes());
        values.extend_from_slice(&9u64.to_be_bytes());
        values.extend_from_slice(&0u32.to_be_bytes());
        values.extend_from_slice(&1u32.to_be_bytes());
        values.extend_from_slice(&1u32.to_be_bytes());
        values.extend_from_slice(&1024u32.to_be_bytes());
        values.extend_from_slice(&255u32.to_be_bytes());
        values.extend_from_slice(&1u32.to_be_bytes());
        let data = fattr_result(&bm, &values);
        reply(
            &mut stream,
            &pathconf,
            &compound_result("pathconf", &[(22, &[]), (9, &data)]),
        )
        .await
    });
    mount.getattr(Bytes::from_static(b"fh")).await.unwrap();
    let info = mount.fsinfo().await.unwrap();
    assert_eq!((info.rtmax, info.wtmax), (65536, 32768));
    let stat = mount.fsstat().await.unwrap();
    assert_eq!((stat.afiles, stat.tbytes), (1, 6));
    let pathconf = mount.pathconf(Bytes::from_static(b"fh")).await.unwrap();
    assert_eq!((pathconf.linkmax, pathconf.name_max), (1024, 255));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn pathconf_tolerates_an_omitted_recommended_case_attribute() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let bitmap = [
            (1 << 0) | (1 << 8) | (1 << 17) | (1 << 18) | (1 << 28) | (1 << 29),
            1 << 2,
        ];
        let mut values = bitmap4_value(&bitmap);
        values.extend_from_slice(&7u64.to_be_bytes());
        values.extend_from_slice(&9u64.to_be_bytes());
        values.extend_from_slice(&1u32.to_be_bytes());
        values.extend_from_slice(&1u32.to_be_bytes());
        values.extend_from_slice(&1024u32.to_be_bytes());
        values.extend_from_slice(&255u32.to_be_bytes());
        values.extend_from_slice(&1u32.to_be_bytes());
        let data = fattr_result(&bitmap, &values);
        reply(
            &mut stream,
            &request,
            &compound_result("pathconf", &[(22, &[]), (9, &data)]),
        )
        .await
    });

    let result = mount
        .pathconf_with_support(Bytes::from_static(b"fh"))
        .await
        .unwrap();
    assert!(!result.available.case_insensitive);
    assert!(result.available.case_preserving);
    assert_eq!(result.fsid, Some((7, 9)));
    assert!(!result.values.case_insensitive);
    assert!(result.values.case_preserving);
    assert_eq!((result.values.linkmax, result.values.name_max), (1024, 255));
    server.await.unwrap().unwrap();
}

fn create_object_result(tag: &str, fh_value: &[u8]) -> Vec<u8> {
    let mut create = vec![0; 20];
    create.extend_from_slice(&0u32.to_be_bytes());
    let mut fh = Vec::new();
    xdr_opaque(&mut fh, fh_value);
    compound_result(tag, &[(22, &[]), (6, &create), (10, &fh), (9, &[0; 8])])
}

#[tokio::test]
async fn scripted_public_create_directory_and_symlink_succeed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let create = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &create,
            &open_result(0, [0x31; 16], b"file-fh"),
        )
        .await?;
        let getattr = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &getattr,
            &compound_result("getattr", &[(22, &[]), (9, &[0; 8])]),
        )
        .await?;

        let mkdir = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &mkdir,
            &create_object_result("mkdir", b"dir-fh"),
        )
        .await?;
        let setattr = read_record(&mut stream).await?;
        let attrsset = [
            2u32.to_be_bytes(),
            0u32.to_be_bytes(),
            (1u32 << 1).to_be_bytes(),
        ]
        .concat();
        reply(
            &mut stream,
            &setattr,
            &compound_result("setattr", &[(22, &[]), (34, &attrsset)]),
        )
        .await?;

        let symlink = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &symlink,
            &create_object_result("symlink", b"link-fh"),
        )
        .await
    });
    let file = mount
        .create(Bytes::from_static(b"root"), "file", None)
        .await
        .unwrap();
    assert_eq!(file.fh, Bytes::from_static(b"file-fh"));
    let directory = mount
        .mkdir(Bytes::from_static(b"root"), "dir", 0o755)
        .await
        .unwrap();
    assert_eq!(directory.fh, Bytes::from_static(b"dir-fh"));
    let symlink = mount
        .symlink("target", Bytes::from_static(b"root"), "link")
        .await
        .unwrap();
    assert_eq!(symlink.fh, Bytes::from_static(b"link-fh"));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_create_setattr_never_removes_an_unchecked_open_target() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let create = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &create,
            &open_result(0, [0x41; 16], b"possibly-existing-fh"),
        )
        .await?;

        let setattr = read_record(&mut stream).await?;
        reply(&mut stream, &setattr, &failed_lock_result("setattr", 34, 5)).await?;

        let close = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x42; 16])]),
        )
        .await?;

        Ok::<bool, std::io::Error>(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_ok(),
        )
    });

    assert!(
        mount
            .create(Bytes::from_static(b"root"), "raced-name", Some(0o600),)
            .await
            .is_err()
    );
    assert!(
        !server.await.unwrap().unwrap(),
        "UNCHECKED4 rollback must not remove a name that may predate this OPEN"
    );
}

#[tokio::test]
async fn stateful_create_preserves_write_only_share_access() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let create = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &create,
            &open_result(0, [0x51; 16], b"write-only-fh"),
        )
        .await?;
        let close = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x52; 16])]),
        )
        .await
    });

    let opened = mount
        .create_path_stateful_with_access("file", None, crate::OPEN_WRITE)
        .await
        .unwrap();
    let fh = opened.file_handle();
    assert!(mount.state.for_fh(&fh, crate::OPEN_WRITE).await.is_some());
    assert!(mount.state.for_fh(&fh, crate::OPEN_READ).await.is_none());
    mount.close_stateful(opened).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn guarded_open_options_preserve_access_confirm_and_race_cleanup() {
    use crate::nfs4::compound::take_opaque;
    use bytes::Buf;
    // New wb / w+b, attribute fallback, concurrent creation, OPEN_CONFIRM,
    // lost create reply, and partial OPEN success followed by GETFH failure.
    for (access, case) in [(2, 0), (3, 0), (2, 1), (2, 2), (2, 3), (2, 4), (2, 5)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let seen = Arc::new(std::sync::Mutex::new(vec![]));
        let recorded = seen.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut opens = 0;
            let mut creates = 0;
            loop {
                let request = read_record(&mut stream).await.unwrap();
                let mut args = Bytes::copy_from_slice(&request[40..]);
                let tag =
                    String::from_utf8(take_opaque(&mut args, "tag").unwrap().to_vec()).unwrap();
                recorded.lock().unwrap().push(tag.clone());
                assert_eq!(args.get_u32(), 0);
                args.advance(4); // opcount
                assert_eq!(args.get_u32(), 22);
                let fh = take_opaque(&mut args, "fh").unwrap();
                let opcode = args.get_u32();
                let response = match tag.as_str() {
                    "lookup" => {
                        assert_eq!(recorded.lock().unwrap().len(), 1, "parent resolved twice");
                        assert_eq!(opcode, 15);
                        assert_eq!(fh.as_ref(), b"root");
                        assert_eq!(take_opaque(&mut args, "name").unwrap().as_ref(), b"parent");
                        let mut parent = vec![];
                        xdr_opaque(&mut parent, b"parent");
                        compound_result(
                            "lookup",
                            &[(22, &[]), (15, &[]), (10, &parent), (9, &[0; 8])],
                        )
                    }
                    "open" | "create_guarded" => {
                        assert_eq!(fh.as_ref(), b"parent");
                        assert_eq!(opcode, 18);
                        assert_eq!(args.get_u32(), 0);
                        assert_eq!(args.get_u32(), access);
                        assert_eq!(args.get_u32(), 0);
                        args.advance(8);
                        take_opaque(&mut args, "owner").unwrap();
                        let create = args.get_u32();
                        if tag == "open" {
                            opens += 1;
                            assert_eq!(create, 0);
                            if opens == 1 {
                                failed_lock_result("open", 18, 2)
                            } else {
                                open_result(0, [7; 16], b"file")
                            }
                        } else {
                            creates += 1;
                            assert_eq!(create, 1);
                            assert_eq!(args.get_u32(), 1, "RFC 7530 GUARDED4");
                            if creates == 1 {
                                assert_eq!(args.get_u32(), 2);
                                assert_eq!(args.get_u32(), 0);
                                assert_eq!(args.get_u32(), 2);
                                assert_eq!(
                                    take_opaque(&mut args, "mode").unwrap().as_ref(),
                                    &0o666u32.to_be_bytes()
                                );
                            } else {
                                assert_eq!(args.get_u32(), 0);
                                assert!(take_opaque(&mut args, "attrs").unwrap().is_empty());
                            }
                            match case {
                                1 if creates == 1 => {
                                    failed_lock_result("create_guarded", 18, 10032)
                                }
                                2 => failed_lock_result("create_guarded", 18, 17),
                                4 => return,
                                5 => {
                                    let mut result = open_result(0, [7; 16], b"file");
                                    // Preserve successful OPEN but replace GETFH success with failure.
                                    result.truncate(result.len() - 8);
                                    let last = result.len() - 4;
                                    result[last..].copy_from_slice(&10032u32.to_be_bytes());
                                    result[..4].copy_from_slice(&10032u32.to_be_bytes());
                                    result
                                }
                                _ => open_result(if case == 3 { 2 } else { 0 }, [7; 16], b"file"),
                            }
                        }
                    }
                    "open_confirm" => {
                        assert_eq!(case, 3);
                        assert_eq!(opcode, 20);
                        compound_result("open_confirm", &[(22, &[]), (20, &[8; 16])])
                    }
                    "setattr" => {
                        assert_eq!(opcode, 34);
                        args.advance(16);
                        if case == 1 {
                            assert_eq!(args.get_u32(), 2);
                            assert_eq!(args.get_u32(), 0);
                            assert_eq!(args.get_u32(), 2);
                            assert_eq!(
                                take_opaque(&mut args, "mode").unwrap().as_ref(),
                                &0o666u32.to_be_bytes()
                            );
                        } else {
                            assert_eq!(case, 2);
                            assert_eq!(args.get_u32(), 1);
                            assert_eq!(args.get_u32(), 1 << 4);
                            assert_eq!(take_opaque(&mut args, "size").unwrap().as_ref(), &[0; 8]);
                        }
                        compound_result("setattr", &[(22, &[]), (34, &[0; 4])])
                    }
                    "close" => {
                        assert_eq!(opcode, 4);
                        compound_result("close", &[(22, &[]), (4, &[9; 16])])
                    }
                    _ => panic!("unexpected RPC {tag}"),
                };
                reply(&mut stream, &request, &response).await.unwrap();
                if tag == "close" || (case == 5 && tag == "create_guarded") {
                    return;
                }
            }
        });
        let result = mount
            .open_path_with_options(
                "parent/new",
                crate::OpenOptions {
                    access,
                    create_mode: Some(0o666),
                    truncate: true,
                },
            )
            .await;
        if case >= 4 {
            assert_eq!(
                result.unwrap_err().operation_outcome().unwrap().outcome,
                crate::OperationOutcome::Uncertain
            );
            assert_eq!(
                seen.lock()
                    .unwrap()
                    .iter()
                    .filter(|tag| *tag == "create_guarded")
                    .count(),
                1
            );
        } else {
            mount.close_stateful(result.unwrap()).await.unwrap();
            assert_eq!(seen.lock().unwrap().last().unwrap(), "close");
            if case == 0 {
                assert_eq!(seen.lock().unwrap().len(), 4);
            }
            if case == 3 {
                assert_eq!(seen.lock().unwrap().len(), 5);
            }
        }
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "scripted server did not finish: access={access}, case={case}, requests={:?}",
                    seen.lock().unwrap()
                )
            })
            .unwrap();
    }
}

async fn register_scripted_open(mount: &Mount40, owner: u64, fh: &'static [u8]) {
    mount
        .state
        .register(OwnerLane {
            owner,
            next_seqid: 1,
            stateid: [0x61; 16],
            fh: Bytes::from_static(fh),
            dir_fh: Bytes::from_static(b"dir"),
            name: "scripted-file".into(),
            access: crate::OPEN_BOTH,
            write_verifier: None,
            io_fence: Arc::new(tokio::sync::RwLock::new(())),
            closing: false,
        })
        .await;
}

async fn register_scripted_lock_range(
    mount: &Mount40,
    owner: u64,
    stateid: [u8; 16],
    lock_type: u32,
    offset: u64,
    length: u64,
) {
    mount
        .locks
        .register(LockLane {
            owner,
            open_owner: owner,
            owner_wire: Bytes::from(format!("scripted-lock-{owner}")),
            next_seqid: 1,
            stateid,
            fh: Bytes::from_static(b"fh"),
            lock_type,
            offset,
            length,
        })
        .await;
}

async fn register_scripted_lock(mount: &Mount40, owner: u64, stateid: [u8; 16]) {
    register_scripted_lock_range(mount, owner, stateid, 2, 0, 1).await;
}

/// Publishes the loss of a whole-file write lock on `fh`, the way recovery does
/// when a reclaim is refused.
async fn publish_lost_whole_file(mount: &Mount40, fh: &[u8]) {
    mount
        .locks
        .mark_lost(
            fh,
            LostLock {
                status: crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID,
                lock_type: 2,
                offset: 0,
                length: u64::MAX,
                owner: Bytes::from_static(b"scripted-lost"),
            },
        )
        .await;
}

#[tokio::test]
async fn public_typed_lock_blocks_close_until_exact_unlock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 41, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &lock,
            &compound_result("lock", &[(22, &[]), (12, &[0x71; 16])]),
        )
        .await?;
        let unlock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &unlock,
            &compound_result("locku", &[(22, &[]), (14, &[0x72; 16])]),
        )
        .await?;
        let cleanup = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &cleanup,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    let token = mount
        .lock_stateful(Bytes::from_static(b"fh"), 2, 7, 11)
        .await
        .unwrap();
    let error = mount.close(Bytes::from_static(b"fh")).await.unwrap_err();
    assert!(error.to_string().contains("outstanding locks"));
    mount.unlock_stateful(token).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_reply_loss_is_uncertain_and_does_not_register_protection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 42, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let _ = read_record(&mut stream).await?;
        socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
        drop(stream);
        Ok::<(), std::io::Error>(())
    });
    let error = mount
        .lock(Bytes::from_static(b"fh"), 2, 0, 1)
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_lock_after_send_finishes_owner_settlement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 43, b"fh").await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        reply(
            &mut stream,
            &request,
            &compound_result("lock", &[(22, &[]), (12, &[0x73; 16])]),
        )
        .await
    });
    let task_mount = Arc::clone(&mount);
    let task =
        tokio::spawn(async move { task_mount.lock(Bytes::from_static(b"fh"), 2, 0, 1).await });
    seen_rx.await.unwrap();
    task.abort();
    let _ = task.await;
    server.await.unwrap().unwrap();
    for _ in 0..20 {
        if mount.locks.has_fh(&Bytes::from_static(b"fh")).await {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("detached LOCK reply did not register protection");
}

#[tokio::test]
async fn locku_reply_loss_is_uncertain_and_retains_local_protection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_lock(&mount, 61, [0x91; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let _ = read_record(&mut stream).await?;
        socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO))?;
        drop(stream);
        Ok::<(), std::io::Error>(())
    });
    let error = mount
        .locku(
            Bytes::from_static(b"fh"),
            Bytes::from_static(&[0x91; 16]),
            2,
            0,
            1,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    assert!(mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn authoritative_locku_error_consumes_owner_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_lock(&mount, 63, [0x94; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        reply(&mut stream, &first, &failed_lock_result("locku", 14, 22)).await?;
        let second = read_record(&mut stream).await?;
        let expected = [
            2u32.to_be_bytes().as_slice(),
            2u32.to_be_bytes().as_slice(),
            &[0x94; 16],
        ]
        .concat();
        assert!(second.windows(expected.len()).any(|wire| wire == expected));
        reply(
            &mut stream,
            &second,
            &compound_result("locku", &[(22, &[]), (14, &[0x95; 16])]),
        )
        .await?;
        let cleanup = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &cleanup,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    let args = || {
        (
            Bytes::from_static(b"fh"),
            Bytes::from_static(&[0x94; 16]),
            2,
            0,
            1,
        )
    };
    let (fh, stateid, kind, offset, length) = args();
    assert!(matches!(
        mount.locku(fh, stateid, kind, offset, length).await,
        Err(NfsError::Nfs4(_))
    ));
    let (fh, stateid, kind, offset, length) = args();
    mount
        .locku(fh, stateid, kind, offset, length)
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_locku_after_send_finishes_release_settlement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_lock(&mount, 62, [0x92; 16]).await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        reply(
            &mut stream,
            &request,
            &compound_result("locku", &[(22, &[]), (14, &[0x93; 16])]),
        )
        .await?;
        let cleanup = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &cleanup,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    let task_mount = Arc::clone(&mount);
    let task = tokio::spawn(async move {
        task_mount
            .locku(
                Bytes::from_static(b"fh"),
                Bytes::from_static(&[0x92; 16]),
                2,
                0,
                1,
            )
            .await
    });
    seen_rx.await.unwrap();
    task.abort();
    let _ = task.await;
    server.await.unwrap().unwrap();
    for _ in 0..20 {
        if !mount.locks.has_fh(&Bytes::from_static(b"fh")).await {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("detached LOCKU reply did not remove released protection");
}

#[tokio::test]
async fn typed_lock_rejects_foreign_or_mismatched_tokens_without_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let foreign = crate::LockToken::new(
        Bytes::from_static(b"fh"),
        Bytes::from_static(&[0x21; 16]),
        2,
        0,
        1,
        mount.issuer ^ 1,
        1,
    );
    assert!(mount.unlock_stateful(foreign).await.is_err());
    let unknown = crate::LockToken::new(
        Bytes::from_static(b"fh"),
        Bytes::from_static(&[0x22; 16]),
        2,
        0,
        1,
        mount.issuer,
        1,
    );
    assert!(mount.unlock_stateful(unknown).await.is_err());
    let stale = crate::LockToken::new(
        Bytes::from_static(b"fh"),
        Bytes::from_static(&[0x23; 16]),
        2,
        0,
        1,
        mount.issuer,
        mount.lease.generation() + 1,
    );
    assert!(mount.unlock_stateful(stale).await.is_err());
}

#[tokio::test]
async fn scripted_public_lock_conflict_exposes_denied_range() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 50, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        reply(&mut stream, &request, &denied_lock_result("lock", 12)).await?;
        let next = read_record(&mut stream).await?;
        let expected = [
            1u32.to_be_bytes().as_slice(),
            2u32.to_be_bytes().as_slice(),
            &[0x61; 16],
        ]
        .concat();
        assert!(next.windows(expected.len()).any(|wire| wire == expected));
        reply(
            &mut stream,
            &next,
            &compound_result("lock", &[(22, &[]), (12, &[0xb1; 16])]),
        )
        .await
    });
    assert!(matches!(
        mount.lock(Bytes::from_static(b"fh"), 2, 0, 1).await,
        Err(NfsError::LockDenied {
            lock_type: 2,
            offset: 7,
            length: 11,
            ..
        })
    ));
    mount
        .lock(Bytes::from_static(b"fh"), 2, 2, 1)
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn independent_opens_of_same_file_lock_without_global_serialization() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 54, b"fh").await;
    register_scripted_open(&mount, 55, b"fh").await;
    let opened_a = mount::OpenFile::with_protocol_state(
        mount::ObjRes {
            fh: Bytes::from_static(b"fh"),
            attr: None,
        },
        encode_owner(mount.issuer, 54),
    );
    let opened_b = mount::OpenFile::with_protocol_state(
        mount::ObjRes {
            fh: Bytes::from_static(b"fh"),
            attr: None,
        },
        encode_owner(mount.issuer, 55),
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        let second =
            tokio::time::timeout(Duration::from_secs(1), read_record(&mut stream)).await??;
        reply(
            &mut stream,
            &first,
            &compound_result("lock", &[(22, &[]), (12, &[0xa1; 16])]),
        )
        .await?;
        reply(
            &mut stream,
            &second,
            &compound_result("lock", &[(22, &[]), (12, &[0xa2; 16])]),
        )
        .await
    });
    let (a, b) = tokio::join!(
        mount.lock_open_stateful(&opened_a, 2, 0, 1),
        mount.lock_open_stateful(&opened_b, 2, 2, 1),
    );
    assert!(a.is_ok() && b.is_ok());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn different_open_owners_send_locks_without_global_serialization() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 51, b"fh-a").await;
    register_scripted_open(&mount, 52, b"fh-b").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        let second =
            tokio::time::timeout(Duration::from_secs(1), read_record(&mut stream)).await??;
        reply(
            &mut stream,
            &first,
            &compound_result("lock", &[(22, &[]), (12, &[0x81; 16])]),
        )
        .await?;
        reply(
            &mut stream,
            &second,
            &compound_result("lock", &[(22, &[]), (12, &[0x82; 16])]),
        )
        .await
    });
    let (a, b) = tokio::join!(
        mount.lock(Bytes::from_static(b"fh-a"), 2, 0, 1),
        mount.lock(Bytes::from_static(b"fh-b"), 2, 0, 1),
    );
    assert!(a.is_ok() && b.is_ok());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn durable_write_batches_unstable_chunks_before_one_commit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 65, b"fh").await;
    let chunk = mount.get_max_write_size() as usize;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut requests = Vec::new();
        let mut offsets = Vec::new();
        // All three requests must arrive before any response is sent.
        // A serial writer times out here; reply order deliberately differs.
        for _ in 0..3 {
            let n = chunk;
            let request =
                tokio::time::timeout(Duration::from_secs(2), read_record(&mut stream)).await??;
            let padded = (n + 3) & !3;
            let stable_at = request.len() - padded - 8;
            assert_eq!(
                &request[stable_at..stable_at + 4],
                &[0; 4],
                "WRITE must request UNSTABLE"
            );
            offsets.push(u64::from_be_bytes(
                request[stable_at - 8..stable_at].try_into().unwrap(),
            ));
            requests.push(request);
        }
        offsets.sort_unstable();
        assert_eq!(offsets, vec![0, chunk as u64, (chunk * 2) as u64]);
        for request in requests.into_iter().rev() {
            let n = chunk;
            let result = [
                (n as u32).to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
                [0x71; 8].as_slice(),
            ]
            .concat();
            reply(
                &mut stream,
                &request,
                &compound_result("write", &[(22, &[]), (38, &result)]),
            )
            .await?;
        }
        let commit = read_record(&mut stream).await?;
        assert!(commit.windows(6).any(|part| part == b"commit"));
        reply(
            &mut stream,
            &commit,
            &compound_result("commit", &[(22, &[]), (5, &[0x71; 8])]),
        )
        .await
    });
    let data = Bytes::from(vec![42; chunk * 3]);
    assert_eq!(
        crate::write_all(&*mount, Bytes::from_static(b"fh"), 0, data)
            .await
            .unwrap(),
        (chunk * 3) as u64
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn same_open_file_writes_use_rpc_xids_without_owner_lane_serialization() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 66, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        let second = tokio::time::timeout(Duration::from_millis(250), read_record(&mut stream))
            .await
            .map_err(|_| {
                std::io::Error::other("second WRITE was serialized behind the first reply")
            })??;
        let write_result = [
            1u32.to_be_bytes().as_slice(),
            2u32.to_be_bytes().as_slice(),
            [0x71; 8].as_slice(),
        ]
        .concat();
        reply(
            &mut stream,
            &second,
            &compound_result("write", &[(22, &[]), (38, &write_result)]),
        )
        .await?;
        reply(
            &mut stream,
            &first,
            &compound_result("write", &[(22, &[]), (38, &write_result)]),
        )
        .await
    });

    let (first, second) = tokio::join!(
        mount.write(Bytes::from_static(b"fh"), 0, Bytes::from_static(b"a")),
        mount.write(Bytes::from_static(b"fh"), 1, Bytes::from_static(b"b")),
    );
    assert_eq!(first.unwrap().count, 1);
    assert_eq!(second.unwrap().count, 1);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_waits_for_an_in_flight_read_to_settle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 67, b"fh").await;
    let (read_seen_tx, read_seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        let _ = read_seen_tx.send(());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "CLOSE overtook an in-flight READ"
        );
        let mut read_result = 1u32.to_be_bytes().to_vec();
        xdr_opaque(&mut read_result, b"x");
        reply(
            &mut stream,
            &read,
            &compound_result("read", &[(22, &[]), (25, &read_result)]),
        )
        .await?;
        let close = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x98; 16])]),
        )
        .await
    });

    let reading_mount = Arc::clone(&mount);
    let read =
        tokio::spawn(async move { reading_mount.read(Bytes::from_static(b"fh"), 0, 1).await });
    read_seen_rx.await.unwrap();
    let close = mount.close(Bytes::from_static(b"fh"));
    let (read, close) = tokio::join!(read, close);
    assert_eq!(read.unwrap().unwrap(), Bytes::from_static(b"x"));
    close.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_read_keeps_its_close_fence_until_rpc_settlement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 69, b"fh").await;
    let (read_seen_tx, read_seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        let _ = read_seen_tx.send(());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "CLOSE overtook a cancelled READ that was still settling"
        );
        let mut read_result = 1u32.to_be_bytes().to_vec();
        xdr_opaque(&mut read_result, b"x");
        reply(
            &mut stream,
            &read,
            &compound_result("read", &[(22, &[]), (25, &read_result)]),
        )
        .await?;
        let close = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x9a; 16])]),
        )
        .await
    });

    let reading_mount = Arc::clone(&mount);
    let read =
        tokio::spawn(async move { reading_mount.read(Bytes::from_static(b"fh"), 0, 1).await });
    read_seen_rx.await.unwrap();
    read.abort();
    let _ = read.await;
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_truncate_keeps_its_close_fence_until_rpc_settlement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 70, b"fh").await;
    let (setattr_seen_tx, setattr_seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let setattr = read_record(&mut stream).await?;
        let _ = setattr_seen_tx.send(());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "CLOSE overtook a cancelled truncate that was still settling"
        );
        reply(&mut stream, &setattr, &setattr_result()).await?;
        let close = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x9b; 16])]),
        )
        .await
    });

    let truncating_mount = Arc::clone(&mount);
    let truncate = tokio::spawn(async move {
        truncating_mount
            .setattr(
                Bytes::from_static(b"fh"),
                None,
                None,
                None,
                None,
                Some(4),
                None,
                None,
            )
            .await
    });
    setattr_seen_rx.await.unwrap();
    truncate.abort();
    let _ = truncate.await;
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_write_keeps_its_close_fence_until_rpc_settlement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 71, b"fh").await;
    let (write_seen_tx, write_seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let write = read_record(&mut stream).await?;
        let _ = write_seen_tx.send(());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "CLOSE overtook a cancelled WRITE that was still settling"
        );
        reply(&mut stream, &write, &write_result(1)).await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &close_result([0x9c; 16])).await
    });

    let writing_mount = Arc::clone(&mount);
    let write = tokio::spawn(async move {
        writing_mount
            .write(Bytes::from_static(b"fh"), 0, Bytes::from_static(b"x"))
            .await
    });
    write_seen_rx.await.unwrap();
    write.abort();
    let _ = write.await;
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_waits_for_concurrent_writes_to_settle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 68, b"fh").await;
    let (writes_seen_tx, writes_seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        let second = read_record(&mut stream).await?;
        let _ = writes_seen_tx.send(());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "CLOSE overtook concurrent WRITEs"
        );
        let write_result = [
            1u32.to_be_bytes().as_slice(),
            2u32.to_be_bytes().as_slice(),
            [0x72; 8].as_slice(),
        ]
        .concat();
        for write in [&second, &first] {
            reply(
                &mut stream,
                write,
                &compound_result("write", &[(22, &[]), (38, &write_result)]),
            )
            .await?;
        }
        let close = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x99; 16])]),
        )
        .await
    });

    let first_mount = Arc::clone(&mount);
    let first = tokio::spawn(async move {
        first_mount
            .write(Bytes::from_static(b"fh"), 0, Bytes::from_static(b"a"))
            .await
    });
    let second_mount = Arc::clone(&mount);
    let second = tokio::spawn(async move {
        second_mount
            .write(Bytes::from_static(b"fh"), 1, Bytes::from_static(b"b"))
            .await
    });
    writes_seen_rx.await.unwrap();
    let close = mount.close(Bytes::from_static(b"fh"));
    let (first, second, close) = tokio::join!(first, second, close);
    assert_eq!(first.unwrap().unwrap().count, 1);
    assert_eq!(second.unwrap().unwrap().count, 1);
    close.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn same_open_owner_serializes_lock_seqid_consumption() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 53, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err()
        );
        reply(
            &mut stream,
            &first,
            &compound_result("lock", &[(22, &[]), (12, &[0x83; 16])]),
        )
        .await?;
        let second = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &second,
            &compound_result("lock", &[(22, &[]), (12, &[0x84; 16])]),
        )
        .await
    });
    let (a, b) = tokio::join!(
        mount.lock(Bytes::from_static(b"fh"), 2, 0, 1),
        mount.lock(Bytes::from_static(b"fh"), 2, 2, 1),
    );
    assert!(a.is_ok() && b.is_ok());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn concurrent_public_closes_emit_one_wire_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 64, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let first = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &first,
            &compound_result("close", &[(22, &[]), (4, &[0x96; 16])]),
        )
        .await?;
        Ok::<bool, std::io::Error>(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_ok(),
        )
    });

    let fh = Bytes::from_static(b"fh");
    let (first, second) = tokio::join!(mount.close(fh.clone()), mount.close(fh));
    assert_eq!(first.is_ok() as u8 + second.is_ok() as u8, 1);
    let rejected = if first.is_err() { first } else { second }.unwrap_err();
    assert!(
        matches!(rejected, NfsError::InvalidInput(ref message) if message.contains("closing or closed")),
        "the losing close must receive a typed local error: {rejected}"
    );
    assert!(!server.await.unwrap().unwrap(), "CLOSE was emitted twice");
}

#[tokio::test]
async fn cancelled_close_waiting_for_recall_gate_still_settles() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let callback = CallbackService::bind_for(listener.local_addr().unwrap())
        .await
        .unwrap();
    let callback_state = callback.state();
    let mut mount = direct_mount(rpc::Client::new(mux, None));
    Arc::get_mut(&mut mount).unwrap().callback_state = Some(Arc::clone(&callback_state));
    register_scripted_open(&mount, 65, b"fh").await;
    let io_guard = callback_state
        .foreground_io(&Bytes::from_static(b"fh"))
        .await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = tokio::time::timeout(Duration::from_secs(1), read_record(&mut stream))
            .await
            .map_err(|_| std::io::Error::other("cancelled CLOSE was never settled"))??;
        reply(
            &mut stream,
            &close,
            &compound_result("close", &[(22, &[]), (4, &[0x97; 16])]),
        )
        .await
    });

    let closing_mount = Arc::clone(&mount);
    let close = tokio::spawn(async move { closing_mount.close(Bytes::from_static(b"fh")).await });
    tokio::task::yield_now().await;
    close.abort();
    let _ = close.await;
    drop(io_guard);
    server.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if mount.state.snapshot().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled CLOSE did not remove settled owner state");
    callback.stop().await;
}
#[tokio::test]
async fn busy_v40_read_retries_delay_with_or_without_open() {
    for stateful in [false, true] {
        for status in [10008, 10013] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mount = connected_direct_mount(&listener).await;
            if stateful {
                register_scripted_open(&mount, 99, b"fh").await;
            }
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let seen = calls.clone();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                while let Ok(request) = read_record(&mut stream).await {
                    let n = seen.fetch_add(1, Ordering::SeqCst);
                    let payload = if n == 0 {
                        failed_lock_result("read", 25, status)
                    } else {
                        let mut data = 1u32.to_be_bytes().to_vec();
                        xdr_opaque(&mut data, b"abc");
                        compound_result("read", &[(22, &[]), (25, &data)])
                    };
                    reply(&mut stream, &request, &payload).await.unwrap();
                }
            });
            let first = mount.read(Bytes::from_static(b"fh"), 0, 3).await;
            assert_eq!(first.unwrap(), b"abc"[..]);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            server.abort();
        }
    }
}

#[tokio::test]
async fn close_with_old_stateid_reports_exhaustion_without_blind_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 209, [0xa9; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        // OLD_STATEID discloses no seqid gap, so the client must not guess
        // it with a blind stateid +1 and resend. Exactly one CLOSE is sent.
        reply(&mut stream, &close, &failed_lock_result("close", 4, 10024)).await
    });
    let error = mount.close(Bytes::from_static(b"fh")).await.unwrap_err();
    assert_eq!(
        error.stateid_failure(),
        Some(crate::StateidFailure::Exhausted)
    );
    assert!(matches!(
        error,
        NfsError::StateRecoveryExhausted(ref failure)
            if failure.status == crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID
    ));
    // The lane survives (the open state still exists server-side) and stays
    // closeable for the caller's reopen; only the consumed open-owner seqid
    // advanced. The stateid seqid is untouched because the server's current
    // seqid is unknown.
    let lane = lane.lock().await;
    assert!(!lane.closing);
    assert_eq!(lane.next_seqid, 2);
    assert_eq!(lane.stateid, [0xa9; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_with_stale_stateid_is_treated_as_released() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_stateid_lane(&mount, 218, [0xcb; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        // STALE_STATEID says the server has no such state, so CLOSE is
        // already satisfied and no seqid probe is needed (unlike
        // BAD_STATEID, where the state may still exist further ahead).
        reply(&mut stream, &close, &failed_lock_result("close", 4, 10023)).await
    });
    mount
        .close(Bytes::from_static(b"fh"))
        .await
        .expect("STALE_STATEID on CLOSE means the server already released the state");
    assert!(mount.state.by_owner(218).await.is_none());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn locku_old_stateid_reports_exhaustion_without_blind_bump() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_lock(&mount, 211, [0xb1; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        // OLD_STATEID does not disclose the server's lock seqid: answer
        // once and let the lane survive with the issued stateid.
        reply(&mut stream, &locku, &failed_lock_result("locku", 14, 10024)).await
    });
    let error = mount
        .locku(
            Bytes::from_static(b"fh"),
            Bytes::from([0xb1; 16].as_slice()),
            2,
            0,
            1,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.stateid_failure(),
        Some(crate::StateidFailure::Exhausted)
    );
    // The lock is still held server-side: the lane must survive with the
    // stateid the server issued, not a locally fabricated one.
    let lane = mount.locks.by_stateid(&[0xb1; 16]).await.unwrap();
    let lane = lane.lock().await;
    assert_eq!(lane.stateid, [0xb1; 16]);
    drop(lane);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn old_stateid_read_recovers_with_fresh_open() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 203, [0xa3; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10024)).await?;
        // OLD_STATEID does not carry the server's current seqid, so the lane
        // is refreshed with exactly one fresh OPEN instead of a blind seqid
        // increment.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xb3; 16], b"fh")).await?;
        let retry = read_record(&mut stream).await?;
        reply(&mut stream, &retry, &read_result(b"data")).await
    });
    let data = mount.read(Bytes::from_static(b"fh"), 0, 4).await.unwrap();
    assert_eq!(data, Bytes::from_static(b"data"));
    let lane = lane.lock().await;
    assert_eq!(lane.stateid, [0xb3; 16]);
    // The fresh OPEN re-syncs the stateid authoritatively and restarts the
    // open-owner seqid at 1 (the OPEN is sent with seqid 0 and succeeds).
    assert_eq!(lane.next_seqid, 1);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_seqid_consumption_follows_the_rfc7530_exception_table() {
    // The COMPOUND overall status cannot distinguish "PUTFH failed" from
    // "CLOSE ran and was rejected", yet only the latter consumes the
    // open-owner seqid. Assert the cursor and the next request frame for
    // both stages and both sides of the §9.1.7 exception table.
    // (label, failure stage, status, expected next unconsumed seqid)
    let cases = [
        ("CLOSE SERVERFAULT", false, 10006u32, 2u32),
        ("CLOSE BAD_SEQID", false, 10026, 1),
        ("CLOSE MOVED", false, 10019, 1),
        ("PUTFH SERVERFAULT", true, 10006, 1),
        ("PUTFH BAD_SEQID", true, 10026, 1),
    ];
    for (index, (label, at_putfh, status, expected)) in cases.into_iter().enumerate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mount = connected_direct_mount(&listener).await;
        let owner = 230 + index as u64;
        let stateid = [0xd0 + index as u8; 16];
        let lane = register_stateid_lane(&mount, owner, stateid, crate::OPEN_BOTH).await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let close = read_record(&mut stream).await?;
            let first = [1u32.to_be_bytes().as_slice(), &stateid].concat();
            assert!(
                close.windows(first.len()).any(|wire| wire == first),
                "{label}: first CLOSE must send seqid 1 and the lane stateid"
            );
            let response = if at_putfh {
                putfh_failure_result("close", status)
            } else {
                failed_lock_result("close", 4, status)
            };
            reply(&mut stream, &close, &response).await?;
            let retry = read_record(&mut stream).await?;
            let expected_frame = [expected.to_be_bytes().as_slice(), &stateid].concat();
            assert!(
                retry
                    .windows(expected_frame.len())
                    .any(|wire| wire == expected_frame),
                "{label}: retry must send seqid {expected} and the lane stateid"
            );
            reply(&mut stream, &retry, &close_result([0xe0 + index as u8; 16])).await
        });
        assert!(
            mount.close(Bytes::from_static(b"fh")).await.is_err(),
            "{label}"
        );
        assert_eq!(lane.lock().await.next_seqid, expected, "{label}");
        mount.close(Bytes::from_static(b"fh")).await.unwrap();
        assert!(mount.state.by_owner(owner).await.is_none(), "{label}");
        server.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn fresh_open_putfh_failure_does_not_consume_the_open_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
        .await
        .unwrap();
    let client = rpc::Client::new(mux, None);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &putfh_failure_result("open", 10006)).await
    });
    let dir = Bytes::from_static(b"dir");
    let spec = crate::nfs40::recovery::FreshOpenSpec {
        dir_fh: &dir,
        name: "file",
        owner_wire: b"owner",
        access: crate::OPEN_READ,
        client_id: 7,
    };
    let attempt = crate::nfs40::recovery::fresh_open_lane(
        &client,
        &Auth::new_null(),
        &spec,
        1,
        "recovery-open",
    )
    .await;
    assert!(
        matches!(attempt.result, Err(NfsError::Nfs4(_))),
        "PUTFH failure must surface the status"
    );
    assert_eq!(
        attempt.next_seqid, 1,
        "a PUTFH failure never ran OPEN and must not consume seqid 1"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn fresh_open_seqid_consumption_matrix() {
    enum OpenStage {
        Failure(u32),
        Success { confirm: Option<u32> },
    }
    // (label, scripted stage, expected next unconsumed seqid, succeeds)
    let cases = [
        ("OPEN ok", OpenStage::Success { confirm: None }, 2u32, true),
        ("OPEN SERVERFAULT", OpenStage::Failure(10006), 2, false),
        ("OPEN BAD_SEQID", OpenStage::Failure(10026), 1, false),
        ("OPEN MOVED", OpenStage::Failure(10019), 1, false),
        (
            "CONFIRM ok",
            OpenStage::Success { confirm: Some(0) },
            3,
            true,
        ),
        (
            "CONFIRM SERVERFAULT",
            OpenStage::Success {
                confirm: Some(10006),
            },
            3,
            false,
        ),
        (
            "CONFIRM BAD_STATEID",
            OpenStage::Success {
                confirm: Some(10025),
            },
            2,
            false,
        ),
        (
            "CONFIRM BAD_SEQID",
            OpenStage::Success {
                confirm: Some(10026),
            },
            2,
            false,
        ),
        (
            "CONFIRM MOVED",
            OpenStage::Success {
                confirm: Some(10019),
            },
            2,
            false,
        ),
    ];
    for (label, stage, expected_next, expected_ok) in cases {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mux = rpc::StreamMux::connect(listener.local_addr().unwrap(), true)
            .await
            .unwrap();
        let client = rpc::Client::new(mux, None);
        let open_stateid = [0x11; 16];
        let confirmed_stateid = [0x12; 16];
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let open = read_record(&mut stream).await?;
            let open_frame = [
                1u32.to_be_bytes().as_slice(),
                crate::OPEN_READ.to_be_bytes().as_slice(),
                0u32.to_be_bytes().as_slice(),
                7u64.to_be_bytes().as_slice(),
            ]
            .concat();
            assert!(
                open.windows(open_frame.len())
                    .any(|wire| wire == open_frame),
                "{label}: OPEN must carry seqid 1"
            );
            match stage {
                OpenStage::Failure(status) => {
                    return reply(&mut stream, &open, &failed_lock_result("open", 18, status))
                        .await;
                }
                OpenStage::Success { confirm } => {
                    let flags = if confirm.is_some() { 2 } else { 0 };
                    reply(&mut stream, &open, &open_result(flags, open_stateid, b"fh")).await?;
                    let Some(status) = confirm else {
                        return Ok(());
                    };
                    let confirm_request = read_record(&mut stream).await?;
                    let confirm_frame =
                        [open_stateid.as_slice(), 2u32.to_be_bytes().as_slice()].concat();
                    assert!(
                        confirm_request
                            .windows(confirm_frame.len())
                            .any(|wire| wire == confirm_frame),
                        "{label}: OPEN_CONFIRM must carry seqid 2"
                    );
                    if status == 0 {
                        reply(
                            &mut stream,
                            &confirm_request,
                            &compound_result(
                                "open_confirm",
                                &[(22, &[]), (20, &confirmed_stateid)],
                            ),
                        )
                        .await
                    } else {
                        reply(
                            &mut stream,
                            &confirm_request,
                            &failed_lock_result("open_confirm", 20, status),
                        )
                        .await
                    }
                }
            }
        });
        let dir = Bytes::from_static(b"dir");
        let spec = crate::nfs40::recovery::FreshOpenSpec {
            dir_fh: &dir,
            name: "file",
            owner_wire: b"owner",
            access: crate::OPEN_READ,
            client_id: 7,
        };
        let attempt = crate::nfs40::recovery::fresh_open_lane(
            &client,
            &Auth::new_null(),
            &spec,
            1,
            "recovery-open",
        )
        .await;
        assert_eq!(attempt.next_seqid, expected_next, "{label}");
        assert_eq!(attempt.result.is_ok(), expected_ok, "{label}");
        match attempt.result {
            Ok(open) => {
                let expected_stateid = if expected_next == 3 {
                    confirmed_stateid
                } else {
                    open_stateid
                };
                assert_eq!(open.stateid, expected_stateid, "{label}");
            }
            Err(error) => assert!(
                matches!(error, NfsError::Nfs4(_)),
                "{label}: unexpected error {error:?}"
            ),
        }
        server.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn lock_overall_status_contradiction_consumes_the_open_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 245, [0xd7; 16], crate::OPEN_BOTH).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &lock,
            &contradictory_result("lock", &[(22, &[]), (12, &[0x5c; 16])]),
        )
        .await
    });
    assert!(
        mount
            .lock(Bytes::from_static(b"fh"), 2, 0, 1)
            .await
            .is_err()
    );
    assert_eq!(lane.lock().await.next_seqid, 2);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_success_across_a_lost_mount_is_not_published() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 243, b"fh").await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(
            &mut stream,
            &lock,
            &compound_result("lock", &[(22, &[]), (12, &[0x92; 16])]),
        )
        .await
    });
    let for_lock = Arc::clone(&mount);
    let lock = tokio::spawn(async move { for_lock.lock(Bytes::from_static(b"fh"), 2, 0, 1).await });
    seen_rx.await.unwrap();
    mount.lease.mark_lost().await;
    let _ = reply_tx.send(());
    assert!(lock.await.unwrap().is_err());
    assert!(
        !mount.locks.has_fh(&Bytes::from_static(b"fh")).await,
        "a lost mount must not keep a lock lane it can never unlock"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_success_while_suspect_keeps_the_granted_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 242, b"fh").await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(
            &mut stream,
            &lock,
            &compound_result("lock", &[(22, &[]), (12, &[0x91; 16])]),
        )
        .await
    });
    let for_lock = Arc::clone(&mount);
    let lock = tokio::spawn(async move { for_lock.lock(Bytes::from_static(b"fh"), 2, 0, 1).await });
    seen_rx.await.unwrap();
    mount.lease.mark_reconnecting();
    let _ = reply_tx.send(());
    assert!(lock.await.unwrap().is_err());
    assert!(
        mount.locks.has_fh(&Bytes::from_static(b"fh")).await,
        "a granted lock must stay tracked while the mount is verifying its lease"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn open_result_across_a_generation_fence_is_not_published() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(&mut stream, &request, &open_result(0, [0x91; 16], b"fh")).await
    });
    let for_open = Arc::clone(&mount);
    let open = tokio::spawn(async move {
        for_open
            .open_file(Bytes::from_static(b"dir"), "file", crate::OPEN_READ, false)
            .await
    });
    seen_rx.await.unwrap();
    mount.lease.mark_lost().await;
    let _ = reply_tx.send(());
    let error = match open.await.unwrap() {
        Err(error) => error,
        Ok(_) => panic!("OPEN must not be published across a generation fence"),
    };
    assert_eq!(
        error.operation_outcome().unwrap().recovery,
        crate::RecoveryAction::Reopen
    );
    assert!(
        mount.state.snapshot().await.is_empty(),
        "a fenced OPEN must not leave a lane behind"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn suspect_open_error_keeps_the_published_lane() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let request = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(&mut stream, &request, &open_result(0, [0x92; 16], b"fh")).await
    });
    let for_open = Arc::clone(&mount);
    let open = tokio::spawn(async move {
        for_open
            .open_file(Bytes::from_static(b"dir"), "file", crate::OPEN_READ, false)
            .await
    });
    seen_rx.await.unwrap();
    // Lifecycle changes without a generation bump: the open still exists
    // and must remain tracked so it can be closed by file handle.
    mount.lease.mark_reconnecting();
    let _ = reply_tx.send(());
    let error = open.await.unwrap().unwrap_err();
    assert!(matches!(error, NfsError::OperationOutcome(_)));
    assert!(
        mount
            .state
            .for_fh(&Bytes::from_static(b"fh"), crate::OPEN_READ)
            .await
            .is_some(),
        "a non-lost mount must keep the published open lane"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn queued_refresh_does_not_open_after_the_mount_is_lost() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 228, [0xf1; 16], crate::OPEN_READ).await;
    let held = lane.lock().await;
    let refresh = {
        let mount = Arc::clone(&mount);
        let lane = Arc::clone(&lane);
        tokio::spawn(async move {
            mount
                .refresh_lane_stateid(&lane, [0xf1; 16], StateidRecovery::Bad, "read")
                .await
        })
    };
    // Let the detached task queue behind the lane lock, then lose the
    // mount before it can dispatch its recovery OPEN.
    tokio::task::yield_now().await;
    mount.lease.mark_lost().await;
    drop(held);
    let error = refresh.await.unwrap().unwrap_err();
    assert!(
        matches!(error, NfsError::OperationOutcome(_)),
        "a queued refresh must report the lifecycle gate, got {error:?}"
    );
    assert_eq!(lane.lock().await.stateid, [0xf1; 16]);
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "a lost mount must not dispatch a recovery OPEN"
        );
        Ok::<(), std::io::Error>(())
    });
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_confirm_failure_records_every_consumed_open_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 221, [0xa5; 16], crate::OPEN_READ).await;
    lane.lock().await.next_seqid = 5;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // seqid=0 probe: the open-owner record survived.
        let probe = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &probe,
            &failed_lock_result("open-recover", 18, 10026),
        )
        .await?;
        // Fallback OPEN(5) succeeds but needs confirmation.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(2, [0xa6; 16], b"fh")).await?;
        // OPEN_CONFIRM(6) fails with SERVERFAULT, which consumes seqid 6.
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &failed_lock_result("open_confirm", 20, 10006),
        )
        .await?;
        // The lane must now expect 7, not the 6 derived from the OPEN alone.
        let close = read_record(&mut stream).await?;
        let expected = [7u32.to_be_bytes().as_slice(), &[0xa5; 16]].concat();
        assert!(
            close.windows(expected.len()).any(|wire| wire == expected),
            "CLOSE must use the seqid consumed by both OPEN and OPEN_CONFIRM"
        );
        reply(&mut stream, &close, &close_result([0xa7; 16])).await
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_SERVERFAULT)
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(lane.lock().await.next_seqid, 7);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_confirm_non_consuming_failure_keeps_open_consumption() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 223, [0xa5; 16], crate::OPEN_READ).await;
    lane.lock().await.next_seqid = 5;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        let probe = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &probe,
            &failed_lock_result("open-recover", 18, 10026),
        )
        .await?;
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(2, [0xa6; 16], b"fh")).await?;
        // BAD_STATEID does not consume seqid 6.
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &failed_lock_result("open_confirm", 20, 10025),
        )
        .await?;
        // OPEN(5) still consumed its seqid, so the next request uses 6.
        let close = read_record(&mut stream).await?;
        let expected = [6u32.to_be_bytes().as_slice(), &[0xa5; 16]].concat();
        assert!(
            close.windows(expected.len()).any(|wire| wire == expected),
            "CLOSE must keep the seqid consumed by the successful OPEN"
        );
        reply(&mut stream, &close, &close_result([0xa7; 16])).await
    });
    assert!(mount.read(Bytes::from_static(b"fh"), 0, 4).await.is_err());
    assert_eq!(lane.lock().await.next_seqid, 6);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_open_old_stateid_is_reported_as_exhaustion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 212, [0xac; 16], crate::OPEN_READ).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        let open = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &open,
            &failed_lock_result("open-recover", 18, 10024),
        )
        .await
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    // OLD_STATEID from the recovery OPEN itself means the bounded recovery
    // already failed; it must not surface as recoverable in place.
    assert_eq!(
        error.stateid_failure(),
        Some(crate::StateidFailure::Exhausted)
    );
    match error {
        NfsError::StateRecoveryExhausted(failure) => {
            assert_eq!(failure.operation, "read");
            assert_eq!(failure.status, crate::Nfs4ErrorCode::NFS4ERR_BAD_STATEID);
            assert!(matches!(
                *failure.source,
                NfsError::Nfs4(crate::Nfs4ErrorCode::NFS4ERR_OLD_STATEID)
            ));
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(lane.lock().await.stateid, [0xac; 16]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_open_returning_same_stateid_reports_exhaustion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 211, [0xab; 16], crate::OPEN_READ).await;
    // A lane that already used seqids: the fresh OPEN restarts at seqid 0,
    // so the recorded value must drop to 1 even though the recovery fails.
    lane.lock().await.next_seqid = 3;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        let open = read_record(&mut stream).await?;
        // A fresh OPEN that answers with the stateid that just failed must
        // not be mistaken for a recovered lane; its seqid is consumed all
        // the same and the lane must record it.
        reply(&mut stream, &open, &open_result(0, [0xab; 16], b"fh")).await
    });
    let error = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .unwrap_err();
    assert_eq!(
        error.stateid_failure(),
        Some(crate::StateidFailure::Exhausted)
    );
    let lane = lane.lock().await;
    assert_eq!(lane.stateid, [0xab; 16]);
    assert_eq!(lane.next_seqid, 1);
    drop(lane);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_putfh_failure_keeps_the_lane_cursor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 246, [0xd8; 16], crate::OPEN_BOTH).await;
    lane.lock().await.next_seqid = 5;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        // The recovery probe starts at seqid 0, but a PUTFH failure means
        // the OPEN never ran: the lane's real cursor must not regress.
        reply(
            &mut stream,
            &open,
            &putfh_failure_result("recovery-open", 10006),
        )
        .await
    });
    assert!(
        mount
            .refresh_lane_stateid(&lane, [0xd8; 16], StateidRecovery::Bad, "read")
            .await
            .is_err()
    );
    assert_eq!(
        lane.lock().await.next_seqid,
        5,
        "a non-executed recovery OPEN must keep the lane's existing cursor"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn release_locks_accepts_success_when_recovery_starts_mid_release() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 46, b"fh").await;
    register_scripted_lock(&mount, 46, [0x77; 16]).await;
    mount.lease.mark_reconnecting();
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0x78; 16])]),
        )
        .await?;
        let release = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &release,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    let for_release = Arc::clone(&mount);
    let release =
        tokio::spawn(async move { for_release.release_locks(Bytes::from_static(b"fh")).await });
    seen_rx.await.unwrap();
    // Recovery starts while the LOCKU is in flight. The successful reply is
    // authoritative; reporting failure would make callers retry an
    // already-released lock.
    mount.lease.mark_recovering();
    let _ = reply_tx.send(());
    release.await.unwrap().unwrap();
    assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn release_locks_ignores_a_failed_release_lockowner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 48, b"fh").await;
    register_scripted_lock(&mount, 48, [0x81; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0x82; 16])]),
        )
        .await?;
        let release = read_record(&mut stream).await?;
        // STALE_CLIENTID: the lock state is already gone even though the
        // cleanup owner release failed.
        reply(
            &mut stream,
            &release,
            &compound_result_with_status(10022, "release-lockowner", &[]),
        )
        .await
    });
    mount
        .release_locks(Bytes::from_static(b"fh"))
        .await
        .unwrap();
    assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn release_locks_recovers_an_abandoned_lock_token() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 44, b"fh").await;
    // Simulate a LOCK that succeeded after its caller was cancelled: the
    // lock stays registered, but the caller never received the token.
    register_scripted_lock(&mount, 44, [0x73; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        let expected = [1u32.to_be_bytes().as_slice(), &[0x73; 16]].concat();
        assert!(locku.windows(expected.len()).any(|wire| wire == expected));
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0x74; 16])]),
        )
        .await?;
        let release = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &release,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &close_result([0x61; 16])).await
    });
    let error = mount.close(Bytes::from_static(b"fh")).await.unwrap_err();
    assert!(
        error.to_string().contains("outstanding locks"),
        "the abandoned lock must still be visible to CLOSE: {error}"
    );
    mount
        .release_locks(Bytes::from_static(b"fh"))
        .await
        .unwrap();
    assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn release_locks_works_while_the_mount_is_verifying_its_lease() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 45, b"fh").await;
    register_scripted_lock(&mount, 45, [0x75; 16]).await;
    // A cancelled LOCK often coincides with a failed RENEW. Teardown must
    // not be blocked by the lifecycle gate, otherwise close stays wedged.
    mount.lease.mark_reconnecting();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        let expected = [1u32.to_be_bytes().as_slice(), &[0x75; 16]].concat();
        assert!(locku.windows(expected.len()).any(|wire| wire == expected));
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0x76; 16])]),
        )
        .await?;
        let release = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &release,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    mount
        .release_locks(Bytes::from_static(b"fh"))
        .await
        .unwrap();
    assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn release_locks_returns_immediately_without_acquisitions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 246, b"fh").await;
    tokio::time::timeout(
        Duration::from_millis(100),
        mount.release_locks(Bytes::from_static(b"fh")),
    )
    .await
    .expect("cleanup without in-flight acquisitions must not wait")
    .unwrap();
}

#[tokio::test]
async fn release_locks_waits_for_a_cancelled_lock_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 247, b"fh").await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(&mut stream, &lock, &failed_lock_result("lock", 12, 10025)).await?;
        // Nothing was granted, so cleanup must go straight to CLOSE.
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &close_result([0x61; 16])).await
    });
    let for_lock = Arc::clone(&mount);
    let lock = tokio::spawn(async move { for_lock.lock(Bytes::from_static(b"fh"), 2, 0, 1).await });
    seen_rx.await.unwrap();
    lock.abort();
    let for_release = Arc::clone(&mount);
    let mut release =
        tokio::spawn(async move { for_release.release_locks(Bytes::from_static(b"fh")).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut release)
            .await
            .is_err(),
        "the failed LOCK must still be tracked as unsettled"
    );
    let _ = reply_tx.send(());
    tokio::time::timeout(Duration::from_secs(5), release)
        .await
        .expect("release_locks must finish once the LOCK failed")
        .unwrap()
        .unwrap();
    assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn release_locks_waits_for_a_cancelled_lock_settlement() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 245, b"fh").await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(
            &mut stream,
            &lock,
            &compound_result("lock", &[(22, &[]), (12, &[0x9a; 16])]),
        )
        .await?;
        // The grant registered by the detached task must be visible to
        // release_locks, which then unlocks it.
        let locku = read_record(&mut stream).await?;
        let expected = [1u32.to_be_bytes().as_slice(), &[0x9a; 16]].concat();
        assert!(
            locku.windows(expected.len()).any(|wire| wire == expected),
            "release_locks must unlock the lock whose caller was cancelled"
        );
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0x9b; 16])]),
        )
        .await?;
        let release = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &release,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &close_result([0x61; 16])).await
    });
    let for_lock = Arc::clone(&mount);
    let lock = tokio::spawn(async move { for_lock.lock(Bytes::from_static(b"fh"), 2, 0, 1).await });
    seen_rx.await.unwrap();
    // The caller goes away while the LOCK reply is still pending; its
    // detached task will register the grant on its own.
    lock.abort();
    let for_release = Arc::clone(&mount);
    let mut release =
        tokio::spawn(async move { for_release.release_locks(Bytes::from_static(b"fh")).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut release)
            .await
            .is_err(),
        "release_locks returned before the in-flight LOCK settled"
    );
    let _ = reply_tx.send(());
    tokio::time::timeout(Duration::from_secs(5), release)
        .await
        .expect("release_locks must finish once the LOCK settles")
        .unwrap()
        .unwrap();
    assert!(!mount.locks.has_fh(&Bytes::from_static(b"fh")).await);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_replacement_filehandle_records_the_cleanup_close_seqid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 227, [0xe1; 16], crate::OPEN_READ).await;
    lane.lock().await.next_seqid = 5;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        // A released owner restarts at seqid 0.
        let probe = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &probe,
            &failed_lock_result("open-recover", 18, 10026),
        )
        .await?;
        // The retry with the lane cursor opens a *different* filehandle.
        let open = read_record(&mut stream).await?;
        reply(&mut stream, &open, &open_result(0, [0xe2; 16], b"other")).await?;
        // OPEN(5) consumed 5, so the cleanup CLOSE uses 6.
        let cleanup = read_record(&mut stream).await?;
        let expected = [6u32.to_be_bytes().as_slice(), &[0xe2; 16]].concat();
        assert!(
            cleanup.windows(expected.len()).any(|wire| wire == expected),
            "cleanup CLOSE must use the seqid after the successful OPEN"
        );
        reply(&mut stream, &cleanup, &close_result([0xe2; 16])).await?;
        // Because cleanup CLOSE(6) succeeded, the server now expects 7.
        let close = read_record(&mut stream).await?;
        let expected = [7u32.to_be_bytes().as_slice(), &[0xe1; 16]].concat();
        assert!(
            close.windows(expected.len()).any(|wire| wire == expected),
            "the lane must record the seqid consumed by the cleanup CLOSE"
        );
        reply(&mut stream, &close, &close_result([0xe1; 16])).await
    });
    assert!(mount.read(Bytes::from_static(b"fh"), 0, 4).await.is_err());
    assert_eq!(lane.lock().await.next_seqid, 7);
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn unknown_cleanup_close_retires_the_recovered_lane() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 231, [0xf2; 16], crate::OPEN_READ).await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (reply_tx, reply_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let _ = reply_rx.await;
        reply(&mut stream, &open, &open_result(0, [0xf3; 16], b"fh")).await?;
        let cleanup = read_record(&mut stream).await?;
        // The recovery OPEN restarts a released owner at seqid 0, so the
        // cleanup CLOSE carries the next seqid.
        let expected = [1u32.to_be_bytes().as_slice(), &[0xf3; 16]].concat();
        assert!(cleanup.windows(expected.len()).any(|wire| wire == expected));
        // Drop the connection without replying: the cleanup CLOSE outcome
        // is unknown and the lane must not stay stuck in `closing`.
        drop(stream);
        Ok::<(), std::io::Error>(())
    });
    let mount_for_refresh = Arc::clone(&mount);
    let refresh = tokio::spawn(async move {
        mount_for_refresh
            .refresh_lane_stateid(&lane, [0xf2; 16], StateidRecovery::Bad, "read")
            .await
    });
    seen_rx.await.unwrap();
    mount.lease.mark_lost().await;
    let _ = reply_tx.send(());
    let error = refresh.await.unwrap().unwrap_err();
    assert!(matches!(error, NfsError::OperationOutcome(_)));
    assert!(
        mount
            .state
            .for_fh(&Bytes::from_static(b"fh"), crate::OPEN_READ)
            .await
            .is_none(),
        "an unknown cleanup CLOSE must retire the lane"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_read_does_not_abandon_stateid_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 222, [0xa8; 16], crate::OPEN_READ).await;
    let (opened_tx, mut opened_rx) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        let open = read_record(&mut stream).await?;
        opened_tx.send(()).ok();
        // Hold the recovery OPEN reply until after the caller is cancelled.
        tokio::time::sleep(Duration::from_millis(100)).await;
        reply(&mut stream, &open, &open_result(0, [0xb1; 16], b"fh")).await
    });
    let mut read = Box::pin(mount.read(Bytes::from_static(b"fh"), 0, 4));
    tokio::select! {
        _ = opened_rx.recv() => {}
        result = &mut read => panic!("read completed before cancellation: {result:?}"),
    }
    drop(read);
    // The detached recovery task must still publish the new stateid and the
    // consumed owner seqid after the caller is gone.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let settled = {
                let guard = lane.lock().await;
                guard.stateid == [0xb1; 16] && guard.next_seqid == 1
            };
            if settled {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a cancelled read abandoned stateid recovery");
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_read_during_open_confirm_still_publishes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let lane = register_stateid_lane(&mount, 224, [0xc1; 16], crate::OPEN_READ).await;
    let (confirm_tx, mut confirm_rx) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &failed_lock_result("read", 25, 10025)).await?;
        let open = read_record(&mut stream).await?;
        // The recovery OPEN needs confirmation, so the caller can be
        // cancelled between the two consuming stages.
        reply(&mut stream, &open, &open_result(2, [0xc2; 16], b"fh")).await?;
        let confirm = read_record(&mut stream).await?;
        confirm_tx.send(()).ok();
        // Hold the OPEN_CONFIRM reply until after the caller is cancelled.
        tokio::time::sleep(Duration::from_millis(100)).await;
        reply(
            &mut stream,
            &confirm,
            &compound_result("open_confirm", &[(22, &[]), (20, &[0xc3; 16])]),
        )
        .await
    });
    let mut read = Box::pin(mount.read(Bytes::from_static(b"fh"), 0, 4));
    tokio::select! {
        _ = confirm_rx.recv() => {}
        result = &mut read => panic!("read completed before cancellation: {result:?}"),
    }
    drop(read);
    // The detached recovery task must finish OPEN_CONFIRM and publish both
    // the confirmed stateid and the seqid consumed by both stages.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let settled = {
                let guard = lane.lock().await;
                guard.stateid == [0xc3; 16] && guard.next_seqid == 2
            };
            if settled {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a cancelled read abandoned OPEN_CONFIRM publication");
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn denied_reclaim_lock_drops_only_that_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 85, b"fh").await;
    // A WRITE lock from 4096 to the end of file; u64::MAX is the RFC 7530
    // end-of-file sentinel and the only form the public LOCK API can express.
    register_scripted_lock_range(&mount, 85, [0xd1; 16], 2, 4096, u64::MAX).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&19u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x79; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        reply(&mut stream, &reclaim, &open_result(0, [0x61; 16], b"fh")).await?;
        let lock = read_record(&mut stream).await?;
        reply(&mut stream, &lock, &denied_lock_result("reclaim-lock", 12)).await?;
        // The gated READ on the affected handle must fail locally, so the next
        // record is the unrelated open's READ.
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &read_result(&[0x42; 4])).await?;
        // Only a lock that restores the lost protection lifts the gate. A
        // disjoint lock, then a weaker READ lock, then the covering WRITE lock.
        for _ in 0..3 {
            let relock = read_record(&mut stream).await?;
            reply(
                &mut stream,
                &relock,
                &succeeded_lock_result("lock", 0, [0xd2; 16]),
            )
            .await?;
        }
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &read_result(&[0x43; 4])).await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x59; 8],
            owner: "scripted-denied-lock".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover_or_lose().await.unwrap();
    assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
    assert!(
        mount.state.by_owner(85).await.is_some(),
        "the denied lock reclaim must not drop its open-owner"
    );
    assert!(
        mount.locks.by_stateid(&[0xd1; 16]).await.is_none(),
        "a denied reclaim LOCK must drop only that lock, not lose the mount"
    );
    // The application must learn that the lock is gone before it touches the
    // surviving open again: the gate fails locally, without a server round trip.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        mount.read(Bytes::from_static(b"fh"), 0, 4),
    )
    .await
    .expect("a lock-loss gated READ must fail without reaching the server")
    .unwrap_err();
    let crate::NfsError::StateRecoveryExhausted(details) = &error else {
        panic!("expected a lock-loss recovery error, got {error:?}");
    };
    assert_eq!(details.operation, "read");
    assert!(matches!(
        details.source.as_ref(),
        crate::NfsError::LockDenied { .. }
    ));
    let write = tokio::time::timeout(
        Duration::from_secs(1),
        mount.write(Bytes::from_static(b"fh"), 0, Bytes::from_static(b"x")),
    )
    .await
    .expect("a lock-loss gated WRITE must fail without reaching the server")
    .unwrap_err();
    let crate::NfsError::StateRecoveryExhausted(details) = &write else {
        panic!("expected a lock-loss recovery error for write, got {write:?}");
    };
    assert_eq!(details.operation, "write");

    // Unrelated opens remain usable.
    register_scripted_open(&mount, 86, b"other").await;
    let unrelated = mount
        .read(Bytes::from_static(b"other"), 0, 4)
        .await
        .unwrap();
    assert_eq!(&unrelated[..], &[0x42; 4]);

    // A disjoint lock and a weaker READ lock do not restore a lost WRITE lock
    // over [4096, EOF): neither may acknowledge the loss. Only the covering
    // WRITE lock over [0, EOF) does.
    for (lock_type, offset, length) in [(2, 0, 4096), (1, 0, u64::MAX), (2, 0, u64::MAX)] {
        mount
            .lock(Bytes::from_static(b"fh"), lock_type, offset, length)
            .await
            .unwrap();
        let gated = tokio::time::timeout(
            Duration::from_secs(1),
            mount.read(Bytes::from_static(b"fh"), 0, 4),
        )
        .await;
        if lock_type == 2 && offset == 0 && length == u64::MAX {
            let after = gated
                .expect("the covering lock must lift the gate")
                .unwrap();
            assert_eq!(&after[..], &[0x43; 4]);
        } else {
            let error = gated
                .expect("a lock that does not restore the loss must keep I/O gated")
                .unwrap_err();
            assert!(
                matches!(&error, crate::NfsError::StateRecoveryExhausted(_)),
                "expected the gate to hold, got {error:?}"
            );
        }
    }
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_lock_released_during_open_recovery_does_not_gate_io() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    // The lock's open lane is already gone, so the reclaim branch that cannot
    // find an open owner is the one under test.
    register_scripted_lock(&mount, 92, [0xb8; 16]).await;
    let (seen_tx, seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        // Hold the LOCKU reply so the lane stays held (and registered) while
        // recovery snapshots it and blocks on its mutex.
        let locku = read_record(&mut stream).await?;
        let _ = seen_tx.send(());
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&22u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7c; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let _ = release_rx.await;
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0xb9; 16])]),
        )
        .await?;
        let cleanup = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &cleanup,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await?;
        // The next request is the caller's stateless READ: a reclaim of the
        // released lock would arrive here first and fail to decode as one.
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &read_result(&[0x55; 4])).await
    });
    let unlock_mount = Arc::clone(&mount);
    let unlock = tokio::spawn(async move {
        unlock_mount
            .locku(
                Bytes::from_static(b"fh"),
                Bytes::from_static(&[0xb8; 16]),
                2,
                0,
                1,
            )
            .await
    });
    seen_rx.await.unwrap();
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5d; 8],
            owner: "scripted-released-lock".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let recovery = tokio::spawn(async move { context.recover_or_lose().await });
    // Give recovery time to snapshot the lock and queue on its mutex before the
    // LOCKU reply retires it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = release_tx.send(());
    recovery.await.unwrap().unwrap();
    unlock.await.unwrap().unwrap();
    assert!(
        mount.locks.lost(&Bytes::from_static(b"fh")).await.is_none(),
        "a lock the caller already released must not gate I/O"
    );
    // With the gate absent the handle still has no open lane, so this is the
    // stateless path.
    let data = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .expect("I/O on a handle whose released lock recovery skipped must proceed");
    assert_eq!(&data[..], &[0x55; 4]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_lock_whose_open_recovery_dropped_still_gates_io_until_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 88, b"fh").await;
    register_scripted_lock(&mount, 88, [0xf1; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&21u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7b; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        // Both the reclaim and its CLAIM_NULL fallback are rejected, so recovery
        // drops the open lane and the lock it carried can never be reclaimed.
        let reclaim = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10038),
        )
        .await?;
        let fallback = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &fallback,
            &failed_lock_result("reclaim-fallback-open", 18, 10038),
        )
        .await?;
        // The gated I/O never reaches the server; once CLOSE lifts the gate the
        // handle has no open lane left, so a stateless READ is the next record.
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &read_result(&[0x44; 4])).await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5c; 8],
            owner: "scripted-open-reclaim-loss".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover_or_lose().await.unwrap();
    assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
    assert!(
        mount.state.by_owner(88).await.is_none(),
        "a rejected reclaim must drop the open lane"
    );
    assert!(
        mount.locks.by_stateid(&[0xf1; 16]).await.is_none(),
        "a lock whose open is gone must be dropped instead of reclaimed"
    );
    // The lock died with its open, so I/O must fail even though no open lane
    // remains to gate it at dispatch: otherwise READ would fall back to the
    // stateless path and run outside the protection the caller still expects.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        mount.read(Bytes::from_static(b"fh"), 0, 4),
    )
    .await
    .expect("a gated READ must fail without reaching the server")
    .unwrap_err();
    let crate::NfsError::StateRecoveryExhausted(details) = &error else {
        panic!("expected a lock-loss recovery error, got {error:?}");
    };
    assert_eq!(details.operation, "read");
    assert_eq!(details.status, crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID);
    let write = tokio::time::timeout(
        Duration::from_secs(1),
        mount.write(Bytes::from_static(b"fh"), 0, Bytes::from_static(b"x")),
    )
    .await
    .expect("a gated WRITE must fail without reaching the server")
    .unwrap_err();
    let crate::NfsError::StateRecoveryExhausted(details) = &write else {
        panic!("expected a lock-loss recovery error for write, got {write:?}");
    };
    assert_eq!(details.operation, "write");
    // CLOSE releases nothing on the server but must lift the gate, otherwise a
    // handle whose open recovery dropped could never be retired.
    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    let data = mount
        .read(Bytes::from_static(b"fh"), 0, 4)
        .await
        .expect("a closed handle must be readable through the stateless path");
    assert_eq!(&data[..], &[0x44; 4]);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_stateful_lifts_the_gate_when_recovery_dropped_the_last_open() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 91, b"fh").await;
    register_scripted_lock(&mount, 91, [0xb7; 16]).await;
    let fh = Bytes::from_static(b"fh");
    // Reproduce what recovery leaves behind when it rejects an open reclaim and
    // drops the lock that depended on it: no lane, but a published lock gate.
    mount.state.remove(91, &fh).await;
    mount.locks.remove(&[0xb7; 16]).await;
    mount
        .locks
        .mark_lost(
            &fh,
            LostLock {
                status: crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID,
                lock_type: 2,
                offset: 0,
                length: 1,
                owner: Bytes::from_static(b"scripted-lock-91"),
            },
        )
        .await;
    assert!(!mount.locks.has_fh(&fh).await);
    // CLOSE releases nothing on the server, but the owner token still names the
    // dropped lane; it must succeed and lift the gate, or the handle could never
    // be retired after its open recovery failed.
    let opened = mount::OpenFile::with_protocol_state(
        mount::ObjRes {
            fh: fh.clone(),
            attr: None,
        },
        encode_owner(mount.issuer, 91),
    );
    mount.close_stateful(opened).await.unwrap();
    assert!(
        mount.locks.lost(&fh).await.is_none(),
        "a successful CLOSE must lift the lock gate"
    );
    // Without the gate the same stale token is still a programming error.
    let stale = mount::OpenFile::with_protocol_state(
        mount::ObjRes {
            fh: fh.clone(),
            attr: None,
        },
        encode_owner(mount.issuer, 91),
    );
    assert!(matches!(
        mount.close_stateful(stale).await,
        Err(NfsError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn a_stateless_read_after_total_state_loss_fails_locally() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 89, b"fh").await;
    register_scripted_lock(&mount, 89, [0xa5; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&22u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7c; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        // A session-wide status cannot be a per-lane loss, so recovery gives up
        // and drops every lane instead of resuming the mount.
        let reclaim = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10022),
        )
        .await?;
        // Neither the READ nor its zero-length lease probe may be sent.
        if let Ok(record) =
            tokio::time::timeout(Duration::from_millis(500), read_record(&mut stream)).await
        {
            panic!(
                "I/O reached the server after state loss: {} bytes",
                record.unwrap().len()
            );
        }
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5d; 8],
            owner: "scripted-total-loss".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover_or_lose().await.unwrap_err();
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState
    );
    assert!(
        mount.state.by_owner(89).await.is_none(),
        "a lost mount must not keep reclaimable lanes"
    );
    assert!(mount.locks.by_stateid(&[0xa5; 16]).await.is_none());
    // With every lane dropped, this handle has no open state and no lock record
    // left, so nothing but the lifecycle check can stop the anonymous-stateid
    // READ from running outside the lock the caller still believes it holds.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        mount.read(Bytes::from_static(b"fh"), 0, 4),
    )
    .await
    .expect("a stateless READ after state loss must fail locally")
    .unwrap_err();
    let details = error
        .operation_outcome()
        .expect("expected the lifecycle gate error");
    assert_eq!(details.recovery, crate::RecoveryAction::Reopen);
    // A zero-length READ never reaches the server, so it stays available.
    let probe = mount.read(Bytes::from_static(b"fh"), 0, 0).await.unwrap();
    assert!(probe.is_empty());
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn setattr_size_after_state_loss_fails_locally_but_metadata_still_works() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 90, b"fh").await;
    register_scripted_lock(&mount, 90, [0xa6; 16]).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&23u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7d; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &reclaim,
            &failed_lock_result("reclaim-open", 18, 10022),
        )
        .await?;
        // Only the metadata-only SETATTR may arrive: a truncated handle has no
        // open lane left, so its anonymous-stateid size change must be refused
        // locally instead of writing outside the lost lock protection.
        let setattr = read_record(&mut stream).await?;
        assert!(
            !setattr.windows(5).any(|wire| wire == b"gated".as_slice()),
            "a gated truncate reached the server"
        );
        assert!(
            setattr.windows(4).any(|wire| wire == b"meta".as_slice()),
            "the metadata-only SETATTR did not reach the server"
        );
        reply(&mut stream, &setattr, &setattr_result()).await
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5e; 8],
            owner: "scripted-truncate-loss".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    context.recover_or_lose().await.unwrap_err();
    assert_eq!(
        mount.health().lifecycle,
        crate::MountLifecycleState::LostState
    );
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        mount.setattr(
            Bytes::from_static(b"gated"),
            None,
            None,
            None,
            None,
            Some(4),
            None,
            None,
        ),
    )
    .await
    .expect("a lane-less truncate must fail locally")
    .unwrap_err();
    let details = error
        .operation_outcome()
        .expect("expected the lifecycle gate error");
    assert_eq!(details.recovery, crate::RecoveryAction::Reopen);
    // Metadata-only changes address the file without an open, so chmod keeps
    // using the anonymous stateid even after the mount lost its state.
    mount
        .setattr(
            Bytes::from_static(b"meta"),
            None,
            Some(0o600),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_read_that_queued_on_a_lane_sees_a_loss_published_during_the_wait() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), read_record(&mut stream))
                .await
                .is_err(),
            "a gated READ must not reach the server"
        );
        Ok::<_, std::io::Error>(())
    });
    // Only a WRITE owner remains, so the READ lookup finds no usable lane but
    // still parks on this lane's mutex.
    let lane = register_stateid_lane(&mount, 93, [0xc2; 16], crate::OPEN_WRITE).await;
    let held = lane.lock().await;
    let read_mount = Arc::clone(&mount);
    let read = tokio::spawn(async move { read_mount.read(Bytes::from_static(b"fh"), 0, 4).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Recovery publishes the loss while the lookup is parked on that lane.
    publish_lost_whole_file(&mount, b"fh").await;
    drop(held);
    let error = read
        .await
        .unwrap()
        .expect_err("a READ that queued on a lane must observe the loss");
    let crate::NfsError::StateRecoveryExhausted(details) = &error else {
        panic!("expected a lock-loss recovery error, got {error:?}");
    };
    assert_eq!(details.operation, "read");
    assert_eq!(details.status, crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_truncate_with_a_surviving_open_still_honours_the_lost_gate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_stateid_lane(&mount, 94, [0xc3; 16], crate::OPEN_WRITE).await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        // The truncate must be refused locally, so the metadata-only SETATTR on
        // another handle is the only request.
        let setattr = read_record(&mut stream).await?;
        assert!(
            setattr.windows(4).any(|wire| wire == b"meta".as_slice()),
            "a gated truncate reached the server through a surviving open"
        );
        reply(&mut stream, &setattr, &setattr_result()).await
    });
    let error = mount
        .setattr(
            Bytes::from_static(b"fh"),
            None,
            None,
            None,
            None,
            Some(4),
            None,
            None,
        )
        .await
        .expect_err("a truncate must honour the lost-lock gate even with an open lane");
    let crate::NfsError::StateRecoveryExhausted(details) = &error else {
        panic!("expected a lock-loss recovery error, got {error:?}");
    };
    assert_eq!(details.operation, "setattr");
    assert_eq!(details.status, crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID);
    mount
        .setattr(
            Bytes::from_static(b"meta"),
            None,
            Some(0o600),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

/// The anonymous-stateid truncate path: no open lane exists for the handle, the
/// mount is `Ready`, and only the lost gate stands between the truncate and the
/// wire. Reachable through the path API, which never opens a lane.
#[tokio::test]
async fn a_truncate_without_any_open_lane_still_honours_the_lost_gate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        // The truncate must be refused locally, so the metadata-only SETATTR on
        // another handle is the only request.
        let setattr = read_record(&mut stream).await?;
        assert!(
            setattr.windows(4).any(|wire| wire == b"meta".as_slice()),
            "an anonymous-stateid truncate reached the server despite the lost gate"
        );
        reply(&mut stream, &setattr, &setattr_result()).await
    });
    let error = mount
        .setattr(
            Bytes::from_static(b"fh"),
            None,
            None,
            None,
            None,
            Some(4),
            None,
            None,
        )
        .await
        .expect_err("an anonymous-stateid truncate must honour the lost gate");
    let crate::NfsError::StateRecoveryExhausted(details) = &error else {
        panic!("expected a lock-loss recovery error, got {error:?}");
    };
    assert_eq!(details.operation, "setattr");
    assert_eq!(details.status, crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID);
    mount
        .setattr(
            Bytes::from_static(b"meta"),
            None,
            Some(0o600),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_truncate_waits_for_the_publish_fence_before_sending() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), read_record(&mut stream))
                .await
                .is_err(),
            "a gated truncate must not reach the server"
        );
        Ok::<_, std::io::Error>(())
    });
    let lane = register_stateid_lane(&mount, 96, [0xc4; 16], crate::OPEN_WRITE).await;
    let fence = Arc::clone(&lane.lock().await.io_fence);
    // Hold the fence a reclaim takes before it publishes a loss, so the truncate
    // must queue behind the publish instead of deciding before it.
    let published = fence.write_owned().await;
    let truncate_mount = Arc::clone(&mount);
    let truncate = tokio::spawn(async move {
        truncate_mount
            .setattr(
                Bytes::from_static(b"fh"),
                None,
                None,
                None,
                None,
                Some(4),
                None,
                None,
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    publish_lost_whole_file(&mount, b"fh").await;
    drop(published);
    let error = truncate
        .await
        .unwrap()
        .expect_err("a truncate must observe a loss published before its fence");
    let crate::NfsError::StateRecoveryExhausted(details) = &error else {
        panic!("expected a lock-loss recovery error, got {error:?}");
    };
    assert_eq!(details.operation, "setattr");
    assert_eq!(details.status, crate::Nfs4ErrorCode::NFS4ERR_STALE_STATEID);
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn io_queued_behind_a_denied_reclaim_is_gated_at_dispatch() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 87, b"fh").await;
    register_scripted_lock(&mount, 87, [0xe1; 16]).await;
    let (held_tx, held_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let identity = read_record(&mut stream).await?;
        let mut identity_data = Vec::new();
        identity_data.extend_from_slice(&20u64.to_be_bytes());
        identity_data.extend_from_slice(&[0x7a; 8]);
        reply(
            &mut stream,
            &identity,
            &compound_result("identity", &[(35, &identity_data)]),
        )
        .await?;
        let confirm = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &confirm,
            &compound_result("confirm", &[(36, &[])]),
        )
        .await?;
        let reclaim = read_record(&mut stream).await?;
        reply(&mut stream, &reclaim, &open_result(0, [0x61; 16], b"fh")).await?;
        // Hold the DENIED reply so I/O can queue on the open lane this reclaim
        // holds, after passing the entry check while no loss is recorded yet.
        let lock = read_record(&mut stream).await?;
        held_tx.send(()).ok();
        release_rx.await.ok();
        reply(&mut stream, &lock, &denied_lock_result("reclaim-lock", 12)).await?;
        // The queued operations must fail locally: nothing else may arrive.
        if let Ok(record) =
            tokio::time::timeout(Duration::from_millis(500), read_record(&mut stream)).await
        {
            panic!(
                "queued I/O reached the server after the denied reclaim: {} bytes",
                record.unwrap().len()
            );
        }
        Ok::<(), std::io::Error>(())
    });
    let context = RecoveryContext {
        rpc: mount.rpc.clone(),
        auth: mount.auth.clone(),
        identity: ClientIdentity {
            verifier: [0x5b; 8],
            owner: "queued-io-behind-denied-reclaim".into(),
        },
        client_id: Arc::clone(&mount.client_id),
        issuer: mount.issuer,
        state: Arc::clone(&mount.state),
        locks: Arc::clone(&mount.locks),
        lease: Arc::clone(&mount.lease),
        callback_addr: None,
    };
    let recovery = tokio::spawn(async move { context.recover_or_lose().await });
    held_rx.await.unwrap();
    let read = tokio::spawn({
        let mount = Arc::clone(&mount);
        async move {
            mount
                .read(Bytes::from_static(b"fh"), 0, 4)
                .await
                .map(|data| data.len())
        }
    });
    let write = tokio::spawn({
        let mount = Arc::clone(&mount);
        async move {
            mount
                .write(Bytes::from_static(b"fh"), 0, Bytes::from_static(b"x"))
                .await
                .map(|outcome| outcome.count as usize)
        }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !read.is_finished() && !write.is_finished(),
        "both operations must be parked on the open lane the reclaim holds"
    );
    release_tx.send(()).unwrap();
    recovery.await.unwrap().unwrap();
    assert_eq!(mount.health().lifecycle, crate::MountLifecycleState::Ready);
    for (operation, result) in [
        (
            "read",
            tokio::time::timeout(Duration::from_secs(2), read).await,
        ),
        (
            "write",
            tokio::time::timeout(Duration::from_secs(2), write).await,
        ),
    ] {
        let error = result
            .expect("a queued operation stayed blocked after recovery")
            .expect("a queued operation panicked")
            .unwrap_err();
        let crate::NfsError::StateRecoveryExhausted(details) = &error else {
            panic!("queued {operation} bypassed the lost-lock gate: {error:?}");
        };
        assert_eq!(details.operation, operation);
        assert!(
            matches!(details.source.as_ref(), crate::NfsError::LockDenied { .. }),
            "the gate must report the lost lock, got {error:?}"
        );
    }
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn open_rejected_during_reconnect_returns_the_granted_delegation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mux = rpc::StreamMux::connect(addr, true).await.unwrap();
    let rpc = rpc::Client::new(mux, None);
    let callback = CallbackService::bind_for(addr).await.unwrap();
    let callback_state = callback.state();
    let recalls = callback.take_recall_receiver().unwrap();
    let worker = CallbackWorker::start(
        recalls,
        rpc.clone(),
        Auth::new_null(),
        Arc::clone(&callback_state),
    );
    let lease = LeaseState::ready(1, 60);
    let lease_for_server = Arc::clone(&lease);
    let mount = Arc::new(Mount40 {
        rpc,
        auth: Auth::new_null(),
        root_fh: Bytes::from_static(b"root"),
        client_id: Arc::new(AtomicU64::new(7)),
        issuer: 9,
        next_owner: AtomicU64::new(1),
        state: Arc::new(OpenState::default()),
        locks: Arc::new(LockState::default()),
        lease,
        _renewal: None,
        _callback: Some(callback),
        _callback_worker: Some(worker),
        callback_state: Some(Arc::clone(&callback_state)),
        dircount: 8192,
        maxcount: 32768,
        rsize: 1_048_576,
        wsize: 1_048_576,
        acl_supported: false,
    });
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let open = read_record(&mut stream).await?;
        // The mount leaves Ready after the OPEN was sent but before it is
        // published, so finish_stateful fails without changing the generation.
        lease_for_server.mark_reconnecting();
        reply(
            &mut stream,
            &open,
            &open_result_with_read_delegation([0x61; 16], [0x71; 16], b"fh"),
        )
        .await?;
        let returned = tokio::time::timeout(Duration::from_secs(2), read_record(&mut stream))
            .await
            .expect("the failed OPEN must return its delegation")?;
        assert!(
            returned.windows(11).any(|part| part == b"delegreturn"),
            "the OPEN error path must DELEGRETURN instead of leaking the grant"
        );
        reply(
            &mut stream,
            &returned,
            &compound_result("delegreturn", &[(22, &[]), (8, &[])]),
        )
        .await
    });
    let error = mount
        .open(Bytes::from_static(b"root"), "file", crate::OPEN_BOTH)
        .await
        .unwrap_err();
    assert!(
        matches!(error, NfsError::OperationOutcome(_)),
        "a lifecycle-gated OPEN reports an uncertain outcome, got {error:?}"
    );
    assert!(
        mount
            .state
            .for_fh(&Bytes::from_static(b"fh"), crate::OPEN_BOTH)
            .await
            .is_some(),
        "a Reconnecting mount keeps the open it just published"
    );
    assert_eq!(
        callback_state.stats().grants_received,
        0,
        "an OPEN that never reached the caller must not register its delegation"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn lock_on_a_closing_lane_does_not_reach_the_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 42, b"fh").await;
    mount.state.by_owner(42).await.unwrap().lock().await.closing = true;
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        mount.lock(Bytes::from_static(b"fh"), 2, 0, 1),
    )
    .await
    .expect("LOCK must fail locally instead of waiting for a server reply");
    match result {
        Err(NfsError::InvalidInput(message)) => {
            assert!(
                message.contains("closing or closed"),
                "unexpected close error: {message}"
            );
        }
        other => panic!("unexpected LOCK result: {other:?}"),
    }
}

#[tokio::test]
async fn zero_lock_stateid_is_still_released_before_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 43, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &lock,
            &compound_result("lock", &[(22, &[]), (12, &[0; 16])]),
        )
        .await?;
        let unlock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &unlock,
            &compound_result("locku", &[(22, &[]), (14, &[0; 16])]),
        )
        .await?;
        let cleanup = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &cleanup,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    let _token = mount
        .lock_stateful(Bytes::from_static(b"fh"), 2, 0, 1)
        .await
        .unwrap();
    mount
        .release_locks(Bytes::from_static(b"fh"))
        .await
        .unwrap();
    assert!(
        !mount.locks.has_fh(&Bytes::from_static(b"fh")).await,
        "an all-zero lock stateid must still be released, otherwise close() stays wedged"
    );
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn zero_lock_stateid_rejected_by_the_server_still_unblocks_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 44, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &lock,
            &compound_result("lock", &[(22, &[]), (12, &[0; 16])]),
        )
        .await?;
        // A conforming server rejects the reserved all-zero stateid. Its seqid
        // is 0, so there is no "previous seqid" probe: the rejection itself is
        // taken as proof the lock is already gone.
        let unlock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &unlock,
            &failed_lock_result("locku", 14, 10025),
        )
        .await?;
        let cleanup = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &cleanup,
            &compound_result("release-lockowner", &[(39, &[])]),
        )
        .await
    });
    let _token = mount
        .lock_stateful(Bytes::from_static(b"fh"), 2, 0, 1)
        .await
        .unwrap();
    mount
        .release_locks(Bytes::from_static(b"fh"))
        .await
        .unwrap();
    assert!(
        !mount.locks.has_fh(&Bytes::from_static(b"fh")).await,
        "a rejected zero stateid must still retire the lane so close() can proceed"
    );
    server.await.unwrap().unwrap();
}

/// Rows 1 and 7 of `docs/nfs40-recovery-invariants.md`: a covering LOCK is the
/// way out of the gate, and LOCK itself is never blocked by it.
#[tokio::test]
async fn a_covering_lock_lifts_the_gate_and_io_resumes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 97, b"fh").await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lock = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &lock,
            &succeeded_lock_result("lock", 0, [0xd3; 16]),
        )
        .await?;
        let read = read_record(&mut stream).await?;
        reply(&mut stream, &read, &read_result(b"x")).await
    });

    let gated = mount
        .read(Bytes::from_static(b"fh"), 0, 1)
        .await
        .unwrap_err();
    assert!(
        matches!(gated, crate::NfsError::StateRecoveryExhausted(_)),
        "the handle must be gated before the lock, got {gated:?}"
    );
    mount
        .lock(Bytes::from_static(b"fh"), 2, 0, u64::MAX)
        .await
        .unwrap();
    assert!(
        mount.locks.lost(b"fh").await.is_none(),
        "a covering lock must lift the gate"
    );
    let data = mount.read(Bytes::from_static(b"fh"), 0, 1).await.unwrap();
    assert_eq!(data, Bytes::from_static(b"x"));
    server.await.unwrap().unwrap();
}

/// Row 8: releasing a lock cannot lift the gate, otherwise the release itself
/// would become a way to bypass the lost protection.
#[tokio::test]
async fn locku_does_not_clear_the_gate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 98, b"fh").await;
    register_scripted_lock_range(&mount, 98, [0xd4; 16], 1, 8, 8).await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0xd4; 16])]),
        )
        .await
    });

    mount
        .locku(
            Bytes::from_static(b"fh"),
            Bytes::copy_from_slice(&[0xd4; 16]),
            1,
            8,
            8,
        )
        .await
        .unwrap();
    assert!(
        mount.locks.lost(b"fh").await.is_some(),
        "LOCKU must not lift the gate"
    );
    server.await.unwrap().unwrap();
}

/// Row 9: COMMIT carries no stateid in NFSv4.0 and only flushes data that was
/// already written, so a lost lock must not block it.
#[tokio::test]
async fn commit_is_not_gated_by_a_lost_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 99, b"fh").await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let commit = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &commit,
            &compound_result("commit", &[(22, &[]), (5, &[0x33; 8])]),
        )
        .await
    });

    mount
        .commit(Bytes::from_static(b"fh"), 0, 1)
        .await
        .expect("COMMIT must not be gated by a lost lock");
    server.await.unwrap().unwrap();
}

/// Row 11: LOCKT installs no state, so the gate must not stop it; a server
/// denial proves the request actually left the client.
#[tokio::test]
async fn lockt_is_not_gated_by_a_lost_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 100, b"fh").await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let lockt = read_record(&mut stream).await?;
        reply(&mut stream, &lockt, &denied_lock_result("lockt", 13)).await
    });

    let error = mount
        .lock_test(Bytes::from_static(b"fh"), 2, 0, 1)
        .await
        .unwrap_err();
    assert!(
        matches!(error, crate::NfsError::LockDenied { .. }),
        "LOCKT must reach the server instead of being gated locally, got {error:?}"
    );
    server.await.unwrap().unwrap();
}

/// Zero-length READ short-circuits before any lookup, the gate or the wire.
#[tokio::test]
async fn a_zero_length_read_short_circuits_before_the_gate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 101, b"fh").await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), read_record(&mut stream))
                .await
                .is_err(),
            "a zero-length READ must not reach the server"
        );
        Ok::<(), std::io::Error>(())
    });

    let data = mount.read(Bytes::from_static(b"fh"), 0, 0).await.unwrap();
    assert!(data.is_empty());
    server.await.unwrap().unwrap();
}

/// The gate is cleared by CLOSE even when the open lane still exists, i.e. the
/// loss came from a sibling owner on the same handle.
#[tokio::test]
async fn close_with_a_surviving_lane_lifts_the_gate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 102, b"fh").await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let close = read_record(&mut stream).await?;
        reply(&mut stream, &close, &close_result([0x9e; 16])).await
    });

    mount.close(Bytes::from_static(b"fh")).await.unwrap();
    assert!(
        mount.locks.lost(b"fh").await.is_none(),
        "CLOSE must lift the gate once no open lane is left"
    );
    server.await.unwrap().unwrap();
}

/// Row 12: metadata has no stateid attached, so the gate must not stop it.
#[tokio::test]
async fn metadata_is_not_gated_by_a_lost_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 103, b"fh").await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let getattr = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &getattr,
            &compound_result("getattr", &[(22, &[]), (9, &[0; 8])]),
        )
        .await
    });

    mount
        .getattr(Bytes::from_static(b"fh"))
        .await
        .expect("GETATTR must not be gated by a lost lock");
    server.await.unwrap().unwrap();
}

/// Tearing down lanes is not one of the gate-clearing events: only CLOSE,
/// a covering LOCK and a session-wide loss are.
#[tokio::test]
async fn release_locks_does_not_lift_the_gate() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mount = connected_direct_mount(&listener).await;
    register_scripted_open(&mount, 104, b"fh").await;
    register_scripted_lock_range(&mount, 104, [0xd5; 16], 2, 0, u64::MAX).await;
    publish_lost_whole_file(&mount, b"fh").await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let locku = read_record(&mut stream).await?;
        reply(
            &mut stream,
            &locku,
            &compound_result("locku", &[(22, &[]), (14, &[0xd5; 16])]),
        )
        .await
    });

    mount
        .release_locks(Bytes::from_static(b"fh"))
        .await
        .unwrap();
    assert!(
        mount.locks.lost(b"fh").await.is_some(),
        "releasing lanes must not lift the gate"
    );
    server.await.unwrap().unwrap();
}
