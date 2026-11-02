#![cfg(test)]
//! RFC 7862/8276 production-wire and ownership regressions.
use super::test_support::{Reply, ok, opaque, putfh, serve_v42};
use super::v42_ops::{clip_read_plus, decode_read_plus};
use crate::nfs4::compound::{take_opaque, xdr_opaque, xdr_u32};
use crate::{Mount, ReadPlusContent, SeekContent, XattrSetMode};
use bytes::{Buf, Bytes};
use std::sync::{Arc, Mutex};

fn u64wire(buf: &mut Vec<u8>, v: u64) {
    buf.extend(v.to_be_bytes());
}
fn attrs(mask: &[u32], values: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    xdr_u32(&mut data, mask.len() as u32);
    for word in mask {
        xdr_u32(&mut data, *word);
    }
    xdr_opaque(&mut data, values);
    data
}
fn support(args: &mut Bytes) -> Vec<Reply> {
    putfh(args);
    assert_eq!(args.get_u32(), 9);
    let n = args.get_u32();
    let mask: Vec<_> = (0..n).map(|_| args.get_u32()).collect();
    let data = if mask == [1] {
        let mut value = vec![];
        for word in [3, 1, 0, 1 << 18] {
            xdr_u32(&mut value, word);
        }
        attrs(&[1], &value)
    } else {
        assert_eq!(mask, [0, 0, 1 << 18]);
        attrs(&[0, 0, 1 << 18], &1u32.to_be_bytes())
    };
    vec![ok(22, vec![]), ok(9, data)]
}
fn change() -> Vec<u8> {
    let mut data = vec![];
    xdr_u32(&mut data, 1);
    u64wire(&mut data, 1);
    u64wire(&mut data, 2);
    data
}

async fn open(m: &super::mount::Mount41, fh: &'static [u8]) {
    m.state
        .register_open(
            &Bytes::from_static(fh),
            super::state::StateId::from_bytes_at(&[7; 16], 1),
            super::state::AccessMode::Both,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn xattrs_use_rfc8276_atomic_modes_and_paginate_opaque_cookies() {
    let seen = Arc::new(Mutex::new(vec![]));
    let observed = seen.clone();
    let (mount, server) = serve_v42(move |tag, mut args| {
        if tag == "v42_attrs" {
            return support(&mut args);
        }
        putfh(&mut args);
        let opcode = args.get_u32();
        observed.lock().unwrap().push(opcode);
        let data = match tag {
            "setxattr42" => {
                assert_eq!(opcode, 73);
                assert_eq!(args.get_u32(), 1);
                assert_eq!(take_opaque(&mut args, "key").unwrap(), "user.probe");
                assert_eq!(take_opaque(&mut args, "value").unwrap(), b"\0a\xff"[..]);
                change()
            }
            "getxattr42" => {
                assert_eq!(opcode, 72);
                opaque(b"\0a\xff")
            }
            "removexattr42" => {
                assert_eq!(opcode, 75);
                change()
            }
            "listxattrs42" => {
                assert_eq!(opcode, 74);
                let cookie = args.get_u64();
                assert_eq!(args.get_u32(), 4096);
                let mut data = vec![];
                u64wire(&mut data, if cookie == 0 { 99 } else { 7 });
                xdr_u32(&mut data, 1);
                xdr_opaque(
                    &mut data,
                    if cookie == 0 {
                        b"user.a"
                    } else {
                        assert_eq!(cookie, 99);
                        b"user.b"
                    },
                );
                xdr_u32(&mut data, u32::from(cookie != 0));
                data
            }
            _ => panic!("unexpected {tag}"),
        };
        vec![ok(22, vec![]), ok(opcode, data)]
    })
    .await;
    let mount = super::mount::test_wrapper(mount);
    mount
        .setxattr_with_mode(
            Bytes::from_static(b"file"),
            "user.probe",
            Bytes::from_static(b"\0a\xff"),
            XattrSetMode::Create,
        )
        .await
        .unwrap();
    assert_eq!(
        mount
            .getxattr(Bytes::from_static(b"file"), "user.probe")
            .await
            .unwrap(),
        b"\0a\xff"[..]
    );
    assert_eq!(
        mount.listxattr(Bytes::from_static(b"file")).await.unwrap(),
        ["user.a", "user.b"]
    );
    mount
        .removexattr(Bytes::from_static(b"file"), "user.probe")
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), [73, 72, 74, 74, 75]);
    server.abort();
}

#[tokio::test]
async fn space_seek_read_plus_and_clone_use_valid_open_state_and_v42_opcodes() {
    let (mount, server) = serve_v42(|tag, mut args| {
        putfh(&mut args);
        if tag == "clone" {
            assert_eq!(args.get_u32(), 32);
            assert_eq!(args.get_u32(), 22);
            take_opaque(&mut args, "dst").unwrap();
            assert_eq!(args.get_u32(), 71);
            assert_eq!(args.split_to(16), [7; 16][..]);
            assert_eq!(args.split_to(16), [7; 16][..]);
            assert_eq!(args.get_u64(), 0);
            assert_eq!(args.get_u64(), 4096);
            assert_eq!(args.get_u64(), 4096);
            assert!(args.is_empty());
            return vec![
                ok(22, vec![]),
                ok(32, vec![]),
                ok(22, vec![]),
                ok(71, vec![]),
            ];
        }
        let op = args.get_u32();
        assert_eq!(args.split_to(16), [7; 16][..]);
        let offset = args.get_u64();
        let data = match tag {
            "allocate" | "deallocate" => {
                assert_eq!(op, if tag == "allocate" { 59 } else { 62 });
                assert_eq!(offset, 1024);
                assert_eq!(args.get_u64(), 4096);
                vec![]
            }
            "seek" => {
                assert_eq!(op, 69);
                assert_eq!(offset, 100);
                assert_eq!(args.get_u32(), 1);
                let mut d = vec![];
                xdr_u32(&mut d, 0);
                u64wire(&mut d, 4096);
                d
            }
            "read_plus" => {
                assert_eq!(op, 68);
                assert_eq!(offset, 0);
                assert_eq!(args.get_u32(), 100);
                let mut d = vec![];
                xdr_u32(&mut d, 0);
                xdr_u32(&mut d, 1);
                xdr_u32(&mut d, 1);
                u64wire(&mut d, 0);
                u64wire(&mut d, 100);
                d
            }
            _ => panic!("unexpected {tag}"),
        };
        vec![ok(22, vec![]), ok(op, data)]
    })
    .await;
    open(&mount, b"src").await;
    open(&mount, b"dst").await;
    let mount = super::mount::test_wrapper(mount);
    mount
        .allocate(Bytes::from_static(b"dst"), 1024, 4096)
        .await
        .unwrap();
    mount
        .deallocate(Bytes::from_static(b"dst"), 1024, 4096)
        .await
        .unwrap();
    assert_eq!(
        mount
            .seek_content(Bytes::from_static(b"src"), 100, SeekContent::Hole)
            .await
            .unwrap()
            .offset,
        4096
    );
    assert_eq!(
        mount
            .read_plus(Bytes::from_static(b"src"), 0, 100)
            .await
            .unwrap()
            .contents,
        [ReadPlusContent::Hole {
            offset: 0,
            length: 100
        }]
    );
    mount
        .clone_range(
            Bytes::from_static(b"src"),
            Bytes::from_static(b"dst"),
            0,
            4096,
            4096,
        )
        .await
        .unwrap();
    server.abort();
}

fn copy_reply(callback: bool, committed: u32) -> Vec<u8> {
    let mut d = vec![];
    xdr_u32(&mut d, u32::from(callback));
    if callback {
        d.extend([8; 16]);
    }
    u64wire(&mut d, 4096);
    xdr_u32(&mut d, committed);
    d.extend([3; 8]);
    xdr_u32(&mut d, 1);
    xdr_u32(&mut d, u32::from(!callback));
    d
}
fn copy_request(args: &mut Bytes) {
    putfh(args);
    assert_eq!(args.get_u32(), 32);
    putfh(args);
    assert_eq!(args.get_u32(), 60);
    assert_eq!(args.split_to(16), [7; 16][..]);
    assert_eq!(args.split_to(16), [7; 16][..]);
    assert_eq!(args.get_u64(), 0);
    assert_eq!(args.get_u64(), 0);
    assert_eq!(args.get_u64(), 4096);
    assert_eq!(args.get_u32(), 1);
    assert_eq!(args.get_u32(), 1);
    assert_eq!(args.get_u32(), 0);
    assert!(args.is_empty());
}
fn copy_response(data: Vec<u8>) -> Vec<Reply> {
    vec![ok(22, vec![]), ok(32, vec![]), ok(22, vec![]), ok(60, data)]
}

#[tokio::test]
async fn synchronous_copy_commits_the_returned_verifier_before_success() {
    let (mount, server) = serve_v42(|tag, mut args| match tag {
        "copy" => {
            copy_request(&mut args);
            copy_response(copy_reply(false, 0))
        }
        "commit" => {
            putfh(&mut args);
            assert_eq!(args.get_u32(), 5);
            assert_eq!(args.get_u64(), 0);
            assert_eq!(args.get_u32(), 4096);
            vec![ok(22, vec![]), ok(5, vec![3; 8])]
        }
        _ => panic!("unexpected {tag}"),
    })
    .await;
    open(&mount, b"src").await;
    open(&mount, b"dst").await;
    let mount = super::mount::test_wrapper(mount);
    assert_eq!(
        mount
            .copy_range(
                Bytes::from_static(b"src"),
                Bytes::from_static(b"dst"),
                0,
                0,
                4096
            )
            .await
            .unwrap()
            .count,
        4096
    );
    server.abort();
}

#[tokio::test]
async fn unexpected_async_copy_is_cancelled_and_never_reported_as_complete() {
    let (mount, server) = serve_v42(|tag, mut args| match tag {
        "copy" => {
            copy_request(&mut args);
            copy_response(copy_reply(true, 2))
        }
        "offload_cancel" => {
            putfh(&mut args);
            assert_eq!(args.get_u32(), 66);
            assert_eq!(args.split_to(16), [8; 16][..]);
            vec![ok(22, vec![]), ok(66, vec![])]
        }
        _ => panic!("unexpected {tag}"),
    })
    .await;
    open(&mount, b"src").await;
    open(&mount, b"dst").await;
    let mount = super::mount::test_wrapper(mount);
    let e = mount
        .copy_range(
            Bytes::from_static(b"src"),
            Bytes::from_static(b"dst"),
            0,
            0,
            4096,
        )
        .await
        .unwrap_err();
    assert_eq!(
        e.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    server.abort();
}

#[tokio::test]
async fn cancelled_copy_retains_open_barriers_until_rpc_settles() {
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered = Some(entered_tx);
    let (mount, server) = super::test_support::serve_v42_with_reply_gate(
        move |tag, mut args| match tag {
            "copy" => {
                copy_request(&mut args);
                entered.take().unwrap().send(()).unwrap();
                copy_response(copy_reply(false, 2))
            }
            "close" => {
                vec![ok(22, vec![]), ok(4, vec![7; 16])]
            }
            _ => panic!("unexpected {tag}"),
        },
        release_rx,
    )
    .await;
    open(&mount, b"src").await;
    open(&mount, b"dst").await;
    let mount = Arc::new(super::mount::test_wrapper(mount));
    let copy_mount = mount.clone();
    let copy = tokio::spawn(async move {
        copy_mount
            .copy_range(
                Bytes::from_static(b"src"),
                Bytes::from_static(b"dst"),
                0,
                0,
                4096,
            )
            .await
    });
    entered_rx.await.unwrap();
    copy.abort();
    let _ = copy.await;
    let close_mount = mount.clone();
    let close = tokio::spawn(async move { close_mount.close(Bytes::from_static(b"dst")).await });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(!close.is_finished());
    release_tx.send(()).unwrap();
    close.await.unwrap().unwrap();
    server.abort();
}

#[test]
fn read_plus_clips_expanded_holes_without_allocating_and_preserves_eof() {
    let reply = crate::ReadPlusResult {
        eof: true,
        contents: vec![ReadPlusContent::Hole {
            offset: 0,
            length: 1 << 40,
        }],
    };
    let clipped = clip_read_plus(reply, 4096, 1024).unwrap();
    assert!(!clipped.eof);
    assert_eq!(
        clipped.contents,
        [ReadPlusContent::Hole {
            offset: 4096,
            length: 1024
        }]
    );
    for content in [
        vec![],
        vec![ReadPlusContent::Hole {
            offset: 1,
            length: 1,
        }],
        vec![ReadPlusContent::Hole {
            offset: 0,
            length: 0,
        }],
        vec![
            ReadPlusContent::Hole {
                offset: 0,
                length: 2,
            },
            ReadPlusContent::Hole {
                offset: 1,
                length: 1,
            },
        ],
    ] {
        assert!(
            clip_read_plus(
                crate::ReadPlusResult {
                    eof: false,
                    contents: content
                },
                0,
                10
            )
            .is_err()
        );
    }
    let mut valid = vec![];
    for word in [1, 1, 0] {
        xdr_u32(&mut valid, word);
    }
    u64wire(&mut valid, 0);
    xdr_opaque(&mut valid, b"abc");
    for length in 0..valid.len() {
        assert!(decode_read_plus(&mut Bytes::copy_from_slice(&valid[..length])).is_err());
    }
    assert_eq!(
        decode_read_plus(&mut Bytes::from(valid)).unwrap().contents,
        [ReadPlusContent::Data {
            offset: 0,
            data: Bytes::from_static(b"abc")
        }]
    );
}

#[test]
fn copy_error_arm_and_new_statuses_decode_without_desynchronizing() {
    let mut response = vec![];
    for word in [10094, 0, 1, 60, 10094, 1, 0] {
        xdr_u32(&mut response, word);
    }
    let parsed = super::compound::CompoundResponse::decode(Bytes::from(response)).unwrap();
    assert!(matches!(
        parsed.check_status(),
        Err(crate::NfsError::Nfs4(
            crate::nfs4::Nfs4ErrorCode::NFS4ERR_OFFLOAD_NO_REQS
        ))
    ));
    for code in [10088, 10089, 10090, 10091, 10092, 10093, 10095, 10096] {
        let mut response = vec![];
        for word in [code, 0, 1, 72, code] {
            xdr_u32(&mut response, word);
        }
        assert!(
            super::compound::CompoundResponse::decode(Bytes::from(response))
                .unwrap()
                .check_status()
                .is_err()
        );
    }
}

#[tokio::test]
async fn xattr_cookie_cycles_and_invalid_file_ranges_fail_closed() {
    let (mount, server) = serve_v42(|tag, mut args| {
        if tag == "v42_attrs" {
            return support(&mut args);
        }
        putfh(&mut args);
        assert_eq!(args.get_u32(), 74);
        let mut d = vec![];
        u64wire(&mut d, 0);
        xdr_u32(&mut d, 1);
        xdr_opaque(&mut d, b"user.a");
        xdr_u32(&mut d, 0);
        vec![ok(22, vec![]), ok(74, d)]
    })
    .await;
    let mount = super::mount::test_wrapper(mount);
    assert!(matches!(
        mount.listxattr(Bytes::from_static(b"file")).await,
        Err(crate::NfsError::Xdr(_))
    ));
    assert!(
        mount
            .allocate(Bytes::from_static(b"file"), u64::MAX, 1)
            .await
            .is_err()
    );
    assert!(
        mount
            .copy_range(
                Bytes::from_static(b"file"),
                Bytes::from_static(b"file"),
                0,
                0,
                1
            )
            .await
            .is_err()
    );
    server.abort();
}

#[tokio::test]
async fn unadvertised_v42_acl_is_unsupported_and_not_malformed_xdr() {
    let (mount, server) = serve_v42(|tag, mut args| {
        assert_eq!(tag, "v42_attrs");
        support(&mut args)
    })
    .await;
    let mount = super::mount::test_wrapper(mount);
    let fh = Bytes::from_static(b"file");
    assert!(matches!(
        mount.getacl(fh.clone()).await,
        Err(crate::NfsError::Unsupported(_))
    ));
    assert!(matches!(
        mount.aclsupport(fh.clone()).await,
        Err(crate::NfsError::Unsupported(_))
    ));
    assert!(matches!(
        mount.getdacl(fh.clone()).await,
        Err(crate::NfsError::Unsupported(_))
    ));
    assert!(matches!(
        mount.getsacl(fh).await,
        Err(crate::NfsError::Unsupported(_))
    ));
    server.abort();
}

#[tokio::test]
async fn rejected_oversized_xattr_never_fences_an_unsent_session_slot() {
    let (mount, server) = serve_v42(|tag, mut args| {
        if tag == "v42_attrs" {
            return support(&mut args);
        }
        assert_eq!(tag, "getxattr42");
        vec![ok(22, vec![]), ok(72, opaque(b"ok"))]
    })
    .await;
    let mount = super::mount::test_wrapper(mount);
    let fh = Bytes::from_static(b"file");
    assert!(
        mount
            .setxattr_with_mode(
                fh.clone(),
                "user.large",
                Bytes::from(vec![0; 8192]),
                XattrSetMode::Either
            )
            .await
            .is_err()
    );
    assert_eq!(mount.getxattr(fh, "user.small").await.unwrap(), b"ok"[..]);
    server.abort();
}

#[test]
fn missing_rfc8276_key_is_a_not_found_error() {
    assert!(crate::NfsError::Nfs4(crate::nfs4::Nfs4ErrorCode::NFS4ERR_NOXATTR).is_not_found());
}

#[tokio::test]
async fn cancelled_queued_mutation_keeps_uncertainty_before_releasing_barrier() {
    let (mount, server) = serve_v42(|_, _| panic!("no RPC expected")).await;
    let mount = Arc::new(mount);
    let core = crate::client_core::ClientCore::new(mount.clone());
    let destination = Bytes::from_static(b"destination");
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut operation = Box::pin(super::v42_ops::run_owned::<(), _, _>(
        mount.clone(),
        core,
        None,
        destination.clone(),
        move |mount| async move {
            let _ = entered_tx.send(());
            Err(mount.mutation_error("copy", crate::NfsError::Rpc("reply lost".into())))
        },
    ));
    assert!(futures::poll!(operation.as_mut()).is_pending());
    // The current-thread executor queues the result before waking this waiter.
    entered_rx.await.unwrap();
    drop(operation);
    let _barrier = mount.writes.barrier(&destination).await;
    let error = mount
        .writes
        .error(&destination)
        .expect("lost queued uncertainty");
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    server.abort();
}

#[tokio::test]
async fn panicked_mutation_publishes_uncertainty_before_releasing_barrier() {
    let (mount, server) = serve_v42(|_, _| panic!("no RPC expected")).await;
    let mount = Arc::new(mount);
    let core = crate::client_core::ClientCore::new(mount.clone());
    let destination = Bytes::from_static(b"destination");
    let error = super::v42_ops::run_owned::<(), _, _>(
        mount.clone(),
        core,
        None,
        destination.clone(),
        |_| async move {
            panic!("injected mutation task failure");
        },
    )
    .await
    .unwrap_err();
    assert_eq!(
        error
            .operation_outcome()
            .expect("task failure must be uncertain")
            .outcome,
        crate::OperationOutcome::Uncertain
    );
    let _barrier = mount.writes.barrier(&destination).await;
    assert!(mount.writes.error(&destination).is_some());
    server.abort();
}

#[test]
fn copy_requires_reply_cache_capacity_and_sets_cachethis() {
    use super::compound::CompoundBuilder;
    let copy = || {
        CompoundBuilder::new("t")
            .sequence(&[1; 16], 1, 0, 0)
            .putfh(b"src")
            .savefh()
            .putfh(b"dst")
            .copy_range(false, &[7; 16], &[7; 16], 0, 0, 4096)
    };
    assert!(copy().apply_sequence_cache_policy(147).is_err());
    let builder = copy().apply_sequence_cache_policy(148).unwrap();
    let mut wire = vec![];
    builder.encode_with_header(
        &crate::rpc::auth::Auth::new_null(),
        super::MinorVersion::V2,
        &mut wire,
    );
    let mut wire = Bytes::from(wire);
    wire.advance(16);
    for _ in 0..2 {
        wire.advance(4);
        take_opaque(&mut wire, "auth").unwrap();
    }
    take_opaque(&mut wire, "tag").unwrap();
    assert_eq!(wire.get_u32(), 2);
    assert_eq!(wire.get_u32(), 5);
    assert_eq!(wire.get_u32(), 53);
    wire.advance(28);
    assert_eq!(wire.get_u32(), 1);
}

#[tokio::test]
async fn copy_verifier_change_is_uncertain_and_does_not_repeat_the_copy() {
    let copies = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = copies.clone();
    let (mount, server) = serve_v42(move |tag, mut args| match tag {
        "copy" => {
            observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            copy_request(&mut args);
            copy_response(copy_reply(false, 0))
        }
        "commit" => vec![ok(22, vec![]), ok(5, vec![4; 8])],
        _ => panic!("unexpected {tag}"),
    })
    .await;
    open(&mount, b"src").await;
    open(&mount, b"dst").await;
    let mount = super::mount::test_wrapper(mount);
    let error = mount
        .copy_range(
            Bytes::from_static(b"src"),
            Bytes::from_static(b"dst"),
            0,
            0,
            4096,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    assert_eq!(
        error.operation_outcome().unwrap().context().protocol,
        crate::NFSVersion::NFSv4p2
    );
    assert_eq!(copies.load(std::sync::atomic::Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn v42_mutation_session_loss_is_uncertain_without_fresh_session_retry() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = calls.clone();
    let (mount, server) = serve_v42(move |tag, mut args| {
        assert_eq!(tag, "copy");
        copy_request(&mut args);
        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        vec![Reply {
            opcode: 53,
            status: 10052,
            data: vec![],
        }]
    })
    .await;
    open(&mount, b"src").await;
    open(&mount, b"dst").await;
    let mount = super::mount::test_wrapper(mount);
    let error = mount
        .copy_range(
            Bytes::from_static(b"src"),
            Bytes::from_static(b"dst"),
            0,
            0,
            4096,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    assert_eq!(
        error.operation_outcome().unwrap().recovery,
        crate::RecoveryAction::VerifyThenResume
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn default_dot_path_resolves_mount_root_without_invalid_lookup() {
    let (mount, server) = serve_v42(|tag, _| panic!("root path must not send LOOKUP: {tag}")).await;
    for path in ["", ".", "/", "./", "/./"] {
        assert_eq!(
            mount.lookup_path(path).await.unwrap().fh,
            Bytes::from_static(b"root")
        );
    }
    server.abort();
}
