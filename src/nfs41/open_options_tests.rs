#![cfg(test)]
//! Stateful create/truncate wire contracts (RFC 8881 §18.16.3).
use super::test_support::{Reply, ok, opaque, putfh, serve};
use crate::nfs4::compound::take_opaque;
use crate::{Mount, OPEN_WRITE, OpenOptions};
use bytes::{Buf, Bytes};
use std::sync::{Arc, Mutex};

fn attrs() -> Vec<u8> {
    // Empty attribute envelope; these tests observe requests and state ownership.
    vec![0; 8]
}

fn opened() -> Vec<Reply> {
    let mut data = vec![7; 16]; // stateid
    data.extend_from_slice(&[0; 20]); // change_info
    data.extend_from_slice(&[0; 12]); // flags, attrset, no delegation
    vec![
        ok(22, vec![]),
        ok(18, data),
        ok(10, opaque(b"file")),
        ok(9, attrs()),
    ]
}

fn check_open(args: &mut Bytes, create: bool) {
    putfh(args);
    assert_eq!(args.get_u32(), 18);
    assert_eq!(args.get_u32(), 0); // seqid
    assert_eq!(args.get_u32() & 3, OPEN_WRITE);
    assert_eq!(args.get_u32(), 0); // deny
    args.advance(8); // clientid
    take_opaque(args, "owner").unwrap();
    assert_eq!(args.get_u32(), u32::from(create));
    if create {
        assert_eq!(
            args.get_u32(),
            1,
            "GUARDED4 must reject competing creations"
        );
        assert_eq!(args.get_u32(), 2); // mode is attribute 33
        assert_eq!(args.get_u32(), 0);
        assert_eq!(args.get_u32(), 2);
        assert_eq!(
            take_opaque(args, "attributes").unwrap().as_ref(),
            &0o666u32.to_be_bytes()
        );
    }
    assert_eq!(args.get_u32(), 0); // CLAIM_NULL
    assert_eq!(take_opaque(args, "name").unwrap().as_ref(), b"new");
}

#[tokio::test]
async fn new_file_open_reuses_parent_and_creates_guarded_without_setattr() {
    let seen = Arc::new(Mutex::new(vec![]));
    let recorded = seen.clone();
    let (mount, server) = serve(move |tag, mut args| {
        recorded.lock().unwrap().push(tag.to_string());
        match tag {
            "lookup_path" => {
                assert_eq!(recorded.lock().unwrap().len(), 1, "parent resolved twice");
                vec![
                    ok(22, vec![]),
                    ok(15, vec![]),
                    ok(10, opaque(b"parent")),
                    ok(9, attrs()),
                ]
            }
            "open" => {
                check_open(&mut args, false);
                vec![
                    ok(22, vec![]),
                    Reply {
                        opcode: 18,
                        status: 2,
                        data: vec![],
                    },
                ]
            }
            "create_guarded" => {
                check_open(&mut args, true);
                opened()
            }
            "close" => vec![ok(22, vec![]), ok(4, vec![8; 16])],
            _ => panic!("unexpected RPC {tag}; fresh creates must not SETATTR"),
        }
    })
    .await;
    let mount = super::mount::test_wrapper(mount);
    let file = mount
        .open_path_with_options(
            "parent/new",
            OpenOptions {
                access: OPEN_WRITE,
                create_mode: Some(0o666),
                truncate: true,
            },
        )
        .await
        .unwrap();
    mount.close_stateful(file).await.unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        ["lookup_path", "open", "create_guarded", "close"]
    );
    server.abort();
}

#[tokio::test]
async fn guarded_create_returns_unsolicited_delegations_even_when_getattr_fails() {
    // RFC 8881 §18.16.2: delegation follows stateid, change_info, flags, attrset.
    for delegation_type in [1u32, 2] {
        for fail_getattr in [false, true] {
            let (mut mount, server) = serve(move |tag, mut args| match tag {
                "create_guarded" => {
                    check_open(&mut args, true);
                    let mut replies = opened();
                    let mut data = vec![7; 16]; // OPEN stateid, distinct from delegation
                    data.extend_from_slice(&1u32.to_be_bytes()); // atomic change_info
                    data.extend_from_slice(&100u64.to_be_bytes()); // before
                    data.extend_from_slice(&101u64.to_be_bytes()); // after
                    data.extend_from_slice(&[0; 8]); // flags, empty attrset
                    data.extend_from_slice(&delegation_type.to_be_bytes());
                    data.extend_from_slice(&[9; 16]); // delegation stateid
                    data.extend_from_slice(&0u32.to_be_bytes()); // recall=false
                    if delegation_type == 2 {
                        data.extend_from_slice(&1u32.to_be_bytes()); // NFS_LIMIT_SIZE
                        data.extend_from_slice(&4096u64.to_be_bytes());
                    }
                    data.extend_from_slice(&[0; 12]); // ACE type, flags, access mask
                    data.extend(opaque(b"OWNER@"));
                    replies[1].data = data;
                    if fail_getattr {
                        replies[3] = Reply {
                            opcode: 9,
                            status: 10008, // DELAY must not replay the successful OPEN
                            data: vec![],
                        };
                    }
                    replies
                }
                "close" => vec![ok(22, vec![]), ok(4, vec![8; 16])],
                _ => panic!("unexpected RPC {tag}"),
            })
            .await;
            let (recall_tx, mut recalls) = tokio::sync::mpsc::channel(1);
            mount.recall_tx = recall_tx;
            let result = mount
                .create_guarded(
                    Bytes::from_static(b"parent"),
                    "new",
                    Some(0o666),
                    OPEN_WRITE,
                )
                .await;
            if fail_getattr {
                assert!(result.is_err());
            } else {
                mount
                    .close_file(result.unwrap().file_handle())
                    .await
                    .unwrap();
            }
            match recalls
                .try_recv()
                .expect("unsolicited delegation must be returned")
            {
                super::callback::RecallNotification::Delegation {
                    stateid,
                    truncate,
                    fh,
                } => {
                    assert_eq!(stateid, [9; 16]);
                    assert!(!truncate);
                    assert_eq!(fh.as_ref(), b"file");
                }
                _ => panic!("expected delegation return"),
            }
            server.abort();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Case {
    Existing,
    Race,
    AttributesRejected,
    AttributesRejectedRace,
    GetattrFailed,
    GetfhFailed,
    TruncateFailed,
}

async fn exercise(case: Case, truncate: bool) -> (crate::Result<()>, Vec<String>) {
    let seen = Arc::new(Mutex::new(vec![]));
    let recorded = seen.clone();
    let mut opens = 0;
    let mut creates = 0;
    let (mount, server) = serve(move |tag, mut args| {
        recorded.lock().unwrap().push(tag.to_string());
        let failed = |opcode, status| Reply {
            opcode,
            status,
            data: vec![],
        };
        match tag {
            "lookup_path" => {
                assert_eq!(recorded.lock().unwrap().len(), 1, "parent resolved twice");
                vec![
                    ok(22, vec![]),
                    ok(15, vec![]),
                    ok(10, opaque(b"parent")),
                    ok(9, attrs()),
                ]
            }
            "open" => {
                opens += 1;
                check_open(&mut args, false);
                if opens == 1 && !matches!(case, Case::Existing | Case::TruncateFailed) {
                    vec![ok(22, vec![]), failed(18, 2)]
                } else {
                    opened()
                }
            }
            "create_guarded" => {
                creates += 1;
                if creates == 1 {
                    check_open(&mut args, true);
                } else {
                    // Attribute fallback must still encode GUARDED4, with no attrs.
                    putfh(&mut args);
                    assert_eq!(args.get_u32(), 18);
                    args.advance(20); // seqid, access, deny, clientid
                    take_opaque(&mut args, "owner").unwrap();
                    assert_eq!(args.get_u32(), 1);
                    assert_eq!(args.get_u32(), 1);
                    assert_eq!(args.get_u32(), 0);
                    assert!(take_opaque(&mut args, "attributes").unwrap().is_empty());
                }
                if creates == 1
                    && matches!(
                        case,
                        Case::AttributesRejected | Case::AttributesRejectedRace
                    )
                {
                    vec![ok(22, vec![]), failed(18, 10032)] // ATTRNOTSUPP
                } else if matches!(case, Case::Race | Case::AttributesRejectedRace) {
                    vec![ok(22, vec![]), failed(18, 17)]
                } else if case == Case::GetattrFailed {
                    let mut replies = opened();
                    replies[3] = failed(9, 10008); // DELAY after OPEN must not replay creation
                    replies
                } else if case == Case::GetfhFailed {
                    let mut replies = opened();
                    replies.truncate(3);
                    replies[2] = failed(10, 10032); // must not take attribute fallback
                    replies
                } else {
                    opened()
                }
            }
            "setattr" => {
                putfh(&mut args);
                assert_eq!(args.get_u32(), 34);
                args.advance(16); // stateid
                let words = args.get_u32();
                if case == Case::AttributesRejected {
                    assert_eq!(words, 2);
                    assert_eq!(args.get_u32(), 0);
                    assert_eq!(args.get_u32(), 2);
                    assert_eq!(
                        take_opaque(&mut args, "mode").unwrap().as_ref(),
                        &0o666u32.to_be_bytes()
                    );
                } else {
                    assert_eq!(words, 1, "existing file mode must be preserved");
                    assert_eq!(args.get_u32(), 1 << 4);
                    assert_eq!(take_opaque(&mut args, "size").unwrap().as_ref(), &[0; 8]);
                }
                if case == Case::TruncateFailed {
                    // SETATTR4res carries attrsset even on an error (RFC 8881 §18.30.2).
                    vec![
                        ok(22, vec![]),
                        Reply {
                            opcode: 34,
                            status: 13,
                            data: vec![0; 4],
                        },
                    ]
                } else {
                    vec![ok(22, vec![]), ok(34, vec![0; 4])]
                }
            }
            "close" => vec![ok(22, vec![]), ok(4, vec![8; 16])],
            _ => panic!("unexpected {tag} in {case:?}"),
        }
    })
    .await;
    let mount = super::mount::test_wrapper(mount);
    let result = match mount
        .open_path_with_options(
            "parent/new",
            OpenOptions {
                access: OPEN_WRITE,
                create_mode: Some(0o666),
                truncate,
            },
        )
        .await
    {
        Ok(file) => mount.close_stateful(file).await,
        Err(error) => Err(error),
    };
    server.abort();
    let requests = seen.lock().unwrap().clone();
    (result, requests)
}

#[tokio::test]
async fn existing_and_raced_files_truncate_without_changing_permissions() {
    for case in [Case::Existing, Case::Race, Case::AttributesRejectedRace] {
        let (result, seen) = exercise(case, true).await;
        result.unwrap();
        assert_eq!(seen.iter().filter(|tag| *tag == "lookup_path").count(), 1);
        assert_eq!(seen.iter().filter(|tag| *tag == "setattr").count(), 1);
        assert_eq!(seen.last().unwrap(), "close");
    }
}

#[tokio::test]
async fn append_open_never_truncates_an_existing_or_raced_target() {
    for case in [Case::Existing, Case::Race, Case::AttributesRejectedRace] {
        let (result, seen) = exercise(case, false).await;
        result.unwrap();
        assert!(!seen.iter().any(|tag| tag == "setattr"));
    }
}

#[tokio::test]
async fn rejected_create_attributes_fall_back_only_after_a_definite_open_rejection() {
    let (result, seen) = exercise(Case::AttributesRejected, true).await;
    result.unwrap();
    assert_eq!(
        seen,
        [
            "lookup_path",
            "open",
            "create_guarded",
            "create_guarded",
            "setattr",
            "close"
        ]
    );
    let (result, seen) = exercise(Case::GetfhFailed, true).await;
    assert_eq!(
        result.unwrap_err().operation_outcome().unwrap().outcome,
        crate::OperationOutcome::Uncertain
    );
    assert_eq!(seen, ["lookup_path", "open", "create_guarded"]);
}

#[tokio::test]
async fn failed_getattr_after_creation_and_failed_truncate_close_the_open() {
    for case in [Case::GetattrFailed, Case::TruncateFailed] {
        let (result, seen) = exercise(case, true).await;
        assert!(result.is_err());
        assert_eq!(seen.last().unwrap(), "close");
        assert!(seen.iter().filter(|tag| *tag == "create_guarded").count() <= 1);
    }
}

#[tokio::test]
async fn invalid_open_options_send_no_rpc() {
    let (mount, server) = serve(|_, _| panic!("invalid options must fail locally")).await;
    let mount = super::mount::test_wrapper(mount);
    for access in [crate::OPEN_READ, 0, 4] {
        let error = mount
            .open_path_with_options(
                "parent/new",
                OpenOptions {
                    access,
                    create_mode: Some(0o666),
                    truncate: true,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, crate::NfsError::InvalidInput(_)));
    }
    server.abort();
}
