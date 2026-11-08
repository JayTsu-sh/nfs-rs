#![cfg(test)]
//! RFC 8881 §15.1.1.3: SEQUENCE rejection retains identity; later DELAY
//! uses a fresh sequence. RFC 7862 §2 retains these rules for minor version 2.
use super::test_support::{Reply, ok, opaque, putfh, serve_observed};
use super::{MinorVersion, layout::DsConnection, mount::Mount41};
use bytes::{Buf, Bytes};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn busy_v41_v42_mds_and_ds_preserve_sequence_rules() {
    for minor in [MinorVersion::V1, MinorVersion::V2] {
        for ds in [false, true] {
            for sequence_failure in [false, true] {
                let mut first = true;
                let observed = Arc::new(Mutex::new(vec![]));
                let (mount, server) = serve_observed(
                    move |tag, mut args| {
                        assert_eq!(tag, "read");
                        putfh(&mut args);
                        assert_eq!(args.get_u32(), 25);
                        if std::mem::take(&mut first) {
                            if sequence_failure {
                                vec![Reply {
                                    opcode: 53,
                                    status: 10008,
                                    data: vec![],
                                }]
                            } else {
                                vec![
                                    ok(22, vec![]),
                                    Reply {
                                        opcode: 25,
                                        status: 10008,
                                        data: vec![],
                                    },
                                ]
                            }
                        } else {
                            let mut data = 1u32.to_be_bytes().to_vec();
                            data.extend(opaque(b"abc"));
                            vec![ok(22, vec![]), ok(25, data)]
                        }
                    },
                    minor,
                    observed.clone(),
                )
                .await;
                if ds {
                    let connection = DsConnection {
                        client: mount.rpc.clone(),
                        session: mount.session_holder.get().await,
                        owner_generation: 1,
                    };
                    let reply = Mount41::compound_ds(&connection, &mount.auth, "read", 3, |b| {
                        b.putfh(b"fh").read(&[0; 16], 0, 3)
                    })
                    .await
                    .unwrap();
                    assert!(reply.op_ok(2).is_ok());
                } else {
                    assert_eq!(
                        mount.read(Bytes::from_static(b"fh"), 0, 3).await.unwrap(),
                        b"abc"[..]
                    );
                }
                let seen = observed.lock().unwrap();
                assert_eq!(
                    seen.len(),
                    2,
                    "minor={minor:?} ds={ds} sequence_failure={sequence_failure}"
                );
                assert_ne!(seen[0].0, seen[1].0, "RPC XID must be fresh");
                if sequence_failure {
                    assert_eq!((seen[0].1, seen[0].2), (seen[1].1, seen[1].2));
                } else {
                    assert_eq!(seen[1].1, seen[0].1 + 1);
                }
                server.abort();
            }
        }
    }
}

#[tokio::test]
async fn busy_v41_v42_never_replays_a_successful_mutation() {
    for minor in [MinorVersion::V1, MinorVersion::V2] {
        let observed = Arc::new(Mutex::new(vec![]));
        let (mount, server) = serve_observed(
            |_, _| {
                vec![
                    ok(22, vec![]),
                    ok(59, vec![]),
                    Reply {
                        opcode: 9,
                        status: 10008,
                        data: vec![],
                    },
                ]
            },
            minor,
            observed.clone(),
        )
        .await;
        let error = mount
            .compound("partial", |b| {
                b.putfh(b"fh").space(true, &[0; 16], 0, 4096).getattr(&[1])
            })
            .await
            .err()
            .expect("partial mutation must not be replayed");
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::Uncertain
        );
        assert_eq!(
            error.operation_outcome().unwrap().recovery,
            crate::RecoveryAction::VerifyThenResume
        );
        assert_eq!(observed.lock().unwrap().len(), 1);
        server.abort();
    }
}

#[tokio::test]
async fn busy_v41_v42_ds_writes_retry_only_definite_rejections() {
    for minor in [MinorVersion::V1, MinorVersion::V2] {
        for status in [10008, 10013] {
            let observed = Arc::new(Mutex::new(vec![]));
            let mut first = true;
            let (mount, server) = serve_observed(
                move |_, mut args| {
                    putfh(&mut args);
                    assert_eq!(args.get_u32(), 38);
                    args.advance(16 + 8 + 4);
                    assert_eq!(
                        crate::nfs4::compound::take_opaque(&mut args, "write data").unwrap(),
                        b"abc"[..]
                    );
                    if std::mem::take(&mut first) {
                        vec![
                            ok(22, vec![]),
                            Reply {
                                opcode: 38,
                                status,
                                data: vec![],
                            },
                        ]
                    } else {
                        let mut data = vec![];
                        data.extend(3u32.to_be_bytes());
                        data.extend(2u32.to_be_bytes());
                        data.extend([7; 8]);
                        vec![ok(22, vec![]), ok(38, data)]
                    }
                },
                minor,
                observed.clone(),
            )
            .await;
            let connection = DsConnection {
                client: mount.rpc.clone(),
                session: mount.session_holder.get().await,
                owner_generation: 1,
            };
            let result = Mount41::compound_ds_write(
                &connection,
                &mount.auth,
                "write",
                Bytes::from_static(b"abc"),
                |b| b.putfh(b"fh").write_header(&[0; 16], 0, 2, 3),
            )
            .await
            .unwrap();
            assert!(result.op_ok(2).is_ok());
            let seen = observed.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[1].1, seen[0].1 + 1);
            server.abort();
        }
    }
}

#[tokio::test]
async fn busy_v41_v42_malformed_status_does_not_trigger_replay() {
    for minor in [MinorVersion::V1, MinorVersion::V2] {
        let observed = Arc::new(Mutex::new(vec![]));
        let (mount, server) = serve_observed(
            |_, _| {
                vec![
                    ok(22, vec![]),
                    Reply {
                        opcode: 9,
                        status: 10008,
                        data: vec![],
                    },
                ]
            },
            minor,
            observed.clone(),
        )
        .await;
        // The response reports GETATTR although this request ends with READ.
        let error = mount
            .compound("bad", |b| b.putfh(b"fh").read(&[0; 16], 0, 3))
            .await
            .err()
            .expect("malformed busy response");
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            crate::OperationOutcome::SafeToRetry
        );
        assert_eq!(observed.lock().unwrap().len(), 1);
        server.abort();
    }
}

// RFC 8881 §2.10.6.1.3: uncached read replies may be queried again with
// a fresh sequence, but never restart a modifying request's replay identity.
#[tokio::test]
async fn uncached_v41_v42_read_requeries_once_with_fresh_sequence() {
    for minor in [MinorVersion::V1, MinorVersion::V2] {
        for ds in [false, true] {
            for repeated in [false, true] {
                let observed = Arc::new(Mutex::new(vec![]));
                let mut first = true;
                let (mount, server) = serve_observed(
                    move |_, _| {
                        if std::mem::take(&mut first) || repeated {
                            vec![Reply {
                                opcode: 22,
                                status: 10068,
                                data: vec![],
                            }]
                        } else {
                            let mut data = 1u32.to_be_bytes().to_vec();
                            data.extend(opaque(b"abc"));
                            vec![ok(22, vec![]), ok(25, data)]
                        }
                    },
                    minor,
                    observed.clone(),
                )
                .await;
                let result = if ds {
                    let connection = DsConnection {
                        client: mount.rpc.clone(),
                        session: mount.session_holder.get().await,
                        owner_generation: 1,
                    };
                    Mount41::compound_ds(&connection, &mount.auth, "read", 3, |b| {
                        b.putfh(b"fh").read(&[0; 16], 0, 3)
                    })
                    .await
                    .map(|_| ())
                } else {
                    mount
                        .read(Bytes::from_static(b"fh"), 0, 3)
                        .await
                        .map(|data| assert_eq!(data, b"abc"[..]))
                };
                if repeated {
                    let error = result.unwrap_err();
                    let outcome = error.operation_outcome().unwrap();
                    assert_eq!(outcome.outcome, crate::OperationOutcome::SafeToRetry);
                    assert!(matches!(
                        *outcome.source,
                        crate::NfsError::Nfs4(
                            crate::nfs4::Nfs4ErrorCode::NFS4ERR_RETRY_UNCACHED_REP
                        )
                    ));
                } else {
                    result.unwrap();
                }
                let seen = observed.lock().unwrap();
                assert_eq!(seen.len(), 2, "minor={minor:?} ds={ds} repeated={repeated}");
                assert_ne!(seen[0].0, seen[1].0);
                assert_eq!(seen[1].1, seen[0].1 + 1);
                server.abort();
            }
        }
    }
}

#[tokio::test]
async fn uncached_v41_v42_does_not_reissue_mutations_or_malformed_reads() {
    for minor in [MinorVersion::V1, MinorVersion::V2] {
        for ds in [false, true] {
            for mutation in [false, true] {
                let observed = Arc::new(Mutex::new(vec![]));
                let (mount, server) = serve_observed(
                    move |_, _| {
                        vec![Reply {
                            opcode: if mutation { 22 } else { 9 },
                            status: 10068,
                            data: vec![],
                        }]
                    },
                    minor,
                    observed.clone(),
                )
                .await;
                let connection = DsConnection {
                    client: mount.rpc.clone(),
                    session: mount.session_holder.get().await,
                    owner_generation: 1,
                };
                let result = if mutation {
                    if ds {
                        Mount41::compound_ds_write(
                            &connection,
                            &mount.auth,
                            "write",
                            Bytes::from_static(b"abc"),
                            |b| b.putfh(b"fh").write_header(&[0; 16], 0, 2, 3),
                        )
                        .await
                    } else {
                        mount
                            .compound_write("write", Bytes::from_static(b"abc"), |b| {
                                b.putfh(b"fh").write_header(&[0; 16], 0, 2, 3)
                            })
                            .await
                    }
                } else if ds {
                    Mount41::compound_ds(&connection, &mount.auth, "read", 3, |b| {
                        b.putfh(b"fh").read(&[0; 16], 0, 3)
                    })
                    .await
                } else {
                    mount
                        .compound("read", |b| b.putfh(b"fh").read(&[0; 16], 0, 3))
                        .await
                };
                let error = result.err().expect("must not reissue");
                if mutation {
                    assert_eq!(
                        error.operation_outcome().unwrap().outcome,
                        crate::OperationOutcome::Uncertain
                    );
                }
                assert_eq!(observed.lock().unwrap().len(), 1);
                server.abort();
            }
        }
    }
}

#[tokio::test]
async fn uncached_read_cancellation_stops_reissues_and_leaves_the_slot_reusable() {
    for minor in [MinorVersion::V1, MinorVersion::V2] {
        let observed = Arc::new(Mutex::new(vec![]));
        let mut calls = 0;
        let (mount, server) = serve_observed(
            move |_, _| {
                calls += 1;
                match calls {
                    1 => vec![Reply {
                        opcode: 22,
                        status: 10068,
                        data: vec![],
                    }],
                    2 => vec![
                        ok(22, vec![]),
                        Reply {
                            opcode: 25,
                            status: 10008,
                            data: vec![],
                        },
                    ],
                    _ => {
                        let mut data = 1u32.to_be_bytes().to_vec();
                        data.extend(opaque(b"abc"));
                        vec![ok(22, vec![]), ok(25, data)]
                    }
                }
            },
            minor,
            observed.clone(),
        )
        .await;
        let mount = Arc::new(mount);
        let reader = mount.clone();
        let task = tokio::spawn(async move { reader.read(Bytes::from_static(b"fh"), 0, 3).await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while observed.lock().unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Wait for the second response to release the sole slot, proving the
        // waiter is in backoff rather than racing SEQUENCE acknowledgement.
        let session = mount.session_holder.get().await;
        let slot = tokio::time::timeout(std::time::Duration::from_secs(1), session.acquire_slot())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(slot.current_sequence_id(), 3);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(slot);
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert_eq!(
            observed.lock().unwrap().len(),
            2,
            "cancelled read must not issue another request"
        );
        assert_eq!(
            mount.read(Bytes::from_static(b"fh"), 0, 3).await.unwrap(),
            b"abc"[..]
        );
        let seen = observed.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[1].1, seen[0].1 + 1);
        assert_eq!(seen[2].1, seen[1].1 + 1);
        server.abort();
    }
}
