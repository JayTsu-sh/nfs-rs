#![cfg(test)]
//! Regression tests through production pNFS routing and RPC encoding.
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};

use super::*;
use crate::nfs4::compound::{take_opaque, xdr_opaque, xdr_u32};
use crate::nfs41::layout::{DeviceInfo, LayoutType};
use crate::nfs41::session::{Session, SessionHolder};
use crate::nfs41::test_support::{Reply, ok, putfh, serve};

const DEVICE: [u8; 16] = [9; 16];

fn xdr_u64(data: &mut Vec<u8>, value: u64) {
    data.extend(value.to_be_bytes());
}

fn layout(offset: u64, length: u64, ds_fh: &[u8]) -> Layout {
    Layout {
        generation: 1,
        stateid: [7; 16],
        return_on_close: false,
        segments: vec![LayoutSegment {
            offset,
            length,
            iomode: IoMode::ReadWrite,
            layout_type: LayoutType::NfsV41Files,
            content: LayoutContent::FilesLayout {
                device_id: DEVICE,
                stripe_unit: 4096,
                is_dense: false,
                commit_thru_mds: false,
                first_stripe_index: 0,
                pattern_offset: 0,
                fh_list: vec![Bytes::copy_from_slice(ds_fh)],
            },
        }],
    }
}

fn layout_reply(offset: u64, length: u64, ds_fh: &[u8]) -> Vec<u8> {
    let mut content = DEVICE.to_vec();
    xdr_u32(&mut content, 4096);
    xdr_u32(&mut content, 0);
    xdr_u64(&mut content, 0);
    xdr_u32(&mut content, 1);
    xdr_opaque(&mut content, ds_fh);
    let mut result = Vec::new();
    xdr_u32(&mut result, 0);
    result.extend([7; 16]);
    xdr_u32(&mut result, 1);
    xdr_u64(&mut result, offset);
    xdr_u64(&mut result, length);
    xdr_u32(&mut result, 2);
    xdr_u32(&mut result, 1);
    xdr_opaque(&mut result, &content);
    result
}

fn write_reply(args: &mut Bytes, count: Option<u32>) -> Vec<Reply> {
    putfh(args);
    assert_eq!(args.get_u32(), 38); // WRITE
    args.advance(16);
    let _offset = args.get_u64();
    assert_eq!(args.get_u32(), 0); // UNSTABLE4
    let data = take_opaque(args, "write payload").unwrap();
    let mut reply = Vec::new();
    xdr_u32(&mut reply, count.unwrap_or(data.len() as u32));
    xdr_u32(&mut reply, 2); // FILE_SYNC4
    reply.extend([3; 8]);
    vec![ok(22, vec![]), ok(38, reply)]
}

async fn enable_pnfs(mount: &mut Mount41) {
    mount.session_holder = Arc::new(SessionHolder::new(Session::for_pnfs_test(1)));
    mount
        .state
        .register_open(
            &Bytes::from_static(b"file"),
            StateId::from_bytes(&[1; 16]),
            AccessMode::Both,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn cached_large_write_has_no_periodic_layout_rpc() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let (mut mount, server) = serve(move |tag, mut args| {
        observed.lock().unwrap().push(tag.to_owned());
        match tag {
            "ds-write-mds" => write_reply(&mut args, None),
            "layoutget" => vec![ok(22, vec![]), ok(50, layout_reply(0, u64::MAX, b"ds"))],
            other => panic!("unexpected RPC {other}"),
        }
    })
    .await;
    enable_pnfs(&mut mount).await;
    mount
        .layout_manager
        .store_layout(&Bytes::from_static(b"file"), layout(0, u64::MAX, b"ds"))
        .await;
    // An unused second endpoint prevents the degenerate-device shortcut.
    mount
        .layout_manager
        .store_device(
            DEVICE,
            DeviceInfo {
                stripe_indices: vec![0],
                ds_addrs: vec![
                    vec![mount.server_addr],
                    vec!["127.0.0.2:2049".parse().unwrap()],
                ],
            },
        )
        .await;
    for gib in [0, 1, 2, 10, 19] {
        mount
            .write_pnfs(
                &Bytes::from_static(b"file"),
                gib * 1024 * 1024 * 1024,
                Bytes::from_static(b"data"),
            )
            .await
            .unwrap()
            .unwrap();
    }
    let calls = calls.lock().unwrap().clone();
    eprintln!("cached-large-write RPCs: {calls:?}");
    assert_eq!(calls, vec!["ds-write-mds"; 5]);
    server.abort();
}

fn stale_reply() -> Vec<Reply> {
    vec![Reply {
        opcode: 22,
        status: Nfs4ErrorCode::NFS4ERR_STALE as u32,
        data: vec![],
    }]
}

fn device_reply(addr: SocketAddr) -> Vec<u8> {
    let mut body = Vec::new();
    for word in [1, 0, 1, 1] {
        xdr_u32(&mut body, word);
    }
    xdr_opaque(&mut body, b"tcp");
    let universal = format!("{}.{}.{}", addr.ip(), addr.port() / 256, addr.port() % 256);
    xdr_opaque(&mut body, universal.as_bytes());
    let mut reply = Vec::new();
    xdr_u32(&mut reply, 1);
    xdr_opaque(&mut reply, &body);
    xdr_u32(&mut reply, 1);
    xdr_u32(&mut reply, 6); // CHANGE + DELETE subscription accepted
    reply
}

struct RemappingCase {
    mount: Mount41,
    calls: Arc<Mutex<Vec<String>>>,
    servers: Vec<tokio::task::JoinHandle<()>>,
    old: SocketAddr,
    new: SocketAddr,
}

impl Drop for RemappingCase {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

async fn remapping_case(
    old_handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
    reject_new: bool,
) -> RemappingCase {
    remapping_case_with_layoutcommit(old_handler, reject_new, 5).await
}

async fn remapping_case_with_layoutcommit(
    mut old_handler: impl FnMut(&str, Bytes) -> Vec<Reply> + Send + 'static,
    reject_new: bool,
    layoutcommit_status: u32,
) -> RemappingCase {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let (old, old_server) = serve(move |tag, args| {
        observed.lock().unwrap().push(format!("old:{tag}"));
        old_handler(tag, args)
    })
    .await;
    let observed = calls.clone();
    let (new, new_server) = serve(move |tag, mut args| {
        observed.lock().unwrap().push(format!("new:{tag}"));
        if tag == "ds-read" {
            putfh(&mut args);
            assert_eq!(args.get_u32(), 25);
            args.advance(24); // stateid + offset
            let count = args.get_u32() as usize;
            let mut data = Vec::new();
            xdr_u32(&mut data, 0); // not EOF
            xdr_opaque(&mut data, &vec![0; count]);
            return vec![ok(22, vec![]), ok(25, data)];
        }
        assert_eq!(tag, "ds-write");
        if reject_new {
            stale_reply()
        } else {
            write_reply(&mut args, None)
        }
    })
    .await;
    let new_addr = new.server_addr;
    let observed = calls.clone();
    let (mut mount, mds_server) = serve(move |tag, mut args| {
        observed.lock().unwrap().push(format!("mds:{tag}"));
        match tag {
            "layoutget" => {
                putfh(&mut args);
                assert_eq!(args.get_u32(), 50);
                args.advance(36); // signal, type, iomode, offset, length, minlength
                assert_eq!(&args[..16], &[7; 16], "keep the existing layout stateid");
                vec![ok(22, vec![]), ok(50, layout_reply(0, u64::MAX, b"new-fh"))]
            }
            "getdeviceinfo" => {
                assert_eq!(args.get_u32(), 24); // PUTROOTFH
                assert_eq!(args.get_u32(), 47);
                assert_eq!(&args.split_to(16)[..], &DEVICE);
                assert_eq!(args.get_u32(), 1);
                args.advance(4); // maxcount
                assert_eq!(args.get_u32(), 1);
                assert_eq!(args.get_u32(), 6);
                vec![ok(24, vec![]), ok(47, device_reply(new_addr))]
            }
            "layoutcommit" => vec![
                ok(22, vec![]),
                Reply {
                    opcode: 49,
                    status: layoutcommit_status,
                    data: if layoutcommit_status == 0 {
                        vec![0; 4]
                    } else {
                        vec![]
                    },
                },
            ],
            other => panic!("unexpected MDS operation {other}"),
        }
    })
    .await;
    enable_pnfs(&mut mount).await;
    let fh = Bytes::from_static(b"file");
    mount
        .layout_manager
        .store_layout(&fh, layout(0, u64::MAX, b"old-fh"))
        .await;
    mount
        .layout_manager
        .store_device(
            DEVICE,
            DeviceInfo {
                stripe_indices: vec![0],
                ds_addrs: vec![vec![old.server_addr]],
            },
        )
        .await;
    mount
        .layout_manager
        .store_test_data_server(old.server_addr, old.rpc.clone())
        .await;
    mount
        .layout_manager
        .store_test_data_server(new.server_addr, new.rpc.clone())
        .await;
    RemappingCase {
        mount,
        calls,
        servers: vec![old_server, new_server, mds_server],
        old: old.server_addr,
        new: new.server_addr,
    }
}

#[tokio::test]
async fn stale_write_refreshes_same_device_id_and_retries_new_ds_once() {
    let case = remapping_case(
        |tag, _| {
            assert_eq!(tag, "ds-write");
            stale_reply()
        },
        false,
    )
    .await;
    let outcome = case
        .mount
        .write_pnfs(&Bytes::from_static(b"file"), 0, Bytes::from_static(b"data"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.count, 4);
    assert_eq!(
        *case.calls.lock().unwrap(),
        [
            "old:ds-write",
            "mds:layoutget",
            "mds:getdeviceinfo",
            "new:ds-write"
        ]
    );
    assert_eq!(
        case.mount
            .layout_manager
            .get_device(&DEVICE)
            .await
            .unwrap()
            .ds_addrs,
        vec![vec![case.new]]
    );
}

#[tokio::test]
async fn repeated_stale_is_bounded_before_safe_mds_fallback() {
    let case = remapping_case(|_, _| stale_reply(), true).await;
    assert!(
        case.mount
            .write_pnfs(&Bytes::from_static(b"file"), 0, Bytes::from_static(b"data"))
            .await
            .is_none()
    );
    assert_eq!(
        *case.calls.lock().unwrap(),
        [
            "old:ds-write",
            "mds:layoutget",
            "mds:getdeviceinfo",
            "new:ds-write"
        ]
    );
}

#[tokio::test]
async fn stale_after_short_write_does_not_refresh_or_replay() {
    let mut writes = 0;
    let case = remapping_case(
        move |_, mut args| {
            writes += 1;
            if writes == 1 {
                write_reply(&mut args, Some(2))
            } else {
                stale_reply()
            }
        },
        false,
    )
    .await;
    let error = case
        .mount
        .write_pnfs(&Bytes::from_static(b"file"), 0, Bytes::from_static(b"data"))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        OperationOutcome::Uncertain
    );
    assert_eq!(
        *case.calls.lock().unwrap(),
        ["old:ds-write", "old:ds-write"]
    );
}

#[tokio::test]
async fn mixed_success_and_stale_does_not_refresh_or_replay() {
    let case = remapping_case(|_, _| stale_reply(), false).await;
    let fh = Bytes::from_static(b"file");
    let mut striped = layout(0, u64::MAX, b"old-fh");
    if let LayoutContent::FilesLayout {
        stripe_unit,
        fh_list,
        ..
    } = &mut striped.segments[0].content
    {
        *stripe_unit = 64;
        fh_list.push(Bytes::from_static(b"new-fh"));
    }
    case.mount.layout_manager.store_layout(&fh, striped).await;
    case.mount
        .layout_manager
        .store_device(
            DEVICE,
            DeviceInfo {
                stripe_indices: vec![0, 1],
                ds_addrs: vec![vec![case.old], vec![case.new]],
            },
        )
        .await;
    let error = case
        .mount
        .write_pnfs(&fh, 0, Bytes::from(vec![1; 128]))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        OperationOutcome::Uncertain
    );
    assert!(
        !case
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.starts_with("mds:"))
    );
    assert!(
        case.mount
            .layout_manager
            .pending_writes(&fh)
            .await
            .iter()
            .any(|write| write.failed())
    );
}

#[tokio::test]
async fn read_retries_on_new_ds_before_mds_fallback() {
    let case = remapping_case(
        |tag, _| {
            assert_eq!(tag, "ds-read");
            stale_reply()
        },
        false,
    )
    .await;
    let data = case
        .mount
        .pnfs_read(&Bytes::from_static(b"file"), 0, 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.as_ref(), &[0; 4]);
    assert_eq!(
        *case.calls.lock().unwrap(),
        [
            "old:ds-read",
            "mds:layoutget",
            "mds:getdeviceinfo",
            "new:ds-read"
        ]
    );
}

#[tokio::test]
async fn preflight_failure_refreshes_without_sending_old_ds_write() {
    let case = remapping_case(|_, _| panic!("no old DS request may be sent"), false).await;
    case.mount.layout_manager.remove_data_server(case.old).await;
    case.servers[0].abort();
    tokio::task::yield_now().await;
    let outcome = case
        .mount
        .write_pnfs(&Bytes::from_static(b"file"), 0, Bytes::from_static(b"data"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.count, 4);
    assert_eq!(
        *case.calls.lock().unwrap(),
        ["mds:layoutget", "mds:getdeviceinfo", "new:ds-write"]
    );
}

#[tokio::test]
async fn failed_old_layout_commit_preserves_dirty_state_and_stops_refresh() {
    let case = remapping_case(|_, _| stale_reply(), false).await;
    let fh = Bytes::from_static(b"file");
    case.mount.layout_manager.mark_dirty(&fh, 0, 4).await;
    assert!(
        case.mount
            .write_pnfs(&fh, 4, Bytes::from_static(b"data"))
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        case.mount
            .layout_manager
            .snapshot_dirty(&fh)
            .await
            .is_some()
    );
    assert!(
        case.mount
            .layout_manager
            .get_layout_covering(&fh, 0)
            .await
            .is_some()
    );
    assert_eq!(
        *case.calls.lock().unwrap(),
        ["old:ds-write", "mds:layoutcommit"]
    );
}

#[tokio::test]
async fn old_unstable_receipt_commits_before_mapping_replacement() {
    // RFC 8881 §13.7: settle the original DS address, filehandle, and verifier
    // before replacing its mapping. The later rejected write is safe to retry.
    let mut writes = 0;
    let case = remapping_case_with_layoutcommit(
        move |tag, mut args| match tag {
            "ds-write" => {
                writes += 1;
                if writes == 1 {
                    let mut replies = write_reply(&mut args, None);
                    replies[1].data[4..8].copy_from_slice(&0_u32.to_be_bytes());
                    replies
                } else {
                    stale_reply()
                }
            }
            "ds-commit" => {
                assert_eq!(args.get_u32(), 22);
                assert_eq!(
                    take_opaque(&mut args, "old DS fh").unwrap().as_ref(),
                    b"old-fh"
                );
                assert_eq!(args.get_u32(), 5);
                assert_eq!(args.get_u64(), 0);
                assert_eq!(args.get_u32(), 4);
                vec![ok(22, vec![]), ok(5, vec![3; 8])]
            }
            other => panic!("unexpected old DS operation {other}"),
        },
        false,
        0,
    )
    .await;
    let fh = Bytes::from_static(b"file");
    let first = case
        .mount
        .write_pnfs(&fh, 0, Bytes::from_static(b"data"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.committed, crate::WriteCommitted::Unstable);
    let receipt = first.pnfs.unwrap();
    assert!(!receipt.is_done());
    let second = case
        .mount
        .write_pnfs(&fh, 4, Bytes::from_static(b"more"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.count, 4);
    assert!(receipt.is_done());
    assert!(!receipt.failed());
    assert_eq!(
        *case.calls.lock().unwrap(),
        [
            "old:ds-write",
            "old:ds-write",
            "old:ds-commit",
            "mds:layoutcommit",
            "mds:layoutget",
            "mds:getdeviceinfo",
            "new:ds-write",
        ]
    );
}

#[tokio::test]
async fn failed_old_ds_commit_retains_original_receipt_and_mapping() {
    let mut writes = 0;
    let case = remapping_case(
        move |tag, mut args| match tag {
            "ds-write" => {
                writes += 1;
                if writes == 1 {
                    let mut replies = write_reply(&mut args, None);
                    replies[1].data[4..8].copy_from_slice(&0_u32.to_be_bytes());
                    replies
                } else {
                    stale_reply()
                }
            }
            "ds-commit" => vec![
                ok(22, vec![]),
                Reply {
                    opcode: 5,
                    status: 5,
                    data: vec![],
                },
            ],
            other => panic!("unexpected old DS operation {other}"),
        },
        false,
    )
    .await;
    let fh = Bytes::from_static(b"file");
    let first = case
        .mount
        .write_pnfs(&fh, 0, Bytes::from_static(b"data"))
        .await
        .unwrap()
        .unwrap();
    let receipt = first.pnfs.unwrap();
    let error = case
        .mount
        .write_pnfs(&fh, 4, Bytes::from_static(b"more"))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        OperationOutcome::Uncertain
    );
    assert!(!receipt.is_done());
    assert!(receipt.failed());
    let retained = case.mount.layout_manager.pending_writes(&fh).await;
    assert_eq!(retained.len(), 1);
    assert!(Arc::ptr_eq(&retained[0], &receipt));
    assert!(
        case.mount
            .layout_manager
            .get_layout_covering(&fh, 0)
            .await
            .is_some()
    );
    assert_eq!(
        case.mount
            .layout_manager
            .get_device(&DEVICE)
            .await
            .unwrap()
            .ds_addrs,
        vec![vec![case.old]]
    );
    assert_eq!(
        *case.calls.lock().unwrap(),
        ["old:ds-write", "old:ds-write", "old:ds-commit"]
    );
}

#[tokio::test]
async fn layout_waiter_cannot_fetch_in_a_new_session_generation() {
    let case = remapping_case(|_, _| panic!("no old DS operation"), false).await;
    let fh = Bytes::from_static(b"file");
    case.mount
        .layout_manager
        .invalidate_layout_segments(&fh)
        .await;
    let gate = case.mount.layout_manager.layout_fetch_gate(&fh).await;
    let held = gate.lock().await;
    let lookup = case.mount.get_or_fetch_layout(&fh, IoMode::ReadWrite, 0);
    tokio::pin!(lookup);
    // Poll exactly until the lookup is waiting behind the old-generation gate.
    std::future::poll_fn(|cx| {
        assert!(lookup.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(
        case.mount
            .session_holder
            .replace_if_current(1, Session::for_pnfs_test(2))
            .await
    );
    case.mount.layout_manager.transition_to(2).await;
    case.mount.state.transition_to(2).await;
    case.mount
        .state
        .register_open(&fh, StateId::from_bytes_at(&[7; 16], 2), AccessMode::Both)
        .await
        .unwrap();
    drop(held);
    assert!(
        lookup.await.is_none(),
        "old gate must not admit a new-generation LAYOUTGET"
    );
    assert!(case.calls.lock().unwrap().is_empty());
    assert!(
        case.mount
            .get_or_fetch_layout(&fh, IoMode::ReadWrite, 0)
            .await
            .is_some()
    );
    assert_eq!(
        *case.calls.lock().unwrap(),
        ["mds:layoutget", "mds:getdeviceinfo"]
    );
}

#[tokio::test]
async fn concurrent_layout_misses_share_one_layoutget_and_device_lookup() {
    let case = remapping_case(|_, _| panic!("no old DS request may be sent"), false).await;
    let fh = Bytes::from_static(b"file");
    case.mount
        .layout_manager
        .invalidate_layout_segments(&fh)
        .await;
    case.mount
        .layout_manager
        .invalidate_device(&DEVICE, 1, false);
    let futures = (0..32).map(|_| case.mount.get_or_fetch_layout(&fh, IoMode::ReadWrite, 0));
    assert!(
        futures::future::join_all(futures)
            .await
            .iter()
            .all(Option::is_some)
    );
    assert_eq!(
        *case.calls.lock().unwrap(),
        ["mds:layoutget", "mds:getdeviceinfo"]
    );
}
