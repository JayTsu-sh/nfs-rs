#![cfg(test)]
//! Regression tests through production pNFS routing and RPC encoding.
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};
use futures::TryStreamExt as _;

use super::*;
use crate::nfs4::compound::{take_opaque, xdr_opaque, xdr_u32};
use crate::nfs41::layout::{DeviceInfo, LayoutType};
use crate::nfs41::session::{Session, SessionHolder};
use crate::nfs41::test_support::{Reply, ok, putfh, serve, serve_with_reply_hook};

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

// RFC 8881 §§13.3, 13.4: stripe count, physical DS count and FH count
// are independent. Exercise the production READ/WRITE wire path, not just
// the stripe arithmetic, including repeated physical DS indices.
async fn single_file_striping_case(is_dense: bool, handles: Vec<Bytes>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut data_servers = Vec::new();
    let mut servers = Vec::new();
    for index in 0..2u8 {
        let observed = calls.clone();
        let (ds, server) = serve(move |tag, mut args| {
            assert_eq!(args.get_u32(), 22);
            let fh = take_opaque(&mut args, "DS filehandle").unwrap();
            let opcode = args.get_u32();
            args.advance(16); // OPEN stateid
            let offset = args.get_u64();
            let mut reply = Vec::new();
            let count = match tag {
                "ds-write" => {
                    assert_eq!(opcode, 38);
                    assert_eq!(args.get_u32(), 0);
                    let payload = take_opaque(&mut args, "WRITE payload").unwrap();
                    assert!(payload.iter().all(|byte| *byte == 0x5a));
                    xdr_u32(&mut reply, payload.len() as u32);
                    xdr_u32(&mut reply, 2); // FILE_SYNC4
                    reply.extend([3; 8]);
                    payload.len() as u32
                }
                "ds-read" => {
                    assert_eq!(opcode, 25);
                    let count = args.get_u32();
                    xdr_u32(&mut reply, 0);
                    xdr_opaque(&mut reply, &vec![index; count as usize]);
                    count
                }
                other => panic!("unexpected DS RPC {other}"),
            };
            observed
                .lock()
                .unwrap()
                .push((opcode, index, fh, offset, count));
            vec![ok(22, vec![]), ok(opcode, reply)]
        })
        .await;
        data_servers.push(ds);
        servers.push(server);
    }
    let (mut mount, server) = serve(|tag, _| panic!("unexpected MDS RPC {tag}")).await;
    servers.push(server);
    enable_pnfs(&mut mount).await;
    mount.rsize = 4096;
    mount.wsize = 4096;
    let fh = Bytes::from_static(b"file");
    let mut granted = layout(0, u64::MAX, b"shared");
    if let LayoutContent::FilesLayout {
        is_dense: dense,
        fh_list,
        ..
    } = &mut granted.segments[0].content
    {
        *dense = is_dense;
        *fh_list = handles.clone();
    }
    mount.layout_manager.store_layout(&fh, granted).await;
    mount
        .layout_manager
        .store_device(
            DEVICE,
            DeviceInfo {
                stripe_indices: vec![1, 0, 1, 0],
                ds_addrs: data_servers.iter().map(|ds| vec![ds.server_addr]).collect(),
            },
        )
        .await;
    for ds in &data_servers {
        mount
            .layout_manager
            .store_test_data_server(ds.server_addr, ds.rpc.clone())
            .await;
    }
    // One logical request spans multiple DSs, so both routing and parallel
    // batch assembly are tested for the same OPEN filehandle.
    let outcome = mount
        .pnfs_write(&fh, 0, Bytes::from(vec![0x5a; 16384]))
        .await;
    assert!(
        matches!(outcome, PnfsWriteOutcome::Attempted(Ok(_))),
        "valid multi-DS layout must not fall back to MDS"
    );
    let read = mount.pnfs_read(&fh, 0, 16384).await.unwrap().unwrap();
    assert_eq!(read.len(), 16384);
    let stripes: Vec<_> = read
        .chunks(4096)
        .map(|chunk| {
            assert!(chunk.iter().all(|byte| *byte == chunk[0]));
            chunk[0]
        })
        .collect();
    assert_eq!(
        stripes,
        [1, 0, 1, 0],
        "single file must read both DSs in stripe order"
    );
    let mut expected_calls = Vec::new();
    for opcode in [25, 38] {
        for (stripe, index) in [1u8, 0, 1, 0].into_iter().enumerate() {
            let ds_fh = if is_dense {
                handles[stripe].clone()
            } else {
                match handles.len() {
                    0 => fh.clone(),
                    1 => handles[0].clone(),
                    _ => handles[index as usize].clone(),
                }
            };
            let offset = if is_dense { 0 } else { stripe as u64 * 4096 };
            expected_calls.push((opcode, index, ds_fh, offset, 4096));
        }
    }
    let mut actual = calls.lock().unwrap().clone();
    actual.sort();
    expected_calls.sort();
    assert_eq!(actual, expected_calls);
    for server in servers {
        server.abort();
    }
}

#[tokio::test]
async fn single_file_sparse_shared_fh_uses_multiple_ds() {
    single_file_striping_case(false, vec![Bytes::from_static(b"shared")]).await;
}

#[tokio::test]
async fn single_file_sparse_open_fh_uses_multiple_ds() {
    single_file_striping_case(false, vec![]).await;
}

#[tokio::test]
async fn single_file_sparse_fhs_are_indexed_by_physical_ds() {
    single_file_striping_case(
        false,
        vec![Bytes::from_static(b"ds0"), Bytes::from_static(b"ds1")],
    )
    .await;
}

#[tokio::test]
async fn single_file_dense_fhs_are_indexed_by_stripe() {
    single_file_striping_case(
        true,
        (0..4)
            .map(|index| Bytes::from(format!("stripe{index}")))
            .collect(),
    )
    .await;
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

#[tokio::test]
async fn invalid_file_layout_mapping_falls_back_before_any_ds_io() {
    let case = remapping_case(|tag, _| panic!("invalid layout sent {tag}"), false).await;
    let fh = Bytes::from_static(b"file");
    // Missing stripe table, out-of-bounds DS, missing address, invalid FH
    // cardinalities and invalid first index must all be rejected before I/O.
    for (indices, addresses, dense, handles, first) in [
        (vec![], vec![vec![case.old]], false, 1, 0),
        (
            vec![0, 2],
            vec![vec![case.old], vec![case.new]],
            false,
            1,
            0,
        ),
        (vec![0, 1], vec![vec![case.old], vec![]], false, 1, 0),
        (
            vec![0, 1],
            vec![vec![case.old], vec![case.new]],
            false,
            3,
            0,
        ),
        (vec![0, 1], vec![vec![case.old], vec![case.new]], true, 1, 0),
        (
            vec![0, 1],
            vec![vec![case.old], vec![case.new]],
            false,
            1,
            2,
        ),
    ] {
        let mut granted = layout(0, u64::MAX, b"shared");
        if let LayoutContent::FilesLayout {
            is_dense,
            fh_list,
            first_stripe_index,
            ..
        } = &mut granted.segments[0].content
        {
            *is_dense = dense;
            *fh_list = vec![Bytes::from_static(b"shared"); handles];
            *first_stripe_index = first;
        }
        case.mount.layout_manager.store_layout(&fh, granted).await;
        case.mount
            .layout_manager
            .store_device(
                DEVICE,
                DeviceInfo {
                    stripe_indices: indices,
                    ds_addrs: addresses,
                },
            )
            .await;
        assert!(matches!(
            case.mount
                .pnfs_write(&fh, 0, Bytes::from(vec![1; 8192]))
                .await,
            PnfsWriteOutcome::NotAttempted
        ));
        assert!(case.mount.pnfs_read(&fh, 0, 8192).await.is_none());
    }
    assert!(case.calls.lock().unwrap().is_empty());
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

// RFC 8881 §18.32: an error on one data server does not undo a sibling WRITE.
async fn mixed_ds_reply_order(failure_first: bool) {
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (delayed_failure, delayed_success) = if failure_first {
        (None, Some(release_rx))
    } else {
        (Some(release_rx), None)
    };
    let mut first_reply = Some(release_tx);
    let first_failure_reply = if failure_first {
        first_reply.take()
    } else {
        None
    };
    let first_success_reply = first_reply;
    let order = Arc::new(Mutex::new(Vec::new()));
    let fail_order = order.clone();
    let (failure_ds, failure_server) = serve_with_reply_hook(
        move |tag, mut args| {
            assert_eq!(tag, "ds-write");
            assert_eq!(args.get_u32(), 22);
            assert_eq!(
                take_opaque(&mut args, "DS filehandle").unwrap().as_ref(),
                b"private-fh-one"
            );
            assert_eq!(args.get_u32(), 38);
            stale_reply()
        },
        delayed_failure,
        {
            let mut sender = first_failure_reply;
            move |_| {
                fail_order.lock().unwrap().push("failure");
                if let Some(sender) = sender.take() {
                    sender.send(()).unwrap();
                }
            }
        },
    )
    .await;
    let success_order = order.clone();
    let (success_ds, success_server) = serve_with_reply_hook(
        move |tag, mut args| {
            assert_eq!(tag, "ds-write");
            let mut checked = args.clone();
            assert_eq!(checked.get_u32(), 22);
            assert_eq!(
                take_opaque(&mut checked, "DS filehandle").unwrap().as_ref(),
                b"private-fh-one"
            );
            write_reply(&mut args, None)
        },
        delayed_success,
        {
            let mut sender = first_success_reply;
            move |_| {
                success_order.lock().unwrap().push("success");
                if let Some(sender) = sender.take() {
                    sender.send(()).unwrap();
                }
            }
        },
    )
    .await;
    let mds_calls = Arc::new(Mutex::new(Vec::new()));
    let observed = mds_calls.clone();
    let (mut mount, mds_server) = serve(move |tag, _| {
        observed.lock().unwrap().push(tag.to_owned());
        stale_reply()
    })
    .await;
    enable_pnfs(&mut mount).await;
    mount.wsize = 64;
    let fh = Bytes::from_static(b"file");
    let mut striped = layout(0, u64::MAX, b"private-fh-one");
    if let LayoutContent::FilesLayout { stripe_unit, .. } = &mut striped.segments[0].content {
        *stripe_unit = 64;
    }
    mount.layout_manager.store_layout(&fh, striped).await;
    mount
        .layout_manager
        .store_device(
            DEVICE,
            DeviceInfo {
                stripe_indices: vec![0, 1],
                ds_addrs: vec![vec![failure_ds.server_addr], vec![success_ds.server_addr]],
            },
        )
        .await;
    mount
        .layout_manager
        .store_test_data_server(failure_ds.server_addr, failure_ds.rpc.clone())
        .await;
    mount
        .layout_manager
        .store_test_data_server(success_ds.server_addr, success_ds.rpc.clone())
        .await;
    let failure_address = failure_ds.server_addr;
    let success_address = success_ds.server_addr;
    let wrapper = crate::nfs41::mount::test_wrapper(mount);
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::Mount::write(&wrapper, fh.clone(), 0, Bytes::from(vec![0x5a; 128])),
    )
    .await
    .expect("both simulated DS replies must settle")
    .unwrap_err();
    let outcome = error.operation_outcome().unwrap();
    assert_eq!(outcome.outcome, OperationOutcome::Uncertain);
    assert_eq!(outcome.recovery, RecoveryAction::VerifyThenResume);
    let diagnostic = format!("{error:?}");
    assert!(diagnostic.contains(&format!(
        "stripe=0 ds={failure_address} attempted=true outcome=error"
    )));
    assert!(diagnostic.contains(&format!(
        "stripe=1 ds={success_address} attempted=true outcome=success"
    )));
    assert!(!diagnostic.contains("private-fh"));
    assert!(!diagnostic.contains("ZZZZ"));
    assert!(!diagnostic.contains("[90, 90, 90"));
    assert!(!diagnostic.contains("5a5a5a5a"));
    assert!(!diagnostic.contains("[112, 114, 105, 118"));
    assert!(!diagnostic.contains("70726976"));
    assert!(
        mds_calls.lock().unwrap().is_empty(),
        "MDS must not receive a replay"
    );
    assert_eq!(
        *order.lock().unwrap(),
        if failure_first {
            vec!["failure", "success"]
        } else {
            vec!["success", "failure"]
        }
    );
    failure_server.abort();
    success_server.abort();
    mds_server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_ds_failure_reply_first_has_no_mds_replay() {
    mixed_ds_reply_order(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_ds_success_reply_first_has_no_mds_replay() {
    mixed_ds_reply_order(false).await;
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

struct AbandonedWriteCase {
    wrapper: Arc<crate::nfs41::mount::Mount41Wrapper>,
    manager: Arc<crate::nfs41::layout::LayoutManager>,
    calls: Arc<Mutex<Vec<String>>>,
    servers: Vec<tokio::task::JoinHandle<()>>,
}
impl Drop for AbandonedWriteCase {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

fn destroy_reply(tag: &str, mut args: Bytes) -> Vec<Reply> {
    let opcode = if tag.ends_with("destroy_session") {
        44
    } else {
        57
    };
    assert_eq!(args.get_u32(), opcode);
    vec![ok(opcode, vec![])]
}

async fn abandoned_write_case(cancel: bool) -> AbandonedWriteCase {
    multids_write_case(cancel, true, false).await
}

async fn multids_write_case(cancel: bool, fail: bool, fail_return: bool) -> AbandonedWriteCase {
    multids_write_case_with_admission_probe(cancel, fail, fail_return, false).await
}

async fn multids_write_case_with_admission_probe(
    cancel: bool,
    fail: bool,
    fail_return: bool,
    probe_admission: bool,
) -> AbandonedWriteCase {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let (fast_tx, fast_rx) = tokio::sync::oneshot::channel();
    let mut fast_tx = Some(fast_tx);
    let (fast, fast_server) = serve_with_reply_hook(
        |tag, mut args| {
            if tag.starts_with("ds_destroy_") {
                destroy_reply(tag, args)
            } else {
                write_reply(&mut args, None)
            }
        },
        None,
        move |tag| {
            if tag == "ds-write"
                && let Some(tx) = fast_tx.take()
            {
                let _ = tx.send(());
            }
        },
    )
    .await;
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
    let mut sent_tx = Some(sent_tx);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (replied_tx, replied_rx) = tokio::sync::oneshot::channel();
    let mut replied_tx = Some(replied_tx);
    let slow_calls = calls.clone();
    let (slow, slow_server) = serve_with_reply_hook(
        move |tag, mut args| {
            slow_calls.lock().unwrap().push(format!("slow:{tag}"));
            if tag.starts_with("ds_destroy_") {
                destroy_reply(tag, args)
            } else if tag == "ds-commit" {
                putfh(&mut args);
                assert_eq!(args.get_u32(), 5);
                vec![ok(22, vec![]), ok(5, vec![3; 8])]
            } else {
                assert_eq!(tag, "ds-write");
                if let Some(tx) = sent_tx.take() {
                    let _ = tx.send(());
                }
                if fail {
                    stale_reply()
                } else {
                    let mut reply = write_reply(&mut args, None);
                    reply[1].data[4..8].copy_from_slice(&0u32.to_be_bytes());
                    reply
                }
            }
        },
        Some(release_rx),
        move |tag| {
            if tag == "ds-write"
                && let Some(tx) = replied_tx.take()
            {
                let _ = tx.send(());
            }
        },
    )
    .await;
    let observed = calls.clone();
    let (mut mount, mds_server) = serve(move |tag, mut args| {
        observed.lock().unwrap().push(tag.to_owned());
        match tag {
            "close" => {
                putfh(&mut args);
                assert_eq!(args.get_u32(), 4);
                vec![ok(22, vec![]), ok(4, vec![1; 16])]
            }
            "layoutreturn" if fail_return => vec![
                ok(22, vec![]),
                Reply {
                    opcode: 51,
                    status: 5,
                    data: vec![],
                },
            ],
            "layoutreturn" => vec![ok(22, vec![]), ok(51, vec![0; 4])],
            "layoutcommit" => vec![ok(22, vec![]), ok(49, vec![0; 4])],
            "destroy_session" | "destroy_clientid" => destroy_reply(tag, args),
            "layoutget" => vec![
                ok(22, vec![]),
                Reply {
                    opcode: 50,
                    status: 10004,
                    data: vec![],
                },
            ],
            "write" => write_reply(&mut args, None),
            other => panic!("unexpected MDS operation {other}"),
        }
    })
    .await;
    enable_pnfs(&mut mount).await;
    mount.wsize = 64;
    let fh = Bytes::from_static(b"file");
    let mut striped = layout(0, u64::MAX, b"shared");
    if let LayoutContent::FilesLayout { stripe_unit, .. } = &mut striped.segments[0].content {
        *stripe_unit = 64;
    }
    mount.layout_manager.store_layout(&fh, striped).await;
    mount
        .layout_manager
        .store_device(
            DEVICE,
            DeviceInfo {
                stripe_indices: vec![0, 1],
                ds_addrs: vec![vec![fast.server_addr], vec![slow.server_addr]],
            },
        )
        .await;
    mount
        .layout_manager
        .store_test_data_server(fast.server_addr, fast.rpc.clone())
        .await;
    mount
        .layout_manager
        .store_test_data_server(slow.server_addr, slow.rpc.clone())
        .await;
    let manager = mount.layout_manager.clone();
    let wrapper = Arc::new(crate::nfs41::mount::test_wrapper(mount));
    let writer = wrapper.clone();
    let task = tokio::spawn(async move {
        crate::Mount::write(
            writer.as_ref(),
            Bytes::from_static(b"file"),
            0,
            Bytes::from(vec![9; 128]),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        fast_rx.await.unwrap();
        sent_rx.await.unwrap();
    })
    .await
    .unwrap();
    if cancel {
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        if probe_admission {
            let mut next = Box::pin(crate::Mount::write(
                wrapper.as_ref(),
                fh.clone(),
                0,
                Bytes::from_static(b"next"),
            ));
            std::future::poll_fn(|cx| {
                assert!(next.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert_eq!(
                crate::nfs41::mount::test_owned_write_count(wrapper.as_ref()),
                1,
                "a later WRITE must not be admitted before the abandoned batch settles"
            );
            drop(next);
        }
    } else {
        release_tx.send(()).unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert_eq!(
            error.operation_outcome().unwrap().outcome,
            OperationOutcome::Uncertain
        );
        return AbandonedWriteCase {
            wrapper,
            manager,
            calls,
            servers: vec![fast_server, slow_server, mds_server],
        };
    }
    release_tx.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), replied_rx)
        .await
        .unwrap()
        .unwrap();
    AbandonedWriteCase {
        wrapper,
        manager,
        calls,
        servers: vec![fast_server, slow_server, mds_server],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_multids_error_closes_open_and_tears_down_with_uncertainty() {
    let case = abandoned_write_case(true).await;
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::Mount::close(case.wrapper.as_ref(), Bytes::from_static(b"file")),
    )
    .await
    .unwrap()
    .expect_err("abandoned WRITE must remain observable at CLOSE");
    assert_eq!(
        error.operation_outcome().unwrap().outcome,
        OperationOutcome::Uncertain
    );
    assert!(
        case.calls.lock().unwrap().iter().any(|tag| tag == "close"),
        "uncertainty must not prevent releasing OPEN"
    );
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::Mount::umount(case.wrapper.as_ref()),
    )
    .await
    .unwrap()
    .expect_err("teardown must preserve the abandoned outcome");
    assert_eq!(
        error.operation_outcome().unwrap().recovery,
        RecoveryAction::VerifyThenResume
    );
    let calls = case.calls.lock().unwrap();
    assert!(calls.iter().any(|tag| tag == "destroy_session"));
    assert!(calls.iter().any(|tag| tag == "destroy_clientid"));
    assert_eq!(
        crate::Mount::health(case.wrapper.as_ref()).lifecycle,
        crate::MountLifecycleState::Closed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_uncertainty_survives_layout_cache_and_generation_reset() {
    for generation_change in [false, true] {
        let case = abandoned_write_case(true).await;
        // CLOSE is the public settlement barrier; it may report the prior error.
        let _ = crate::Mount::close(case.wrapper.as_ref(), Bytes::from_static(b"file")).await;
        if generation_change {
            case.manager.transition_to(2).await;
        } else {
            case.manager.clear().await;
        }
        let before = case.calls.lock().unwrap().clone();
        let error = crate::Mount::write(
            case.wrapper.as_ref(),
            Bytes::from_static(b"file"),
            0,
            Bytes::from_static(b"next"),
        )
        .await
        .expect_err("cache reset must not enable replay of an abandoned WRITE");
        assert_eq!(
            error.operation_outcome().unwrap().recovery,
            RecoveryAction::VerifyThenResume
        );
        assert_eq!(
            *case.calls.lock().unwrap(),
            before,
            "uncertainty check must precede layout fetch or MDS WRITE"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observed_multids_error_does_not_poison_close_or_umount() {
    let case = abandoned_write_case(false).await;
    crate::Mount::close(case.wrapper.as_ref(), Bytes::from_static(b"file"))
        .await
        .unwrap();
    crate::Mount::umount(case.wrapper.as_ref()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_success_is_committed_before_close() {
    let case = multids_write_case(true, false, false).await;
    crate::Mount::close(case.wrapper.as_ref(), Bytes::from_static(b"file"))
        .await
        .unwrap();
    let calls = case.calls.lock().unwrap().clone();
    let commit = calls
        .iter()
        .position(|tag| tag == "slow:ds-commit")
        .expect("unclaimed UNSTABLE receipt must be committed");
    let layoutcommit = calls.iter().position(|tag| tag == "layoutcommit").unwrap();
    let close = calls.iter().position(|tag| tag == "close").unwrap();
    assert!(commit < layoutcommit && layoutcommit < close);
    crate::Mount::umount(case.wrapper.as_ref()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_layoutreturn_does_not_skip_open_or_session_cleanup() {
    let case = multids_write_case(false, true, true).await;
    assert!(crate::Mount::umount(case.wrapper.as_ref()).await.is_err());
    let calls = case.calls.lock().unwrap();
    for tag in [
        "layoutreturn",
        "close",
        "destroy_session",
        "destroy_clientid",
    ] {
        assert!(
            calls.iter().any(|seen| seen == tag),
            "cleanup omitted {tag}: {calls:?}"
        );
    }
    assert_eq!(
        crate::Mount::health(case.wrapper.as_ref()).lifecycle,
        crate::MountLifecycleState::Closed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_umount_waiter_does_not_cancel_cleanup_or_admit_new_opens() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let (mut mount, server) = serve_with_reply_hook(
        move |tag, mut args| {
            observed.lock().unwrap().push(tag.to_owned());
            if tag == "close" {
                if let Some(tx) = entered_tx.take() {
                    let _ = tx.send(());
                }
                putfh(&mut args);
                assert_eq!(args.get_u32(), 4);
                vec![ok(22, vec![]), ok(4, vec![1; 16])]
            } else {
                destroy_reply(tag, args)
            }
        },
        Some(release_rx),
        |_| {},
    )
    .await;
    enable_pnfs(&mut mount).await;
    let wrapper = Arc::new(crate::nfs41::mount::test_wrapper(mount));
    let closing = wrapper.clone();
    let waiter = tokio::spawn(async move { crate::Mount::umount(closing.as_ref()).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
        .await
        .unwrap()
        .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let error = crate::Mount::open_path(wrapper.as_ref(), "another", crate::OPEN_BOTH)
        .await
        .unwrap_err();
    assert!(matches!(error, NfsError::ClientClosed(_)));
    let error = crate::Mount::write(
        wrapper.as_ref(),
        Bytes::from_static(b"file"),
        0,
        Bytes::from_static(b"next"),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, NfsError::ClientClosed(_)));
    release_tx.send(()).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::Mount::umount(wrapper.as_ref()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        ["close", "destroy_session", "destroy_clientid"]
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_write_holds_later_admission_until_the_batch_settles() {
    let case = multids_write_case_with_admission_probe(true, true, false, true).await;
    let error = crate::Mount::close(case.wrapper.as_ref(), Bytes::from_static(b"file"))
        .await
        .unwrap_err();
    assert_eq!(
        error.operation_outcome().unwrap().recovery,
        RecoveryAction::VerifyThenResume
    );
}

#[tokio::test]
async fn destroy_protocol_errors_are_reported_after_remaining_cleanup() {
    for rejected in ["destroy_session", "destroy_clientid"] {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        let (mount, server) = serve(move |tag, args| {
            observed.lock().unwrap().push(tag.to_owned());
            let mut reply = destroy_reply(tag, args);
            if tag == rejected {
                reply[0].status = Nfs4ErrorCode::NFS4ERR_CLIENTID_BUSY as u32;
            }
            reply
        })
        .await;
        let wrapper = crate::nfs41::mount::test_wrapper(mount);
        let error = crate::Mount::umount(&wrapper)
            .await
            .expect_err("NFS destroy rejection must not count as successful teardown");
        assert!(format!("{error:?}").contains("NFS4ERR_CLIENTID_BUSY"));
        assert_eq!(
            *calls.lock().unwrap(),
            ["destroy_session", "destroy_clientid"]
        );
        assert_eq!(
            crate::Mount::health(&wrapper).lifecycle,
            crate::MountLifecycleState::Closed
        );
        let before = calls.lock().unwrap().clone();
        assert!(crate::Mount::umount(&wrapper).await.is_err());
        assert_eq!(
            *calls.lock().unwrap(),
            before,
            "terminal teardown must not be replayed"
        );
        server.abort();
    }
}

#[tokio::test]
async fn idle_directory_streams_do_not_block_umount_or_start_new_pages_afterward() {
    use futures::StreamExt as _;
    for plus in [false, true] {
        let (mount, server) = serve(destroy_reply).await;
        let wrapper = crate::nfs41::mount::test_wrapper(mount);
        let mut stream = if plus {
            crate::Mount::readdirplus(&wrapper, Bytes::from_static(b"root"))
                .await
                .map_ok(|_| ())
                .boxed()
        } else {
            crate::Mount::readdir(&wrapper, Bytes::from_static(b"root"))
                .await
                .map_ok(|_| ())
                .boxed()
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::Mount::umount(&wrapper),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(
            stream.try_next().await,
            Err(NfsError::ClientClosed(_))
        ));
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_directory_pages_are_admitted_until_the_reply_is_consumed() {
    use futures::StreamExt as _;
    for plus in [false, true] {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let mut entered_tx = Some(entered_tx);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = calls.clone();
        let (mount, server) = serve_with_reply_hook(
            move |tag, args| {
                observed.lock().unwrap().push(tag.to_owned());
                if tag.starts_with("readdir") {
                    if let Some(tx) = entered_tx.take() {
                        let _ = tx.send(());
                    }
                    vec![
                        ok(22, vec![]),
                        ok(
                            26,
                            crate::nfs41::test_support::directory_page(Some(1), true, [0; 8]),
                        ),
                    ]
                } else {
                    destroy_reply(tag, args)
                }
            },
            Some(release_rx),
            |_| {},
        )
        .await;
        let wrapper = Arc::new(crate::nfs41::mount::test_wrapper(mount));
        let reading = wrapper.clone();
        let reader = tokio::spawn(async move {
            let mut stream = if plus {
                crate::Mount::readdirplus(reading.as_ref(), Bytes::from_static(b"root"))
                    .await
                    .map_ok(|_| ())
                    .boxed()
            } else {
                crate::Mount::readdir(reading.as_ref(), Bytes::from_static(b"root"))
                    .await
                    .map_ok(|_| ())
                    .boxed()
            };
            stream.try_next().await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            crate::nfs41::mount::test_operation_count(wrapper.as_ref()),
            1,
            "the lazy page RPC must participate in close draining"
        );
        let closing = wrapper.clone();
        let closer = tokio::spawn(async move { crate::Mount::umount(closing.as_ref()).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::nfs41::mount::test_wait_closing(wrapper.as_ref()),
        )
        .await
        .unwrap();
        release_tx.send(()).unwrap();
        assert!(matches!(
            reader.await.unwrap(),
            Err(NfsError::ClientClosed(_))
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), closer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let calls = calls.lock().unwrap().clone();
        assert_eq!(
            calls,
            [
                if plus { "readdirplus" } else { "readdir" },
                "destroy_session",
                "destroy_clientid"
            ]
        );
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_directory_next_does_not_keep_an_unpolled_stream_in_flight() {
    use futures::StreamExt as _;
    for plus in [false, true] {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let mut entered_tx = Some(entered_tx);
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (replied_tx, replied_rx) = tokio::sync::oneshot::channel();
        let mut replied_tx = Some(replied_tx);
        let (mount, server) = serve_with_reply_hook(
            move |tag, args| {
                if tag.starts_with("readdir") {
                    if let Some(tx) = entered_tx.take() {
                        let _ = tx.send(());
                    }
                    vec![
                        ok(22, vec![]),
                        ok(
                            26,
                            crate::nfs41::test_support::directory_page(Some(1), true, [0; 8]),
                        ),
                    ]
                } else {
                    destroy_reply(tag, args)
                }
            },
            Some(release_rx),
            move |tag| {
                if tag.starts_with("readdir")
                    && let Some(tx) = replied_tx.take()
                {
                    let _ = tx.send(());
                }
            },
        )
        .await;
        let wrapper = crate::nfs41::mount::test_wrapper(mount);
        let mut stream = if plus {
            crate::Mount::readdirplus(&wrapper, Bytes::from_static(b"root"))
                .await
                .map_ok(|_| ())
                .boxed()
        } else {
            crate::Mount::readdir(&wrapper, Bytes::from_static(b"root"))
                .await
                .map_ok(|_| ())
                .boxed()
        };
        let mut next = Box::pin(stream.try_next());
        tokio::select! {
            received=entered_rx => received.unwrap(),
            item=&mut next => panic!("page completed before release: {item:?}"),
        }
        drop(next); // retain the stream and its pending try_unfold state
        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), replied_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            crate::Mount::umount(&wrapper),
        )
        .await
        .expect("UMOUNT must not require repolling an abandoned directory page")
        .unwrap();
        assert!(matches!(
            stream.try_next().await,
            Err(NfsError::ClientClosed(_))
        ));
        server.abort();
    }
}
