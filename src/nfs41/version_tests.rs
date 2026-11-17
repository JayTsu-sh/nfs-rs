#![cfg(test)]
//! RFC 7862 §2, §§3.2–3.3 and RFC 8178 §8.1: exercise actual RPC codecs.
use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

use super::MinorVersion;
use super::layout::DsConnection;
use super::mount::Mount41;
use super::session::{ClientIdentity, Session};
use super::test_support::{Reply, ok, opaque};
use crate::nfs4::compound::{take_opaque, xdr_opaque, xdr_u32};
use crate::rpc::{Client, StreamMux, auth::Auth};

struct Step {
    minor: u32,
    tag: &'static str,
    replies: Vec<Reply>,
    reconnect: bool,
    drop_reply: bool,
}

fn step(minor: u32, tag: &'static str, replies: Vec<Reply>) -> Step {
    Step {
        minor,
        tag,
        replies,
        reconnect: false,
        drop_reply: false,
    }
}

fn exchange(minor: u32) -> Step {
    let mut data = 7u64.to_be_bytes().to_vec();
    xdr_u32(&mut data, 1);
    xdr_u32(&mut data, 0); // non-pNFS for MDS tests
    xdr_u32(&mut data, 0); // SP4_NONE
    data.extend(0u64.to_be_bytes()); // server_owner minor_id
    xdr_opaque(&mut data, b"server");
    xdr_opaque(&mut data, b"scope");
    xdr_u32(&mut data, 0); // implementation IDs
    step(minor, "exchange_id", vec![ok(42, data)])
}

fn create_session(minor: u32) -> Step {
    let mut data = vec![7; 16];
    xdr_u32(&mut data, 1);
    xdr_u32(&mut data, 2);
    for word in [
        0, 1048576, 1048576, 4096, 16, 4, 0, 0, 4096, 4096, 4096, 2, 1, 0,
    ] {
        xdr_u32(&mut data, word);
    }
    step(minor, "create_session", vec![ok(43, data)])
}

fn bootstrap(minor: u32) -> Vec<Step> {
    let mut attrs = Vec::new();
    xdr_u32(&mut attrs, 1);
    xdr_u32(&mut attrs, (1 << 10) | (1 << 30) | (1 << 31));
    let mut values = 90u32.to_be_bytes().to_vec();
    values.extend(1048576u64.to_be_bytes());
    values.extend(1048576u64.to_be_bytes());
    xdr_opaque(&mut attrs, &values);
    let mut bind = vec![7; 16];
    xdr_u32(&mut bind, 3);
    xdr_u32(&mut bind, 0);
    vec![
        exchange(minor),
        create_session(minor),
        step(minor, "reclaim_complete", vec![ok(58, vec![])]),
        step(
            minor,
            "navigate",
            vec![ok(24, vec![]), ok(15, vec![]), ok(10, opaque(b"root"))],
        ),
        step(minor, "fsinfo", vec![ok(22, vec![]), ok(9, attrs)]),
        step(minor, "bind_conn", vec![ok(41, bind)]),
    ]
}

// Check every request on the wire, including bootstrap and bare teardown calls.
async fn server(steps: Vec<Step>) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut previous_owner = None;
        for step in steps {
            let reconnect = step.reconnect || step.drop_reply;
            answer(&mut stream, step, &mut previous_owner).await;
            if reconnect {
                drop(stream);
                stream = listener.accept().await.unwrap().0;
            }
        }
    });
    (addr, task)
}

// Read one request and check it against `step`; reply unless the step drops it.
async fn answer<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    step: Step,
    previous_owner: &mut Option<(u32, Bytes)>,
) {
    let marker = stream.read_u32().await.unwrap();
    let mut frame = vec![0; (marker & 0x7fffffff) as usize];
    stream.read_exact(&mut frame).await.unwrap();
    let mut args = Bytes::from(frame);
    let xid = args.get_u32();
    args.advance(20);
    for _ in 0..2 {
        args.advance(4);
        take_opaque(&mut args, "auth").unwrap();
    }
    let tag = take_opaque(&mut args, "tag").unwrap();
    assert_eq!(tag.as_ref(), step.tag.as_bytes());
    assert_eq!(args.get_u32(), step.minor, "minor version for {}", step.tag);
    if step.drop_reply {
        return;
    }
    let _count = args.get_u32();
    let opcode = args.get_u32();
    if opcode == 42 {
        let mut exchange_args = args.clone();
        exchange_args.advance(8); // verifier
        let owner = take_opaque(&mut exchange_args, "owner").unwrap();
        let flags = exchange_args.get_u32();
        assert_eq!(flags & 4 != 0, step.minor == 2);
        if let Some((minor, previous)) = previous_owner.take() {
            if minor == step.minor {
                assert_eq!(owner, previous);
            } else {
                assert_ne!(owner, previous);
            }
        }
        *previous_owner = Some((step.minor, owner));
    }
    let mut replies = step.replies;
    if opcode == 53 {
        let mut sequence = args.split_to(28).to_vec();
        xdr_u32(&mut sequence, 0); // target highest slot
        xdr_u32(&mut sequence, 0); // flags
        replies.insert(0, ok(53, sequence));
    }
    let mut response = Vec::new();
    for word in [xid, 1, 0, 0, 0, 0] {
        xdr_u32(&mut response, word);
    }
    xdr_u32(
        &mut response,
        replies.last().map_or(10021, |reply| reply.status),
    );
    xdr_opaque(&mut response, &tag);
    xdr_u32(&mut response, replies.len() as u32);
    for reply in replies {
        xdr_u32(&mut response, reply.opcode);
        xdr_u32(&mut response, reply.status);
        response.extend(reply.data);
    }
    stream
        .write_u32(0x80000000 | response.len() as u32)
        .await
        .unwrap();
    stream.write_all(&response).await.unwrap();
}

#[tokio::test]
async fn v42_public_mount_recovery_and_teardown_preserve_minor_version() {
    let mut steps = bootstrap(2);
    steps.push(step(
        2,
        "lookup",
        vec![Reply {
            opcode: 22,
            status: 10052,
            data: vec![],
        }],
    ));
    steps.extend([
        exchange(2),
        create_session(2),
        step(2, "reclaim_complete", vec![ok(58, vec![])]),
    ]);
    // A definite NOENT proves the recovered ordinary request was processed.
    steps.push(step(
        2,
        "lookup",
        vec![
            ok(22, vec![]),
            Reply {
                opcode: 15,
                status: 2,
                data: vec![],
            },
        ],
    ));
    steps.push(step(2, "destroy_session", vec![ok(44, vec![])]));
    steps.push(step(2, "destroy_clientid", vec![ok(57, vec![])]));
    let (addr, task) = server(steps).await;
    let mount =
        crate::parse_url_and_mount(&format!("nfs://{addr}/export?version=4.2&noresvport=true"))
            .await
            .unwrap();
    assert_eq!(mount.version(), crate::NFSVersion::NFSv4p2);
    assert!(matches!(
        mount.lookup(mount.getfh().await, "missing").await,
        Err(crate::NfsError::Nfs4(
            crate::nfs4::fastxdr::nfsstat4::NFS4ERR_NOENT
        ))
    ));
    mount.umount().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn exact_v42_rejection_is_preserved() {
    let (addr, task) = server(vec![step(2, "exchange_id", vec![])]).await;
    let error =
        crate::parse_url_and_mount(&format!("nfs://{addr}/export?version=4.2&noresvport=true"))
            .await
            .unwrap_err();
    assert!(matches!(
        error,
        crate::NfsError::Nfs4(crate::nfs4::fastxdr::nfsstat4::NFS4ERR_MINOR_VERS_MISMATCH)
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn v42_ds_downgrades_only_after_minor_mismatch_and_uses_v41_for_io_and_cleanup() {
    for ds_minor in [2, 1] {
        let mut read = Vec::new();
        xdr_u32(&mut read, 1);
        xdr_opaque(&mut read, b"data");
        let mut write = Vec::new();
        xdr_u32(&mut write, 4);
        xdr_u32(&mut write, 2);
        write.extend([3; 8]);
        let mut steps = if ds_minor == 1 {
            vec![step(2, "exchange_id", vec![])]
        } else {
            vec![]
        };
        steps.extend(vec![
            exchange(ds_minor),
            create_session(ds_minor),
            step(ds_minor, "ds-read", vec![ok(22, vec![]), ok(25, read)]),
            step(ds_minor, "ds-write", vec![ok(22, vec![]), ok(38, write)]),
            step(ds_minor, "ds_destroy_session", vec![ok(44, vec![])]),
            step(ds_minor, "ds_destroy_clientid", vec![ok(57, vec![])]),
        ]);
        let (addr, task) = server(steps).await;
        let client = Client::new(StreamMux::connect(addr, true).await.unwrap(), None);
        let auth = Auth::new_null();
        let identity = ClientIdentity::with_minor_version(MinorVersion::V2);
        let session = Session::establish_ds(&client, &auth, &identity)
            .await
            .unwrap();
        assert_eq!(session.minor_version() as u32, ds_minor);
        assert_eq!(identity.minor_version, MinorVersion::V2);
        let ds = DsConnection {
            client,
            session: std::sync::Arc::new(session),
            owner_generation: 1,
        };
        Mount41::compound_ds(&ds, &auth, "ds-read", 4, |b| {
            b.putfh(b"fh").read(&[0; 16], 0, 4)
        })
        .await
        .unwrap();
        Mount41::compound_ds_write(&ds, &auth, "ds-write", Bytes::from_static(b"data"), |b| {
            b.putfh(b"fh").write_header(&[0; 16], 0, 0, 4)
        })
        .await
        .unwrap();
        ds.destroy(&auth).await;
        task.await.unwrap();
    }
}

#[tokio::test]
async fn v42_ds_does_not_downgrade_on_access_denied() {
    let (addr, task) = server(vec![step(
        2,
        "exchange_id",
        vec![Reply {
            opcode: 42,
            status: 13,
            data: vec![],
        }],
    )])
    .await;
    let client = Client::new(StreamMux::connect(addr, true).await.unwrap(), None);
    let identity = ClientIdentity::with_minor_version(MinorVersion::V2);
    let error = Session::establish_ds(&client, &Auth::new_null(), &identity)
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        crate::NfsError::Nfs4(crate::nfs4::fastxdr::nfsstat4::NFS4ERR_ACCESS)
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn explicit_v42_v41_preference_falls_back_to_v41() {
    let mut rejection = step(2, "exchange_id", vec![]);
    rejection.reconnect = true;
    let mut steps = vec![rejection];
    steps.extend(bootstrap(1));
    steps.push(step(1, "destroy_session", vec![ok(44, vec![])]));
    steps.push(step(1, "destroy_clientid", vec![ok(57, vec![])]));
    let (addr, task) = server(steps).await;
    let mount = crate::parse_url_and_mount(&format!(
        "nfs://{addr}/export?version=4.2,4.1&noresvport=true"
    ))
    .await
    .unwrap();
    assert_eq!(mount.version(), crate::NFSVersion::NFSv4p1);
    mount.umount().await.unwrap();
    task.await.unwrap();
}

/// An NFSv4.1 mount through a custom dialer: the server name reaches the
/// dialer unresolved, and the session, the backchannel and teardown all run
/// on the one stream it opened.
#[tokio::test]
async fn v41_mount_with_dials_the_server_through_the_dialer() {
    let dialer = std::sync::Arc::new(crate::dialer::ScriptedDialer::default());
    let mut stream = dialer.accept();
    let mut steps = bootstrap(1);
    steps.push(step(1, "destroy_session", vec![ok(44, vec![])]));
    steps.push(step(1, "destroy_clientid", vec![ok(57, vec![])]));
    let task = tokio::spawn(async move {
        let mut previous_owner = None;
        for step in steps {
            answer(&mut stream, step, &mut previous_owner).await;
        }
    });
    let options =
        crate::MountOptions::from_url("nfs://nfs.example.internal/export?version=4.1").unwrap();
    let mount = crate::mount_with(options, dialer.clone()).await.unwrap();
    assert_eq!(mount.version(), crate::NFSVersion::NFSv4p1);
    mount.umount().await.unwrap();
    task.await.unwrap();
    assert_eq!(
        dialer.dialed(),
        vec![crate::Endpoint::new("nfs.example.internal", 2049)]
    );
}

#[tokio::test]
async fn v42_lease_renewal_preserves_minor_version() {
    let steps = vec![
        exchange(2),
        create_session(2),
        step(2, "reclaim_complete", vec![ok(58, vec![])]),
        step(2, "renew", vec![]),
    ];
    let (addr, task) = server(steps).await;
    let client = Client::new(StreamMux::connect(addr, true).await.unwrap(), None);
    let auth = Auth::new_null();
    let identity = ClientIdentity::with_minor_version(MinorVersion::V2);
    let session = Session::establish(&client, &auth, &identity).await.unwrap();
    let renewal = super::lease::LeaseRenewal::start(
        client,
        auth,
        std::sync::Arc::new(super::session::SessionHolder::new(session)),
        std::time::Duration::from_millis(10),
    );
    task.await.unwrap();
    renewal.stop();
}

#[tokio::test]
async fn v42_reconnect_rebind_preserves_minor_version() {
    let mut steps = bootstrap(2);
    let mut dropped_read = step(2, "lookup", vec![]);
    dropped_read.drop_reply = true;
    steps.push(dropped_read);
    let mut bind = vec![7; 16];
    xdr_u32(&mut bind, 3);
    xdr_u32(&mut bind, 0);
    steps.push(step(2, "bind_conn", vec![ok(41, bind)]));
    steps.push(step(
        2,
        "lookup",
        vec![
            ok(22, vec![]),
            Reply {
                opcode: 15,
                status: 2,
                data: vec![],
            },
        ],
    ));
    steps.push(step(2, "destroy_session", vec![ok(44, vec![])]));
    steps.push(step(2, "destroy_clientid", vec![ok(57, vec![])]));
    let (addr, task) = server(steps).await;
    let mount =
        crate::parse_url_and_mount(&format!("nfs://{addr}/export?version=4.2&noresvport=true"))
            .await
            .unwrap();
    let result = mount.lookup(mount.getfh().await, "missing").await;
    assert!(
        matches!(
            &result,
            Err(crate::NfsError::Nfs4(
                crate::nfs4::fastxdr::nfsstat4::NFS4ERR_NOENT
            ))
        ),
        "lookup result: {result:?}"
    );
    mount.umount().await.unwrap();
    task.await.unwrap();
}

#[test]
fn v42_abandoned_write_context_uses_mount_version() {
    let owner = super::write_owner::WriteOwner::new(MinorVersion::V2);
    let fh = Bytes::from_static(b"fh");
    owner.record(fh.clone(), crate::NfsError::Rpc("task failed".into()));
    let error = owner.error(&fh).unwrap();
    assert_eq!(
        error.operation_outcome().unwrap().context().protocol,
        crate::NFSVersion::NFSv4p2
    );
}
