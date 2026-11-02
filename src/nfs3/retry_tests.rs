#![cfg(test)]
//! RFC 1813 section 2.6: JUKEBOX reissues use a fresh RPC XID.

use super::{Mount, nfsstat3};
use crate::{Auth, rpc};
use bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn fixture(first_status: u32) -> (Mount, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    fixture_for(first_status, 6).await
}

async fn fixture_for(
    first_status: u32,
    procedure: u32,
) -> (Mount, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut previous_xid = None;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        while let Ok(marker) = stream.read_u32().await {
            assert_ne!(marker & 0x80000000, 0);
            let mut request = vec![0; (marker & 0x7fffffff) as usize];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(
                u32::from_be_bytes(request[20..24].try_into().unwrap()),
                procedure
            );
            let xid = u32::from_be_bytes(request[..4].try_into().unwrap());
            assert_ne!(previous_xid, Some(xid));
            previous_xid = Some(xid);
            let number = observed.fetch_add(1, Ordering::SeqCst);
            let status = if number == 0 { first_status } else { 0 };
            let mut reply = request[..4].to_vec();
            for word in [1u32, 0, 0, 0, 0, status] {
                reply.extend(word.to_be_bytes());
            }
            if procedure == 6 {
                reply.extend(0u32.to_be_bytes());
            }
            if status == 0 && procedure == 1 {
                let mut attr = vec![0; 84];
                attr[..4].copy_from_slice(&1u32.to_be_bytes());
                reply.extend(attr);
            }
            if status == 0 && procedure == 6 {
                for word in [3u32, 1, 3] {
                    reply.extend(word.to_be_bytes());
                }
                reply.extend(b"abc\0");
            }
            stream
                .write_u32(0x80000000 | reply.len() as u32)
                .await
                .unwrap();
            stream.write_all(&reply).await.unwrap();
        }
    });
    let mux = rpc::StreamMux::connect(address, true).await.unwrap();
    let mount = Mount {
        rpc: rpc::Client::new(mux, None),
        auth: Auth::new_null(),
        fh: Bytes::from_static(b"file"),
        dir: "/".into(),
        dircount: 4096,
        maxcount: 4096,
        rsize: 4096,
        wsize: 4096,
    };
    (mount, calls, server)
}

#[tokio::test]
async fn busy_v3_read_success_is_single_rpc() {
    let (mount, calls, server) = fixture(0).await;
    assert_eq!(mount.read(mount.getfh(), 0, 3).await.unwrap(), b"abc"[..]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn busy_v3_read_noent_is_not_retried() {
    let (mount, calls, server) = fixture(2).await;
    let error = mount.read(mount.getfh(), 0, 3).await.unwrap_err();
    assert!(matches!(
        error,
        crate::NfsError::Nfs3(nfsstat3::NFS3ERR_NOENT)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn busy_v3_read_retries_jukebox_with_fresh_rpc() {
    let (mount, calls, server) = fixture(10008).await;
    let first = mount.read(mount.getfh(), 0, 3).await;
    eprintln!(
        "first result={first:?}; READ requests={}",
        calls.load(Ordering::SeqCst)
    );
    server.abort();
    assert_eq!(
        first.expect("transient JUKEBOX escaped without protocol retry"),
        b"abc"[..]
    );
}

#[tokio::test]
async fn busy_v3_cancelled_read_does_not_issue_another_request() {
    let (mount, calls, server) = fixture(10008).await;
    let task = tokio::spawn(async move { mount.read(mount.getfh(), 0, 3).await });
    while calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn busy_v3_metadata_wrappers_retry_jukebox() {
    let (mount, calls, server) = fixture_for(10008, 1).await;
    mount.getattr(mount.getfh()).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
}
