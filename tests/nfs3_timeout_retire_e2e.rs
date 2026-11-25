//! End-to-end coverage for retiring a stalled NFSv3 connection.
//!
//! The test needs a local kernel NFSv3 server; it is skipped unless
//! `NFS_RS_NFS3_E2E=1`. Preparation (as root):
//!
//! ```text
//! mkdir -p /tmp/nfs-export && chmod 777 /tmp/nfs-export
//! echo '/tmp/nfs-export *(rw,no_root_squash,insecure,no_subtree_check,fsid=0)' > /etc/exports
//! rpcbind -w; exportfs -r; rpc.nfsd 8; rpc.mountd
//! NFS_RS_NFS3_E2E=1 cargo test --test nfs3_timeout_retire_e2e -- --nocapture
//! ```
//!
//! Only the NFS data port is proxied, so the fault hits the same connection the
//! client uses for `CREATE`/`WRITE` while mount and portmap stay healthy — the
//! single-connection failure shape seen in the field.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PROXY_PORT: u16 = 12049;
const MOUNT_PORT: u16 = 20048;
const PAYLOAD_BYTES: usize = 64 * 1024;

/// Forwards the NFS data port to the real server. The first accepted connection
/// can be "armed": once armed, replies coming back on that connection are
/// dropped, so the client sees a request that was sent but never answered.
struct FaultProxy {
    enabled: Arc<AtomicBool>,
    addr: SocketAddr,
}

impl FaultProxy {
    async fn start(target: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", PROXY_PORT)).await?;
        let addr = listener.local_addr()?;
        let enabled = Arc::new(AtomicBool::new(false));
        let accept_enabled = Arc::clone(&enabled);
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    return;
                };
                let is_first = first;
                first = false;
                let enabled = Arc::clone(&accept_enabled);
                tokio::spawn(async move {
                    relay(inbound, target, enabled, is_first).await;
                });
            }
        });
        Ok(Self { enabled, addr })
    }

    fn arm(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    fn disarm(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }
}

async fn relay(inbound: TcpStream, target: SocketAddr, enabled: Arc<AtomicBool>, is_first: bool) {
    let Ok(outbound) = TcpStream::connect(target).await else {
        return;
    };
    let (mut client_read, mut client_write) = inbound.into_split();
    let (mut server_read, mut server_write) = outbound.into_split();
    let drop_replies = Arc::new(AtomicBool::new(false));

    let up_drop = Arc::clone(&drop_replies);
    let up_enabled = Arc::clone(&enabled);
    let upstream = tokio::spawn(async move {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = match client_read.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            if is_first && up_enabled.load(Ordering::SeqCst) {
                up_drop.store(true, Ordering::SeqCst);
            }
            if server_write.write_all(&buffer[..read]).await.is_err() {
                break;
            }
        }
        let _ = server_write.shutdown().await;
    });

    let down_drop = Arc::clone(&drop_replies);
    let downstream = tokio::spawn(async move {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = match server_read.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            if down_drop.load(Ordering::SeqCst) {
                // Keep the socket open, just never answer the client.
                continue;
            }
            if client_write.write_all(&buffer[..read]).await.is_err() {
                break;
            }
        }
        let _ = client_write.shutdown().await;
    });

    let _ = tokio::join!(upstream, downstream);
}

#[tokio::test]
async fn stalled_write_does_not_cascade_to_later_files() -> Result<(), Box<dyn Error + Send + Sync>>
{
    if std::env::var("NFS_RS_NFS3_E2E").ok().as_deref() != Some("1") {
        eprintln!("skipping: set NFS_RS_NFS3_E2E=1 with a local NFSv3 export");
        return Ok(());
    }
    let export =
        std::env::var("NFS_RS_NFS3_E2E_EXPORT").unwrap_or_else(|_| "/tmp/nfs-export".to_string());

    let proxy = FaultProxy::start("127.0.0.1:2049".parse()?).await?;
    let url = format!("nfs://127.0.0.1{export}?nfsport={PROXY_PORT}&mountport={MOUNT_PORT}");
    let mount = nfs_rs::parse_url_and_mount(&url).await?;

    let directory = std::path::Path::new(&export).join("nfs3-retire-e2e");
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory)?;

    // Warm-up: prove the proxy passes traffic through before it is armed.
    let warm = mount
        .create_path("nfs3-retire-e2e/warm.bin", Some(0o600))
        .await?;
    nfs_rs::write_all(
        &*mount,
        warm.fh.clone(),
        0,
        Bytes::from(vec![1u8; PAYLOAD_BYTES]),
    )
    .await?;
    mount
        .commit(warm.fh.clone(), 0, PAYLOAD_BYTES as u32)
        .await?;
    mount.close(warm.fh).await?;

    // Create first: `create_path` performs a LOOKUP, and arming earlier would
    // hit that metadata call instead of the WRITE we want to stall.
    let stalled = mount
        .create_path("nfs3-retire-e2e/stalled.bin", Some(0o600))
        .await?;
    proxy.arm();
    let started = Instant::now();
    let error = nfs_rs::write_all(
        &*mount,
        stalled.fh.clone(),
        0,
        Bytes::from(vec![2u8; PAYLOAD_BYTES]),
    )
    .await
    .expect_err("the armed proxy must swallow the WRITE reply");
    let stalled_elapsed = started.elapsed();
    assert_eq!(
        error.request_transmission(),
        Some(nfs_rs::RequestTransmission::Sent),
        "a swallowed reply must be reported as sent-but-unacknowledged: {error:?}"
    );
    assert!(
        stalled_elapsed >= Duration::from_secs(9),
        "expected the request to reach its data timeout, took {stalled_elapsed:?}"
    );

    proxy.disarm();

    // The next file must go over a fresh connection instead of queueing behind
    // the stalled one; before the fix this call would block for another timeout.
    let started = Instant::now();
    let recovered = mount
        .create_path("nfs3-retire-e2e/recovered.bin", Some(0o600))
        .await?;
    nfs_rs::write_all(
        &*mount,
        recovered.fh.clone(),
        0,
        Bytes::from(vec![3u8; PAYLOAD_BYTES]),
    )
    .await?;
    mount
        .commit(recovered.fh.clone(), 0, PAYLOAD_BYTES as u32)
        .await?;
    mount.close(recovered.fh).await?;
    let recovery_elapsed = started.elapsed();
    assert!(
        recovery_elapsed < Duration::from_secs(6),
        "the next file reused the stalled connection and took {recovery_elapsed:?}"
    );

    assert_eq!(
        mount
            .getattr(mount.lookup_path("nfs3-retire-e2e/recovered.bin").await?.fh)
            .await?
            .filesize,
        PAYLOAD_BYTES as u64
    );

    let _ = std::fs::remove_dir_all(&directory);
    let _ = proxy.addr;
    Ok(())
}
