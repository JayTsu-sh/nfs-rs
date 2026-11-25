//! End-to-end coverage for a stalled NFS connection across protocol versions.
//!
//! Requires a local kernel NFS server exporting one directory with `fsid=0`
//! (served as both the NFSv3 export and the NFSv4 pseudo-root). Skipped unless
//! `NFS_RS_TIMEOUT_RETIRE_E2E=1`. Preparation (as root):
//!
//! ```text
//! mkdir -p /tmp/nfs-export && chmod 777 /tmp/nfs-export
//! echo '/tmp/nfs-export *(rw,no_root_squash,insecure,no_subtree_check,fsid=0)' > /etc/exports
//! rpcbind -w; exportfs -r; rpc.nfsd 8; rpc.mountd
//! NFS_RS_TIMEOUT_RETIRE_E2E=1 cargo test --test timeout_retire_e2e -- --test-threads=1 --nocapture
//! ```
//!
//! Only the NFS data port is proxied, so the fault hits the same connection the
//! client uses for data/metadata while mount and portmap stay healthy — the
//! single-connection failure shape seen in the field.
//!
//! Version coverage:
//!
//! - NFSv3 `WRITE` is `ONE_ATTEMPT`: a swallowed reply retires the link and the
//!   next file must go over a fresh connection.
//! - NFSv4.0 `SETATTR` is `ONE_ATTEMPT` through `Mount40::settled_call`: same
//!   retirement contract, exercised while v4 mount/open state is live.
//! - NFSv4.1 `COMPOUND` always uses `NFS_REPLAY`, so a timeout is reported as
//!   `Sent`/`VerifyThenResume` and the link is intentionally *not* retired. The
//!   test closes the stalled socket afterwards: the reader observes EOF, the mux
//!   rebinds the session and the next file must recover quickly.

use std::error::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MOUNT_PORT: u16 = 20048;
const PAYLOAD_BYTES: usize = 64 * 1024;

/// Forwards the NFS data port to the real server. The first accepted connection
/// can be "armed": once armed, replies coming back on that connection are
/// dropped, so the client sees a request that was sent but never answered.
/// `kill` closes the relayed sockets to make the client observe EOF.
struct FaultProxy {
    enabled: Arc<AtomicBool>,
    kill: Arc<tokio::sync::Notify>,
    stop: Arc<tokio::sync::Notify>,
    addr: SocketAddr,
}

impl FaultProxy {
    async fn start(target: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let enabled = Arc::new(AtomicBool::new(false));
        let kill = Arc::new(tokio::sync::Notify::new());
        let stop = Arc::new(tokio::sync::Notify::new());
        let accept_enabled = Arc::clone(&enabled);
        let accept_kill = Arc::clone(&kill);
        let accept_stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let accepted = tokio::select! {
                    _ = accept_stop.notified() => return,
                    accepted = listener.accept() => accepted,
                };
                let Ok((inbound, _)) = accepted else {
                    return;
                };
                let is_first = first;
                first = false;
                let enabled = Arc::clone(&accept_enabled);
                let kill = Arc::clone(&accept_kill);
                tokio::spawn(async move {
                    relay(inbound, target, enabled, is_first, kill).await;
                });
            }
        });
        Ok(Self {
            enabled,
            kill,
            stop,
            addr,
        })
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    fn arm(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    fn disarm(&self) {
        self.enabled.store(false, Ordering::SeqCst);
    }

    fn kill(&self) {
        self.kill.notify_waiters();
    }

    /// Drops the listener, so a client redial gets ECONNREFUSED while existing
    /// relayed connections keep running.
    fn refuse_redial(&self) {
        self.stop.notify_one();
    }
}

async fn relay(
    inbound: TcpStream,
    target: SocketAddr,
    enabled: Arc<AtomicBool>,
    is_first: bool,
    kill: Arc<tokio::sync::Notify>,
) {
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
    let down_kill = Arc::clone(&kill);
    let downstream = tokio::spawn(async move {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            let read = tokio::select! {
                _ = down_kill.notified() => break,
                read = server_read.read(&mut buffer) => match read {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                },
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

fn enabled() -> bool {
    std::env::var("NFS_RS_TIMEOUT_RETIRE_E2E").ok().as_deref() == Some("1")
}

fn shared_dir() -> String {
    std::env::var("NFS_RS_E2E_SHARED_DIR").unwrap_or_else(|_| "/tmp/nfs-export".to_string())
}

fn prepare_dir(directory: &Path) -> std::io::Result<()> {
    let _ = std::fs::remove_dir_all(directory);
    std::fs::create_dir_all(directory)
}

fn v4_root() -> String {
    std::env::var("NFS_RS_E2E_V4_ROOT").unwrap_or_else(|_| "/".to_string())
}

/// Creates a file, writes one full payload and closes it.
async fn write_file(
    mount: &dyn nfs_rs::Mount,
    path: &str,
    value: u8,
) -> Result<Bytes, Box<dyn Error + Send + Sync>> {
    let file = mount.create_path(path, Some(0o600)).await?;
    nfs_rs::write_all(
        mount,
        file.fh.clone(),
        0,
        Bytes::from(vec![value; PAYLOAD_BYTES]),
    )
    .await?;
    mount
        .commit(file.fh.clone(), 0, PAYLOAD_BYTES as u32)
        .await?;
    mount.close(file.fh.clone()).await?;
    Ok(file.fh)
}

async fn assert_payload(
    mount: &dyn nfs_rs::Mount,
    path: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let file = mount.lookup_path(path).await?;
    let attrs = mount.getattr(file.fh).await?;
    assert_eq!(
        attrs.filesize, PAYLOAD_BYTES as u64,
        "{path} must contain the replayed payload"
    );
    Ok(())
}

#[tokio::test]
async fn v3_stalled_write_retires_connection() -> Result<(), Box<dyn Error + Send + Sync>> {
    if !enabled() {
        eprintln!("skipping: set NFS_RS_TIMEOUT_RETIRE_E2E=1 with a local NFSv3 export");
        return Ok(());
    }
    let shared = shared_dir();
    let directory = PathBuf::from(&shared).join("timeout-retire-e2e-v3");
    prepare_dir(&directory)?;

    let proxy = FaultProxy::start("127.0.0.1:2049".parse()?).await?;
    let url = format!(
        "nfs://127.0.0.1{shared}?nfsport={}&mountport={MOUNT_PORT}",
        proxy.port()
    );
    let mount = nfs_rs::parse_url_and_mount(&url).await?;
    let prefix = "timeout-retire-e2e-v3";

    write_file(&*mount, &format!("{prefix}/warm.bin"), 1).await?;

    // Create first: `create_path` performs a LOOKUP, and arming earlier would
    // hit that metadata call instead of the WRITE we want to stall.
    let stalled = mount
        .create_path(&format!("{prefix}/stalled.bin"), Some(0o600))
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
    write_file(&*mount, &format!("{prefix}/recovered.bin"), 3).await?;
    let recovery_elapsed = started.elapsed();
    assert!(
        recovery_elapsed < Duration::from_secs(6),
        "the next file reused the stalled connection and took {recovery_elapsed:?}"
    );
    assert_payload(&*mount, &format!("{prefix}/recovered.bin")).await?;

    drop(mount);
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn v40_stalled_setattr_retires_connection() -> Result<(), Box<dyn Error + Send + Sync>> {
    if !enabled() {
        eprintln!("skipping: set NFS_RS_TIMEOUT_RETIRE_E2E=1 with a local NFSv4.0 server");
        return Ok(());
    }
    let shared = shared_dir();
    let prefix = "timeout-retire-e2e-v40";
    let directory = PathBuf::from(&shared).join(prefix);
    prepare_dir(&directory)?;

    let proxy = FaultProxy::start("127.0.0.1:2049".parse()?).await?;
    let url = format!(
        "nfs://127.0.0.1{}?version=4.0&nfsport={}",
        v4_root(),
        proxy.port()
    );
    let mount = nfs_rs::parse_url_and_mount(&url).await?;

    write_file(&*mount, &format!("{prefix}/warm.bin"), 1).await?;

    // `Mount40::settled_call` maps SETATTR to `ONE_ATTEMPT`, so a swallowed
    // metadata reply must retire the connection just like the v3 WRITE.
    let stalled = mount
        .create_path(&format!("{prefix}/stalled.bin"), Some(0o600))
        .await?;
    proxy.arm();
    let started = Instant::now();
    let error = mount
        .setattr(
            stalled.fh.clone(),
            None,
            Some(0o644),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect_err("the armed proxy must swallow the SETATTR reply");
    let stalled_elapsed = started.elapsed();
    assert_eq!(
        error.request_transmission(),
        Some(nfs_rs::RequestTransmission::Sent),
        "a swallowed v4.0 metadata reply must keep Sent evidence: {error:?}"
    );
    assert!(
        stalled_elapsed >= Duration::from_secs(9),
        "expected the request to reach its data timeout, took {stalled_elapsed:?}"
    );

    proxy.disarm();

    let started = Instant::now();
    write_file(&*mount, &format!("{prefix}/recovered.bin"), 3).await?;
    let recovery_elapsed = started.elapsed();
    assert!(
        recovery_elapsed < Duration::from_secs(6),
        "the next v4.0 file reused the stalled connection and took {recovery_elapsed:?}"
    );
    assert_payload(&*mount, &format!("{prefix}/recovered.bin")).await?;

    drop(mount);
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

#[tokio::test]
async fn v41_stalled_compound_preserves_evidence_and_rebinds()
-> Result<(), Box<dyn Error + Send + Sync>> {
    if !enabled() {
        eprintln!("skipping: set NFS_RS_TIMEOUT_RETIRE_E2E=1 with a local NFSv4.1 server");
        return Ok(());
    }
    let shared = shared_dir();
    let prefix = "timeout-retire-e2e-v41";
    let directory = PathBuf::from(&shared).join(prefix);
    prepare_dir(&directory)?;

    let proxy = FaultProxy::start("127.0.0.1:2049".parse()?).await?;
    let url = format!(
        "nfs://127.0.0.1{}?version=4.1&nfsport={}",
        v4_root(),
        proxy.port()
    );
    let mount = nfs_rs::parse_url_and_mount(&url).await?;

    write_file(&*mount, &format!("{prefix}/warm.bin"), 1).await?;

    // Every v4.1 COMPOUND uses `NFS_REPLAY`, so the transport must not retire
    // the link after the final timeout; it must still report Sent evidence.
    let stalled = mount
        .create_path(&format!("{prefix}/stalled.bin"), Some(0o600))
        .await?;
    proxy.arm();
    let started = Instant::now();
    let error = mount
        .setattr(
            stalled.fh.clone(),
            None,
            Some(0o644),
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect_err("the armed proxy must swallow the SETATTR reply");
    let stalled_elapsed = started.elapsed();
    assert_eq!(
        error.request_transmission(),
        Some(nfs_rs::RequestTransmission::Sent),
        "a swallowed v4.1 compound must keep Sent evidence: {error:?}"
    );
    assert!(
        stalled_elapsed >= Duration::from_secs(9),
        "expected replay attempts to exhaust their timeout, took {stalled_elapsed:?}"
    );

    proxy.disarm();
    // The half-open link is not retired for replayable calls. Let the reader
    // observe EOF so the session rebinds and the next request can be replayed.
    proxy.kill();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = Instant::now();
    write_file(&*mount, &format!("{prefix}/recovered.bin"), 3).await?;
    let recovery_elapsed = started.elapsed();
    assert!(
        recovery_elapsed < Duration::from_secs(6),
        "v4.1 session rebind did not recover in time: {recovery_elapsed:?}"
    );
    assert_payload(&*mount, &format!("{prefix}/recovered.bin")).await?;

    drop(mount);
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}

/// Request-level concurrency, not just parallel test cases: a replayable READ
/// and a `ONE_ATTEMPT` WRITE are in flight on the same stalled connection. The
/// WRITE times out and retires the link; the redial is then refused, so the
/// READ's next preflight fails. The READ was already sent and must keep `Sent`
/// evidence instead of being downgraded to `NotSent`.
#[tokio::test]
async fn v3_concurrent_retire_keeps_sent_evidence_when_redial_is_refused()
-> Result<(), Box<dyn Error + Send + Sync>> {
    if !enabled() {
        eprintln!("skipping: set NFS_RS_TIMEOUT_RETIRE_E2E=1 with a local NFSv3 export");
        return Ok(());
    }
    let shared = shared_dir();
    let prefix = "concurrent-retire-e2e-v3";
    let directory = PathBuf::from(&shared).join(prefix);
    prepare_dir(&directory)?;

    let proxy = FaultProxy::start("127.0.0.1:2049".parse()?).await?;
    let url = format!(
        "nfs://127.0.0.1{shared}?nfsport={}&mountport={MOUNT_PORT}",
        proxy.port()
    );
    let mount = nfs_rs::parse_url_and_mount(&url).await?;

    let readable = write_file(&*mount, &format!("{prefix}/read.bin"), 7).await?;
    let writable = mount
        .create_path(&format!("{prefix}/write.bin"), Some(0o600))
        .await?;

    proxy.arm();

    let started = Instant::now();
    // READ is replayable (`NFS_REPLAY`); WRITE is `ONE_ATTEMPT` and will retire
    // the connection when its swallowed reply times out.
    let read = mount.read(readable, 0, 4096);
    let write = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        nfs_rs::write_all(
            &*mount,
            writable.fh.clone(),
            0,
            Bytes::from(vec![9u8; PAYLOAD_BYTES]),
        )
        .await
    };
    // Refuse new connections while both requests are in flight; the existing
    // relayed connection keeps dropping replies.
    let refuse = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        proxy.refuse_redial();
    };
    let (read_result, write_result, ()) = tokio::join!(read, write, refuse);
    let elapsed = started.elapsed();

    let write_error = write_result.expect_err("the armed proxy must swallow the WRITE reply");
    assert_eq!(
        write_error.request_transmission(),
        Some(nfs_rs::RequestTransmission::Sent),
        "the retiring WRITE must be sent-but-unacknowledged: {write_error:?}"
    );

    let read_error = read_result.expect_err("the armed proxy must swallow the READ reply");
    assert_eq!(
        read_error.request_transmission(),
        Some(nfs_rs::RequestTransmission::Sent),
        "an already-sent replayable READ must not become NotSent when the retirement redial fails: {read_error:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(9),
        "expected both requests to reach their data timeout, took {elapsed:?}"
    );

    drop(mount);
    let _ = std::fs::remove_dir_all(&directory);
    Ok(())
}
