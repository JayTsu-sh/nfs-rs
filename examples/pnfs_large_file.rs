//! Run-scoped physical pNFS diagnostic. Use the lab lock and a unique file path.
//! The workload matches the 20 GiB AFX baseline: 64 MiB buffered windows, full
//! zero-payload read-back verification, durable close, and file removal.
//! Pass `pattern` after the size in GiB for a separate nonzero integrity run.
use bytes::Bytes;
use nfs_rs::{BufferedFile, Mount, OPEN_READ};
use std::error::Error;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

const GIB: u64 = 1024 * 1024 * 1024;
const WINDOW: usize = 64 * 1024 * 1024;

fn progress(phase: &str, completed: u64, total: u64) {
    println!(
        "{phase}_progress_gib={:.0}/{:.0}",
        completed as f64 / GIB as f64,
        total as f64 / GIB as f64
    );
    let _ = io::stdout().flush();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let mut args = std::env::args().skip(1);
    let url = args.next().ok_or("missing NFS URL")?;
    let path = args.next().ok_or("missing unique remote path")?;
    if !path.starts_with("release-") || path.contains('/') || path.contains("..") {
        return Err("remote path must be a unique release-* filename".into());
    }
    let total = args
        .next()
        .unwrap_or_else(|| "20".into())
        .parse::<u64>()?
        .checked_mul(GIB)
        .ok_or("file size overflow")?;
    let patterned = args.next().as_deref() == Some("pattern");
    let mount: Arc<dyn Mount> = Arc::from(nfs_rs::parse_url_and_mount(&url).await?);
    println!(
        "connected version={:?} max_read={} max_write={} pnfs={}",
        mount.version(),
        mount.get_max_read_size(),
        mount.get_max_write_size(),
        mount.capabilities().pnfs
    );
    let created = mount.create_path(&path, Some(0o600)).await?;
    let result = async {
        let writer = BufferedFile::new(mount.clone(), created.fh);
        let payload = if patterned {
            Bytes::from(
                (0..WINDOW)
                    .map(|index| ((index * 29 + 43) % 251) as u8)
                    .collect::<Vec<_>>(),
            )
        } else {
            Bytes::from(vec![0_u8; WINDOW])
        };
        println!(
            "payload_pattern={}",
            if patterned { "nonzero" } else { "zero" }
        );
        let write_started = Instant::now();
        let mut offset = 0_u64;
        while offset < total {
            let len = usize::try_from((total - offset).min(WINDOW as u64))?;
            writer.write_at(offset, payload.slice(..len)).await?;
            offset += len as u64;
            if offset.is_multiple_of(GIB) || offset == total {
                progress("write", offset, total);
            }
        }
        writer.close().await?;
        let write_seconds = write_started.elapsed().as_secs_f64();
        println!(
            "write_done seconds={write_seconds:.3} mib_s={:.1}",
            total as f64 / 1024.0 / 1024.0 / write_seconds
        );
        let opened = mount.open_path(&path, OPEN_READ).await?;
        let reader = BufferedFile::new(mount.clone(), opened.fh);
        let read_started = Instant::now();
        offset = 0;
        while offset < total {
            let len = usize::try_from((total - offset).min(WINDOW as u64))?;
            let data = reader.read_at(offset, len.try_into()?).await?;
            if data.len() != len {
                return Err(format!("short read at {offset}: {} != {len}", data.len()).into());
            }
            if data.as_ref() != &payload[..len] {
                return Err(format!("data mismatch at window starting {offset}").into());
            }
            offset += len as u64;
            if offset.is_multiple_of(GIB) || offset == total {
                progress("read", offset, total);
            }
        }
        reader.close().await?;
        let read_seconds = read_started.elapsed().as_secs_f64();
        println!(
            "read_done seconds={read_seconds:.3} mib_s={:.1} verified=true",
            total as f64 / 1024.0 / 1024.0 / read_seconds
        );
        Ok::<(), Box<dyn Error>>(())
    }
    .await;
    let remove = mount.remove_path(&path).await;
    let unmount = mount.umount().await;
    result?;
    remove?;
    unmount?;
    println!("test_status=pass");
    Ok(())
}
