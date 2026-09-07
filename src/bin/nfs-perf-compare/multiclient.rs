use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Instant;

use tokio::task::spawn_blocking;

use super::backend::{Backend, BenchError, Result};
use super::cli::{ClientMode, Config};
use super::data::write_file;
use super::stats::{Series, mibps};

/// Spawns `clients` independent worker processes (each with its own
/// connection) that read a full `size`-byte file concurrently. Returns the
/// aggregate/per-client series and the largest worker peak RSS in KiB.
pub async fn run(
    b: Arc<dyn Backend>,
    cfg: &Config,
    size: u64,
    clients: usize,
    mode: ClientMode,
    repeat: usize,
) -> Result<(Vec<Series>, u64)> {
    let files = if mode == ClientMode::Same { 1 } else { clients };
    let paths: Vec<String> = (0..files)
        .map(|i| format!("{}/mc{i}.bin", cfg.workdir))
        .collect();
    let mut max_rss = 0u64;
    let result: Result<Vec<Series>> = async {
        for p in &paths {
            write_file(b.as_ref(), p, size, 8).await?;
        }
        let mut aggregate = Series::mibps("aggregate_read");
        let mut per_client = Series::mibps("per_client_read");
        let exe = std::env::current_exe()?;
        let target = cfg.target.as_arg();
        let bytes = size.to_string();
        for _ in 0..repeat {
            b.drop_caches().await?;
            let started = Instant::now();
            let mut children = Vec::with_capacity(clients);
            for c in 0..clients {
                let child = Command::new(&exe)
                    .args([
                        "--target",
                        &target,
                        "--io",
                        cfg.io.as_str(),
                        "--workdir",
                        &cfg.workdir,
                        "worker-read",
                        "--path",
                        &paths[c % files],
                        "--bytes",
                        &bytes,
                        "--qd",
                        "1",
                    ])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()?;
                children.push(child);
            }
            for child in children {
                let out = spawn_blocking(move || child.wait_with_output())
                    .await
                    .map_err(|e| BenchError::Join(e.to_string()))??;
                if !out.status.success() {
                    return Err(BenchError::Other(format!(
                        "worker failed: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    )));
                }
                let v: serde_json::Value = serde_json::from_slice(&out.stdout)
                    .map_err(|e| BenchError::Other(format!("worker output: {e}")))?;
                let seconds = v["seconds"].as_f64().unwrap_or(f64::NAN);
                per_client.samples.push(mibps(size, seconds));
                max_rss = max_rss.max(v["peak_rss_kib"].as_u64().unwrap_or(0));
            }
            aggregate.samples.push(mibps(
                size * clients as u64,
                started.elapsed().as_secs_f64(),
            ));
        }
        Ok(vec![aggregate, per_client])
    }
    .await;
    for p in &paths {
        let _ = b.remove(p).await;
    }
    Ok((result?, max_rss))
}
