mod backend;
mod cli;
mod data;
mod metadata;
mod multiclient;
mod nfsrs;
mod pattern;
mod posix;
mod stats;

use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use backend::{Backend, BenchError, Result};
use cli::{Config, IoMode, Suite, Target};
use stats::Series;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match cli::parse_args(env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn connect(config: &Config) -> Result<Arc<dyn Backend>> {
    Ok(match &config.target {
        Target::Nfs(url) => Arc::new(nfsrs::NfsRsBackend::connect(url).await?),
        Target::Posix(root) => Arc::new(posix::PosixBackend::new(root.clone(), config.io)),
    })
}

fn proc_line(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
}

/// (series, params, worker peak RSS, caches dropped)
type SuiteOutcome = (Vec<Series>, Value, u64, bool);

async fn run_suite(config: &Config, b: &Arc<dyn Backend>, is_posix: bool) -> Result<SuiteOutcome> {
    match &config.suite {
        Suite::Metadata {
            iters,
            readdir_entries,
            readdir_iters,
        } => {
            let series = metadata::run(
                b.as_ref(),
                &config.workdir,
                *iters,
                *readdir_entries,
                *readdir_iters,
            )
            .await?;
            let params = json!({
                "iters": iters,
                "readdir_entries": readdir_entries,
                "readdir_iters": readdir_iters,
            });
            Ok((series, params, 0, false))
        }
        Suite::Data {
            size,
            size_label,
            qd,
            repeat,
            iters,
        } => {
            let hot_read = is_posix && config.io == IoMode::Buffered;
            let (series, dropped) = data::run(
                Arc::clone(b),
                &config.workdir,
                *size,
                *qd,
                *repeat,
                *iters,
                hot_read,
            )
            .await?;
            let params = json!({
                "size": size_label,
                "bytes": size,
                "qd": qd,
                "repeat": repeat,
                "iters": iters,
            });
            Ok((series, params, 0, dropped))
        }
        Suite::Multiclient {
            size,
            size_label,
            clients,
            mode,
            repeat,
        } => {
            let (series, rss) =
                multiclient::run(Arc::clone(b), config, *size, *clients, *mode, *repeat).await?;
            let params = json!({
                "size": size_label,
                "bytes": size,
                "clients": clients,
                "mode": mode.as_str(),
                "repeat": repeat,
            });
            Ok((series, params, rss, false))
        }
        Suite::WorkerRead { .. } => {
            Err(BenchError::Other("worker-read is handled by run()".into()))
        }
    }
}

async fn run(config: Config) -> Result<()> {
    let b = connect(&config).await?;
    if let Suite::WorkerRead { path, bytes, qd } = &config.suite {
        let seconds = data::read_file(b.as_ref(), path, *bytes, *qd).await?;
        b.shutdown().await?;
        println!(
            "{}",
            json!({"seconds": seconds, "bytes": bytes, "peak_rss_kib": stats::peak_rss_kib()})
        );
        return Ok(());
    }
    let is_posix = matches!(config.target, Target::Posix(_));
    b.mkdir(&config.workdir).await?;
    let outcome = run_suite(&config, &b, is_posix).await;
    let _ = b.rmdir(&config.workdir).await;
    let (series, params, worker_rss, dropped) = outcome?;
    let info = b.info();
    b.shutdown().await?;
    let captured_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let report = json!({
        "schema": 1,
        "harness": "rust",
        "backend": info.backend,
        "protocol": info.protocol.or_else(|| env::var("PERF_PROTOCOL").ok()),
        "target": config.target.as_arg(),
        "mount_variant": if is_posix { env::var("PERF_MOUNT_VARIANT").ok() } else { None },
        "io_mode": if is_posix { Some(config.io.as_str()) } else { None },
        "suite": config.suite.name(),
        "smoke": config.smoke,
        "params": params,
        "env": {
            "hostname": proc_line("/proc/sys/kernel/hostname"),
            "kernel": proc_line("/proc/sys/kernel/osrelease"),
            "nfs_rs_version": env!("CARGO_PKG_VERSION"),
            "commit": env::var("PERF_COMMIT").ok(),
            "rsize": info.rsize,
            "wsize": info.wsize,
            "captured_at_unix": captured_at,
            "drop_caches": dropped,
        },
        "peak_rss_kib": stats::peak_rss_kib().unwrap_or(0).max(worker_rss),
        "results": series.iter().map(stats::series_json).collect::<Vec<_>>(),
    });
    let text =
        serde_json::to_string_pretty(&report).map_err(|e| BenchError::Other(e.to_string()))?;
    if config.json.as_os_str() == "/dev/stdout" {
        println!("{text}");
    } else {
        std::fs::write(&config.json, text)?;
    }
    Ok(())
}
