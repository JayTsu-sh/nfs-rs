use std::time::Instant;

use super::backend::{Backend, BenchError, Result};
use super::stats::Series;

fn ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

macro_rules! timed {
    ($series:expr, $call:expr) => {{
        let started = Instant::now();
        $call.await?;
        $series.samples.push(ms(started));
    }};
}

/// Runs every metadata operation `iters` times on unique names, then times
/// `readdir_iters` listings of a directory holding `readdir_entries` files.
/// The caller owns `workdir`; everything created here is removed again.
pub async fn run(
    b: &dyn Backend,
    workdir: &str,
    iters: usize,
    readdir_entries: usize,
    readdir_iters: usize,
) -> Result<Vec<Series>> {
    let names = [
        "mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir",
    ];
    let mut series: Vec<Series> = names.iter().map(|n| Series::ms(n)).collect();
    let m = format!("{workdir}/m");
    b.mkdir(&m).await?;
    for i in 0..iters {
        let d = format!("{m}/d{i}");
        let f = format!("{d}/f");
        let g = format!("{d}/g");
        timed!(series[0], b.mkdir(&d));
        timed!(series[1], b.create(&f));
        timed!(series[2], b.stat(&f));
        timed!(series[3], b.access(&f));
        timed!(series[4], b.chmod(&f, 0o644));
        timed!(series[5], b.rename(&f, &g));
        timed!(series[6], b.remove(&g));
        timed!(series[7], b.rmdir(&d));
    }
    b.rmdir(&m).await?;

    let big = format!("{workdir}/big");
    b.mkdir(&big).await?;
    for i in 0..readdir_entries {
        b.create(&format!("{big}/e{i}")).await?;
    }
    let mut readdir = Series::ms("readdir");
    let mut seen = readdir_entries;
    for _ in 0..readdir_iters {
        let started = Instant::now();
        seen = b.readdir_count(&big).await?;
        readdir.samples.push(ms(started));
        if seen != readdir_entries {
            break;
        }
    }
    for i in 0..readdir_entries {
        b.remove(&format!("{big}/e{i}")).await?;
    }
    b.rmdir(&big).await?;
    if seen != readdir_entries {
        return Err(BenchError::Integrity(format!(
            "readdir saw {seen} entries, expected {readdir_entries}"
        )));
    }
    series.push(readdir);
    Ok(series)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::IoMode;
    use crate::posix::PosixBackend;

    #[tokio::test]
    async fn produces_one_series_per_operation_and_cleans_up() {
        let dir = std::env::temp_dir().join(format!("perfcmp-meta-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let b = PosixBackend::new(dir.clone(), IoMode::Buffered);
        b.mkdir("w").await.unwrap();
        let series = run(&b, "w", 3, 5, 2).await.unwrap();
        let names: Vec<&str> = series.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir",
                "readdir"
            ]
        );
        assert!(series.iter().take(8).all(|s| s.samples.len() == 3));
        assert_eq!(series[8].samples.len(), 2);
        assert_eq!(std::fs::read_dir(dir.join("w")).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
