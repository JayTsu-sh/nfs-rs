use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::task::JoinSet;

use super::backend::{Backend, BenchError, FileHandle, Result};
use super::pattern::{pattern_block, verify};
use super::stats::{Series, mibps};

type SharedHandle = Arc<Box<dyn FileHandle>>;

fn chunk_plan(size: u64, chunk: u64) -> u64 {
    size.div_ceil(chunk)
}

async fn close_shared(handle: SharedHandle) -> Result<()> {
    match Arc::try_unwrap(handle) {
        Ok(h) => h.close().await,
        Err(_) => Err(BenchError::Other("file handle still shared".into())),
    }
}

/// Writes `size` bytes of pattern data with `qd` concurrent in-flight chunks,
/// then syncs. Returns elapsed seconds (create → sync complete).
pub async fn write_file(b: &dyn Backend, path: &str, size: u64, qd: usize) -> Result<f64> {
    let chunk = b.chunk_size();
    let handle: SharedHandle = Arc::new(b.open_write(path).await?);
    let block = pattern_block();
    let next = Arc::new(AtomicU64::new(0));
    let total = chunk_plan(size, chunk);
    let started = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..qd {
        let (h, next, block) = (Arc::clone(&handle), Arc::clone(&next), block.clone());
        set.spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    return Ok::<(), BenchError>(());
                }
                let offset = i * chunk;
                let len = (size - offset).min(chunk) as usize;
                h.write_at(offset, block.slice(..len)).await?;
            }
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.map_err(|e| BenchError::Join(e.to_string()))??;
    }
    handle.sync().await?;
    let seconds = started.elapsed().as_secs_f64();
    close_shared(handle).await?;
    Ok(seconds)
}

/// Reads `size` bytes with `qd` concurrent in-flight chunks and verifies the
/// pattern inline. Returns elapsed seconds with verification time subtracted.
pub async fn read_file(b: &dyn Backend, path: &str, size: u64, qd: usize) -> Result<f64> {
    let chunk = b.chunk_size();
    let handle: SharedHandle = Arc::new(b.open_read(path).await?);
    let block: Bytes = pattern_block();
    let next = Arc::new(AtomicU64::new(0));
    let total = chunk_plan(size, chunk);
    let started = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..qd {
        let (h, next, block) = (Arc::clone(&handle), Arc::clone(&next), block.clone());
        set.spawn(async move {
            let mut verify_time = Duration::ZERO;
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    return Ok::<Duration, BenchError>(verify_time);
                }
                let offset = i * chunk;
                let len = (size - offset).min(chunk) as usize;
                let data = h.read_at(offset, len).await?;
                let v = Instant::now();
                if data.len() != len || !verify(&block, offset, &data) {
                    return Err(BenchError::Integrity(format!(
                        "chunk at offset {offset} mismatch ({} of {len} bytes)",
                        data.len()
                    )));
                }
                verify_time += v.elapsed();
            }
        });
    }
    let mut verify_total = Duration::ZERO;
    while let Some(joined) = set.join_next().await {
        verify_total += joined.map_err(|e| BenchError::Join(e.to_string()))??;
    }
    let elapsed = started.elapsed();
    let seconds = elapsed
        .saturating_sub(verify_total / qd.max(1) as u32)
        .as_secs_f64();
    close_shared(handle).await?;
    Ok(seconds)
}

fn record(series: &mut Series, small: bool, size: u64, seconds: f64) {
    series.samples.push(if small {
        seconds * 1000.0
    } else {
        mibps(size, seconds)
    });
}

/// Data suite. Small (≤ one chunk) sizes run `iters` files and report latency;
/// larger sizes run `repeat` files and report throughput. Returns the series
/// and whether the page cache was actually dropped before the cold reads.
pub async fn run(
    b: Arc<dyn Backend>,
    workdir: &str,
    size: u64,
    qd: usize,
    repeat: usize,
    iters: usize,
    hot_read: bool,
) -> Result<(Vec<Series>, bool)> {
    let small = size <= b.chunk_size();
    let count = if small { iters } else { repeat };
    let paths: Vec<String> = (0..count).map(|i| format!("{workdir}/f{i}.bin")).collect();
    let mut dropped = false;
    let result: Result<Vec<Series>> = async {
        let (w, r, h) = if small {
            ("write_ms", "read_ms", "read_hot_ms")
        } else {
            ("write", "read", "read_hot")
        };
        let mut write = if small {
            Series::ms(w)
        } else {
            Series::mibps(w)
        };
        let mut read = if small {
            Series::ms(r)
        } else {
            Series::mibps(r)
        };
        let mut hot = if small {
            Series::ms(h)
        } else {
            Series::mibps(h)
        };
        hot.reference_only = true;
        for p in &paths {
            let s = write_file(b.as_ref(), p, size, qd).await?;
            record(&mut write, small, size, s);
        }
        dropped = b.drop_caches().await?;
        for p in &paths {
            let s = read_file(b.as_ref(), p, size, qd).await?;
            record(&mut read, small, size, s);
        }
        let mut out = vec![write, read];
        if hot_read {
            for p in &paths {
                let s = read_file(b.as_ref(), p, size, qd).await?;
                record(&mut hot, small, size, s);
            }
            out.push(hot);
        }
        Ok(out)
    }
    .await;
    for p in &paths {
        let _ = b.remove(p).await;
    }
    Ok((result?, dropped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::IoMode;
    use crate::posix::PosixBackend;

    #[tokio::test]
    async fn small_and_chunked_roundtrip() {
        let dir = std::env::temp_dir().join(format!("perfcmp-data-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let b: Arc<dyn Backend> = Arc::new(PosixBackend::new(dir.clone(), IoMode::Buffered));
        b.mkdir("w").await.unwrap();
        let (small, _) = run(Arc::clone(&b), "w", 4096, 1, 1, 3, true).await.unwrap();
        assert_eq!(small[0].name, "write_ms");
        assert_eq!(small[0].samples.len(), 3);
        let (large, _) = run(Arc::clone(&b), "w", 3 * 1048576 + 4096, 8, 2, 1, true)
            .await
            .unwrap();
        let names: Vec<&str> = large.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["write", "read", "read_hot"]);
        assert_eq!(large[0].samples.len(), 2);
        assert!(large[2].reference_only);
        assert_eq!(std::fs::read_dir(dir.join("w")).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
