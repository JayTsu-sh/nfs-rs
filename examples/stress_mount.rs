// Diagnostic: fire N concurrent mounts and report per-task result + timing.
// Used to investigate the "32 concurrent mount partial-failure" symptom — captures
// the *actual* error variant for each failure instead of relying on a code
// comment's claim.
//
// Run from inside nfs-rs/:
//   cargo run --release --example stress_mount -- nfs://10.131.9.13/export/nfs 32

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: stress_mount <nfs-url> <N>");
        std::process::exit(2);
    }
    let url = args[1].clone();
    let n: usize = args[2].parse().expect("N must be a number");

    println!("stress_mount: url={url} N={n}");
    let start = Instant::now();
    let started = Arc::new(AtomicUsize::new(0));
    let mut set: JoinSet<(usize, Duration, std::result::Result<(), String>)> = JoinSet::new();

    for i in 0..n {
        let url = url.clone();
        let started = Arc::clone(&started);
        set.spawn(async move {
            started.fetch_add(1, Ordering::Relaxed);
            let t0 = Instant::now();
            let res = nfs_rs::parse_url_and_mount(&url).await;
            let dt = t0.elapsed();
            match res {
                Ok(mount) => {
                    drop(mount);
                    (i, dt, Ok(()))
                }
                Err(e) => (i, dt, Err(format!("{e:?}"))),
            }
        });
    }

    let mut oks = 0usize;
    let mut fails: Vec<(usize, Duration, String)> = Vec::new();
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((i, dt, Ok(()))) => {
                println!("OK   #{i:03} {:>7.3}ms", dt.as_secs_f64() * 1000.0);
                oks += 1;
            }
            Ok((i, dt, Err(msg))) => {
                println!("FAIL #{i:03} {:>7.3}ms {msg}", dt.as_secs_f64() * 1000.0);
                fails.push((i, dt, msg));
            }
            Err(e) => {
                println!("JOIN_ERR {e:?}");
            }
        }
    }

    let total = start.elapsed();
    println!("--- summary ---");
    println!(
        "N={n}  ok={oks}  fail={}  total_wall={:.3}s",
        fails.len(),
        total.as_secs_f64()
    );
    if !fails.is_empty() {
        println!("--- failure modes (deduped) ---");
        let mut buckets: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for (_, _, msg) in &fails {
            *buckets.entry(msg.clone()).or_insert(0) += 1;
        }
        for (msg, count) in buckets {
            println!("{count:>4}x  {msg}");
        }
    }
}
