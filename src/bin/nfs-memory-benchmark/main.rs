//! Opt-in Linux memory diagnostic. Run only inside a prepared lab run directory.
mod metrics;
mod pattern;

use futures::{StreamExt, stream};
use nfs_rs::{BufferedFile, Mount, NFSVersion, OPEN_READ};
use pattern::{MIB, Pattern};
use serde_json::{Value, json};
use std::io::Write;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOCATOR: metrics::TrackingAllocator = metrics::TrackingAllocator;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

const USAGE: &str = "nfs-memory-benchmark --url NFS_URL --run-id release-ID --case NAME \
    [--mode write-read|write|read] [--mib 1024] [--qd 8] [--rounds 3] \
    [--wait-for-stdin] [--validate-only]\n\
    write creates a fresh case directory and retains its seed for read processes.\n\
    Caller must prepare/clean the run directory and hold shared lab locks.";

struct Config {
    url: String,
    version: String,
    run_id: String,
    case: String,
    mode: String,
    size: u64,
    qd: usize,
    rounds: usize,
    wait_for_stdin: bool,
    validate_only: bool,
}

fn leaf(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

fn parse(args: impl Iterator<Item = String>) -> Result<Config> {
    let mut args = args;
    let mut url = None;
    let mut run_id = None;
    let mut case = None;
    let mut mode = "write-read".to_string();
    let mut mib = 1024u64;
    let mut qd = 8usize;
    let mut rounds = 3usize;
    let mut wait_for_stdin = false;
    let mut validate_only = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--wait-for-stdin" => wait_for_stdin = true,
            "--validate-only" => validate_only = true,
            "--url" => url = Some(args.next().ok_or("--url needs a value")?),
            "--run-id" => run_id = Some(args.next().ok_or("--run-id needs a value")?),
            "--case" => case = Some(args.next().ok_or("--case needs a value")?),
            "--mode" => mode = args.next().ok_or("--mode needs a value")?,
            "--mib" => mib = args.next().ok_or("--mib needs a value")?.parse()?,
            "--qd" => qd = args.next().ok_or("--qd needs a value")?.parse()?,
            "--rounds" => rounds = args.next().ok_or("--rounds needs a value")?.parse()?,
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let url = url.ok_or("--url is required")?;
    let parsed = url::Url::parse(&url)?;
    if parsed.scheme() != "nfs"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err("an NFS URL without embedded credentials is required".into());
    }
    let versions: Vec<_> = parsed
        .query_pairs()
        .filter(|(k, _)| k == "version")
        .collect();
    if versions.len() != 1 || !matches!(versions[0].1.as_ref(), "3" | "4.0" | "4.1") {
        return Err("URL must select one exact version=3, 4.0 or 4.1".into());
    }
    let version = versions[0].1.to_string();
    let run_id = run_id.ok_or("--run-id is required")?;
    let suffix = run_id
        .strip_prefix("release-")
        .or_else(|| run_id.strip_prefix("nightly-"));
    if !suffix.is_some_and(leaf) {
        return Err("run ID must be a unique release-* or nightly-* identifier".into());
    }
    let case = case.ok_or("--case is required")?;
    if !leaf(&case)
        || !matches!(mode.as_str(), "write-read" | "write" | "read")
        || !(1..=64).contains(&qd)
        || rounds == 0
        || mib == 0
    {
        return Err("invalid case, mode, QD (1..64), rounds or payload".into());
    }
    if wait_for_stdin && mode != "read" {
        return Err("--wait-for-stdin is only supported by read mode".into());
    }
    let size = mib
        .checked_mul(MIB as u64)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or("payload size overflow")?;
    Ok(Config {
        url,
        version,
        run_id,
        case,
        mode,
        size,
        qd,
        rounds,
        wait_for_stdin,
        validate_only,
    })
}

impl Config {
    fn json(&self) -> Value {
        json!({"url": self.url, "protocol": self.version, "run_id": self.run_id,
            "case": self.case, "mode": self.mode, "payload_bytes": self.size,
            "qd": self.qd, "rounds": self.rounds})
    }
}

async fn write_file(
    mount: &Arc<dyn Mount>,
    path: &str,
    config: &Config,
    pattern: &Pattern,
) -> Result<()> {
    let object = mount.create_path(path, Some(0o600)).await?;
    let fh = object.fh;
    let result: Result<()> = async {
        let chunk = (mount.get_max_write_size() as usize).clamp(1, MIB);
        let mut offset = 0;
        while offset < config.size {
            let end = config.size.min(offset + (chunk * config.qd) as u64);
            // Settle siblings even after an error, then commit this bounded batch.
            let results = stream::iter((offset..end).step_by(chunk))
                .map(|at| {
                    let fh = fh.clone();
                    async move {
                        let len = (end - at).min(chunk as u64) as usize;
                        let data = pattern.at(at, len);
                        let mut done = 0;
                        let mut writes = Vec::new();
                        while done < len {
                            let reply = mount
                                .write(fh.clone(), at + done as u64, data.slice(done..))
                                .await?;
                            let n = reply.count as usize;
                            if n == 0 || n > len - done {
                                return Err("invalid short WRITE count".into());
                            }
                            done += n;
                            writes.push(reply);
                        }
                        Ok::<_, Error>(writes)
                    }
                })
                .buffer_unordered(config.qd)
                .collect::<Vec<_>>()
                .await;
            let writes = results
                .into_iter()
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            mount
                .commit_write_batch(fh.clone(), offset, (end - offset) as u32, &writes)
                .await?;
            offset = end;
        }
        Ok(())
    }
    .await;
    let closed = mount.close(fh).await;
    result?;
    closed?;
    Ok(())
}

async fn read_round(
    file: &Arc<BufferedFile>,
    config: &Config,
    pattern: &Arc<Pattern>,
    chunk: usize,
) -> Result<()> {
    let next = Arc::new(AtomicU64::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..config.qd {
        let (file, pattern, next) = (file.clone(), pattern.clone(), next.clone());
        let size = config.size;
        tasks.spawn(async move {
            loop {
                let at = next.fetch_add(chunk as u64, Relaxed);
                if at >= size {
                    break;
                }
                let len = (size - at).min(chunk as u64) as u32;
                let data = file.read_at(at, len).await?;
                if data.len() != len as usize || !pattern.verify(at, &data) {
                    return Err(format!("content mismatch at {at}").into());
                }
            }
            Ok::<_, Error>(())
        });
    }
    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        let result = result.map_err(|e| -> Error { e.into() }).and_then(|r| r);
        if failure.is_none() {
            failure = result.err();
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    if !file.read_at(config.size, 1).await?.is_empty() {
        return Err("file has unexpected data past the requested payload".into());
    }
    Ok(())
}

async fn workload(mount: &Arc<dyn Mount>, config: &Config, phases: &mut Vec<Value>) -> Result<()> {
    let directory = format!("{}/{}", config.run_id, config.case);
    let path = format!("{directory}/payload.bin");
    let pattern = Arc::new(Pattern::new());
    if config.mode != "read" {
        // A collision must fail before touching an existing seed or another run.
        mount.mkdir_path(&directory, 0o700).await?;
        let phase = metrics::start();
        let started = Instant::now();
        write_file(mount, &path, config, &pattern).await?;
        phases.push(metrics::sample(
            "write",
            &phase,
            started.elapsed().as_secs_f64(),
        )?);
    }
    if config.mode != "write" {
        let object = mount.open_path(&path, OPEN_READ).await?;
        let file = Arc::new(BufferedFile::new(mount.clone(), object.fh));
        let result: Result<()> = async {
            if config.wait_for_stdin {
                println!("{}", json!({"status":"ready"}));
                std::io::stdout().flush()?;
                let mut line = String::new();
                std::io::stdin().read_line(&mut line)?;
                if line.trim() != "go" {
                    return Err("reader barrier was not released".into());
                }
            }
            let chunk = (mount.get_max_read_size() as usize).clamp(1, MIB);
            for round in 0..config.rounds {
                let phase = metrics::start();
                let started = Instant::now();
                read_round(&file, config, &pattern, chunk).await?;
                phases.push(metrics::sample(
                    &format!("read-{round}"),
                    &phase,
                    started.elapsed().as_secs_f64(),
                )?);
                tokio::time::sleep(Duration::from_millis(100)).await;
                phases.push(metrics::sample(&format!("idle-{round}"), &phase, 0.0)?);
            }
            Ok(())
        }
        .await;
        let closed = file.close().await;
        result?;
        closed?;
    }
    Ok(())
}

async fn run(config: Config) -> Result<bool> {
    if config.validate_only {
        println!(
            "{}",
            json!({"status":"configuration_valid", "config":config.json()})
        );
        return Ok(true);
    }
    if std::env::consts::OS != "linux" {
        return Err("memory measurements require Linux /proc/self/status".into());
    }
    let phase = metrics::start();
    let baseline_large = metrics::large_live();
    let mut phases = vec![metrics::sample("runtime", &phase, 0.0)?];
    let mount: Arc<dyn Mount> = Arc::from(nfs_rs::parse_url_and_mount(&config.url).await?);
    let version = match mount.version() {
        NFSVersion::NFSv3 => "3",
        NFSVersion::NFSv4p0 => "4.0",
        NFSVersion::NFSv4p1 => "4.1",
        _ => "unsupported",
    };
    let limits = json!({"read":mount.get_max_read_size(), "write":mount.get_max_write_size()});
    phases.push(metrics::sample("mounted", &phase, 0.0)?);
    let result = if version == config.version {
        workload(&mount, &config, &mut phases).await
    } else {
        Err("unexpected negotiated protocol".into())
    };
    let unmounted = mount.umount().await;
    drop(mount);
    result?;
    unmounted?;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if metrics::large_live() <= baseline_large || Instant::now() >= deadline {
            break;
        }
    }
    let phase = metrics::start();
    phases.push(metrics::sample("unmounted", &phase, 0.0)?);
    let released = metrics::large_live() <= baseline_large;
    println!(
        "{}",
        json!({"schema_version":1, "status":if released {"pass"} else {"retained_large_allocations"},
        "config":config.json(), "io_limits":limits, "phases":phases,
        "large_allocations_released":released, "rust_version_requirement":env!("CARGO_PKG_RUST_VERSION"),
        "nfs_rs_version":env!("CARGO_PKG_VERSION"),
        "allocator_environment": {
            "MALLOC_ARENA_MAX":std::env::var("MALLOC_ARENA_MAX").ok(),
            "MALLOC_MMAP_THRESHOLD_":std::env::var("MALLOC_MMAP_THRESHOLD_").ok(),
            "TOKIO_WORKER_THREADS":std::env::var("TOKIO_WORKER_THREADS").ok()}})
    );
    Ok(released)
}

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match parse(std::env::args().skip(1)) {
        Ok(config) => match run(config).await {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::from(2),
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("{error}\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
