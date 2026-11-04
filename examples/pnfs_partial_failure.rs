//! Run-scoped AFX T15 probe, driven by tests/lab/afx_t15.py.
//! RFC 8881 §§13.4.4, 2.10.6.1: preserve uncertain striped WRITE outcomes.
use bytes::Bytes;
use nfs_rs::{
    BufferedFile, Mount, NFSVersion, OPEN_BOTH, OPEN_READ, OperationOutcome, RecoveryAction,
};
use serde_json::{Value, json};
use std::error::Error;
use std::io::{self, BufRead, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;
const WINDOW: usize = 8 * 1024 * 1024;
const TOTAL: u64 = 20 * 1024 * 1024 * 1024;
const WRITE: usize = 65536;
const LEAF: &str = "payload.bin";

struct Pattern(Bytes);
impl Pattern {
    fn new() -> Self {
        Self(Bytes::from(
            (0..WINDOW + 251)
                .map(|i| ((i % 251 * 29) % 251 + 1) as u8)
                .collect::<Vec<_>>(),
        ))
    }
    fn at(&self, offset: u64, len: usize) -> Bytes {
        let start = (offset % 251) as usize;
        self.0.slice(start..start + len)
    }
}

fn event(mut value: Value) -> Result {
    value["unix_ns"] = json!(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos()
            .to_string()
    );
    let mut out = io::stdout().lock();
    writeln!(out, "{value}")?;
    out.flush()?;
    Ok(())
}

async fn connect(url: &str) -> Result<Arc<dyn Mount>> {
    let mount: Arc<dyn Mount> = Arc::from(nfs_rs::parse_url_and_mount(url).await?);
    if mount.version() != NFSVersion::NFSv4p1 || mount.get_max_write_size() < WRITE as u32 {
        return Err("T15 requires exact NFSv4.1 and a negotiated WRITE of at least 64 KiB".into());
    }
    event(json!({"phase":"connected", "pid":std::process::id(),
        "pnfs":mount.capabilities().pnfs, "write_max":mount.get_max_write_size()}))?;
    Ok(mount)
}

async fn seed(mount: Arc<dyn Mount>, pattern: &Pattern) -> Result {
    let created = mount.create_path(LEAF, Some(0o600)).await?;
    let file = BufferedFile::new(mount.clone(), created.fh);
    let mut offset = 0;
    while offset < TOTAL {
        let len = (TOTAL - offset).min(WINDOW as u64) as usize;
        file.write_at(offset, pattern.at(offset, len)).await?;
        offset += len as u64;
        if offset.is_multiple_of(1024 * 1024 * 1024) {
            event(json!({"phase":"seed", "bytes":offset}))?;
        }
    }
    file.close().await?;
    mount.umount().await?;
    event(json!({"phase":"seed_done", "bytes":TOTAL}))
}

async fn verify(url: &str, pattern: &Pattern) -> Result {
    let mount = connect(url).await?;
    let opened = mount.open_path(LEAF, OPEN_READ).await?;
    if mount.getattr(opened.fh.clone()).await?.filesize != TOTAL {
        return Err("recovered file size differs from the seeded 20 GiB".into());
    }
    let file = BufferedFile::new(mount.clone(), opened.fh);
    // coreutils is pre-provisioned on the Linux executor. Feed the actual READ
    // bytes, not the expected pattern, to produce an independently checked hash.
    let mut hasher = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut input = hasher.stdin.take().ok_or("missing checksum input")?;
    let mut offset = 0;
    while offset < TOTAL {
        let len = (TOTAL - offset).min(WINDOW as u64) as usize;
        let actual = file.read_at(offset, len as u32).await?;
        if actual != pattern.at(offset, len) {
            return Err(format!("full-file verification failed at offset {offset}").into());
        }
        input.write_all(&actual)?;
        offset += len as u64;
        if offset.is_multiple_of(1024 * 1024 * 1024) {
            event(json!({"phase":"verify", "bytes":offset}))?;
        }
    }
    drop(input);
    let digest = hasher.wait_with_output()?;
    if !digest.status.success() {
        return Err("checksum helper failed".into());
    }
    let text = String::from_utf8(digest.stdout)?;
    let hash = text.split_whitespace().next().ok_or("missing checksum")?;
    if hash.len() != 64 || !hash.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid checksum helper output".into());
    }
    file.close().await?;
    mount.umount().await?;
    event(json!({"phase":"verify_done", "bytes":TOTAL, "sha256":hash}))
}

async fn interactive(mount: Arc<dyn Mount>, url: &str, pattern: &Pattern) -> Result {
    let opened = mount.open_path(LEAF, OPEN_BOTH).await?;
    let fh = opened.fh;
    event(json!({"phase":"ready"}))?;
    for line in io::stdin().lock().lines() {
        let line = line?;
        let args: Vec<_> = line.split_whitespace().collect();
        match args.as_slice() {
            ["close"] => {
                mount.close(fh.clone()).await?;
                event(json!({"phase":"close", "ok":true}))?;
                mount.umount().await?;
                return event(json!({"phase":"umount", "ok":true}));
            }
            ["verify"] => {
                mount.close(fh.clone()).await?;
                mount.umount().await?;
                return verify(url, pattern).await;
            }
            [kind @ ("warm" | "fault" | "resume" | "inspect"), offset] => {
                let offset: u64 = offset.parse()?;
                if offset
                    .checked_add(WRITE as u64)
                    .is_none_or(|end| end > TOTAL)
                {
                    return Err("requested range is outside the owned file".into());
                }
                let expected = pattern.at(offset, WRITE);
                if *kind == "inspect" {
                    let mut data = Vec::with_capacity(WRITE);
                    while data.len() < WRITE {
                        let bytes = mount
                            .read(
                                fh.clone(),
                                offset + data.len() as u64,
                                (WRITE - data.len()) as u32,
                            )
                            .await?;
                        if bytes.is_empty() || bytes.len() > WRITE - data.len() {
                            return Err("invalid read while inspecting partial mutation".into());
                        }
                        data.extend_from_slice(&bytes);
                    }
                    let classify = |range: std::ops::Range<usize>| {
                        if range.clone().all(|i| data[i] == (expected[i] ^ 0xa5)) {
                            "fault"
                        } else if range.clone().all(|i| data[i] == expected[i]) {
                            "seed"
                        } else {
                            "other"
                        }
                    };
                    event(json!({"phase":"inspect", "first":classify(0..WRITE/2),
                        "second":classify(WRITE/2..WRITE)}))?;
                    continue;
                }
                let data = if *kind == "fault" {
                    Bytes::from(expected.iter().map(|b| b ^ 0xa5).collect::<Vec<_>>())
                } else {
                    expected
                };
                event(json!({"phase":"write_begin", "kind":kind,
                    "offset":offset, "length":WRITE}))?;
                match mount.write(fh.clone(), offset, data).await {
                    Ok(written) => {
                        let count = written.count;
                        mount
                            .commit_write_batch(fh.clone(), offset, count, &[written])
                            .await?;
                        event(json!({"phase":"write_done", "kind":kind, "count":count}))?;
                    }
                    Err(error) => {
                        let outcome = error.operation_outcome();
                        // Structured booleans only: no FH, session identifier or payload.
                        event(json!({"phase":"write_error", "kind":kind,
                            "uncertain":outcome.is_some_and(|o| o.outcome == OperationOutcome::Uncertain),
                            "verify_then_resume":outcome.is_some_and(|o| o.recovery == RecoveryAction::VerifyThenResume)}))?;
                        if *kind != "fault" {
                            return Err(error.into());
                        }
                    }
                }
            }
            _ => return Err("invalid diagnostic command".into()),
        }
    }
    Err("controller disconnected before explicit CLOSE".into())
}

async fn run() -> Result {
    tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_ansi(false)
        .with_writer(io::stderr)
        .init();
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: pnfs_partial_failure <seed|probe> <url> <run-id>".into());
    }
    let run = &args[3];
    if !(run.starts_with("nightly-") || run.starts_with("release-"))
        || run.len() > 88
        || !run
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
    {
        return Err("invalid run ID".into());
    }
    let url = url::Url::parse(&args[2])?;
    if !url.username().is_empty()
        || url.password().is_some()
        || !url.path().ends_with(&format!("/{run}"))
    {
        return Err("URL must select only the owned run directory".into());
    }
    let mount = connect(&args[2]).await?;
    let pattern = Pattern::new();
    match args[1].as_str() {
        "seed" => seed(mount, &pattern).await,
        "probe" => interactive(mount, &args[2], &pattern).await,
        _ => Err("unknown diagnostic mode".into()),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("T15 probe failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
