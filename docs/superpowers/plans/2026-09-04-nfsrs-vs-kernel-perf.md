# nfs-rs vs 内核 mount 性能对比基准 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建 Rust + Python 两套同 CLI/同 JSON 的对比 harness，在 node181 上对 FAS2750 跑 v3/v4.0/v4.1 × 元数据/4 KiB/40 MiB/1 GiB × QD1/QD8 × 多客户端矩阵，产出 Markdown + HTML 报告。

**Architecture:** `src/bin/nfs-perf-compare/` 是一个多模块 Rust bin：`Backend` trait 抽象 nfs-rs 直连与 POSIX 内核挂载，三个 suite（metadata/data/multiclient）只依赖 trait；`tests/benchmarks/compare/perf_compare.py` 用同样的结构镜像一份 Python 实现；`run.sh` 在 node181 上驱动矩阵，`report.py` 汇总 JSON 成报告。

**Tech Stack:** Rust 2024 / tokio / bytes / serde_json / nix::libc（已有依赖，不新增）；Python 3.11 标准库 + `nfs_rs` 0.6.1 wheel；ONTAP REST API（`urllib`）。

**Spec:** `docs/superpowers/specs/2026-09-04-nfsrs-vs-kernel-perf-design.md`

## Global Constraints

- 生产代码禁止 `.unwrap()` / `.expect()`（测试代码除外）；错误用 `thiserror` 枚举。
- `use` 全部置于文件顶部；表达式内路径不超过三层。
- 不新增 Cargo 依赖；`O_DIRECT` 取 `nix::libc::O_DIRECT`。
- 不修改 `src/bin/nfs-storage-benchmark.rs`、`tests/benchmarks/baselines/**` 及现有 gate 脚本。
- 数据传输用 `Bytes`，不 clone `Vec<u8>`；共享计数用 `AtomicU64`。
- 所有工作在分支 `perf/nfsrs-vs-kernel` 上提交（不直接提交到 `main`）。
- node181 无法访问 crates.io：构建必须 `cargo build --release --offline`，registry 由本机 rsync。
- 存储凭据通过环境变量 `ONTAP_USER` / `ONTAP_PASS` 传入，绝不写入仓库。
- 大小映射：`4k`=4096、`40m`=41943040、`1g`=1073741824 字节；分块 1 MiB=1048576。
- 数据模式：1 MiB 周期块，`block[i] = (i*17+29) % 251`，Rust/Python 必须一致。

---

## File Structure

```
src/bin/nfs-perf-compare/
  main.rs        入口：解析 CLI → 建 backend → 跑 suite → 写 JSON；--worker-read 子进程入口
  cli.rs         Config / Suite / IoMode / 参数解析（纯函数，可单测）
  stats.rs       percentile / summary / Series → JSON
  pattern.rs     1 MiB 模式块、对齐缓冲、chunk 校验
  backend.rs     Backend / FileHandle trait + BackendInfo
  posix.rs       内核挂载 backend（std::fs + spawn_blocking，O_DIRECT）
  nfsrs.rs       nfs-rs backend（Box<dyn Mount>）
  metadata.rs    metadata suite
  data.rs        data suite（QD 并发、drop_caches、热读）
  multiclient.rs multiclient suite（spawn 自身为 worker）
tests/nfs_perf_compare_cli.rs      bin 集成测试（posix backend 对 tmpdir 跑 --smoke）
tests/benchmarks/compare/
  perf_compare.py                  Python harness（同 CLI、同 JSON）
  test_perf_compare.py             pytest：posix backend 对 tmpdir
  report.py                        results → Markdown + HTML
  test_report.py                   pytest：合成 JSON → 报告片段断言
  ontap_prepare.py                 REST 准备/回滚（--dry-run 可单测）
  test_ontap_prepare.py
  run.sh                           node181 矩阵驱动
  deploy.sh                        本机 → node181 同步 + 离线构建 + venv
docs/benchmarks/fas2750-nfsrs-vs-kernel-2026-09-04.md   最终报告
```

---

### Task 1: 分支、Rust bin 骨架、CLI 解析与统计

**Files:**
- Create: `src/bin/nfs-perf-compare/main.rs`, `cli.rs`, `stats.rs`
- Modify: `Cargo.toml`（无需改：`src/bin/<name>/main.rs` 自动发现）

**Interfaces:**
- Produces: `cli::Config { target: Target, workdir: String, json: PathBuf, suite: Suite, io: IoMode, smoke: bool }`,
  `enum Target { Nfs(String), Posix(PathBuf) }`,
  `enum Suite { Metadata { iters: usize, readdir_entries: usize, readdir_iters: usize }, Data { size: u64, size_label: String, qd: usize, repeat: usize, iters: usize }, Multiclient { size: u64, size_label: String, clients: usize, mode: ClientMode, repeat: usize }, WorkerRead { path: String, qd: usize } }`,
  `enum IoMode { Direct, Buffered }`, `enum ClientMode { Same, Distinct }`,
  `fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, CliError>`,
  `stats::Series { name: String, unit: Unit, samples: Vec<f64>, reference_only: bool }`, `enum Unit { Ms, MiBps }`,
  `fn series_json(s: &Series) -> serde_json::Value`, `fn percentile(v: &[f64], p: f64) -> f64`.

- [ ] **Step 1: 建分支**

```bash
git checkout -b perf/nfsrs-vs-kernel
```

- [ ] **Step 2: 写 `cli.rs` 的失败测试（放在文件底部 `#[cfg(test)]`）**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Config, CliError> {
        parse_args(s.split_whitespace().map(str::to_string))
    }

    #[test]
    fn infers_backend_from_target() {
        let c = parse("--target nfs://h/e?version=3 --workdir w --json o.json metadata").unwrap();
        assert!(matches!(c.target, Target::Nfs(_)));
        let c = parse("--target /mnt/x --workdir w --json o.json metadata").unwrap();
        assert!(matches!(c.target, Target::Posix(_)));
    }

    #[test]
    fn parses_data_suite_sizes_and_qd() {
        let c = parse("--target /mnt/x --workdir w --json o.json data --size 40m --qd 8").unwrap();
        match c.suite {
            Suite::Data { size, qd, repeat, iters, .. } => {
                assert_eq!(size, 40 * 1024 * 1024);
                assert_eq!(qd, 8);
                assert_eq!(repeat, 5);
                assert_eq!(iters, 200);
            }
            _ => panic!("expected data suite"),
        }
    }

    #[test]
    fn smoke_reduces_iterations_to_one() {
        let c = parse("--target /mnt/x --workdir w --json o.json --smoke data --size 1g --qd 1").unwrap();
        match c.suite {
            Suite::Data { repeat, iters, .. } => assert_eq!((repeat, iters), (1, 1)),
            _ => panic!(),
        }
    }

    #[test]
    fn rejects_bad_size_and_qd() {
        assert!(parse("--target /mnt/x --workdir w --json o.json data --size 5m --qd 1").is_err());
        assert!(parse("--target /mnt/x --workdir w --json o.json data --size 4k --qd 3").is_err());
    }

    #[test]
    fn io_defaults_to_direct() {
        let c = parse("--target /mnt/x --workdir w --json o.json data --size 4k --qd 1").unwrap();
        assert!(matches!(c.io, IoMode::Direct));
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test --bin nfs-perf-compare cli:: 2>&1 | tail -5`
Expected: 编译错误（模块不存在）。

- [ ] **Step 4: 实现 `cli.rs`**

```rust
use std::path::PathBuf;

use thiserror::Error;

pub const CHUNK: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Usage(String),
}

#[derive(Debug, Clone)]
pub enum Target {
    Nfs(String),
    Posix(PathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    Direct,
    Buffered,
}

impl IoMode {
    pub fn as_str(self) -> &'static str {
        match self {
            IoMode::Direct => "direct",
            IoMode::Buffered => "buffered",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMode {
    Same,
    Distinct,
}

impl ClientMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ClientMode::Same => "same",
            ClientMode::Distinct => "distinct",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Suite {
    Metadata { iters: usize, readdir_entries: usize, readdir_iters: usize },
    Data { size: u64, size_label: String, qd: usize, repeat: usize, iters: usize },
    Multiclient { size: u64, size_label: String, clients: usize, mode: ClientMode, repeat: usize },
    WorkerRead { path: String, qd: usize },
}

impl Suite {
    pub fn name(&self) -> &'static str {
        match self {
            Suite::Metadata { .. } => "metadata",
            Suite::Data { .. } => "data",
            Suite::Multiclient { .. } => "multiclient",
            Suite::WorkerRead { .. } => "worker-read",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub target: Target,
    pub workdir: String,
    pub json: PathBuf,
    pub suite: Suite,
    pub io: IoMode,
    pub smoke: bool,
}

pub fn parse_size(label: &str) -> Result<u64, CliError> {
    match label {
        "4k" => Ok(4096),
        "40m" => Ok(40 * CHUNK),
        "1g" => Ok(1024 * CHUNK),
        other => Err(CliError::Usage(format!("--size must be 4k|40m|1g, got {other}"))),
    }
}

fn usize_arg(name: &str, value: &str) -> Result<usize, CliError> {
    value
        .parse::<usize>()
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| CliError::Usage(format!("{name} must be a positive integer")))
}

pub fn parse_args(args: impl Iterator<Item = String>) -> Result<Config, CliError> {
    let args: Vec<String> = args.collect();
    let mut target = None;
    let mut workdir = None;
    let mut json = None;
    let mut io = IoMode::Direct;
    let mut smoke = false;
    let mut i = 0;
    // global options come first; the first bare word is the suite name
    while i < args.len() && args[i].starts_with("--") {
        match args[i].as_str() {
            "--smoke" => {
                smoke = true;
                i += 1;
            }
            "--target" | "--workdir" | "--json" | "--io" => {
                let value = args
                    .get(i + 1)
                    .ok_or_else(|| CliError::Usage(format!("{} needs a value", args[i])))?;
                match args[i].as_str() {
                    "--target" => {
                        target = Some(if value.starts_with("nfs://") {
                            Target::Nfs(value.clone())
                        } else if value.starts_with('/') {
                            Target::Posix(PathBuf::from(value))
                        } else {
                            return Err(CliError::Usage("--target must be nfs://... or an absolute path".into()));
                        });
                    }
                    "--workdir" => workdir = Some(value.clone()),
                    "--json" => json = Some(PathBuf::from(value)),
                    _ => {
                        io = match value.as_str() {
                            "direct" => IoMode::Direct,
                            "buffered" => IoMode::Buffered,
                            _ => return Err(CliError::Usage("--io must be direct|buffered".into())),
                        }
                    }
                }
                i += 2;
            }
            other => return Err(CliError::Usage(format!("unknown option {other}"))),
        }
    }
    let suite_name = args.get(i).ok_or_else(|| CliError::Usage("suite name is required".into()))?;
    let rest = &args[i + 1..];
    let mut opts = std::collections::HashMap::new();
    let mut j = 0;
    while j < rest.len() {
        let value = rest
            .get(j + 1)
            .ok_or_else(|| CliError::Usage(format!("{} needs a value", rest[j])))?;
        opts.insert(rest[j].clone(), value.clone());
        j += 2;
    }
    let get = |k: &str, d: usize| -> Result<usize, CliError> {
        opts.get(k).map(|v| usize_arg(k, v)).unwrap_or(Ok(d))
    };
    let suite = match suite_name.as_str() {
        "metadata" => Suite::Metadata {
            iters: if smoke { 1 } else { get("--iters", 200)? },
            readdir_entries: if smoke { 10 } else { get("--readdir-entries", 1000)? },
            readdir_iters: if smoke { 1 } else { get("--readdir-iters", 20)? },
        },
        "data" => {
            let label = opts.get("--size").ok_or_else(|| CliError::Usage("--size is required".into()))?;
            let qd = get("--qd", 1)?;
            if qd != 1 && qd != 8 {
                return Err(CliError::Usage("--qd must be 1 or 8".into()));
            }
            Suite::Data {
                size: parse_size(label)?,
                size_label: label.clone(),
                qd,
                repeat: if smoke { 1 } else { get("--repeat", 5)? },
                iters: if smoke { 1 } else { get("--iters", 200)? },
            }
        }
        "multiclient" => {
            let label = opts.get("--size").ok_or_else(|| CliError::Usage("--size is required".into()))?;
            let mode = match opts.get("--mode").map(String::as_str) {
                Some("same") | None => ClientMode::Same,
                Some("distinct") => ClientMode::Distinct,
                _ => return Err(CliError::Usage("--mode must be same|distinct".into())),
            };
            Suite::Multiclient {
                size: parse_size(label)?,
                size_label: label.clone(),
                clients: get("--clients", 8)?,
                mode,
                repeat: if smoke { 1 } else { get("--repeat", 3)? },
            }
        }
        "worker-read" => Suite::WorkerRead {
            path: opts.get("--path").cloned().ok_or_else(|| CliError::Usage("--path is required".into()))?,
            qd: get("--qd", 1)?,
        },
        other => return Err(CliError::Usage(format!("unknown suite {other}"))),
    };
    Ok(Config {
        target: target.ok_or_else(|| CliError::Usage("--target is required".into()))?,
        workdir: workdir.ok_or_else(|| CliError::Usage("--workdir is required".into()))?,
        json: json.unwrap_or_else(|| PathBuf::from("/dev/stdout")),
        suite,
        io,
        smoke,
    })
}
```

- [ ] **Step 5: 实现 `stats.rs`（含测试）**

```rust
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    Ms,
    MiBps,
}

#[derive(Debug, Clone)]
pub struct Series {
    pub name: String,
    pub unit: Unit,
    pub samples: Vec<f64>,
    pub reference_only: bool,
}

impl Series {
    pub fn ms(name: &str) -> Self {
        Self { name: name.to_string(), unit: Unit::Ms, samples: Vec::new(), reference_only: false }
    }
    pub fn mibps(name: &str) -> Self {
        Self { name: name.to_string(), unit: Unit::MiBps, samples: Vec::new(), reference_only: false }
    }
}

pub fn percentile(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 * p).ceil() as usize).saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

pub fn mibps(bytes: u64, seconds: f64) -> f64 {
    bytes as f64 / 1048576.0 / seconds
}

pub fn series_json(s: &Series) -> Value {
    let mean = if s.samples.is_empty() { f64::NAN } else { s.samples.iter().sum::<f64>() / s.samples.len() as f64 };
    match s.unit {
        Unit::Ms => json!({
            "name": s.name, "unit": "ms", "reference_only": s.reference_only,
            "samples": s.samples,
            "p50": percentile(&s.samples, 0.5), "p95": percentile(&s.samples, 0.95),
            "p99": percentile(&s.samples, 0.99), "mean": mean,
            "ops_s": if mean > 0.0 { 1000.0 / mean } else { f64::NAN },
        }),
        Unit::MiBps => json!({
            "name": s.name, "unit": "MiB/s", "reference_only": s.reference_only,
            "samples": s.samples,
            "median": percentile(&s.samples, 0.5),
            "min": s.samples.iter().copied().fold(f64::INFINITY, f64::min),
            "max": s.samples.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        }),
    }
}

pub fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|l| l.starts_with("VmHWM:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_matches_existing_benchmark_convention() {
        let v = [5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(percentile(&v, 0.5), 3.0);
        assert_eq!(percentile(&v, 0.95), 5.0);
    }

    #[test]
    fn ms_series_reports_ops_per_second() {
        let mut s = Series::ms("create");
        s.samples = vec![2.0, 2.0];
        let j = series_json(&s);
        assert_eq!(j["ops_s"], 500.0);
        assert_eq!(j["unit"], "ms");
    }
}
```

- [ ] **Step 6: 最小 `main.rs` 让 bin 可编译**

```rust
mod cli;
mod stats;

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    match cli::parse_args(env::args().skip(1)) {
        Ok(config) => {
            println!("{:?}", config.suite.name());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}
```

- [ ] **Step 7: 跑测试**

Run: `cargo test --bin nfs-perf-compare 2>&1 | tail -5`
Expected: 7 passed（5 cli + 2 stats）。`cargo clippy --bin nfs-perf-compare` 无警告。

- [ ] **Step 8: Commit**

```bash
git add src/bin/nfs-perf-compare docs/superpowers
git commit -m "feat(perf-compare): add CLI parsing and statistics for nfs-rs vs kernel benchmark"
```

---

### Task 2: 模式块、对齐缓冲与 Backend trait + POSIX backend

**Files:**
- Create: `src/bin/nfs-perf-compare/pattern.rs`, `backend.rs`, `posix.rs`
- Modify: `main.rs`（加 `mod`）

**Interfaces:**
- Consumes: `cli::IoMode`, `cli::CHUNK`.
- Produces:
  - `pattern::pattern_block() -> Bytes`（1 MiB，4096 对齐）、`pattern::aligned_bytes(len: usize) -> (Vec<u8>, usize)`、`pattern::verify(offset: u64, chunk: &[u8]) -> bool`。
  - `backend::BenchError`（thiserror：`Io`, `Nfs(NfsError)`, `Integrity(String)`, `Join(String)`），`pub type Result<T> = std::result::Result<T, BenchError>`。
  - `backend::BackendInfo { backend: &'static str, protocol: Option<String>, rsize: u64, wsize: u64 }`。
  - `#[async_trait] trait Backend: Send + Sync { async fn mkdir(&self, p: &str) -> Result<()>; async fn create(&self, p: &str) -> Result<()>; async fn stat(&self, p: &str) -> Result<()>; async fn access(&self, p: &str) -> Result<()>; async fn chmod(&self, p: &str, mode: u32) -> Result<()>; async fn rename(&self, from: &str, to: &str) -> Result<()>; async fn readdir_count(&self, p: &str) -> Result<usize>; async fn remove(&self, p: &str) -> Result<()>; async fn rmdir(&self, p: &str) -> Result<()>; async fn open_write(&self, p: &str) -> Result<Box<dyn FileHandle>>; async fn open_read(&self, p: &str) -> Result<Box<dyn FileHandle>>; fn chunk_size(&self) -> u64; fn info(&self) -> BackendInfo; async fn drop_caches(&self) -> Result<bool>; async fn shutdown(&self) -> Result<()>; }`
  - `#[async_trait] trait FileHandle: Send + Sync { async fn write_at(&self, offset: u64, data: Bytes) -> Result<()>; async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>; async fn sync(&self) -> Result<()>; async fn close(self: Box<Self>) -> Result<()>; }`
  - `posix::PosixBackend::new(root: PathBuf, io: IoMode) -> PosixBackend`。所有路径参数都是相对 root 的相对路径（`a/b/c`）。

- [ ] **Step 1: `pattern.rs` 含测试**

```rust
use bytes::Bytes;

use super::cli::CHUNK;

pub const ALIGN: usize = 4096;

/// Over-allocate and return (vec, offset) such that vec[offset..offset+len] is 4096-aligned.
pub fn aligned_bytes(len: usize) -> (Vec<u8>, usize) {
    let v = vec![0u8; len + ALIGN];
    let offset = (ALIGN - (v.as_ptr() as usize % ALIGN)) % ALIGN;
    (v, offset)
}

pub fn pattern_block() -> Bytes {
    let len = CHUNK as usize;
    let (mut v, off) = aligned_bytes(len);
    for (i, b) in v[off..off + len].iter_mut().enumerate() {
        *b = ((i * 17 + 29) % 251) as u8;
    }
    Bytes::from(v).slice(off..off + len)
}

pub fn verify(offset: u64, chunk: &[u8]) -> bool {
    let block = pattern_block();
    let mut pos = (offset % CHUNK) as usize;
    let mut rest = chunk;
    while !rest.is_empty() {
        let n = rest.len().min(block.len() - pos);
        if rest[..n] != block[pos..pos + n] {
            return false;
        }
        rest = &rest[n..];
        pos = (pos + n) % block.len();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_is_aligned_and_periodic() {
        let b = pattern_block();
        assert_eq!(b.as_ptr() as usize % ALIGN, 0);
        assert_eq!(b[0], 29);
        assert!(verify(0, &b));
        assert!(verify(CHUNK * 7 + 100, &b[100..]));
        let mut bad = b.to_vec();
        bad[5] ^= 1;
        assert!(!verify(0, &bad));
    }
}
```

- [ ] **Step 2: `backend.rs`**

```rust
use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BenchError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("nfs-rs error: {0}")]
    Nfs(#[from] nfs_rs::NfsError),
    #[error("data integrity: {0}")]
    Integrity(String),
    #[error("task failed: {0}")]
    Join(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, Clone)]
pub struct BackendInfo {
    pub backend: &'static str,
    pub protocol: Option<String>,
    pub rsize: u64,
    pub wsize: u64,
}

#[async_trait]
pub trait FileHandle: Send + Sync {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()>;
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;
    async fn sync(&self) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;
}

#[async_trait]
pub trait Backend: Send + Sync {
    async fn mkdir(&self, path: &str) -> Result<()>;
    async fn create(&self, path: &str) -> Result<()>;
    async fn stat(&self, path: &str) -> Result<()>;
    async fn access(&self, path: &str) -> Result<()>;
    async fn chmod(&self, path: &str, mode: u32) -> Result<()>;
    async fn rename(&self, from: &str, to: &str) -> Result<()>;
    async fn readdir_count(&self, path: &str) -> Result<usize>;
    async fn remove(&self, path: &str) -> Result<()>;
    async fn rmdir(&self, path: &str) -> Result<()>;
    async fn open_write(&self, path: &str) -> Result<Box<dyn FileHandle>>;
    async fn open_read(&self, path: &str) -> Result<Box<dyn FileHandle>>;
    fn chunk_size(&self) -> u64;
    fn info(&self) -> BackendInfo;
    /// Returns Ok(true) if caches were dropped, Ok(false) if not applicable.
    async fn drop_caches(&self) -> Result<bool>;
    async fn shutdown(&self) -> Result<()>;
}
```

- [ ] **Step 3: `posix.rs` 失败测试（在文件底部）**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::{pattern_block, verify};

    #[tokio::test]
    async fn buffered_roundtrip_and_metadata_on_tmpdir() {
        let dir = std::env::temp_dir().join(format!("perfcmp-posix-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let b = PosixBackend::new(dir.clone(), IoMode::Buffered);
        b.mkdir("d").await.unwrap();
        b.create("d/f").await.unwrap();
        b.stat("d/f").await.unwrap();
        b.access("d/f").await.unwrap();
        b.chmod("d/f", 0o644).await.unwrap();
        b.rename("d/f", "d/g").await.unwrap();
        assert_eq!(b.readdir_count("d").await.unwrap(), 1);
        let h = b.open_write("d/g").await.unwrap();
        h.write_at(0, pattern_block().slice(..8192)).await.unwrap();
        h.sync().await.unwrap();
        h.close().await.unwrap();
        let h = b.open_read("d/g").await.unwrap();
        let got = h.read_at(4096, 4096).await.unwrap();
        assert!(verify(4096, &got));
        h.close().await.unwrap();
        b.remove("d/g").await.unwrap();
        b.rmdir("d").await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}
```

- [ ] **Step 4: 实现 `posix.rs`**

```rust
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nix::libc::O_DIRECT;
use tokio::task::spawn_blocking;

use super::backend::{Backend, BackendInfo, BenchError, FileHandle, Result};
use super::cli::{CHUNK, IoMode};
use super::pattern::aligned_bytes;

pub struct PosixBackend {
    root: PathBuf,
    io: IoMode,
}

impl PosixBackend {
    pub fn new(root: PathBuf, io: IoMode) -> Self {
        Self { root, io }
    }
    fn abs(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> std::io::Result<T> + Send + 'static) -> Result<T> {
    spawn_blocking(f)
        .await
        .map_err(|e| BenchError::Join(e.to_string()))?
        .map_err(BenchError::Io)
}

#[async_trait]
impl Backend for PosixBackend {
    async fn mkdir(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::create_dir(p)).await
    }
    async fn create(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || OpenOptions::new().write(true).create_new(true).open(p).map(drop)).await
    }
    async fn stat(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::metadata(p).map(drop)).await
    }
    async fn access(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || {
            let c = std::ffi::CString::new(p.as_os_str().as_encoded_bytes())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            // SAFETY: c is a valid NUL-terminated path; access() has no other preconditions.
            if unsafe { nix::libc::access(c.as_ptr(), nix::libc::R_OK) } == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        })
        .await
    }
    async fn chmod(&self, path: &str, mode: u32) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode))).await
    }
    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let (f, t) = (self.abs(from), self.abs(to));
        blocking(move || std::fs::rename(f, t)).await
    }
    async fn readdir_count(&self, path: &str) -> Result<usize> {
        let p = self.abs(path);
        blocking(move || Ok(std::fs::read_dir(p)?.count())).await
    }
    async fn remove(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::remove_file(p)).await
    }
    async fn rmdir(&self, path: &str) -> Result<()> {
        let p = self.abs(path);
        blocking(move || std::fs::remove_dir(p)).await
    }
    async fn open_write(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        let p = self.abs(path);
        let direct = self.io == IoMode::Direct;
        let file = blocking(move || {
            let mut o = OpenOptions::new();
            o.write(true).create(true).truncate(true);
            if direct {
                o.custom_flags(O_DIRECT);
            }
            o.open(p)
        })
        .await?;
        Ok(Box::new(PosixFile { file: Arc::new(file), direct }))
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        let p = self.abs(path);
        let direct = self.io == IoMode::Direct;
        let file = blocking(move || {
            let mut o = OpenOptions::new();
            o.read(true);
            if direct {
                o.custom_flags(O_DIRECT);
            }
            o.open(p)
        })
        .await?;
        Ok(Box::new(PosixFile { file: Arc::new(file), direct }))
    }
    fn chunk_size(&self) -> u64 {
        CHUNK
    }
    fn info(&self) -> BackendInfo {
        BackendInfo { backend: "posix", protocol: None, rsize: CHUNK, wsize: CHUNK }
    }
    async fn drop_caches(&self) -> Result<bool> {
        if self.io == IoMode::Direct {
            return Ok(false);
        }
        blocking(|| {
            // SAFETY: sync() has no preconditions.
            unsafe { nix::libc::sync() };
            match std::fs::write("/proc/sys/vm/drop_caches", b"3\n") {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Ok(false),
                Err(e) => Err(e),
            }
        })
        .await
    }
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

struct PosixFile {
    file: Arc<File>,
    direct: bool,
}

#[async_trait]
impl FileHandle for PosixFile {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        let file = Arc::clone(&self.file);
        blocking(move || {
            let mut done = 0usize;
            while done < data.len() {
                let n = file.write_at(&data[done..], offset + done as u64)?;
                if n == 0 {
                    return Err(std::io::Error::other("short write"));
                }
                done += n;
            }
            Ok(())
        })
        .await
    }
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let file = Arc::clone(&self.file);
        let direct = self.direct;
        blocking(move || {
            let (mut v, off) = if direct { aligned_bytes(len) } else { (vec![0u8; len], 0) };
            let mut done = 0usize;
            while done < len {
                let n = file.read_at(&mut v[off + done..off + len], offset + done as u64)?;
                if n == 0 {
                    break;
                }
                done += n;
            }
            Ok(Bytes::from(v).slice(off..off + done))
        })
        .await
    }
    async fn sync(&self) -> Result<()> {
        let file = Arc::clone(&self.file);
        blocking(move || file.sync_all()).await
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
```

- [ ] **Step 5: `main.rs` 加 `mod backend; mod pattern; mod posix;`，跑测试**

Run: `cargo test --bin nfs-perf-compare 2>&1 | tail -5`
Expected: 9 passed。`cargo clippy --bin nfs-perf-compare -- -D warnings` 通过（对 dead_code 可临时 `#[allow(dead_code)]` 在 main.rs 顶部，Task 6 移除）。

- [ ] **Step 6: Commit**

```bash
git add src/bin/nfs-perf-compare
git commit -m "feat(perf-compare): add Backend trait, pattern block, and POSIX backend"
```

---

### Task 3: nfs-rs backend

**Files:**
- Create: `src/bin/nfs-perf-compare/nfsrs.rs`
- Modify: `main.rs`（加 `mod nfsrs;`）

**Interfaces:**
- Consumes: Task 2 的 `Backend`/`FileHandle`/`BenchError`。
- Produces: `nfsrs::NfsRsBackend::connect(url: &str) -> Result<NfsRsBackend>`。路径参数同样是相对导出根的相对路径。

- [ ] **Step 1: 实现 `nfsrs.rs`**（无法离线测试；只保证编译与 clippy）

```rust
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use nfs_rs::{Mount, NFSVersion, OPEN_READ, OPEN_WRITE, parse_url_and_mount};

use super::backend::{Backend, BackendInfo, BenchError, FileHandle, Result};
use super::cli::CHUNK;

pub struct NfsRsBackend {
    mount: Arc<Box<dyn Mount>>,
}

impl NfsRsBackend {
    pub async fn connect(url: &str) -> Result<Self> {
        let mount = parse_url_and_mount(url).await?;
        Ok(Self { mount: Arc::new(mount) })
    }
}

fn protocol_label(v: NFSVersion) -> String {
    match v {
        NFSVersion::NFSv3 => "3".to_string(),
        NFSVersion::NFSv4p0 => "4.0".to_string(),
        NFSVersion::NFSv4p1 => "4.1".to_string(),
        other => format!("{other:?}"),
    }
}

#[async_trait]
impl Backend for NfsRsBackend {
    async fn mkdir(&self, path: &str) -> Result<()> {
        self.mount.mkdir_path(path, 0o755).await.map(drop).map_err(Into::into)
    }
    async fn create(&self, path: &str) -> Result<()> {
        let obj = self.mount.create_path(path, Some(0o644)).await?;
        self.mount.close(obj.fh).await.map_err(Into::into)
    }
    async fn stat(&self, path: &str) -> Result<()> {
        self.mount.getattr_path(path).await.map(drop).map_err(Into::into)
    }
    async fn access(&self, path: &str) -> Result<()> {
        self.mount.access_path(path, 4).await.map(drop).map_err(Into::into)
    }
    async fn chmod(&self, path: &str, mode: u32) -> Result<()> {
        self.mount
            .setattr_path(path, false, Some(mode), None, None, None, None, None)
            .await
            .map_err(Into::into)
    }
    async fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.mount.rename_path(from, to).await.map_err(Into::into)
    }
    async fn readdir_count(&self, path: &str) -> Result<usize> {
        let mut stream = self.mount.readdir_path(path).await?;
        let mut n = 0usize;
        while let Some(entry) = stream.try_next().await? {
            if entry.file_name != "." && entry.file_name != ".." {
                n += 1;
            }
        }
        Ok(n)
    }
    async fn remove(&self, path: &str) -> Result<()> {
        self.mount.remove_path(path).await.map_err(Into::into)
    }
    async fn rmdir(&self, path: &str) -> Result<()> {
        self.mount.rmdir_path(path).await.map_err(Into::into)
    }
    async fn open_write(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        let obj = self.mount.create_path(path, Some(0o644)).await?;
        let _ = OPEN_WRITE;
        Ok(Box::new(NfsFile { mount: Arc::clone(&self.mount), fh: obj.fh }))
    }
    async fn open_read(&self, path: &str) -> Result<Box<dyn FileHandle>> {
        let obj = self.mount.open_path(path, OPEN_READ).await?;
        Ok(Box::new(NfsFile { mount: Arc::clone(&self.mount), fh: obj.fh }))
    }
    fn chunk_size(&self) -> u64 {
        u64::from(self.mount.get_max_read_size().min(self.mount.get_max_write_size())).min(CHUNK)
    }
    fn info(&self) -> BackendInfo {
        BackendInfo {
            backend: "nfsrs",
            protocol: Some(protocol_label(self.mount.version())),
            rsize: u64::from(self.mount.get_max_read_size()),
            wsize: u64::from(self.mount.get_max_write_size()),
        }
    }
    async fn drop_caches(&self) -> Result<bool> {
        Ok(false)
    }
    async fn shutdown(&self) -> Result<()> {
        self.mount.umount().await.map_err(Into::into)
    }
}

struct NfsFile {
    mount: Arc<Box<dyn Mount>>,
    fh: Bytes,
}

#[async_trait]
impl FileHandle for NfsFile {
    async fn write_at(&self, offset: u64, data: Bytes) -> Result<()> {
        let mut done = 0usize;
        while done < data.len() {
            let n = self.mount.write(self.fh.clone(), offset + done as u64, data.slice(done..)).await? as usize;
            if n == 0 {
                return Err(BenchError::Other("server accepted zero bytes".into()));
            }
            done += n;
        }
        Ok(())
    }
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let first = self.mount.read(self.fh.clone(), offset, len as u32).await?;
        if first.len() >= len || first.is_empty() {
            return Ok(first);
        }
        let mut buf = bytes::BytesMut::with_capacity(len);
        buf.extend_from_slice(&first);
        while buf.len() < len {
            let part = self.mount.read(self.fh.clone(), offset + buf.len() as u64, (len - buf.len()) as u32).await?;
            if part.is_empty() {
                break;
            }
            buf.extend_from_slice(&part);
        }
        Ok(buf.freeze())
    }
    async fn sync(&self) -> Result<()> {
        self.mount.commit(self.fh.clone(), 0, 0).await.map_err(Into::into)
    }
    async fn close(self: Box<Self>) -> Result<()> {
        self.mount.close(self.fh).await.map_err(Into::into)
    }
}
```

注意：`open_write` 用 `create_path`（文件不存在时创建；data suite 每次都用新文件名）。`NFSVersion` 若不是 `Copy`，`version()` 返回值按实际类型调整。若 `NFSVersion` 只有三个变体，删掉 `other =>` 分支。

- [ ] **Step 2: 编译与 clippy**

Run: `cargo clippy --bin nfs-perf-compare -- -D warnings 2>&1 | tail -5`
Expected: 通过（去掉 `let _ = OPEN_WRITE;` 及未用导入）。

- [ ] **Step 3: Commit**

```bash
git add src/bin/nfs-perf-compare/nfsrs.rs src/bin/nfs-perf-compare/main.rs
git commit -m "feat(perf-compare): add nfs-rs userspace backend"
```

---

### Task 4: metadata suite

**Files:**
- Create: `src/bin/nfs-perf-compare/metadata.rs`

**Interfaces:**
- Consumes: `Backend`, `Series`。
- Produces: `pub async fn run(b: &dyn Backend, workdir: &str, iters: usize, readdir_entries: usize, readdir_iters: usize) -> Result<Vec<Series>>`。调用方负责 `mkdir(workdir)` 与最终 `rmdir(workdir)`。

- [ ] **Step 1: 失败测试（文件底部）**

```rust
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
        assert_eq!(names, ["mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir", "readdir"]);
        assert!(series.iter().take(8).all(|s| s.samples.len() == 3));
        assert_eq!(series[8].samples.len(), 2);
        assert_eq!(std::fs::read_dir(dir.join("w")).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
```

- [ ] **Step 2: 实现**

```rust
use std::time::Instant;

use super::backend::{Backend, Result};
use super::stats::Series;

fn ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

pub async fn run(
    b: &dyn Backend,
    workdir: &str,
    iters: usize,
    readdir_entries: usize,
    readdir_iters: usize,
) -> Result<Vec<Series>> {
    let names = ["mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir"];
    let mut series: Vec<Series> = names.iter().map(|n| Series::ms(n)).collect();
    let m = format!("{workdir}/m");
    b.mkdir(&m).await?;
    for i in 0..iters {
        let d = format!("{m}/d{i}");
        let f = format!("{d}/f");
        let g = format!("{d}/g");
        let t = Instant::now(); b.mkdir(&d).await?; series[0].samples.push(ms(t));
        let t = Instant::now(); b.create(&f).await?; series[1].samples.push(ms(t));
        let t = Instant::now(); b.stat(&f).await?; series[2].samples.push(ms(t));
        let t = Instant::now(); b.access(&f).await?; series[3].samples.push(ms(t));
        let t = Instant::now(); b.chmod(&f, 0o644).await?; series[4].samples.push(ms(t));
        let t = Instant::now(); b.rename(&f, &g).await?; series[5].samples.push(ms(t));
        let t = Instant::now(); b.remove(&g).await?; series[6].samples.push(ms(t));
        let t = Instant::now(); b.rmdir(&d).await?; series[7].samples.push(ms(t));
    }
    b.rmdir(&m).await?;

    let big = format!("{workdir}/big");
    b.mkdir(&big).await?;
    for i in 0..readdir_entries {
        b.create(&format!("{big}/e{i}")).await?;
    }
    let mut readdir = Series::ms("readdir");
    for _ in 0..readdir_iters {
        let t = Instant::now();
        let n = b.readdir_count(&big).await?;
        readdir.samples.push(ms(t));
        if n != readdir_entries {
            return Err(super::backend::BenchError::Integrity(format!("readdir saw {n} entries, expected {readdir_entries}")));
        }
    }
    for i in 0..readdir_entries {
        b.remove(&format!("{big}/e{i}")).await?;
    }
    b.rmdir(&big).await?;
    series.push(readdir);
    Ok(series)
}
```

- [ ] **Step 3: 跑测试、clippy、commit**

```bash
cargo test --bin nfs-perf-compare metadata 2>&1 | tail -3
git add src/bin/nfs-perf-compare && git commit -m "feat(perf-compare): add metadata suite"
```

---

### Task 5: data suite（QD 并发、冷/热读、校验）

**Files:**
- Create: `src/bin/nfs-perf-compare/data.rs`

**Interfaces:**
- Consumes: `Backend`（`Arc<dyn Backend>`）、`pattern`、`Series`、`stats::mibps`。
- Produces:
  - `pub async fn write_file(b: &dyn Backend, path: &str, size: u64, qd: usize) -> Result<f64>`（返回秒，含 sync）
  - `pub async fn read_file(b: &dyn Backend, path: &str, size: u64, qd: usize) -> Result<f64>`（返回秒，已扣除校验耗时；校验失败返回 `Integrity`）
  - `pub async fn run(b: Arc<dyn Backend>, workdir: &str, size: u64, qd: usize, repeat: usize, iters: usize, buffered_posix: bool) -> Result<(Vec<Series>, bool)>`（第二个返回值 = 是否成功 drop_caches）

- [ ] **Step 1: 测试**

```rust
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
        let (large, _) = run(Arc::clone(&b), "w", 3 * 1048576 + 4096, 8, 2, 1, true).await.unwrap();
        let names: Vec<&str> = large.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["write", "read", "read_hot"]);
        assert_eq!(large[0].samples.len(), 2);
        assert!(large[2].reference_only);
        assert_eq!(std::fs::read_dir(dir.join("w")).unwrap().count(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
```

- [ ] **Step 2: 实现**

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use super::backend::{Backend, BenchError, FileHandle, Result};
use super::pattern::{pattern_block, verify};
use super::stats::{Series, mibps};

fn chunks(size: u64, chunk: u64) -> u64 {
    size.div_ceil(chunk)
}

pub async fn write_file(b: &dyn Backend, path: &str, size: u64, qd: usize) -> Result<f64> {
    let chunk = b.chunk_size();
    let handle: Arc<Box<dyn FileHandle>> = Arc::new(b.open_write(path).await?);
    let block = pattern_block();
    let next = Arc::new(AtomicU64::new(0));
    let total = chunks(size, chunk);
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
    while let Some(r) = set.join_next().await {
        r.map_err(|e| BenchError::Join(e.to_string()))??;
    }
    handle.sync().await?;
    let seconds = started.elapsed().as_secs_f64();
    match Arc::try_unwrap(handle) {
        Ok(h) => h.close().await?,
        Err(_) => return Err(BenchError::Other("file handle still shared".into())),
    }
    Ok(seconds)
}

pub async fn read_file(b: &dyn Backend, path: &str, size: u64, qd: usize) -> Result<f64> {
    let chunk = b.chunk_size();
    let handle: Arc<Box<dyn FileHandle>> = Arc::new(b.open_read(path).await?);
    let next = Arc::new(AtomicU64::new(0));
    let total = chunks(size, chunk);
    let started = Instant::now();
    let mut set = JoinSet::new();
    for _ in 0..qd {
        let (h, next) = (Arc::clone(&handle), Arc::clone(&next));
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
                if data.len() != len || !verify(offset, &data) {
                    return Err(BenchError::Integrity(format!("chunk at offset {offset} mismatch")));
                }
                verify_time += v.elapsed();
            }
        });
    }
    let mut verify_total = Duration::ZERO;
    while let Some(r) = set.join_next().await {
        verify_total += r.map_err(|e| BenchError::Join(e.to_string()))??;
    }
    let seconds = (started.elapsed().saturating_sub(verify_total / qd as u32)).as_secs_f64();
    match Arc::try_unwrap(handle) {
        Ok(h) => h.close().await?,
        Err(_) => return Err(BenchError::Other("file handle still shared".into())),
    }
    Ok(seconds)
}

pub async fn run(
    b: Arc<dyn Backend>,
    workdir: &str,
    size: u64,
    qd: usize,
    repeat: usize,
    iters: usize,
    buffered_posix: bool,
) -> Result<(Vec<Series>, bool)> {
    let small = size <= b.chunk_size();
    let count = if small { iters } else { repeat };
    let paths: Vec<String> = (0..count).map(|i| format!("{workdir}/f{i}.bin")).collect();
    let mut dropped = false;
    let result: Result<Vec<Series>> = async {
        let mut write = if small { Series::ms("write_ms") } else { Series::mibps("write") };
        let mut read = if small { Series::ms("read_ms") } else { Series::mibps("read") };
        let mut hot = if small { Series::ms("read_hot_ms") } else { Series::mibps("read_hot") };
        hot.reference_only = true;
        for p in &paths {
            let s = write_file(b.as_ref(), p, size, qd).await?;
            write.samples.push(if small { s * 1000.0 } else { mibps(size, s) });
        }
        dropped = b.drop_caches().await?;
        for p in &paths {
            let s = read_file(b.as_ref(), p, size, qd).await?;
            read.samples.push(if small { s * 1000.0 } else { mibps(size, s) });
        }
        let mut out = vec![write, read];
        if buffered_posix {
            for p in &paths {
                let s = read_file(b.as_ref(), p, size, qd).await?;
                hot.samples.push(if small { s * 1000.0 } else { mibps(size, s) });
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
```

- [ ] **Step 3: 跑测试、clippy、commit**

```bash
cargo test --bin nfs-perf-compare data 2>&1 | tail -3
git add src/bin/nfs-perf-compare && git commit -m "feat(perf-compare): add data suite with QD concurrency and cold/hot reads"
```

---

### Task 6: multiclient suite + main 接线 + JSON 输出 + 集成测试

**Files:**
- Create: `src/bin/nfs-perf-compare/multiclient.rs`, `tests/nfs_perf_compare_cli.rs`
- Modify: `src/bin/nfs-perf-compare/main.rs`

**Interfaces:**
- `multiclient::run(b: Arc<dyn Backend>, cfg: &Config, size: u64, clients: usize, mode: ClientMode, repeat: usize) -> Result<(Vec<Series>, u64 /*max worker rss kib*/)>`
- worker 协议：子进程 `nfs-perf-compare --target T --io M worker-read --path P --qd 1`，stdout 一行 JSON `{"seconds": f, "bytes": n, "peak_rss_kib": n}`。
- 输出 JSON 顶层字段：`schema, harness:"rust", backend, protocol, target, mount_variant, io_mode, suite, params, env{hostname,kernel,nfs_rs_version,commit,rsize,wsize,captured_at_unix,drop_caches}, peak_rss_kib, results[]`。`mount_variant` 从环境变量 `PERF_MOUNT_VARIANT` 读取（run.sh 设置），`commit` 从 `PERF_COMMIT`，`protocol` 对 posix 从 `PERF_PROTOCOL` 读取。

- [ ] **Step 1: `multiclient.rs`**

```rust
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;

use tokio::process::Command;

use super::backend::{Backend, BenchError, Result};
use super::cli::{ClientMode, Config, Target};
use super::data::write_file;
use super::stats::{Series, mibps};

pub async fn run(
    b: Arc<dyn Backend>,
    cfg: &Config,
    size: u64,
    clients: usize,
    mode: ClientMode,
    repeat: usize,
) -> Result<(Vec<Series>, u64)> {
    let files = if mode == ClientMode::Same { 1 } else { clients };
    let paths: Vec<String> = (0..files).map(|i| format!("{}/mc{i}.bin", cfg.workdir)).collect();
    let mut max_rss = 0u64;
    let result: Result<Vec<Series>> = async {
        for p in &paths {
            write_file(b.as_ref(), p, size, 8).await?;
        }
        let mut agg = Series::mibps("aggregate_read");
        let mut per_client = Series::mibps("per_client_read");
        let exe = std::env::current_exe()?;
        let target = match &cfg.target {
            Target::Nfs(u) => u.clone(),
            Target::Posix(p) => p.to_string_lossy().into_owned(),
        };
        for _ in 0..repeat {
            b.drop_caches().await?;
            let started = Instant::now();
            let mut children = Vec::new();
            for c in 0..clients {
                let path = &paths[c % files];
                let child = Command::new(&exe)
                    .args(["--target", &target, "--io", cfg.io.as_str(), "worker-read", "--path", path, "--qd", "1"])
                    .stdout(Stdio::piped())
                    .spawn()?;
                children.push(child);
            }
            for child in children {
                let out = child.wait_with_output().await?;
                if !out.status.success() {
                    return Err(BenchError::Other(format!("worker failed: {}", String::from_utf8_lossy(&out.stderr))));
                }
                let v: serde_json::Value = serde_json::from_slice(&out.stdout)
                    .map_err(|e| BenchError::Other(format!("worker output: {e}")))?;
                let seconds = v["seconds"].as_f64().unwrap_or(f64::NAN);
                per_client.samples.push(mibps(size, seconds));
                max_rss = max_rss.max(v["peak_rss_kib"].as_u64().unwrap_or(0));
            }
            agg.samples.push(mibps(size * clients as u64, started.elapsed().as_secs_f64()));
        }
        Ok(vec![agg, per_client])
    }
    .await;
    for p in &paths {
        let _ = b.remove(p).await;
    }
    Ok((result?, max_rss))
}
```

worker 侧（在 main.rs）：连接 backend，`read_file(b, path, size_of_file, qd)`，其中文件大小通过 `--path` 对应文件的 stat 获得——为简化，worker 由父进程额外传 `--bytes N`（在 cli 的 `WorkerRead` 加 `bytes: u64` 字段，`--bytes` 必填）。输出 `{"seconds","bytes","peak_rss_kib"}` 后 `shutdown()`。

- [ ] **Step 2: `main.rs` 完整接线**

```rust
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

use serde_json::json;

use backend::{Backend, BenchError, Result};
use cli::{Config, Suite, Target};

#[tokio::main]
async fn main() -> ExitCode {
    let config = match cli::parse_args(env::args().skip(1)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
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

async fn run(config: Config) -> Result<()> {
    let b = connect(&config).await?;
    if let Suite::WorkerRead { path, qd, bytes } = &config.suite {
        let seconds = data::read_file(b.as_ref(), path, *bytes, *qd).await?;
        b.shutdown().await?;
        println!("{}", json!({"seconds": seconds, "bytes": bytes, "peak_rss_kib": stats::peak_rss_kib()}));
        return Ok(());
    }
    let is_posix = matches!(config.target, Target::Posix(_));
    b.mkdir(&config.workdir).await?;
    let outcome: Result<(Vec<stats::Series>, serde_json::Value, u64, bool)> = async {
        match &config.suite {
            Suite::Metadata { iters, readdir_entries, readdir_iters } => {
                let s = metadata::run(b.as_ref(), &config.workdir, *iters, *readdir_entries, *readdir_iters).await?;
                Ok((s, json!({"iters": iters, "readdir_entries": readdir_entries, "readdir_iters": readdir_iters}), 0, false))
            }
            Suite::Data { size, size_label, qd, repeat, iters } => {
                let buffered_posix = is_posix && config.io == cli::IoMode::Buffered;
                let (s, dropped) = data::run(Arc::clone(&b), &config.workdir, *size, *qd, *repeat, *iters, buffered_posix).await?;
                Ok((s, json!({"size": size_label, "bytes": size, "qd": qd, "repeat": repeat, "iters": iters}), 0, dropped))
            }
            Suite::Multiclient { size, size_label, clients, mode, repeat } => {
                let (s, rss) = multiclient::run(Arc::clone(&b), &config, *size, *clients, *mode, *repeat).await?;
                Ok((s, json!({"size": size_label, "bytes": size, "clients": clients, "mode": mode.as_str(), "repeat": repeat}), rss, false))
            }
            Suite::WorkerRead { .. } => Err(BenchError::Other("unreachable".into())),
        }
    }
    .await;
    let _ = b.rmdir(&config.workdir).await;
    let (series, params, worker_rss, dropped) = outcome?;
    let info = b.info();
    b.shutdown().await?;
    let report = json!({
        "schema": 1,
        "harness": "rust",
        "backend": info.backend,
        "protocol": info.protocol.or_else(|| env::var("PERF_PROTOCOL").ok()),
        "target": match &config.target { Target::Nfs(u) => u.clone(), Target::Posix(p) => p.to_string_lossy().into_owned() },
        "mount_variant": if is_posix { env::var("PERF_MOUNT_VARIANT").ok() } else { None },
        "io_mode": if is_posix { Some(config.io.as_str()) } else { None },
        "suite": config.suite.name(),
        "params": params,
        "env": {
            "hostname": env::var("HOSTNAME").ok(),
            "kernel": std::fs::read_to_string("/proc/sys/kernel/osrelease").map(|s| s.trim().to_string()).ok(),
            "nfs_rs_version": env!("CARGO_PKG_VERSION"),
            "commit": env::var("PERF_COMMIT").ok(),
            "rsize": info.rsize, "wsize": info.wsize,
            "captured_at_unix": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
            "drop_caches": dropped,
        },
        "peak_rss_kib": stats::peak_rss_kib().unwrap_or(0).max(worker_rss),
        "results": series.iter().map(stats::series_json).collect::<Vec<_>>(),
    });
    let text = serde_json::to_string_pretty(&report).map_err(|e| BenchError::Other(e.to_string()))?;
    if config.json.as_os_str() == "/dev/stdout" {
        println!("{text}");
    } else {
        std::fs::write(&config.json, text)?;
    }
    Ok(())
}
```

`HOSTNAME` 在非交互 shell 可能为空：改用 `std::fs::read_to_string("/proc/sys/kernel/hostname")`。

- [ ] **Step 3: 集成测试 `tests/nfs_perf_compare_cli.rs`**

```rust
use std::process::Command;

fn run(args: &[&str]) -> serde_json::Value {
    let output = Command::new(env!("CARGO_BIN_EXE_nfs-perf-compare"))
        .args(args)
        .output()
        .expect("harness should start");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).expect("JSON report")
}

#[test]
fn smoke_runs_all_suites_against_a_local_directory() {
    let root = std::env::temp_dir().join(format!("perfcmp-cli-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let target = root.to_str().unwrap();
    let common = ["--target", target, "--io", "buffered", "--workdir", "w", "--smoke"];

    let meta = run(&[&common[..], &["metadata"]].concat());
    assert_eq!(meta["backend"], "posix");
    assert_eq!(meta["results"].as_array().unwrap().len(), 9);

    let small = run(&[&common[..], &["data", "--size", "4k", "--qd", "1"]].concat());
    assert_eq!(small["results"][0]["name"], "write_ms");

    let large = run(&[&common[..], &["data", "--size", "40m", "--qd", "8"]].concat());
    assert_eq!(large["results"][1]["name"], "read");
    assert!(large["results"][1]["median"].as_f64().unwrap() > 0.0);

    let mc = run(&[&common[..], &["multiclient", "--size", "40m", "--clients", "2", "--mode", "same"]].concat());
    assert_eq!(mc["results"][0]["name"], "aggregate_read");
    assert!(mc["peak_rss_kib"].as_u64().unwrap() > 0);

    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    std::fs::remove_dir_all(root).unwrap();
}
```

- [ ] **Step 4: 跑全部、clippy、grep unwrap、commit**

```bash
cargo test --bin nfs-perf-compare --test nfs_perf_compare_cli 2>&1 | tail -5
cargo clippy --bin nfs-perf-compare -- -D warnings
grep -n '\.unwrap()\|\.expect(' src/bin/nfs-perf-compare/*.rs | grep -v "cfg(test)" # 只允许出现在 tests 模块内
git add src/bin/nfs-perf-compare tests/nfs_perf_compare_cli.rs
git commit -m "feat(perf-compare): add multiclient suite, JSON report, and CLI smoke test"
```

---

### Task 7: Python harness `perf_compare.py`

**Files:**
- Create: `tests/benchmarks/compare/perf_compare.py`, `tests/benchmarks/compare/test_perf_compare.py`

**Interfaces:** CLI 与 Rust 完全一致；JSON `harness: "python"`；`env.nfs_rs_version` 取 `nfs_rs.__version__`。仅标准库 + `nfs_rs`（`nfs_rs` 延迟导入，posix backend 无需安装它）。

结构（单文件，约 450 行）：
- `PATTERN = bytes((i*17+29) % 251 for i in range(CHUNK))`；`verify(offset, data)`。
- `class PosixBackend`：`__init__(root, io)`；元数据方法用 `os.*`；`open_write/open_read` 返回 `PosixFile`（`os.open` + `O_DIRECT` 可选；读用 `mmap.mmap(-1, len)` 作对齐缓冲 + `os.preadv(fd, [buf], off)`；写用 `os.pwritev(fd, [memoryview(PATTERN)[:len]], off)`——`PATTERN` 是 `bytes`，对齐不保证，**O_DIRECT 写需把 PATTERN 拷进一个 `mmap` 块**：`PATTERN_MM = mmap.mmap(-1, CHUNK); PATTERN_MM.write(PATTERN)`）；`sync` = `os.fsync`；并发用 `ThreadPoolExecutor(qd)`，任务共享 `itertools.count()` + `threading.Lock` 取块号。`drop_caches`：buffered 时 `os.sync()` + 写 `/proc/sys/vm/drop_caches`，`PermissionError` → False。
- `class NfsRsBackend`：`asyncio` 实现。`connect(url)` 解析 URL 的 `version/uid/gid/rsize/wsize` 查询参数 → `nfs_rs.AsyncClient.connect(base_url, versions=(v,), uid=..., gid=..., rsize=..., wsize=...)`。元数据：`mkdir/stat/chmod/rename/listdir/remove/rmdir`，`create` = `open(p, "wb")` 然后 `close()`，`access` 不支持 → 该 series 省略。文件：`AsyncFile.write_at(data, off)` / `read_at(off, len)` / `flush()` / `close()`；QD 并发用 `asyncio.gather` 的 qd 个协程共享一个计数器。`chunk_size = min(CHUNK, io_limits.max_read, io_limits.max_write)`。
- 两个 backend 都暴露同步接口给 suite 层：`NfsRsBackend` 在内部维护一个后台线程上的事件循环（`asyncio.new_event_loop()` + `threading.Thread(target=loop.run_forever)`），公开方法用 `asyncio.run_coroutine_threadsafe(...).result()`。这样 suite 代码（metadata/data/multiclient）只写一遍、同步风格。
- `run_metadata / run_data / run_multiclient` 与 Rust 同名同参、同 series 名称。multiclient 用 `subprocess.Popen([sys.executable, __file__, ..., "worker-read", "--path", p, "--bytes", n, "--qd", "1"])`。
- `peak_rss_kib()` 读 `/proc/self/status` 的 `VmHWM`。
- `main()`：`argparse` 两层（全局选项 + 子命令），`--smoke` 语义同 Rust。

- [ ] **Step 1: 失败测试 `test_perf_compare.py`**

```python
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HARNESS = Path(__file__).with_name("perf_compare.py")


def _run(tmp_path: Path, *suite: str) -> dict:
    out = tmp_path / "out.json"
    common = [sys.executable, str(HARNESS), "--target", str(tmp_path), "--io", "buffered",
              "--workdir", "w", "--json", str(out), "--smoke"]
    subprocess.run(common + list(suite), check=True, capture_output=True, text=True)
    return json.loads(out.read_text())


def test_metadata_suite_series_names(tmp_path: Path) -> None:
    report = _run(tmp_path, "metadata")
    assert report["harness"] == "python" and report["backend"] == "posix"
    assert [s["name"] for s in report["results"]] == [
        "mkdir", "create", "stat", "access", "chmod", "rename", "remove", "rmdir", "readdir"]


def test_data_suite_small_and_large(tmp_path: Path) -> None:
    small = _run(tmp_path, "data", "--size", "4k", "--qd", "1")
    assert small["results"][0]["name"] == "write_ms"
    large = _run(tmp_path, "data", "--size", "40m", "--qd", "8")
    assert [s["name"] for s in large["results"]] == ["write", "read", "read_hot"]
    assert large["results"][1]["median"] > 0


def test_multiclient_and_cleanup(tmp_path: Path) -> None:
    report = _run(tmp_path, "multiclient", "--size", "40m", "--clients", "2", "--mode", "distinct")
    assert report["results"][0]["name"] == "aggregate_read"
    assert report["peak_rss_kib"] > 0
    assert not any(p.name == "w" for p in tmp_path.iterdir())


def test_pattern_matches_rust_convention() -> None:
    sys.path.insert(0, str(HARNESS.parent))
    import perf_compare
    assert perf_compare.PATTERN[0] == 29 and perf_compare.PATTERN[1] == 46
    assert perf_compare.verify(perf_compare.CHUNK * 3 + 10, perf_compare.PATTERN[10:100])
```

- [ ] **Step 2: 实现 `perf_compare.py`**（按上面的结构写全，函数名/series 名与 Rust 一致）

- [ ] **Step 3: 跑测试、commit**

```bash
python3 -m pytest tests/benchmarks/compare/test_perf_compare.py -q
git add tests/benchmarks/compare && git commit -m "feat(perf-compare): add Python harness mirroring the Rust CLI and JSON schema"
```

---

### Task 8: `report.py`（Markdown + HTML）

**Files:**
- Create: `tests/benchmarks/compare/report.py`, `test_report.py`

**Interfaces:** `python3 report.py --results-dir DIR --md OUT.md --html OUT.html [--title T]`。
输入：DIR 下递归所有 `*.json`（schema 1）+ 可选 `failures.txt`（每行 `<文件名>\t<原因>`）。

逻辑：
- 索引 key = `(protocol, harness, backend, mount_variant, io_mode, suite, params 摘要)`。
- 执行摘要表：每协议一行，列：`meta p50 ratio (rust)`, `meta p50 ratio (py)`, `1g qd8 write ratio`, `1g qd8 read ratio`, `multiclient same ratio`（rust）；ratio = nfsrs / posix(default, direct)；元数据 ratio 取 9 个 op 的 p50 几何平均之比。
- 元数据表（每协议）：行 = op，列 = `rust-nfsrs | rust-posix(default) | rust-posix(nolookup) | py-nfsrs | py-posix(default) | py-posix(nolookup)`，值 = `p50 / p95 ms`。
- 数据表（每协议）：行 = size × qd × 方向，列 = `rust-nfsrs | rust-posix direct | rust-posix buffered(cold) | rust-posix buffered(hot)* | py-...`；4k 用 ms p50，其它 MiB/s median。
- 多客户端表：行 = mode，列同上（aggregate MiB/s）。
- RSS 表：每 harness×backend 的 1g qd8 data 与 multiclient 的 `peak_rss_kib`。
- 缺失格 → `N/A`，若 failures.txt 有对应文件名则脚注原因。
- HTML：自包含，内联 CSS，同一组表 + 摘要；不加载外部资源。
- 分析与限制章节由 `--notes NOTES.md` 原样拼接（人工撰写）。

- [ ] **Step 1: 测试**：用 `tmp_path` 合成 4 个 JSON（v3：rust-nfsrs data 1g qd8、rust-posix default direct data 1g qd8、两个 metadata）→ 断言 md 里出现 `| 3 |` 摘要行、ratio 数值正确（如 nfsrs 100 MiB/s vs posix 200 → `0.50`）、缺失格为 `N/A`、html 含 `<table`。

- [ ] **Step 2: 实现；Step 3: 测试通过后 commit** `feat(perf-compare): add report generator`。

---

### Task 9: `ontap_prepare.py`（REST 准备 / 回滚）

**Files:**
- Create: `tests/benchmarks/compare/ontap_prepare.py`, `test_ontap_prepare.py`

**Interfaces:** `python3 ontap_prepare.py --mgmt 10.128.61.20 --svm lizy --volume nfsrs_perf --size-gb 50 --client 10.131.6.181 (prepare|rollback|status) [--dry-run] [--restore-transfer-size]`；凭据 `ONTAP_USER`/`ONTAP_PASS`；`urllib` + 忽略自签证书（`ssl._create_unverified_context`）。

prepare 步骤（幂等，每步先 GET 检查存在）：
1. `GET /api/protocols/nfs/export-policies?svm.name=lizy&name=nfsrs_perf` → 无则 `POST` `{svm:{name}, name, rules:[{clients:[{match:"10.131.6.181/32"}], protocols:["nfs3","nfs4"], ro_rule:["sys"], rw_rule:["sys"], superuser:["sys"]}]}`。
2. `GET /api/storage/aggregates?fields=space.block_storage.available` → 选可用最大者；`GET /api/storage/volumes?svm.name=lizy&name=nfsrs_perf` → 无则 `POST {svm:{name}, name, size: 50*2**30, aggregates:[{name}], nas:{path:"/nfsrs_perf", export_policy:{name:"nfsrs_perf"}, security_style:"unix", unix_permissions:"777"}}`，轮询 job 至 success。
3. `GET /api/protocols/nfs/services?svm.name=lizy&fields=transport.tcp_max_transfer_size` → 记录原值到 stdout；若 ≠1048576 则 `PATCH /api/protocols/nfs/services/{svm.uuid}` `{transport:{tcp_max_transfer_size:1048576}}`。

rollback：`--restore-transfer-size` 时 PATCH 回 65536；卷/策略默认保留（`--delete-volume` 时先 offline 再 DELETE）。
`--dry-run` 只打印将执行的 (method, path, body)。

- [ ] **Step 1: 测试**：`--dry-run prepare` 输出中包含 `POST /api/protocols/nfs/export-policies` 与 `"protocols": ["nfs3", "nfs4"]`；`plan_prepare(existing_policy=True, existing_volume=True, current_size=1048576)` 返回空列表（幂等）。把请求规划写成纯函数 `plan_prepare(state) -> list[Request]` 便于测试。
- [ ] **Step 2: 实现；Step 3: commit** `feat(perf-compare): add ONTAP preparation script`。

---

### Task 10: `deploy.sh` 与 `run.sh`

**Files:**
- Create: `tests/benchmarks/compare/deploy.sh`, `run.sh`

`deploy.sh`（本机执行）：
```bash
#!/usr/bin/env bash
set -euo pipefail
host="${1:-10.131.6.181}"; remote="/root/nfs-rs-perf"
cargo fetch --locked
rsync -az --delete --exclude target --exclude .git --exclude '*.pyc' ./ "root@$host:$remote/repo/"
rsync -az ~/.cargo/registry/ "root@$host:/root/.cargo/registry/"
ssh "root@$host" "cd $remote/repo && cargo build --release --offline --bin nfs-perf-compare \
  && python3 -m venv $remote/venv && $remote/venv/bin/pip install -q nfs-rs==0.6.1 \
  && $remote/venv/bin/python -c 'import nfs_rs; print(nfs_rs.__version__)'"
```

`run.sh`（node181 执行，`run.sh RUN_ID [protocols...]`）：
- 变量：`LIF=10.128.61.200`, `EXPORT=/nfsrs_perf`, `MNT=/mnt/nfsrs_perf`, `BIN=repo/target/release/nfs-perf-compare`, `PY="$VENV/bin/python repo/tests/benchmarks/compare/perf_compare.py"`, `OUT=results/$RUN_ID`, `export PERF_COMMIT=$(git -C repo rev-parse --short HEAD)`.
- `invoke NAME CMD...`：执行，失败则 `echo "$NAME\t$(tail -1 stderr)" >> $OUT/failures.txt`，不中断。
- 函数 `mount_kernel PROTO VARIANT`：`mount -t nfs -o vers=$PROTO,rsize=1048576,wsize=1048576,hard,proto=tcp[,lookupcache=none] $LIF:$EXPORT $MNT`；`export PERF_MOUNT_VARIANT=$VARIANT PERF_PROTOCOL=$PROTO`。
- 矩阵（每协议）：
  ```
  mount default
    for H in rust python:
      metadata (io 无关)
      for io in direct buffered: for size in 4k 40m 1g: for qd in 1 8: [4k 只跑 qd 1] data
      for io in direct buffered: for mode in same distinct: multiclient --size 1g
  umount; mount nolookup; metadata ×2 harness; umount
  nfsrs URL=nfs://$LIF$EXPORT?version=$PROTO&rsize=1048576&wsize=1048576&uid=0&gid=0
    for H in rust python: metadata; data 全组合(无 io); multiclient same/distinct
  ```
- 文件名：`$OUT/$PROTO/$H-$BACKEND-${VARIANT:-na}-${IO:-na}-$SUITE-$PARAMS.json`。
- 交叉验证：`LIF=10.128.61.201` 只跑 rust nfsrs 与 rust posix(default,direct) 的 `data --size 1g --qd 8`，输出到 `$OUT/lif-201/`。
- 结束 `trap`：umount（若已挂载）、通过 nfsrs 或挂载删除 `$EXPORT/$RUN_ID` 残留。
- 每个用例前 `echo "[$(date +%T)] $NAME"` 到 `$OUT/progress.log`。

- [ ] **Step 1:** 写两个脚本，`bash -n` 语法检查，`shellcheck`（若有）。
- [ ] **Step 2:** commit `feat(perf-compare): add deploy and matrix driver scripts`。

---

### Task 11: 执行与报告

- [ ] **Step 1: 存储准备**：`ONTAP_USER=admin ONTAP_PASS=... python3 tests/benchmarks/compare/ontap_prepare.py --mgmt 10.128.61.20 --svm lizy --client 10.131.6.181 status`，再 `prepare`；记录原 `tcp_max_transfer_size`。
- [ ] **Step 2: 部署**：`tests/benchmarks/compare/deploy.sh 10.131.6.181`。
- [ ] **Step 3: 验证挂载**：node181 上 `showmount -e 10.128.61.200 | grep nfsrs_perf`；`for v in 3 4.0 4.1: mount -o vers=$v ... && touch && umount`。
- [ ] **Step 4: smoke**：node181 上 v3，4 种 harness×backend 各跑 `--smoke metadata`、`--smoke data --size 40m --qd 8`；检查 JSON 与 rsize/wsize=1048576。
- [ ] **Step 5: 全量**：`nohup run.sh perf-20260904 3 4.0 4.1 > run.log 2>&1 &`，轮询 `progress.log`；完成后 rsync `results/` 回本机 `tests/benchmarks/compare/results/2026-09-04/`。
- [ ] **Step 6: 报告**：撰写 `notes.md`（分析 + 限制），`report.py --results-dir ... --notes notes.md --md docs/benchmarks/fas2750-nfsrs-vs-kernel-2026-09-04.md --html /tmp/.../report.html`；人工核对每格；commit `docs: add FAS2750 nfs-rs vs kernel benchmark report`。
- [ ] **Step 7: 回滚询问**：向用户报告，并询问是否 `rollback --restore-transfer-size` / 删卷。
