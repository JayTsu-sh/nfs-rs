# Design: nfs-rs vs 内核 mount 性能对比基准（FAS2750）

Status: Draft
Date: 2026-09-04

## 1. 目标

在同一台客户端、同一套存储、同一操作序列下，量化 nfs-rs 用户态客户端
（Rust API 与 Python 封装）相对于 Linux 内核 NFS 挂载的性能差距，覆盖：

- 协议：NFSv3、NFSv4.0、NFSv4.1
- 负载：元数据操作；4 KiB / 40 MiB / 1 GiB 数据读写
- 并发：单客户端 QD=1、QD=8；8 路独立客户端读同一文件 / 不同文件

产出一份可追溯的报告（Markdown 进仓库 + 同内容 HTML），结构参考
`nfs-rs-test-report.html`（执行摘要 → 环境 → 方法 → 结果 → 分析 → 限制）。

非目标：不做 CI gate、不改动现有 `nfs-storage-benchmark` 及其基线体系、
不优化 nfs-rs 本身。发现的性能问题只记录，不在本任务内修。

## 2. 环境

### 2.1 存储（FAS2750，ONTAP 9.19.1）

| 项 | 值 |
|---|---|
| SVM | `lizy`（v3 / v4.0 / v4.1 均已启用；`tcp_max_transfer_size` 当前 64 KiB） |
| 数据 LIF | `10.128.61.200`（主）、`10.128.61.201`（交叉验证） |
| 管理 | `10.128.61.20`，REST API，凭据由运行者在环境变量 `ONTAP_USER` / `ONTAP_PASS` 提供，不进仓库 |

**准备步骤（通过 REST 执行，每一步都是幂等的、可回滚的）：**

1. 在 `lizy` 创建导出策略 `nfsrs_perf`，规则：clients `10.131.6.181/32`，
   protocols `nfs3,nfs4`，ro/rw/superuser `sys`。
2. 在 `lizy` 剩余空间最大的 aggregate 上创建 flexvol `nfsrs_perf`，50 GB，
   junction `/nfsrs_perf`，绑定策略 `nfsrs_perf`，`security_style=unix`。
3. `lizy` NFS 服务 `transport.tcp_max_transfer_size` 64 KiB → 1 MiB
   （SVM 级、对已有挂载无影响；新挂载可协商到 1 MiB）。

**回滚：** 测试结束后询问用户；默认保留卷和策略、`tcp_max_transfer_size` 改回 64 KiB。

### 2.2 客户端（node181，10.131.6.181）

Rocky 9.4，kernel 5.14.0-427，16 核 / 31 GB，10 GbE `ens192`，到 LIF RTT ≈ 0.28 ms，
glibc 2.34，rustc 1.96，Python 3.11.7，`mount.nfs` 可用，root。

已知噪声源：节点运行 k3s / clickhouse / prometheus，空闲 load ≈ 1.5，
内存已用 19 GB。报告中如实注明；每个吞吐用例重复 5 次取中位数以抑制抖动。

网络限制：`crates.io` 被封（403），PyPI 走清华镜像可用。因此：

- **Rust**：本机 `cargo fetch --locked` 后 rsync `~/.cargo/registry` 到 node181，
  在 node181 上 `cargo build --release --offline --bin nfs-perf-compare`
  （不能在 WSL 交叉构建，glibc 2.39 二进制无法在 2.34 上运行）。
- **Python**：node181 上建 venv，`pip install nfs-rs==0.6.1`
  （manylinux_2_17 wheel，与仓库 HEAD `3aa5654` 同一发布提交，也是用户实际安装的形态）。

工作目录：`/root/nfs-rs-perf/`（仓库 rsync 副本 + venv + 结果）。

### 2.3 挂载与 URL

内核挂载（`/mnt/nfsrs_perf`）：

| 变体 | 选项 | 用途 |
|---|---|---|
| `default` | `vers={3,4.0,4.1},rsize=1048576,wsize=1048576,hard,proto=tcp` | 数据路径 + 元数据 |
| `nolookup` | 同上 + `lookupcache=none` | 仅元数据（对齐生产挂载配置） |

nfs-rs URL：`nfs://10.128.61.200/nfsrs_perf?version={3,4.0,4.1}&rsize=1048576&wsize=1048576&uid=0&gid=0`

两侧都在导出内各自的子目录下工作（`/nfsrs_perf/<run_id>/<harness>-<backend>/`），
互不干扰，跑完删除。

## 3. 测试矩阵

| 维度 | 取值 |
|---|---|
| 协议 | 3 / 4.0 / 4.1 |
| harness | `rust` / `python` |
| backend | `nfsrs`（用户态直连）/ `posix`（内核挂载） |
| 内核数据 I/O 模式 | `direct`（`O_DIRECT`，与用户态同口径，作结论依据）/ `buffered`（冷读前 `drop_caches`，用户真实体验；再读一次作热读参考） |
| 大小 | 4 KiB / 40 MiB / 1 GiB |
| 单客户端并发 | QD=1 / QD=8 |
| 多客户端 | 8 路独立进程读 1 GiB：`same`（同一文件）/ `distinct`（各自文件） |

### 3.1 元数据套件（`metadata`）

每种操作 200 次，**每次用唯一名字**（`m/d_<i>` → `m/d_<i>/f` → 改名为 `g`），
避免内核 dcache/属性缓存把重复路径变成零成本；readdir 单独在一个预建的
1000 项目录上执行 20 次。报 p50 / p95 / p99 / mean（ms）和 ops/s。

| 操作 | Rust nfs-rs | Python nfs-rs | POSIX（Rust `std::fs` / Python `os`） |
|---|---|---|---|
| mkdir | `mkdir_path` | `client.mkdir` | `mkdir` |
| create | `create_path` + `close` | `client.touch` | `open(O_CREAT\|O_EXCL)` + `close` |
| stat | `getattr_path` | `client.stat` | `stat` |
| access | `access_path` | — | `access` |
| chmod | `setattr_path` | `client.chmod` | `chmod` |
| rename | `rename_path` | `client.rename` | `rename` |
| readdir | `readdir` 流耗尽 | `client.listdir` | `read_dir` / `os.listdir` |
| remove | `remove_path` | `client.remove` | `unlink` |
| rmdir | `rmdir_path` | `client.rmdir` | `rmdir` |

固有差异如实标注，不做"惩罚性"配置：内核 stat 可能命中 create 返回的属性缓存；
nfs-rs 的 `_path` 方法每次从根逐级 LOOKUP、无缓存（路径保持 3–4 层）。

### 3.2 数据套件（`data`）

- 写：创建文件 → 按 1 MiB 分块写满 S 字节（QD 路并发 in-flight）→ COMMIT/`fsync` → close。
  计时范围 create → fsync 完成。
- 读：open → 按 1 MiB 分块读满（QD 路并发）→ close，计时后**在计时外**校验内容。
- 数据模式：1 MiB 周期性模式块重复（`byte = (i*17+29) % 251`），Rust/Python 用
  相同模式，校验只需按块比较。
- 4 KiB：单次 write+commit、单次 read，200 个文件，报延迟分布；QD 维度不适用。
- 40 MiB / 1 GiB：重复 5 次（每次新文件名），报 MiB/s 的中位数及 min/max。
- `O_DIRECT`：缓冲区 4096 对齐（Rust `std::alloc` 指定对齐；Python 匿名 `mmap`），
  三种大小都是 4096 的倍数。
- 并发实现：Rust nfs-rs 用 `Arc<dyn Mount>` + QD 个 tokio 任务从 `AtomicU64`
  取块号；Rust POSIX 用 `std::thread::scope` QD 线程 `pread`/`pwrite`；
  Python nfs-rs 用 `AsyncClient` + `asyncio.gather` 的 QD 个协程调用
  `AsyncFile.read_at`/`write_at`；Python POSIX 用 `ThreadPoolExecutor(QD)` +
  `os.preadv`/`os.pwritev`（释放 GIL）。
- 短读/短写按返回长度循环补齐。COMMIT 用 `count=0`（RFC 1813 §3.3.21：到文件末尾）。

### 3.3 多客户端套件（`multiclient`）

harness 以 `--worker` 参数重新拉起自身 8 个进程（Python 用 `multiprocessing`），
每个进程独立建立连接（nfs-rs）或独立打开文件（POSIX），各自完整读 1 GiB，
父进程测总墙钟，报聚合 MiB/s = 8 GiB / 墙钟。
内核侧跑 `direct` 和 `buffered`（后者正是 page cache 共享场景）两种。

## 4. 代码结构

```
src/bin/nfs-perf-compare.rs             Rust harness（随 crate 一起发布，与现有 bin 同模式）
tests/benchmarks/compare/
  perf_compare.py                       Python harness（同一 CLI、同一 JSON schema）
  ontap_prepare.py                      存储准备/回滚（REST，幂等）
  run.sh                                node181 上的矩阵驱动：挂载→跑→卸载→收集
  report.py                             results/*.json → Markdown + HTML
  results/<date>/                       原始 JSON（进仓库，作为报告依据）
docs/benchmarks/fas2750-nfsrs-vs-kernel-<date>.md   报告
```

新增依赖：无。`O_DIRECT` 常量取自已依赖的 `nix::libc`。Python harness 仅用标准库 + `nfs_rs`。

### 4.1 CLI（Rust 与 Python 完全一致）

```
nfs-perf-compare --target <nfs://... | /mnt/path> --workdir <name> --json <out>
    metadata   [--iters 200] [--readdir-entries 1000]
    data       --size 4k|40m|1g --qd 1|8 [--io direct|buffered] [--repeat 5] [--iters 200]
    multiclient --size 1g --clients 8 --mode same|distinct [--io direct|buffered]
    --worker ...（内部使用）
```

backend 由 `--target` 的形态推断（`nfs://` → nfsrs，绝对路径 → posix）。
`--io` 只对 posix 有效，对 nfsrs 忽略。`--smoke` 把 iters/repeat 压到 1 用于连通性验证。

### 4.2 结果 JSON schema

```json
{
  "schema": 1,
  "harness": "rust | python",
  "backend": "nfsrs | posix",
  "protocol": "3 | 4.0 | 4.1",
  "target": "...",
  "mount_variant": "default | nolookup | null",
  "io_mode": "direct | buffered | null",
  "suite": "metadata | data | multiclient",
  "params": {"size": "1g", "qd": 8, "iters": 200, "repeat": 5, "clients": 8, "mode": "same"},
  "env": {"hostname": "node181", "kernel": "...", "nfs_rs_version": "0.6.1",
          "commit": "3aa5654", "rsize": 1048576, "wsize": 1048576, "captured_at_unix": 0},
  "peak_rss_kib": 0,
  "results": [
    {"name": "create", "unit": "ms", "samples": [], "p50": 0, "p95": 0, "p99": 0, "mean": 0, "ops_s": 0},
    {"name": "write",  "unit": "MiB/s", "samples": [], "median": 0, "min": 0, "max": 0},
    {"name": "read_hot", "unit": "MiB/s", "reference_only": true, "samples": [], "median": 0}
  ]
}
```

`peak_rss_kib` 取自进程结束前 `/proc/self/status` 的 `VmHWM`；multiclient 取 8 个 worker 的最大值。

### 4.3 驱动流程（`run.sh`）

```
for proto in 3 4.0 4.1:
  mount default  → rust posix {metadata, data×(size,qd,io), multiclient×(mode,io)}
                 → python posix 同上
                 → umount
  mount nolookup → rust posix metadata; python posix metadata → umount
  rust nfsrs   {metadata, data×(size,qd), multiclient×mode}
  python nfsrs 同上
LIF .201：仅 rust nfsrs + rust posix(default, direct) 的 data 1g qd=8 读写，作交叉验证
```

每个用例一个 JSON 文件：`<proto>/<harness>-<backend>-<variant>-<suite>-<params>.json`。
任一用例失败不中断矩阵，记录到 `failures.txt`，报告里标 N/A 并附原因。

### 4.4 报告（`report.py`）

- 执行摘要：每协议一行 —— 元数据 p50 比值（nfs-rs / kernel）、1 GiB QD=8 读写比值、
  多客户端同文件比值，Rust 与 Python 各一列。
- 结果表按协议分节：元数据（default vs nolookup 两组内核列）、数据（size × QD，
  direct 列为结论依据，buffered 冷/热为参考）、多客户端。
- 分析：差距来源（RTT × 块大小 / 流水线深度 / 路径解析缓存 / page cache 共享 /
  Python 封装开销 = python-nfsrs vs rust-nfsrs）。
- 限制：node181 噪声、单 SVM 共享、单次日期、4.0 experimental 状态。
- HTML 由同一数据生成，自包含、无外部资源。

## 5. 错误与清理

- harness：任何 I/O 错误 → 非零退出 + stderr 一行原因，JSON 不写出；
  `Drop`/`finally` 中尽力删除本用例创建的文件。
- 读回校验失败视为用例失败（数据不可信）。
- `run.sh` 结束时 `always` 卸载挂载点、删除 `/nfsrs_perf/<run_id>`。
- 遵守仓库规范：生产代码无 `unwrap`/`expect`，错误用 `thiserror` 枚举。

## 6. 验证方式

1. 存储准备后：node181 `showmount -e 10.128.61.200 | grep nfsrs_perf`，
   三种协议各手工 mount/umount 一次。
2. harness `--smoke`：4 个 (harness × backend) 组合在 v3 上各跑一次 metadata +
   data 4k + data 40m qd=8，读回校验通过。
3. 全量矩阵，预计 ≈ 1.5 小时（1 GiB 用例：3 协议 × ~10 组合 × 5 次 × ~3 s ≈ 8 min；
   多客户端 8 GiB × 3 × 4 × 2 模式 ≈ 10 min；其余为元数据和小文件）。
4. 报告生成后人工核对：每个格子都有数据或有失败原因；比值方向正确。

## 7. 备选方案（已否决）

- 扩展 `nfs-storage-benchmark`：被 nightly/release gate 消费，schema 和参数校验耦合，改动风险高。
- fio 做数据路径：与 nfs-rs 的 I/O 形态不等价，且 node181 无 fio、crates.io/EPEL 可达性未知。
- 在 WSL 本机测：NAT 虚拟网卡、无 `nfs-common`，绝对值无意义。
