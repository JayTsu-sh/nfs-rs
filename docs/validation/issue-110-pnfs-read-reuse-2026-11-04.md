# Issue #110：pNFS READ 复用与 AFX 回归

日期：2026-11-04。方案、代码、确定性验证和实机结果分别列出；内存测量是
补充对照，不替代现有发布性能基线与门禁。

## 方案与源码

- 修改前：`01209c958087c6124a94e899c64955c9a359c7d2`，nfs-rs 0.11.2。
- 修改后受测代码：`feec7bbe607782ac81599c506af0ff419a6dbe6c`，同为 0.11.2。
  Cargo.toml / Cargo.lock 未修改，Rust 1.95.0、release 构建和依赖一致。
- 改动位于 [`pnfs_io.rs`](../../src/nfs41/pnfs_io.rs)：私有 `ReadAssembly`
  用于每个 stripe 的短响应组装和最终 stripe 拼接。第一段非空 `Bytes` 直接
  保留 owner，第二段非空内容出现后才分配连续缓冲区；空片段不触发复制。
- 单个 DS 响应、单个有效 stripe 和提前 EOF 的单段结果可直接移交 backing。
  独立 DS backing 通常仍需一次最终拼接；不承诺所有读取都变成零复制。
- DS 长度检查、短读偏移、协商上限、EOF 前缀、generation、layout 刷新与
  fallback 保持原有逻辑。没有修改 session/replay、WRITE 或进程 allocator。
- 公开 README 和 Unreleased changelog 已更新。返回的切片仍可持有完整 RPC
  backing，其释放时点由最后一个 owner 决定。

调研与因果对照见[调研报告](../issue-110-nfs41-rss-research.md)。本轮生产
二进制没有私有强制 MDS 开关或路径计数插桩。

## 确定性与审查

单响应 backing 指针测试在旧复制行为上失败，在新实现上通过。新增组装
测试覆盖空片段、顺序和 owner 释放；真实 TCP/RPC 编解码用例覆盖短读续传、
DS 协商上限及较早 stripe EOF 截断后续成功结果。既有稀疏/密集多 DS、
错误刷新、取消、generation、WRITE 不确定结果和清理用例继续通过。

- pNFS 测试组：59 passed。
- fmt、clippy all-targets `-D warnings`：通过。
- Rust all-targets：745 passed、0 failed、30 个物理/压力用例默认 ignored。
- doctest：48 passed；Python bindings Rust 组合：708 passed。
- 当前 checkout 的独立 test-support wheel：设置 `NFS_RS_TEST_INSTALLED=1`
  后 398 passed、1 项因未配置实机 URL 跳过；stubtest 通过。
  该 wheel 仅用于本地契约验证，没有发布，也没有用于 AFX 物理验收。
- Standards / Spec 双项代码审查：0 个遗留问题。
- [受测代码的 GitHub CI](https://github.com/JayTsu-sh/nfs-rs/actions/runs/41759582786)
  通过，包含 Rust、Python 3.11/3.14 和 manylinux2014 产物检查。

## FAS2750 内存对照

同一执行器（Linux 6.8.0-142-generic、glibc 2.39、24 CPU），相同
`/deep_sync_nfs` fixture。A/B 为同一 SVM 的 `.200/.201` 两个入口，精确
NFSv4.1、默认保留源端口、UID/GID 0。所有 malloc/Tokio 调优环境变量均未
设置。数据使用共享两周期 backing，READ 完整校验内容和 EOF。

每版两个 LIF × 三次独立重复，分别包含 seed WRITE、QD1/QD8 READ-only、
QD1/QD8 完整写读和八进程同步读取。单进程读三轮，并发 worker 各读一轮。
每版 78 个进程，两版 156 个。以下为三个独立进程的中位数，单位 MiB；
累计分配取第二轮 `read-1`，RSS 为整个进程各阶段的最大 VmHWM。

| LIF / case | 峰值 RSS 前 → 后 | READ 累计分配前 → 后 |
| --- | --- | --- |
| A / READ-only QD1 | 12.60 → 11.81 | 195.47 → 131.48 |
| A / READ-only QD8 | 40.54 → 19.35 | 195.47 → 131.48 |
| A / 完整写读 QD8 | 38.21 → 22.64 | 195.47 → 131.48 |
| B / READ-only QD1 | 12.29 → 12.07 | 65.77 → 65.77 |
| B / READ-only QD8 | 19.97 → 16.88 | 65.77 → 65.77 |
| B / 完整写读 QD8 | 16.28 → 16.17 | 65.77 → 65.77 |

A 每读取 64 MiB 少约 64 MiB 的累计分配，符合删除 stripe 中间复制的预期；
最终多段拼接仍有真实成本。A/QD8 READ-only 的中位峰值 RSS 下降约 52.3%，
不是对所有环境的收益承诺。B 的 MDS 路径累计分配基本不变，其 RSS 差异
不能归因于本次没有执行到的 pNFS 优化。

八组同 case 的中位 READ 吞吐比（after/before）范围为 `0.9993–1.0324`。
计时包含逐字节校验，不能将这些补充数字当作发布吞吐验收或显著提速证明。
所有适用的存活堆尾段检查和卸载后大块释放检查通过；八进程 worker 的单轮
读取没有被记作稳态通过，也没有将各 worker 的个体峰值求和当作整机峰值。

### 原始 1 GiB 规模扩展

每个 LIF、每版一个新进程，QD8 写入再完整读取一次，共四个进程。
这是单样本扩展，不计算稳定百分比收益；单轮不评估稳态。

| LIF | 峰值 RSS MiB 前 → 后 | READ 累计分配 MiB 前 → 后 | READ MiB/s 前 → 后 |
| --- | --- | --- | --- |
| A | 42.39 → 21.18 | 3137.37 → 2113.53 | 100.24 → 99.10 |
| B | 17.49 → 18.03 | 1051.59 → 1051.59 | 100.42 → 101.33 |

两版数据、EOF 与释放检查全部通过。四组 FAS run 均持有 tests/performance
两把共享锁，唯一 `release-*` 目录通过 prepare 创建，EXIT 无条件 cleanup，
最终状态均为 0。所有编译在采样开始前完成。

## AFX 物理回归

使用既有 AFX / ONTAP 9.19.1P2、`svm2:/pnfs_test2`、数据执行器
`192.168.9.17`。静态 musl 二进制从受测提交构建；不将 musl/AFX 的 RSS 与
上面的 glibc/FAS 数据混作性能对照。未修改服务端配置或 LIF 状态。

### 四 LIF 正常读取与释放

run ID：`release-issue110-afx-normal-20261104T022049Z`。
`.101/.102/.103/.104` 各执行精确 v4.1、64 MiB、QD1/QD8、完整写读与
八进程读取，单进程读取三轮，共 52 个进程报告。内容、EOF、适用的尾段
检查及卸载后新增大块释放全部通过，`FINAL_STATUS=0`，fixture 已清理。
该组本身不作为每个 LIF 都已抓包证明独立 DS 的证据。

执行器使用导出的现有 benchmark/controller 和目录工具；为脱离 Git 元数据的
私有脚本集合执行，输出目录检查以启动时的脚本根目录代替 `git rev-parse`。
测试判定、run ID/路径校验和准备/清理逻辑未改；额外持有 AFX 共享锁
`/tmp/nfsrs-svm2-issue40.lock`。

### T15：单次 WRITE 双 DS 部分失败与恢复 READ

run ID：`release-issue110-afx-t15-20261104T022202Z`。
使用现有 [`run-afx-t15-e2e.sh`](../../tests/lab/run-afx-t15-e2e.sh) 与严格
抓包验收。20 GiB seed，单次 64 KiB WRITE 起点为 `1073709056`，跨 1 GiB
边界拆成两个 32 KiB stripe。正常 DS `.102`，故障 DS `.103`，二者均不等于
MDS 入口 `.101`。故障只匹配本次 probe 所属的坏 DS TCP socket。

| 验收项 | 结果 |
| --- | --- |
| 成功 sibling 应答 | 32768 bytes、status 0 |
| 故障结果与耗时 | Uncertain / VerifyThenResume，30.002 s |
| 故障规则命中 | 10 packets |
| MDS WRITE fallback | 0 |
| 恢复后实际内容 | 前 32 KiB 新模式、后 32 KiB 旧模式 |
| CLOSE / UMOUNT / reopen / resume | 通过 |
| 新挂载全文件内容与长度校验 | 21474836480 bytes，通过 |
| 完整故障窗口 pcap | 142 captured / 142 received / 0 kernel drops |
| 故障恢复及 fixture 清理 | pass / pass |

全文件 SHA-256：
`e203deb8ab1412f5e7d6a3b26cae117f4c36f296100b53943154fb6866ee4ffd`。

另用限制长度/数量的 RPC 请求头采样，解码 962 个含 READ 的 packet rows、
967 个 `ds-read` COMPOUND；TCP 合并包可能有多个请求。`.102/.103` 均出现，
FH 相同且与 T15 WRITE 的 FH 匹配，值不公开。恢复 inspect 的起止事件窗口内，
两个 DS READ 分别为 `(1073709056, 32768)` 与 `(1073741824, 32768)`；因此实际
执行到了跨 DS 的 pNFS READ 组装，且新旧半段内容检查通过。此采样只证明所
观察到的请求路径，不是完整 READ 流量捕获或 replay/取消的物理验收。

T15 的源码 SHA、probe hash、pcap hash 和私有归档逐一核对通过；故障表检查
确认移除，正常测试和 T15 的执行器临时目录均已移除。采样 tcpdump 正常退出，
其执行器文件在归档后删除。原始数据留在 checkout 外的私有证据目录，另在
controller 的持久私有目录保留归档；没有管理凭据、FH 或 session ID 入库。

## 二进制身份与范围

| 工件 | SHA-256 |
| --- | --- |
| FAS before | `a7157bf29a870278e1e48c6c7640a6368da86cc0f6a3cd7dde59516bf1675686` |
| FAS after | `70ba1b408dfb0443e250fbf4aaf3f4291df655acfd40454d8fbe5fead5bf010e` |
| AFX memory | `3273f672cf6fd2c9a0e2712374b1d1dbd57b68740bb7e5cd3b4533c679e40a44` |
| AFX T15 probe | `e44e0c6f3e6726a6fbf6e6d1ba3a343c3e263cd369c41d9b78381ab27368f128` |

本轮共 212 个 memory benchmark 进程报告（156 + 4 + 52），均为 pass；所有
适用的内容、EOF 和释放检查通过，T15 另列。没有执行数小时压力、AFX 物理取消/相反完成顺序、
节点宕机或整个 nightly；相应确定性用例没有替代这些物理场景。没有调整
accepted baseline/阈值，也没有据此声明所有 pNFS RSS 差异消失或发布完成。

复现工具、参数与安全约束见[内存基准说明](../benchmarks/memory-regression.md)
和[lab README](../../tests/lab/README.md)。
