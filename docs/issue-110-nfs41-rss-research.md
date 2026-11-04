# Issue #110：NFSv4.1 峰值 RSS 调研

日期：2026-11-03。当前源码快照：`c393c94330a1375ca58a3f4e6c4d8a1cce16366e`。
范围：[issue #110](https://github.com/JayTsu-sh/nfs-rs/issues/110) 中的
FAS2750 客户端内存差异、历史基准与当前实现。本文把源码事实、历史测量和
本次补充测量分别记录；补充测量不替代发布性能验收。


本次结论：前向 session slot 缓存 READ reply 的猜测不成立。当前和历史实现
的物理对照均显示，RSS 明显受到已释放页的 allocator 驻留影响；历史基准还
对 v4.1 额外复制了大多数写块。并发/预读缓冲区确有真实内存成本，但不能将
整段 VmHWM 当作仍存活的 reply。以下记录证据和未覆盖范围，本次没有产品
修复或发布验收结论。

后续已实施单段读取复用和公平内存基准；修改前后实测、RSS 改善的限制与
验证范围见[实施验证报告](validation/issue-110-memory.md)。本文保留调研阶段记录。

## 源码结论

当前实现与历史报告使用的实现，都没有在前向会话槽位中保存 READ 响应。
因此，不能把 issue 中约 30–50 MiB 的差异直接解释成“每个 slot 留存一份
1 MiB reply”。差异需要从实际存活分配量、并发缓冲区和分配器驻留量区分。

| 环节 | 当前源码证据 | 对内存判断的意义 |
| --- | --- | --- |
| 前向会话槽位 | [`session.rs`](../src/nfs41/session.rs)，`Slot` / `SlotTable`，约 155–208 行。`Slot` 仅包含 `AtomicU32` 序列号，表持有索引池、信号量和状态位。 | 没有 `Bytes`、reply 或 1 MiB 大小的数据字段，也没有按协商最大响应大小预分配 payload。 |
| 槽位归还 | 同文件 `release`，约 274–284 行；[`mount.rs`](../src/nfs41/mount.rs) 的 `compound_budgeted`，约 432–643 行。 | 槽位只保存序列状态；正常返回 `resp` 后，局部槽位守卫销毁并归还槽位。轮转槽位不能保留响应。 |
| RPC 接收 | [`rpc/mod.rs`](../src/rpc/mod.rs)，`read_one_response`，约 810–858 行。每条 record 新建 `BytesMut`，读取后 `freeze()`。多片 record 可扩大该次分配。 | 没有跨 record 复用且永不缩容的接收大缓冲区。一次正常 READ 对应一份接收 backing。 |
| RPC 分发 | 同文件 `reader_loop`，约 695–754 行。`pending.remove(xid)` 后将 `Bytes` 移交给 oneshot。 | pending 表持有等待者发送端；成功完成时移除条目，没有把 reply 放进持久字典。 |
| COMPOUND 解码 | [`compound.rs`](../src/nfs41/compound.rs)，`CompoundResponse::decode`，约 1284–1332 行。各 `OpResponse.data` 是剩余 `Bytes` 的浅克隆。 | SEQUENCE、PUTFH、READ 视图共享同一接收分配；三个视图不等于三份 READ payload。 |
| READ 返回 | [`read.rs`](../src/nfs41/read.rs)，`mds_read`，约 45–77 行。返回 `data.slice(..data_len)`，局部 `resp` 退出作用域。 | 调用者持有切片时，完整 RPC backing 继续存活；最后一个视图释放后即可归还分配器。源码中没有将本次 READ 存入 session 的路径。 |
| 上层并发读 | [`fileio.rs`](../src/fileio.rs)，`read_chunks_with`，约 344–407 行。`buffered(READ_CONCURRENCY)`，并发上限 8。 | 有序消费时，完成但尚未交付的响应仍可存活；调用方自己的输出缓冲区需另算。 |
| 回调缓存 | [`callback.rs`](../src/nfs41/callback.rs)，`CallbackSlot`，约 54–75 行；`parse_compound`，约 245–335 行。 | 客户端确实缓存回调请求和响应，但这是反向通道 CB_COMPOUND；正常协商仅一个 slot、请求上限 4096 字节，不是前向 READ。 |

上述生命周期也符合锁定依赖 `bytes 1.11.1` 的共享语义：`Bytes::clone` 和
`slice` 共享 backing，不复制 payload。小切片可以延长大 backing 的寿命，
但需要找到一个长期持有该切片的 owner 才能认定泄漏。本次追踪的正常 MDS READ
路径中未找到这样的持久 owner。[Bytes 文档](https://docs.rs/bytes/1.11.1/bytes/struct.Bytes.html#sharing)

## 协议层面的澄清

SEQUENCE 的 `sa_cachethis` 请求的是**服务端**的 reply cache。
客户端作为 callback 的接收者时，才承担相应的反向通道 reply cache。
`ca_maxresponsesize_cached` 是协商上限，不表示客户端为每个前向槽位分配对应
大小的缓存。[RFC 5661 §18.46.3](https://www.rfc-editor.org/rfc/rfc5661.html#section-18.46.3)、
[§18.36.4](https://www.rfc-editor.org/rfc/rfc5661.html#section-18.36.4)

当前 `CompoundBuilder::apply_sequence_cache_policy` 根据操作类别设置
`cachethis`：READ 为只读，WRITE 等变更操作需要缓存。降低缓存请求或删除
变更操作的重放保护不能作为客户端 RSS 优化：既不能直接释放客户端大块，
也会影响故障后的执行语义。相关实现在
[`compound.rs`](../src/nfs41/compound.rs)，约 307–363 行；协议要求见
[RFC 5661 §2.10.6.1.3](https://www.rfc-editor.org/rfc/rfc5661.html#section-2.10.6.1.3)。

## 历史数据和可比性

issue 引用的两份报告已不在当前目录中，但可从提交
`4d9ab966e22b635f9fae5af4d494a233069f8899` 读取：

- [2026-09-04 报告](https://github.com/JayTsu-sh/nfs-rs/blob/4d9ab966e22b635f9fae5af4d494a233069f8899/docs/benchmarks/fas2750-nfsrs-vs-kernel-2026-09-04.md)：Rust QD8 写加读的 VmHWM 为 v3 14 MiB、v4.0 19 MiB、v4.1 47 MiB；报告记录 nfs-rs 0.6.1；环境表写 `9aad89d`，方法段另写 `31126d9`，历史提交元数据并不完全一致。
- [2026-09-05 报告](https://github.com/JayTsu-sh/nfs-rs/blob/4d9ab966e22b635f9fae5af4d494a233069f8899/docs/benchmarks/fas2750-nfsrs-readahead-writeback-2026-09-05.md)：v4.1 开窗前 QD1 14 MiB、QD8 48 MiB，开窗后 QD1 52 MiB、QD8 74 MiB。1 GiB 与多进程案例来自 `d6b1b42`；4 KiB/40 MiB 补测来自 `8a0de2c`。

历史 harness 实际位于 `src/bin/nfs-perf-compare/` 目录，入口为 `main.rs`，
不是 `src/bin/nfs-perf-compare.rs`。当前 checkout 已移除这个 binary。
历史实现可从同一提交的
[data.rs](https://github.com/JayTsu-sh/nfs-rs/blob/4d9ab966e22b635f9fae5af4d494a233069f8899/src/bin/nfs-perf-compare/data.rs)、
[nfsrs.rs](https://github.com/JayTsu-sh/nfs-rs/blob/4d9ab966e22b635f9fae5af4d494a233069f8899/src/bin/nfs-perf-compare/nfsrs.rs)、
[stats.rs](https://github.com/JayTsu-sh/nfs-rs/blob/4d9ab966e22b635f9fae5af4d494a233069f8899/src/bin/nfs-perf-compare/stats.rs)
核查。

历史 harness 有以下测量限制：

1. `peak_rss_kib` 读取 `/proc/self/status` 的 `VmHWM`，在完整写读及 shutdown
   之后记录。它是整个进程生命周期的峰值，不能定位来自写、读还是预读。
2. QD8 使用八个 `JoinSet::spawn` worker；每个 worker 读取一块、完整校验后
   再读取下一块。其默认 Tokio 多线程运行时可能把不同请求调度到不同线程。
3. `NfsRsBackend` 使用当时的 `BufferedFile`。在 `d6b1b42`，预读状态实际
   持有 `BTreeMap<u64, JoinHandle<Result<Bytes>>>`，回写状态还持有未 COMMIT
   的 `Vec<(u64, Bytes)>`。它们是 payload 的真实 owner，应与 slot 区分。
   当前 `BufferedFile` 已采用另一套有界批量 I/O，实现和参数不能等同。
4. 检查 `9aad89d`、`31126d9` 与 `d6b1b42` 的 `src/nfs41/session.rs`：历史 `Slot`
   同样只有序列号，`release` 同样只归还索引；COMPOUND/READ 同样返回共享
   `Bytes`。因此第二份报告把差异归因于 slot reply 保留的表述仍是猜测，
   不能作为已确认根因引用。

还有一个直接影响三协议公平性的差异：历史
[pattern.rs](https://github.com/JayTsu-sh/nfs-rs/blob/4d9ab966e22b635f9fae5af4d494a233069f8899/src/bin/nfs-perf-compare/pattern.rs)
的 `pattern_at` 以 1 MiB 为模式周期。v3/v4.0 的 1 MiB WRITE 总能引用
同一 `pattern_block` 的切片；v4.1 的 1020 KiB WRITE 通常跨周期，函数会新建
约 1 MiB 的 `BytesMut` 并复制模式数据。按连续 1020 KiB 块计算，每 256 块
有 254 块需要复制。因此历史“相同 I/O”包含不同的客户端分配工作量。
这是已确认的 harness 行为，尚不能单凭源码量化它对峰值 RSS 的贡献。
应对照共享两周期 backing 的等价模式切片，并将 WRITE 与独立进程 READ-only
的峰值分开记录。

当前的 raw `Mount` 对照可以检验当前传输/协议路径是否仍有额外驻留，不能
单独复现历史 `readahead=8&writeback=8` 的全部行为。需要复现旧版本时，须将
旧源码和 harness 放在独立 checkout，并保持同一 executor、负载与分配器。

## 可检验的候选原因

| 候选原因 | 支持或限制 | 区分实验 |
| --- | --- | --- |
| 有界并发响应和输出副本 | QD、预读、已完成未消费响应都可增加实际存活大块；COMPOUND 视图自身不复制 payload。 | 记录 `>=512 KiB` 存活分配量及峰值，按写、第一次读、第二次读分别采样；对照 QD1/QD8。 |
| 历史模式数据生成器的协议相关复制 | 1020 KiB v4.1 块大多跨越 1 MiB 模式周期，每次 WRITE 新建大分配；1 MiB v3/v4.0 块共享 backing。 | 相同 wire 请求使用两周期共享 backing 对照逐块复制；独立 READ-only 进程排除 WRITE 阶段的 allocator 历史。 |
| glibc arenas / 释放后的页仍驻留 | 多线程分配与释放可改变 arena 布局；RSS 包含已归还 malloc、尚未归还内核的页。仅凭 VmHWM 无法证明泄漏。 | 对照默认、`MALLOC_ARENA_MAX=1`、`MALLOC_MMAP_THRESHOLD_=131072`；若实际存活量不变而 RSS 明显下降，支持分配器驻留解释。 |
| Tokio worker 数与线程调度 | 默认多线程 worker 数取 CPU 数；`tokio::spawn` 读路径和额外后台工作会改变哪个线程分配 payload。线程栈及分配器状态也是 RSS 的一部分。 | 保持负载与协议，单独设置 `TOKIO_WORKER_THREADS=1`，再与 mmap threshold 组合；不能把线程数变化直接称为 slot 修复。 |
| 持久 payload owner / 泄漏 | 正常 MDS READ 槽位与 pending 表不支持这个解释；仍需动态测量覆盖上层、pNFS、异常路径。 | 同一 mount 重复读；所有返回值释放后检查大块存活量是否回落，空闲后和 umount 后再次采样。 |

glibc 官方文档说明 mmap 阈值默认会随分配模式动态调整，超过静态阈值的大块
可通过 mmap 分配并在 free 时返还内核；arena 数也可单独控制。这些是诊断变量，
本次调研不据此修改库的进程级 allocator 配置。
[glibc malloc tunables](https://sourceware.org/glibc/manual/latest/html_node/Memory-Allocation-Tunables.html)

`malloc_trim(0)` 可以作为 Linux/glibc 诊断对照，但它只是尝试返还空闲页；
返回后 RSS 降低支持存在可返还空闲内存，不说明应用此前仍持有 payload。
不应将其直接加入常规每次 READ 路径。
[Linux malloc_trim(3)](https://man7.org/linux/man-pages/man3/malloc_trim.3.html)

## 本次 FAS2750 补充测量

只读管理发现确认设备为 FAS2750、ONTAP 9.19.1，源、目标 LIF 属于同一 SVM。
下表以 A/B 表示这两个独立测量入口，不将它们视作两个独立存储系统。
使用既有、允许三个协议的导出；未修改服务端配置。执行器为 Linux 6.8、
glibc 2.39、24 个可用 CPU，Rust 1.95.0 release 构建。

原始脚本、管理命令输出与逐阶段机器本地 JSON 保留在 checkout 外。
每次运行持有既有 tests/performance 两把共享锁，使用唯一 `release-*` 目录，
通过显式重定向到 FAS 导出的通用目录辅助器执行 `prepare-run.sh`，并在 EXIT
清理中无条件执行 `cleanup-run.sh`。辅助器的 `afx` profile 名称不代表本次硬件；
其临时 NFSv4.2 挂载仅用于准备和清理目录。

首轮 v3 非保留端口挂载被拒绝，未计作有效样本且已清理；有效测量使用默认
保留源端口。此访问限制没有通过修改 export policy 绕过。

当前实现的测量方法：当前 release 构建，raw `Mount` 逐块写入 1 GiB、COMMIT、完整
校验读回两轮；两个 LIF 分别测试精确 v3/v4.0/v4.1，记录协商 I/O 大小。
用 `GlobalAlloc(System)` 包装器记录 Rust 存活/峰值分配字节和大块分配，
同时独立记录 RSS/VmHWM。补测 QD1、单 worker、单 arena、静态 mmap 阈值和
单 worker 加静态阈值的组合。allocator 包装器只覆盖 Rust allocator 的请求，
不能等同于 libc 全部内部状态、线程栈或进程完整 resident set。

数据准备必须使用共享 backing。当前 WRITE/COMMIT 实现可以保留待确认写入
的数据以处理 verifier 变化；若为每个块新建独立 payload 并直到 1 GiB 结束
才 COMMIT，可能测到整批未提交数据的必要留存。该结果不能与历史稳定写或
16 MiB 周期 COMMIT 的 `BufferedFile` 对比，也不能归因于 READ reply 泄漏。

### 当前实现：两个 LIF 的直接接口对照

每格为一个新进程的一次测量，单位 MiB。QD8 为最多八个并发 raw `Mount`
调用，通过 `buffer_unordered` 驱动；这不是历史 harness 的八个 spawn worker。
写入使用两周期共享模式 backing，全部 WRITE 结束后检查批次 COMMIT，关闭后
重新 OPEN 并完整校验两轮 READ。没有缓存整份 1 GiB 文件。
峰值 RSS 列取各阶段读取的最大 VmHWM；存活堆列为第二轮读完、返回值释放后。

| LIF | 协议 | 峰值 RSS | 第二轮读后 RSS | 此时存活堆 | trim 后 RSS（仍 OPEN） | umount 后存活大块 |
| --- | --- | --- | --- | --- | --- | --- |
| A | 3 | 9.27 | 9.27 | 2.10 | 9.18 | 0.00 |
| A | 4.0 | 13.80 | 13.80 | 1.10 | 11.64 | 0.00 |
| A | 4.1 | 27.99 | 24.04 | 2.11 | 12.42 | 0.00 |
| B | 3 | 10.08 | 10.08 | 2.10 | 9.99 | 0.00 |
| B | 4.0 | 13.95 | 13.95 | 1.10 | 11.80 | 0.00 |
| B | 4.1 | 12.98 | 12.98 | 1.11 | 11.71 | 0.00 |

两个 LIF、三个协议的所有读回校验均成功。v4.1 峰值仍高于 v3，但读完后实际
存活堆回到约 1–2 MiB；`malloc_trim(0)` 在仍持有 mount/OPEN 时已经能降低
RSS，而存活堆基本不变。umount 后所有 `>=512 KiB` 的 Rust 分配均释放。
这支持本次正常读路径中的显著 allocator 驻留，不能把这些 RSS 都计为仍被
session 持有的 reply；两轮重复读不能证明无限期压力下绝无增长。

### 当前实现：A 入口 v4.1 的进程级诊断对照

每行一个独立进程，写 1 GiB、完整校验读两轮，其余设置与上表一致。
环境变量分别为 `TOKIO_WORKER_THREADS=1`、`MALLOC_ARENA_MAX=1`、
`MALLOC_MMAP_THRESHOLD_=131072` 及线程数加 mmap 阈值组合。堆峰值与耗时
列均取第二轮 READ 阶段；这些计时包含验证 CPU 时间，不是发布吞吐验收。

| 配置 | 峰值 RSS MiB | 第二轮读后 RSS MiB | 此轮堆峰值 MiB | 读后存活堆 MiB | 此轮读耗时 s |
| --- | --- | --- | --- | --- | --- |
| 默认 QD8 / 24 workers | 27.99 | 24.04 | 12.45 | 2.11 | 10.09 |
| QD1 / 24 workers | 12.81 | 12.81 | 4.15 | 2.11 | 13.03 |
| QD8 / 1 worker | 25.64 | 20.62 | 12.43 | 2.04 | 10.12 |
| QD8 / 单 arena | 24.05 | 11.10 | 12.46 | 2.11 | 10.13 |
| QD8 / mmap 阈值 128 KiB | 16.52 | 11.58 | 12.60 | 2.11 | 10.66 |
| QD8 / 1 worker + 阈值 128 KiB | 12.96 | 9.68 | 12.44 | 2.04 | 10.44 |

在相同 QD8 下，第二轮实际堆峰值仍约 12.4 MiB、读完存活约 2.1 MiB，
但默认与静态 mmap 阈值的读后 RSS 分别约 24 MiB 和 12 MiB。单 worker 加
静态阈值降到约 10 MiB，完整性均通过。QD1 还减少了真实在途分配，故不能
将 QD1 的全部收益解释成分配器效果。单 worker 单独不能消除全部 RSS 差异。
这些对照直接支持 allocator/调度历史的贡献，不支持通过缩小前向 reply cache
来优化客户端内存。

### 历史实现：同一执行器上的重建对照

从 `d6b1b42b98aa4edc54d28367f2df425378e2b23a` 导出完整源码到 checkout 外，
使用其自己的 `Cargo.lock`，`cargo build --release --locked`，不切换当前工作树。
先保存未经修改的原始 binary，再在临时副本加入同样的分配计数和写/读阶段
采样。使用历史 `BufferedFile`、八个 `JoinSet::spawn` worker、1 GiB 写加一次
完整校验读，`--repeat 1`。该轮明确复现历史调用模型，但执行器/kernel 与
2026-09 原环境不同；不要求绝对值等于原报告的 47–69 MiB。

单位均为 MiB。读后存活堆在历史 `read_file` 返回前取样，包含尚在作用域的
模式块；它不能直接与当前探针已释放模式块后的数字相减。

| LIF | 协议 | 峰值 RSS | WRITE 堆峰值 | READ 堆峰值 | 读后 RSS | 读后存活堆 |
| --- | --- | --- | --- | --- | --- | --- |
| A | 3 | 14.57 | 3.12 | 6.12 | 12.95 | 3.10 |
| A | 4.0 | 10.82 | 2.14 | 4.14 | 10.66 | 2.10 |
| A | 4.1 | 31.12 | 11.24 | 10.15 | 26.77 | 3.11 |
| B | 3 | 10.27 | 3.12 | 5.12 | 10.27 | 3.10 |
| B | 4.0 | 13.37 | 2.14 | 4.14 | 11.41 | 2.10 |
| B | 4.1 | 33.84 | 10.12 | 4.15 | 27.63 | 2.11 |

随后保持同一历史 binary、文件内容和线上的请求大小，仅通过诊断环境变量
改变模式 backing 或分配器。共享模式块将原来的一个 1 MiB 周期扩为两个相同
周期，仍按 `offset % 1 MiB` 切片，使跨周期 WRITE 也无需分配；它额外保留一个
1 MiB 的公共模式块。预读/回写对照使用原来的 `readahead=8&writeback=8`，
外层 QD 为 1，不能与 QD8 表格混为同一负载。

| LIF | v4.1 配置 | 峰值 RSS MiB | WRITE 堆峰值 MiB | READ 堆峰值 MiB |
| --- | --- | --- | --- | --- |
| A | 原始生成器 / QD8 | 31.12 | 11.24 | 10.15 |
| A | 共享两周期 / QD8 | 22.80 | 4.27 | 8.91 |
| A | 原始生成器 / QD8 / mmap 128 KiB | 15.45 | 11.24 | 10.11 |
| A | 原始生成器 / QD1 / 预读回写各 8 | 27.90 | 19.08 | 12.13 |
| A | 共享两周期 / QD1 / 预读回写各 8 | 30.34 | 4.14 | 13.11 |
| B | 原始生成器 / QD8 | 33.84 | 10.12 | 4.15 |
| B | 共享两周期 / QD8 | 17.83 | 3.15 | 5.15 |
| B | 原始生成器 / QD8 / mmap 128 KiB | 15.29 | 10.12 | 4.15 |
| B | 原始生成器 / QD1 / 预读回写各 8 | 28.69 | 19.08 | 9.10 |
| B | 共享两周期 / QD1 / 预读回写各 8 | 28.74 | 4.14 | 12.09 |

A 入口还测了原生成器 QD8 的 单 arena 17.23 MiB、单 Tokio worker 17.18 MiB。
未插入分配计数/阶段采样的原始 binary，单次 v4.1 QD8 峰值为 39.49 MiB。
它与插入计数后的 31.12 MiB 不完全一致，说明调度、allocator 历史及测量方式
对绝对峰值有明显影响；不能从各一次采样声称稳定的百分比提升。

这些结果支持以下区分：

- **生成器复制有实测成本。** A 的 WRITE 存活堆峰值从 11.24 降到 4.27 MiB，
  B 从 10.12 降到 3.15 MiB；预读回写案例则从约 19.08 降到 4.14 MiB。
  这说明部分在途/未提交数据实际属于模式生成器和 `BufferedFile`，不是 slot。
- **allocator 驻留有独立证据。** B 在原生成器下，仅固定 mmap 阈值，WRITE
  堆峰值仍为 10.12 MiB、READ 仍为 4.15 MiB，但峰值 RSS 从 33.84 降到
  15.29 MiB，读后 RSS 从 27.63 降到 9.32 MiB。A 的结果方向相同。
- **共享 backing 不保证总峰值下降。** 预读回写开启后，A 的总峰值从 27.90
  变为 30.34 MiB，B 从 28.69 变为 28.74 MiB，即使 WRITE 分配显著减少。
  读响应、输出副本及分配器历史仍可主导总峰值，不能把 issue 的全部差异归给
  `pattern_at`，更不能把修正生成器描述为已经修复库的内存问题。

## 验证范围与后续处理

- 有效物理案例共 28 个新进程：当前实现 11 个、历史实现 17 个。
  当前每例读两轮，历史每例读一轮，全部完整内容校验通过；两轮 run 的
  prepare/cleanup 均成功，最终状态为 0。初始非保留端口失败及为排除未提交
  payload 留存而取消的探针不计入有效样本；其 run-owned 目录也已清理。
- 原始生成器和共享两周期生成器分别运行已有
  `pattern::tests::block_is_aligned_and_periodic`，各 1 项通过。
  探针、历史源码副本、逐阶段 JSON、命令日志均留在本机 checkout 外；仓库仅
  新增本调研文档，未写入管理凭据。文档空白检查通过。
- RSS 为 `/proc/self/status` 的近似计数。部分阶段的 VmHWM 读数出现回落；
  本文表格取阶段样本及最终报告中的最大值，原始 harness 的 JSON 仅取末次值，
  两者可能略有差异。Linux 文档明确 VmRSS/VmHWM 都不是精确计数；需要更精细
  的 resident 分析时，应补 `smaps_rollup`/分配调用栈采样。
  [proc_pid_status(5)](https://man7.org/linux/man-pages/man5/proc_pid_status.5.html)
- 每个配置只有一次独立进程测量；未复跑原八进程读、Python、长期稳态压力、
  故障/取消恢复，也未采集独立 DS 路由证据。因此本次不覆盖所有异常 owner
  生命周期，不构成 pNFS 物理证明或发布性能/内存验收。产品源码未改动，
  未运行全量 Rust/Python gate。

建议先更正 issue 的 slot/reply cache 归因；若恢复可复用内存基准，应统一
三协议的模式 backing，分别记录 WRITE、全新进程 READ-only、完整写读的
RSS 与存活分配，固定 executor/worker/QD，并重复采样。库侧后续优化应针对
已量化的缓冲区 owner/副本和并发预算；不要为降低客户端 RSS 删除服务端重放
保护，也不要在库中擅自修改进程级 glibc 参数或给每次 READ 加 `malloc_trim`。

## 2026-11-04：当前主分支与剩余问题复核

复核提交：`01209c958087c6124a94e899c64955c9a359c7d2`。本节保留上文历史
调研及其样本边界；以下单独列出本轮重现，未将既有 520 份报告改称本轮
重跑结果。本轮已通过私有插桩和同文件强制 MDS 对照，确认 A 的额外 READ
分配主要来自 pNFS 读取路径的两层复制；默认 RSS 的稳定改善仍待后续验证。

单段响应复用和公平内存基准已经随
[PR #145 / `806dcfa`](https://github.com/JayTsu-sh/nfs-rs/pull/145) 合入。
该提交与本轮主分支的 `src` tree 均为
`15f61267e822269f35c3372881efa2f9ac451995`，`Cargo.toml` / `Cargo.lock`
也无差异。因此已有[实施验证报告](validation/issue-110-memory.md)针对的是
当前生产实现，不应再次提出同一项输出复制修复。GitHub #110 仍打开、没有
评论；原 issue 中的 slot/reply-cache 猜测尚未同步更正。

### 已确认与尚未解决的边界

| 问题 | 已有证据 | 本轮判断 |
| --- | --- | --- |
| 前向 slot 是否永久持有 READ payload | 上述源码生命周期与历史实现检查；正常读完后存活大块回落 | 原归因已被否定，不能继续以“清空 slot reply”作为方案。 |
| 单段输出复制是否删除 | 指针共享回归；每读 64 MiB 的累计分配减少约 64 MiB | 已实施且已验证。 |
| 默认 RSS 差异是否消除 | 1 GiB QD8 的 A/v4.1 修改后仍为 41.16 MiB，B/v4.1 为 16.70 MiB；A 的 32 轮峰值仍为 76.58 MiB | 未消除；不能把复制优化或全部诊断通过等同于 issue 症状解决。 |
| A/B 分配成本为何不同 | 1 GiB READ 累计分配 A/v4.1 为 3137.37 MiB，B 为 1051.59 MiB | 本轮同源码的路径计数及强制 MDS 对照确认，主要额外分配来自 pNFS 的中间和最终复制；见下。 |
| 是否持续保留 payload | 既有 32 轮案例尾段大块范围为 0；全部 520 份报告的整体存活堆尾段范围最大 5128 bytes | 已测负载未显示持续增长；不覆盖数小时、物理故障、取消和全部 pNFS 路径。 |

这些数值来自既有实施验证报告。64 MiB 矩阵每配置重复三次；1 GiB、八进程
和 32 轮扩展各只有一组样本。A/B 是同一 SVM 的不同入口，不能当作独立设备
重复，也不能将其 RSS 混合建立统一 baseline。

### 当前提交的物理重现

本轮先用公平 backing 的现有诊断工具执行 64 MiB READ-only / QD8：每配置
一个新进程，完整读取三轮。累计分配取第二轮 READ（`read-1`，零起始编号），RSS 为进程阶段峰值，
单位 MiB。这是复现信号，尚不是多进程重复的稳定提升或发布验收。
未修改基线二进制 SHA-256 为
`a7157bf29a870278e1e48c6c7640a6368da86cc0f6a3cd7dde59516bf1675686`。

| 入口 / 协议 | 峰值 RSS | 第二轮 READ 累计分配（`read-1`） |
| --- | --- | --- |
| A / v3 | 15.473 | 64.313 |
| A / v4.1 | 34.063 | 195.469 |
| B / v4.1 | 20.254 | 65.773 |

A/v4.1 的峰值 RSS 约为同入口 v3 的 2.20 倍，本轮不是原 issue 的固定
三倍；A/B 的 v4.1 累计 READ 分配比为 2.972。两个 v4.1 入口协商的最大
READ/WRITE 均为 1044480 bytes，因此剩余差异不能仅由这两个协商上限的
不同解释；协商上限相同仍不能证明实际 DS 路由、分片和拷贝路径相同。

三个配置的内容、EOF 和释放检查均通过，准备/清理最终状态为 0。
idle 时 A/v4.1 的存活堆为 4.116 MiB（其中大块 4 MiB），B/v4.1 为
3.112 MiB（其中大块 3 MiB），仍包含模式 backing 和运行时对象。UMOUNT
后新增大块全部释放。这再次重现较高 RSS 和累计分配，没有提供持续 payload
泄漏证据。

将文件缩小至 8 MiB 后，同样的两个 v4.1 入口仍重现差异：A 的累计 READ
分配为 23.631 MiB、峰值 RSS 19.5625 MiB；B 为 8.273 MiB、16.6211 MiB。
内容、释放和清理检查仍通过，说明无需 1 GiB 文件就能观察额外分配。

### 因果对照：pNFS READ 的两层复制

在 checkout 外从当前提交导出源码，仅为诊断 binary 加入分阶段路径、复制
字节和 RPC record 计数，并提供私有强制 MDS READ 对照开关。所有配置使用
同一插桩 binary、同一已准备文件和新的独立进程；对同一入口只改变该诊断
开关。它不属于公开 URL/API，也没有写入产品源码。
插桩 binary SHA-256：
`d4b6215e4e86a87523ccc1ce0a44a7baccc036d72073f130d0a3b86dd923167c`。
执行器确认仍为 Linux `6.8.0-142-generic`、glibc 2.39、24 个可用 CPU。
这组对照的 malloc arena、mmap 阈值和 Tokio worker 环境变量均未设置；没有
通过 allocator 调参制造差值。

以下为 64 MiB READ-only / QD8 的第二轮 READ（`read-1`），单位 MiB；每配置一个新进程。

| 配置 | 累计分配 | 峰值 RSS | stripe 合并分配 / 复制 | 最终拼接分配 / 复制 | MDS READ payload |
| --- | --- | --- | --- | --- | --- |
| A / 默认路径 | 195.469 | 36.871 | 64.000001 / 64.000000 | 62.753906 / 62.753906 | 0 |
| A / 强制 MDS READ | 65.757 | 18.020 | 0 / 0 | 0 / 0 | 64 |
| B / 默认路径 | 65.773 | 18.711 | 0 / 0 | 0 / 0 | 64 |
| B / 强制 MDS READ | 65.757 | 15.793 | 0 / 0 | 0 / 0 | 64 |

A 默认实际进入 pNFS 读取路径，计数 129 个 stripe；B 默认已走 MDS READ。
所以“同为 v4.1、协商大小相同”没有使两个入口的客户端分配路径相同。
这项证据来自客户端实际路径计数，不将其扩展成已抓包证明独立 DS 拓扑。

已确认的复制链如下：

1. [pnfs_read_layout](../src/nfs41/pnfs_io.rs) 约 920、943 行：为每个 stripe
   分配 `combined`，将 DS 响应复制进去。本轮分配 67108865 bytes（含 EOF
   路径的 1-byte reserve），实际复制 67108864 bytes。
2. 同函数约 969–971 行：多个 stripe chunk 再分配最终连续结果并复制，
   本轮分配与复制各为 65802240 bytes。
3. 普通 [MDS READ](../src/nfs41/read.rs) 约 76 行：直接返回响应 `Bytes`
   切片，避开上述两层 pNFS 合并。现有 `BufferedFile::read_at` 单段复用
   已在上层生效，未消除下层 `pnfs_read_layout` 的复制。

这两层额外 payload 分配共约 126.754 MiB，解释了 A 默认相对同文件 MDS
对照约 129.712 MiB 累计分配增量的大部分；剩余约 2.958 MiB 尚未逐项归属。
所有配置的内容、EOF、释放检查和清理均通过。较高复制/并发分配会增加瞬时
内存压力，既有 allocator 对照也说明释放后的页可以继续驻留；不能把累计
分配或 RSS 峰值等同于仍存活的同等大小 payload。

A 默认三轮 READ 累计分配约为 196.487、195.469、195.469 MiB，强制 MDS
为 65.758、65.757、65.757 MiB，差异在重复轮次中保持。所有案例 UMOUNT 后
新增存活大块均为 0。插桩报告自身会将每阶段计数 JSON 保存在报告向量中，
整体 live 因此每轮增加约 5.6 KiB；不能将其说成整体堆增长严格为零，或把
这部分工具元数据直接归为库的 payload 泄漏。未重新执行 32 轮长期案例。

RPC record 增长假设在该负载中被否定：A 默认为 129 records / 129 fragments，
MDS 对照为 66 / 66，所有配置的 record 增长分配均为 0。A 初次 record
分配总量为 67123312 bytes，MDS 为 67115992 bytes，均接近实际 payload
加协议头；这里没有多片 record 扩容造成额外约 128 MiB 的现象。

原始诊断 binary 与插桩 binary 的 A 累计分配同为约 195.469 MiB，但峰值 RSS
分别为 34.063 和 36.871 MiB；B 默认和强制 MDS 的分配接近，RSS 也不同。
这说明不能把本轮单样本 RSS 差值换算成可复现的优化百分比。本轮三次矩阵
（64 MiB 重现、8 MiB 最小化、64 MiB 因果对照）准备/清理最终状态均为 0。
逐份核对报告 JSON：三次矩阵分别有 39、26、28 个进程报告，合计 93 个；
全部 `status=pass` 且 `large_allocations_released=true`。这 93 个报告与既有
520 个修改前后验证报告分开统计。

| 候选解释 | 本轮结论 |
| --- | --- |
| pNFS 与 MDS 路径不同、pNFS 中间及最终复制 | 实际计数和同文件单变量对照确认，是累计分配差异的主要来源。 |
| RPC 多片接收增长 | 本轮每条 record 一片、增长分配为 0，不解释此案例。 |
| allocator / 调度历史导致驻留 | 既有单变量实测支持；本轮 RSS 变化再次提示其影响，尚未重新量化全部驻留页。 |
| 前向 slot 持久缓存 READ reply | 原源码追踪不支持；本轮也没有持续存活大块增长证据。 |

### 一手资料再次核对

RFC 8881 将缓存责任放在请求的 **replier**：前向 READ 的 replier 是服务端；
客户端在反向 CB_COMPOUND 中才是 replier。`sa_cachethis` 控制是否要求对方
缓存完整结果，不能由协商的缓存上限推导客户端已分配等额 payload。
该语义也不支持削弱 WRITE 重放保护来压低客户端 RSS。
[RFC 8881 §2.10.6.1](https://www.rfc-editor.org/rfc/rfc8881.html#section-2.10.6.1)、
[§2.10.6.1.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-2.10.6.1.3)

`Bytes::slice` 共享底层分配并增加引用计数；可见长度缩短不表示 backing
等量缩小。因此需要同时记录 payload 长度、实际 backing 分配和最后一个 owner
的释放时点。[bytes 1.11.1 官方文档](https://docs.rs/bytes/1.11.1/bytes/struct.Bytes.html#method.slice)

以实测执行器的 glibc 2.39 为准：默认 mmap 阈值会动态调整；显式设置阈值
会禁用该调整。64 位平台默认 arena 上限与在线 CPU 数相关；默认 tcache
服务的最大请求仅为 1032 bytes，不能将 1 MiB READ backing 的驻留直接叫作
“tcache 缓存”。arena、碎片和动态 mmap 分配历史应分别调查。
[glibc 2.39 官方手册](https://sourceware.org/glibc/manual/2.39/html_node/Memory-Allocation-Tunables.html)

`VmHWM` 是驻留峰值，`VmRSS` 是当前驻留；Linux 的可扩展 RSS 计数存在近似性。
区分实际存活量与驻留页时，应同时保留 Rust 分配计数和 `smaps_rollup`/`smaps`
采样。`malloc_trim` 只尝试返还空闲页，其诊断收益不证明此前仍有 payload owner。
[Linux 内核 proc 文档](https://www.kernel.org/doc/html/latest/filesystems/proc.html#process-specific-subdirectories)、
[malloc_trim(3)](https://man7.org/linux/man-pages/man3/malloc_trim.3.html)

### 后续验收目标

建议的最小库侧改动是让 pNFS 的单次 DS 响应保留 owned `Bytes`，只有确实
需要合并多个短读片段时才分配 stripe 缓冲区；保留单 chunk 直接移交结果、
多个 chunk 最终只拼接一次的逻辑。该方案优先去掉本例约 64 MiB 的冗余
stripe 复制，须由回归和相同计数对照验证，尚未实施。

公开 READ 接口要求返回连续 `Bytes`。多个独立 DS backing 通常仍需一次
最终拼接，不能承诺所有 pNFS READ 都做到 MDS 的零复制或将累计分配从三倍
降到一倍。应覆盖单响应指针复用、多片/短读、EOF、顺序、错误和取消释放，
并检查共享 backing 的最后一个 owner 生命周期。私有强制 MDS 开关只是
因果对照工具，不建议作为绕开 pNFS 的产品方案；会话和 WRITE 重放语义不变。

实现后应保持同一 executor、导出、精确协议、运行时线程数和公平 payload，
分别重复默认配置的 QD1/QD8、全新进程 READ-only、完整写读及八进程读取。
按阶段比较累计分配、同时存活峰值、idle 后存活量和 RSS，并补充足够轮次的
稳态检查。allocator 参数只作单变量诊断对照，不能据其结果宣称默认应用
行为已经改善。当前计数尚未逐项归属的约 2.958 MiB 增量可随后单独跟踪。

现有 issue 没有给出明确 RSS 预算；诊断工具的 1 MiB 尾段噪声预算也不是 RSS
验收阈值。在默认配置症状、路径归属和可重复的改善目标明确前，应保持 issue
打开并更新其错误归因。内存专项数据仍不能替代已有发布性能基线和门禁。
