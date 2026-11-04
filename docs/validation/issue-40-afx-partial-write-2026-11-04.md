# Issue #40：AFX 双 DS 部分写失败实机验收

日期：2026-11-04（UTC+8）。本轮在 AFX 上完成两次独立的单次逻辑 WRITE
跨 DS 故障验收，均通过。此前保留的 T15 同次写入部分失败物理证据缺口，
已在下述主分支源码上补充验证；这是手工实机验收，不是 scheduled nightly。

## 源码和环境

- 主分支：`806dcfa09638f5a01f78620deeee1901ae82ebcf`，nfs-rs 0.11.2。
- `src` tree：`15f61267e822269f35c3372881efa2f9ac451995`。
  从该提交导出源码，在 checkout 外增加诊断 example；生产库源码逐文件比较
  一致，没有修改 layout、RPC、错误分类或重试实现来制造结果。
- Rust 1.95.0，`--release --locked --target x86_64-unknown-linux-musl`。
  诊断二进制 SHA-256：
  `4f55ac5be5c1816912968e04ff3a577e8da4ae38ab1f7c3115c974470fb0f5f0`。
- 执行器：`10.128.56.17`，数据地址 `192.168.9.17`；NFSv4.1，
  AUTH_SYS，UID/GID 0，非保留源端口。
- AFX / ONTAP 9.19.1P2，`svm2:/pnfs_test2`，现有跨四节点 FlexGroup。
  `.101` 为 MDS 入口；管理面确认 `.103` 当前在 AFX-01，`.102` 在 AFX-03，
  `.104` 在 AFX-04。四个 LIF 在线，v4.1-pNFS 已启用。
- 协商 READ/WRITE 最大值均为 65536 bytes。实际 LAYOUTGET 给出
  1 GiB stripe unit、一个共享 DS 文件句柄；GETDEVICEINFO 给出多个 DS 映射。
  测试目标依据实际请求和应答选择，没有硬编码客户端 layout。

## 故障和验收方法

每轮持有执行器 `/tmp/nfsrs-svm2-issue40.lock`，使用唯一 `nightly-*` 目录，
调用仓库 `prepare-run.sh`，并在成功或失败退出时无条件清理已成功准备的目录。

1. 用未修改的生产客户端写入 20 GiB 确定性非零模式数据并持久化关闭。
2. 新开文件，通过公开 `Mount::write` 发送一次 64 KiB 写入，起点为
   `1073709056`，即 1 GiB 边界前 32 KiB。抓包确认它拆成两个 32 KiB
   WRITE，发往两个独立 DS，使用同一个文件句柄；预热时两段均收到成功应答。
3. 通过进程 PID 和 `ss` 确认坏 DS socket 的所有权。新增本次 run 独占的
   nftables 表，仅丢弃该 DS 到此进程特定本地端口的 TCP/2049 入站包。
   其他连接和 MDS 入口不匹配该规则；独立的 180 秒看门狗负责兜底恢复。
4. 再调用一次 64 KiB `Mount::write`，数据改为原模式 XOR `0xa5`，
   确保能区分新旧内容。要求得到 `Uncertain / VerifyThenResume`，
   并核对成功 sibling 的应答、逐 stripe 诊断及没有 MDS WRITE 回退。
5. 删除故障规则，原挂载 CLOSE、UMOUNT。新进程重新挂载并读取目标范围，
   检查实际部分写入；显式重写完整 64 KiB 并提交，再关闭和重新打开文件，
   按窗口读取整个 20 GiB，与预期模式逐字节比较并确认长度。

规则也会丢弃坏 DS 的 TCP ACK，因此本轮确实阻断了坏 stripe 的完整请求
传输，而不只是隐藏服务器的成功应答。抓包中该 RPC record 应为 32988 bytes，
故障期间仅出现 14700 个不同的 TCP payload bytes；WRITE 请求头可独立解析。
恢复后读取也确认前半段为新内容、后半段仍为旧内容。

## 两轮结果

两轮成功 DS 均为 `192.168.9.103 / AFX-01`，MDS 入口为 `.101`。
两个数据 DS 都不同于 MDS，因此本轮没有把正常 DS WRITE 与 MDS fallback
混为一谈。时长由执行器自身的 API 事件时间计算，不混用控制机时钟。

| 项目 | 第一轮 | 第二轮 |
|---|---|---|
| run ID | `nightly-issue40-afx-20261103T225535` | `nightly-issue40-afx-20261103T230000` |
| 故障 DS | `.102 / AFX-03` | `.104 / AFX-04` |
| 单次逻辑写入 / stripe 大小 | 65536 / 32768 bytes | 65536 / 32768 bytes |
| 成功 DS 确认 | 32768 bytes，NFS status 0 | 32768 bytes，NFS status 0 |
| 返回结果 | Uncertain / VerifyThenResume | Uncertain / VerifyThenResume |
| 返回耗时 | 30.002 s | 30.002 s |
| 故障规则命中 | 9 包 | 10 包 |
| MDS WRITE fallback | 0 | 0 |
| 恢复后的范围读取 | 前 32 KiB 新值，后 32 KiB 旧值 | 前 32 KiB 新值，后 32 KiB 旧值 |
| 原挂载 CLOSE / UMOUNT | 通过 / 通过 | 通过 / 通过 |
| 显式恢复写入并提交 | 65536 bytes，通过 | 65536 bytes，通过 |
| 新挂载全文件校验 | 21474836480 bytes，全内容一致 | 21474836480 bytes，全内容一致 |
| fixture 清理退出码 | 0 | 0 |

每轮诊断均保留 `attempted=true outcome=success` 和
`attempted=true outcome=error` 的逐 stripe 结果，错误指导为
`VerifyThenResume`，没有输出文件句柄或 payload。坏 DS 请求设置
`cachethis=true`。本轮每个坏 DS 仅观察到一个 RPC 请求头，未触发 NFS
协议层重复请求；TCP 重传不作为协议重试证据。

## 证据完整性和清理

第一轮抓包为 347 captured / 371 received-by-filter / 0 kernel drops。
它在 UMOUNT 返回后立即停止，存在尾包尚未写入 pcap 的限制，未宣称全程捕获
完整。针对该限制，第二轮启用 immediate mode，并在停止抓包前等待 2 秒：
**385 captured / 385 received-by-filter / 0 kernel drops**。
第二轮完整捕获再次验证同一验收结论，且更换了故障 DS。

最初另有一次预热试跑，控制器将 Wireshark 的应答 count 字段误用于请求，
在故障注入前停止并清理。请求改用 write-data-length 字段后，已用该抓包
验证选择器，再运行上述两轮；这个试跑不计为能力不支持或成功故障验收。

抓包离线审计关联单次 API 的起止事件、请求头、成功应答、目标 offset、
同一文件句柄和两个 endpoint，并检查没有 MDS WRITE。坏 stripe 的 RPC
record 不完整，因此还逐字段解析了完整的 RPC/NFS 请求头，不能仅依赖
需要整个 TCP record 重组完成的 Wireshark WRITE 列表。

原始 pcap、逐事件日志、规则计数、socket 所有权、构建与验收脚本均保存在
checkout 外的私有证据目录，没有复制管理凭据。保留了首轮证据限制和所有
试跑结果。fixture 清理后另用只读内核挂载检查全部本次目录均不存在，
核实本次故障表和探针进程已退出，再归档证据并移除执行器临时程序。

## 确定性检查和剩余范围

当前主分支另外重跑：

```sh
cargo test --locked --lib nfs41::pnfs_io::refresh_tests::
```

结果为 **30 passed，0 failed，0 ignored**。覆盖 failure-first /
success-first 不回退、取消后 admission 和清理、部分写入、布局刷新与旧收据
提交。之前核查中的 6 项 DS batch 测试也通过。

该主分支已有的 [CI](https://github.com/JayTsu-sh/nfs-rs/actions/runs/41669417175)
与[发布验证](https://github.com/JayTsu-sh/nfs-rs/actions/runs/41669456997)
对应相同源码 SHA，但并未执行本报告中的 AFX 手工控制器。

后续回查确认，[Nightly lab 41735417714](https://github.com/JayTsu-sh/nfs-rs/actions/runs/41735417714)
也在同一提交 `806dcfa` 上成功。兼容测试，以及 pNFS pre-WRITE DS failure、
DS reset、layout recall 有实际执行结果。LAYOUTCOMMIT failure 步骤虽为
success，日志明确为 `all-FILE_SYNC commit-skipped=1` 和
`pending-commit-fault=not-exercised`，只能确认对应重开校验通过，不能计为
实际 pending COMMIT 故障验收。该 nightly 没有调用本报告的 AFX 双 DS
控制器，因此也不能将其成功描述为 AFX T15 已自动化运行。

本轮补齐的是 AFX 上的 T15 手工实机故障证据。scheduled nightly 接入、完整
T11–T14 物理故障矩阵、本次物理取消/相反完成顺序、服务器重启/真实节点宕机
均未在这里执行；没有把确定性测试当作这些物理场景通过，也没有据此宣称
整个 issue 所有验收项已完成。测试没有修改服务端配置或发布新版本。

协议依据：[RFC 8881 §13.4.4](https://www.rfc-editor.org/rfc/rfc8881.html#section-13.4.4)
的文件条带映射，以及 [§2.10.6.1](https://www.rfc-editor.org/rfc/rfc8881.html#section-2.10.6.1)
的 session 与重放约束。
