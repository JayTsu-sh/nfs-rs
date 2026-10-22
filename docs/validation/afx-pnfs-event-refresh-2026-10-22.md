# AFX pNFS 按事件刷新：实现和物理验证

## 实现

有效 layout 不再每跨 1 GiB 阻塞写入并强制 LAYOUTGET。当前实现保留缓存，
在范围不覆盖目标 offset 时获取新 segment；DS preflight 失败或本批全部
WRITE 明确返回 `NFS4ERR_STALE` 时，提交旧收据、失效旧映射并重试一次。
第二次仍被明确拒绝才允许安全 MDS fallback。READ 也先刷新并重试一次。

短写已成功后再收到 STALE、部分 stripe 成功、以及传输结果不确定的批次
不会整体重发。刷新前使用原始 DS 收据完成旧写入的提交；失败时保留 dirty
状态。失效 byte mapping 时保留 layout stateid，供后续 LAYOUTGET 使用。

GETDEVICEINFO 订阅 CHANGE/DELETE；CB_NOTIFY_DEVICEID 在应答之前同步失效
设备映射。版本检查拒绝通知之前发起、通知之后返回的旧 GETDEVICEINFO 响应。
正常命中不增加 RPC，并发 layout/device cache miss 使用 singleflight。

协议依据：[RFC 8881 §12.5.2](https://datatracker.ietf.org/doc/html/rfc8881#section-12.5.2)、
[§18.40.4](https://datatracker.ietf.org/doc/html/rfc8881#section-18.40.4)、
[§20.12](https://datatracker.ietf.org/doc/html/rfc8881#section-20.12)。

## AFX 环境和相同工作负载对比

- 2026-10-22，测试机 `client1`（`10.128.56.232`），
  Linux `4.18.0-553.134.1.el8_10.x86_64`。
- AFX MDS/export：`192.168.100.201:/fg01`；协商 READ/WRITE 为 64 KiB。
- 全程持有 `/tmp/terrasync-lab-tests.lock` 和
  `/tmp/terrasync-lab-performance.lock`。
- 每轮独立创建 20 GiB 单文件；64 MiB 窗口调用 BufferedFile，底层每批最多
  8 个并发 RPC，写入持久化后返回；关闭再打开，完整读取并逐字节验证。
- 对比负载均为零数据，不清除服务端缓存，不作为冷盘吞吐测试。
- 每轮同时 `tcpdump -i any -nn -s 1024 -B 65536 -U`，过滤 `tcp port 2049`。
- 基线为上一轮保存的、含原始 1 GiB 刷新策略的静态二进制，SHA-256：
  `fbae963cb7f4218fedd2b72f258018aaf1c5b9739635eb3d2937a1302af4adbe`。
- 三轮优化版性能对比二进制 SHA-256：
  `ba9b7784dc1564e2429ef2710f6e059fe97df63fd3f5ace473ee0a06ff77ff3b`。

| 版本 / 轮次 | 写入秒 | 写入 MiB/s | 读取秒 | 读取 MiB/s | 全内容校验 |
|---|---:|---:|---:|---:|---|
| 基线 1 | 31.095 | 658.6 | 45.341 | 451.7 | 通过 |
| 基线 2 | 33.771 | 606.4 | 48.303 | 424.0 | 通过 |
| 基线 3 | 41.296 | 495.9 | 49.967 | 409.9 | 通过 |
| 优化 1 | 34.530 | 593.1 | 40.349 | 507.6 | 通过 |
| 优化 2 | 38.177 | 536.4 | 42.503 | 481.8 | 通过 |
| 优化 3 | 30.603 | 669.2 | 39.528 | 518.1 | 通过 |

写入中位数 606.4 → 593.1 MiB/s（-2.2%），读取中位数
424.0 → 507.6 MiB/s（+19.7%）。写入结果落在基线观测波动范围内；
这三轮诊断没有显示明显吞吐回退，但不同文件的实际 DS 分配和物理负载有差异，
不能据此承诺统计意义上的零回退或固定的性能提升。

这是 AFX 补充诊断。仓库基准 manifest 中另一个 NetApp export 的 accepted
baseline 不适用于此 AFX export；本次未运行或修改跨环境发布性能 gate。

## 抓包中的刷新和 DS 更新

以下为按 TCP record 头识别的请求次数，包含并发请求；计数范围是本轮读写。
1024-byte snaplen 不保存完整数据，但控制请求头及实际 TCP payload 长度可用于
统计。完整内容校验由客户端实际读回完成。

| 版本 / 轮次 | LAYOUTGET | GETDEVICEINFO | MDS WRITE | MDS READ |
|---|---:|---:|---:|---:|
| 基线 1 | 49 | 2 | 4 | 4 |
| 基线 2 | 54 | 2 | 4 | 5 |
| 基线 3 | 32 | 2 | 1 | 0 |
| 优化 1 | 2 | 2 | 0 | 0 |
| 优化 2 | 2 | 2 | 0 | 0 |
| 优化 3 | 8 | 9 | 1 | 0 |

LAYOUTGET 总数 135 → 12，减少约 91.1%。GETDEVICEINFO 在真实失效时重新
获取，而不是无限使用旧 device_id 缓存。优化第 3 轮遇到多次实际 STALE，
因此请求数比前两轮多；同一次 I/O 连续被拒绝后的一次 MDS WRITE 是有界安全
fallback，不能把它描述为全程没有 MDS 数据 I/O。

优化第 1 轮提供了明确的跨 endpoint 更新证据（Unix 秒）：

1. `192.168.100.204` 承载前段写入，最后 bulk 包为 `1792636570.944017`。
2. 该 DS 在 `1792636570.944186` 返回 RPC COMPOUND status 70
   （`NFS4ERR_STALE`）。
3. 客户端在 `1792636570.959182` 发送新的 LAYOUTGET，offset 为
   `10737942528`，随后 GETDEVICEINFO 获取新映射。
4. `192.168.100.203` 在 `1792636570.960353` 开始承载后段 bulk 写入。
5. 本轮没有 MDS READ/WRITE；完整 20 GiB 读回也通过 DS，校验成功。

这里的 DS 指网络 endpoint，不据此断言物理控制器归属。所有六轮抓包的内核
丢包计数均为 0。最初六轮在程序退出后立即停止抓包，末尾少量清理报文可能尚
未被 tcpdump 消费；写读阶段的布局请求和应用完成结果均已记录。另做的非零
验证在程序完成后等待 2 秒再停抓包，使 captured 等于 received-by-filter。

## 非零数据完整性验证

另用确定性非零字节模式做了三轮 20 GiB 写入和完整读回，实际出现多次写/读
STALE，刷新 layout 后完成，全部全内容校验通过。模式、校验方式和日志级别
不同，不混入上面的性能中位数。

| 非零验证 | 写入 MiB/s | 读取 MiB/s | captured / received | dropped |
|---|---:|---:|---:|---:|
| 首次 | 677.2 | 496.9 | 2,881,306 / 2,881,306 | 0 |
| 中间确认 | 668.2 | 421.4 | 3,107,508 / 3,107,508 | 0 |
| 最终源码构建 | 630.1 | 436.3 | 2,973,200 / 2,973,200 | 0 |

最终构建在 AFX 上写入 32.505 s、读取 46.944 s；实际从
`192.168.200.203` 更新到 `192.168.100.203` endpoint，并在后续多次 STALE
后继续完成。该切换不据此断言物理 DS 节点变化。最终二进制 SHA-256：
`3e5d481b7a02efb8626475d2ff017870cf15807cd3136d89be577cbf3dbaa007`。
最终日志和抓包前缀为 `/tmp/release-pnfs-event-20261022-afx-verified`。

复现命令（不使用内核 mount；直接验证本项目实现）：

```bash
cargo build --locked --release --target x86_64-unknown-linux-musl \
  --example pnfs_large_file

# 在 AFX 测试机上持有上述共享锁后执行；文件名必须唯一。
./pnfs_large_file \
  'nfs://192.168.100.201/fg01?version=4.1&uid=0&gid=0&noresvport=true' \
  release-<unique-run-id>.bin 20 pattern
```

## 自动化验证和环境限制

- Rust all-targets：628 passed，27 个物理 lab 用例保持 ignored。
- Rust doctests：45 passed；Clippy all-targets（`-D warnings`）通过。
- 新回归覆盖正常跨 GiB 写无控制 RPC、同 device_id 地址更新、新 DS 重试、
  有界 fallback、短写后 STALE、部分成功、READ 恢复、preflight 恢复、
  旧 dirty 提交失败保留、并发 cache miss 合并，以及 callback 重放和通知竞态。
- 本次编辑的 Rust 文件格式检查通过。全仓 `cargo fmt --all -- --check`
  仍被既有 `src/nfs3/readdir.rs:169` 的多余空行阻挡，未改动该无关内容。
- AFX 自然产生的 STALE 和 DS 更新已经实际验证；未主动迁移 LIF 或修改服务端
  配置来制造 CB_NOTIFY_DEVICEID，设备通知的 wire/竞态验证来自确定性测试。
- `prepare-run.sh` / `cleanup-run.sh` 已调用；当前执行机缺少另一套 Terrasync
  CI 专用 SSH 私钥，无法连接其基线主机。AFX 本次创建的文件均由诊断程序删除，
  会话正常 umount；没有创建内核挂载点。pcap/日志仅保留在测试机 `/tmp` 下的
  `release-pnfs-event-20261022-afx*`，未将原始机器证据或凭据加入仓库。
