# 2026-09-09 review 修复与验证

本记录对应代码走读中提出的 P1/P2 问题。修改保留了工作区已有更改，未发布版本；下面的验证使用当前源码和本地 TCP fixture，没有访问真实 NFS 服务器。

## 修复范围

| 优先级 | 问题 | 修复与回归验证 |
|---|---|---|
| P1 | named attribute 覆盖残留和短写 | OPEN 显式 SIZE=0；按协商大小写入并完成短写；检查 FILE_SYNC 回复；聚合 WRITE/CLOSE 错误。实际 RPC fixture 验证长值、短值、空值与失败后的不确定结果。 |
| P1 | named attribute 短读截断 | 按协商大小读取至 EOF；零进展报错；不设置固定完整值上限。验证超过 1 MiB 的完整值在短读、短写下往返一致。 |
| P1 | RPC 发送和排队不受超时约束 | 单次 deadline 覆盖 readiness、writer 锁、发送和回复；总重试预算包含 backoff/reconnect；TCP connect 上限五秒。 |
| P1 | 发送中取消导致半帧复用 | 未完成帧的 guard 在错误、超时或取消时同步关闭 socket，再释放 writer 锁。fixture 验证旧连接结束且后续请求在新连接上成功。 |
| P1 | 非 EOF 空页或重复 cookie | v3/v4.1 共用分页游标检查，识别空页无进展和循环 cookie；不再把异常当成功结束。v3 READDIR/READDIRPLUS 都经过 XDR 解码验证。 |
| P1 | GETPORT 短结果 panic | 检查完整四字节结果和端口范围，零端口返回服务未注册错误。 |
| P2 | listxattr NOENT/verifier | 使用 NFS4ERR_NOENT 枚举；复用目录分页逻辑保留 cookieverf。 |
| P2 | v4.1 忽略 readdir-buffer | 使用 mount 参数，并扣除 RPC/COMPOUND 响应开销后限制在 session 容量内；验证实际发出的参数。 |
| P2 | BufferedFile 关闭状态 | 成功关闭幂等；关闭后拒绝读、写和 flush；失败/取消后禁止再次释放同一 OPEN 引用，残留状态交给 mount 清理。 |
| P2 | v4.1 挂载丢失底层原因 | 返回最后一个地址的结构化错误，目标地址仍保留在诊断日志；验证 ConnectionRefused 类型。 |

## 测试与 CI

移除固定完整值上限后，重新运行 all-targets（615 通过、27 忽略）、Clippy、格式检查和具体测试映射检查，全部通过。新增大值回归覆盖 1 MiB + 1 字节的已有值读取、短写替换及完整读回。下列 Python wheel、bindings 和文档测试结果来自此前完整验证，本次未重跑。

- `cargo test --all-targets --no-fail-fast`：615 通过，27 个真实实验室用例忽略。
- `cargo test --doc`：45 通过。
- `cargo test --features python-bindings --lib`：577 通过（包含常规单元测试，不与上一项简单相加）。
- `cargo clippy --all-targets -- -D warnings`：通过。
- 当前源码构建的 Python test-support wheel，在隔离 CPython 3.12 环境运行 `NFS_RS_TEST_INSTALLED=1 python -m pytest -q python/tests`：364 通过，1 跳过。
- `mypy.stubtest`：通过，检查 4 个模块。
- `scripts/check-reliability-test-results.py`：所有具体映射都在实际成功执行日志中找到。
- `cargo fmt --all -- --check`、`git diff --check`：通过。

关键回归测试完整名称存于
[`tests/nfs41-reliability-coverage.json`](../../tests/nfs41-reliability-coverage.json)
的 `review_regressions`；CI 核对测试是否实际通过，缺失、改名、忽略或失败都会使具体映射检查失败。
历史规格 T11/T16/T20 尚无具体测试映射，T14/T15/T18 为部分映射，继续显式报告，不视为全面覆盖。

生产 `.unwrap()` / `.expect()` 检查改为解析 Rust AST，跳过明确的测试专用项后继续扫描后续生产代码。
新增测试验证文件顶部测试导入、测试模块之后以及 impl 方法中的生产调用都不会漏检。
CI 新增文档测试和 Python bindings 的 Rust 单元测试。

## 文档和兼容性

已更新 README、PyPI README、Python API、CLAUDE.md、CONTRIBUTING.md、可靠性规格和 CHANGELOG。
需要调用方注意：

- named attribute 不设置客户端固定完整值上限；仍受服务端和文件系统限制。协商 read/write size 仅限制单次请求，getxattr 将完整值缓存在内存中。
- OPENATTR named attributes 不是 NFSv4.2 GETXATTR/SETXATTR，也不保证映射为 Linux POSIX xattr。
- 覆盖不是跨客户端原子替换；截断后失败必须验证远端值。
- 一个 BufferedFile 拥有一次 OPEN 引用；多个独立包装对象需要各自 open。
- RPC deadline 不是整次多 RPC 业务操作的 deadline。取消/超时也不会撤销已经发生的远端修改。

## 尚需真实服务器验证

`validate-python-real-api.py` 已增加同步/异步 xattr 较短值和空值替换检查，只有真实服务器支持该能力时才执行。
真实故障实验、release/nightly 运行及跨服务器互操作结果不在本次本地验证结论内；既有 blocked-capability 项仍需对应实验室证据。

协议依据：[RFC 5661 §18.16、§18.22、§18.23、§18.32](https://www.rfc-editor.org/rfc/rfc5661.txt)
和 [RFC 5531 §11](https://www.rfc-editor.org/rfc/rfc5531.txt)。

## 补充 review：截断 OPEN 后 GETFH 失败

COMPOUND 调用现在可保留指定操作成功后的部分结果，避免后续 GETFH 错误丢失 OPEN 已执行的证据，或因 DELAY/GRACE 重试整次截断。setxattr 在 OPEN 成功后遇到 GETFH 或结果解析失败时返回 Uncertain、Sent、VerifyThenResume，保留原始错误，已写入字节数为 0。

本地 RPC 回归覆盖 OPEN 本身失败（保留旧值和普通状态错误），以及 OPEN 成功后 GETFH 返回 IO、DELAY（旧值已截断、保留恢复指导、不重试）。未获取有效文件句柄时不能直接发送 CLOSE，本次修复不声称完成该路径的远端 OPEN 清理。

本次补充修复验证：Rust all-targets 616 通过、27 忽略；文档测试 45 通过；Clippy、格式、diff 和具体测试映射检查通过。未重跑 Python wheel 或真实服务器测试。

## 合并前基于 0.8.1 主分支验证

本次修复从原工作区单独提取并迁入主分支 `92c7934`，不包含原工作区的其他未提交改动。保留 0.8.1 的完整 Python 文档入口，并同步 wheel 内 GUIDE.md。重新运行 Rust all-targets：616 通过、27 忽略；文档测试：45 通过；Clippy、格式和具体测试映射检查通过。重新构建 0.8.1 test-support wheel，隔离环境 Python 测试：367 通过、1 跳过；stubtest：4 个模块通过。真实服务器实验未运行。
