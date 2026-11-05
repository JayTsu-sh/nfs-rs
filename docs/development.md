# 协议与开发规范

AI 工作流程、外部环境异常、验证与发布约束以 [AGENTS.md](../AGENTS.md) 为准。本页只维护协议与编码规则，不复制实现中的重试常量、接口清单和服务器能力；当前 API 及支持范围查 [README](../README.md)、[Python API](python-api.md) 和对应源码。

## 协议变更先查规范

涉及协议字段、错误码、状态机、重放或恢复时，先查对应 RFC，实施或测试中标明相关章节；服务端私有扩展须记录实际契约与能力限制。

| 协议 | 参考 |
| --- | --- |
| NFSv3 | RFC 1813 |
| NFSv4.0 | RFC 7530 |
| NFSv4.1 / 扩展规则 | RFC 5661、RFC 8178；涉及更新后的 4.1 规范时查 RFC 8881 |
| NFSv4.2 / XDR | RFC 7862、RFC 7863 |
| ONC RPC / XDR | RFC 5531、RFC 4506 |
| RPCSEC_GSS | RFC 2203；引用协议规范不代表本库已支持该认证 |
| NFSv4 xattr | RFC 8276 |
| NFSv4 ACL | 对应 NFSv4 主规范的 ACL 属性章节 |
| NFSv3 ACL 私有扩展 | 按目标服务实际能力与扩展契约验证，不能假定厂商一致 |

## Rust 和异步代码

- 工具链、edition 与 MSRV 以 rust-toolchain.toml、Cargo.toml 和 CI 为准。公共 Mount 跨协议契约保持一致，实际能力差异显式表达。
- use 放文件顶部，测试模块按现有约定处理；表达式中过深路径通过顶部导入缩短，常用 crate／标准库路径以可读性为准。
- 生产代码禁止 unwrap／expect；通过 Result、模式匹配或具有明确语义的默认值处理失败。错误以有类型的 NfsError/source 链跨边界传递，保留原始原因和不确定结果，不将结构化原因转换成 String 丢失分类。
- 公共类型沿用 src/lib.rs 导出边界。XDR 变更先确认维护的请求编码器、源定义与 build.rs 生成内容的归属，修改源定义而非生成输出。
- 结构重构保持可观察行为；发现行为问题另作修复，不能修改测试迁就重构结果。保留解释 RFC、并发、恢复与不确定性的注释。
- 数据通路优先复用 Bytes／BytesMut，避免大缓冲区无谓 clone；共享简单计数使用原子类型。CPU 密集或阻塞工作使用现有阻塞执行机制，异步任务之间的队列保持有界，锁的选择和持有范围须符合当前异步契约。

## 按场景验证

| 场景 | 依据 |
| --- | --- |
| Rust 变更 | [CI](../.github/workflows/ci.yml) 的 fmt、clippy、Rust 回归与 doc 测试；使用固定工具链 |
| Python／PyO3 变更 | [Python API](python-api.md)、CI 的当前源码 wheel 安装、NFS_RS_TEST_INSTALLED=1、类型与 stub 验证 |
| RPC／会话／pNFS 恢复 | AGENTS.md 的故障约束、[可靠性规范](nfs41-migration-reliability-spec.md)、[覆盖映射](../tests/nfs41-reliability-coverage.json) 及对应协议测试 |
| 真实服务验证 | [实验室约束](../tests/lab/README.md)，按实际协议和环境提供证据 |
| 性能 | tests/benchmarks/baselines/ 及现有 benchmark 清单／门禁，保持对应环境基线与阈值 |
| 发布 | [发布工作流](../.github/workflows/release.yml) 及 AGENTS.md 的不可变产物约束 |

## 验证、review 和发布

- 按影响范围执行固定工具链的 fmt、clippy、Rust 回归与文档测试。修改 Python／PyO3 时，用当前源码构建并安装独立 wheel，以 NFS_RS_TEST_INSTALLED=1 验证真实 Rust 适配层，并执行现有类型／stub 检查。仅修改指引文档时检查文档、链接及 diff，不要求无关运行时测试。
- 真实服务测试遵守 [tests/lab/README.md](../tests/lab/README.md) 的允许范围、独立运行身份、共享锁和无条件清理；凭据及本机证据保留在仓库外。分别报告确定性测试与实际运行的物理环境。
- 性能变更使用现有 benchmark 清单及对应环境基线，报告吞吐、延迟和内存；保持验收阈值，不能复用不同端点／协议的基线或用补充测量替代正式验收。
- review 关注协议正确性、数据完整性、重试／取消安全、状态清理、Rust／Python 契约一致及外部异常的验证证据。对外行为变化同步更新文档与 changelog。
- 发布依照 [.github/workflows/release.yml](../.github/workflows/release.yml) 和现有发布流程，保持 crate、Python 元数据、changelog 与 tag 版本一致；发布准确验证过的同一份产物。测试支持 wheel 不得发布，tag 和已发布版本保持不可变。只有发布工作流成功且 crates.io／PyPI 可见后才能宣告完成。
- 完成报告说明改动、职责归属、实际验证、未测环境及剩余风险；不能把跳过项或能力缺失写成通过。
