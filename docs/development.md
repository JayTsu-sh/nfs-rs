# 协议与开发规范

AI 工作流程、外部环境异常、验证与发布约束以 [AGENTS.md](../AGENTS.md) 为准。本页只维护协议与编码规则，不复制实现中的重试常量、接口清单和服务器能力；当前 API 及支持范围查 [README](../README.md)、[Python API](python-api.md) 和对应源码。


## 协议库职责与变更验收

本项目实现 NFS 协议、RPC 传输、文件资源生命周期和 Rust／Python 对外接口。共同接口表达一致语义，协议差异留在对应引擎；传输层记录连接和发送证据，协议层判断重放及状态恢复，应用层决定产品任务何时失败。新增配置或抽象必须说明真实协议／调用方变化点，避免把内部实现细节扩散到所有调用方。

| 修改范围 | 设计约束与必须验证的结果 |
| --- | --- |
| XDR、网络响应与参数校验 | 依据对应 RFC 和生成源归属修改；长度、计数、偏移、cookie、聚合大小和整数运算有明确上限，异常／恶意响应及时拒绝，不允许从不可信输入无界分配或静默截断；覆盖编码往返、边界与畸形数据 |
| RPC、重连与重放 | 按操作和协议版本证明是否可重放，区分未发送、部分发送、已发送未知结果与确认完成。原始断连证据不能被后续绑定失败覆盖；单个恢复预算覆盖相关排队、发送、等待和绑定，测试完整时间边界及取消 |
| NFS 状态、pNFS 与资源生命周期 | 分别核验句柄、OPEN 引用、锁、session／sequence、lease 与 layout；元数据服务恢复不代表数据服务或旧状态有效。并发和取消时验证引用／句柄归属、半帧隔离、后台完成及最终 close，避免重复释放、悬挂请求与错误复用 |
| 公共 Rust／Python 接口 | 同步 Rust、PyO3、类型／stub 和公开文档；验证实际安装的当前源码 wheel。测试请求的能力、资源关闭、取消与异常映射跨语言一致，不能只以 Python facade 或 mock 测试替代真实适配层 |
| I/O 优化与发布 | 验证完整数据、顺序／持久化语义、正常及故障时的吞吐／延迟／内存；按对应协议和服务基线比较。共享缓冲区和队列保持有界；按当前发布流程检查精确版本与同一产物，测试专用能力不得改变生产默认或进入发布 wheel |

外部环境异常按 [故障约束](external-dependency-failures.md) 验证本层实际依赖；产品扫描时限和数据库／通知策略由上层制定。协议或设备尚未验证的能力必须保持显式限制。

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
