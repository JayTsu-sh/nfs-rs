# Codex 项目指令

本文件是统一的 AI 开发入口，适用于整个仓库；子目录 AGENTS.md 可补充对应范围的约束。修改协议、Rust／Python 接口、错误处理或恢复逻辑前阅读 [开发规范](docs/development.md) 的对应章节；公共 Python 行为与生命周期另见 [docs/python-api.md](docs/python-api.md)。通用开发、验证及外部环境异常约束以本文件为准，不依赖 Claude 自动加载或专属 agent。

## 工作方式

- 修改前明确可观察行为、影响边界与验证目标，区分 bug 修复和保持语义的结构重构。
- 检查 git status 并保护共享目录中的其他工作，仅提交本次任务文件，保留无关改动。
- 按实际责任集中协议、传输、恢复与错误处理，复用现有机制；新增策略须有当前使用场景和验证依据。

<!-- CODEGRAPH_START -->
## CodeGraph

When `.codegraph/` exists, use CodeGraph before broad text search or file reads
to locate code, understand dependencies, or assess a change:

- Prefer the `codegraph_explore` MCP tool. Ask about concrete symbols, files,
  behavior, callers, or data flow; it returns current source and call paths.
- If the MCP tool is unavailable, run
  `codegraph explore "<symbols, files, or question>"` from the repository root.
- Use `codegraph node`, `codegraph callers`, `codegraph callees`,
  `codegraph impact`, or `codegraph affected` when the narrower query matches
  the task.
- After source edits, run `codegraph sync`, then `codegraph status` if index
  freshness matters to the remaining work.

Use `rg` for exact text and non-code assets after CodeGraph has provided the
code context. If `.codegraph/` is absent, use the normal repository tools and
do not initialize an index unless the user requests it.
<!-- CODEGRAPH_END -->

## 外部环境异常：设计、开发和测试约束

涉及网络 I/O、协议、恢复或 Rust／Python 生命周期的设计、实现和 review，必须分析外部环境异常。按实际协议列出 NFS／MOUNT／portmapper、元数据与 pNFS 数据服务、回调链路及其 DNS、网络和认证依赖；这里的回调是协议机制，产品 callback、ClickHouse 和扫描任务终态由上层负责。

- **设计依据**：在设计或 PR 中记录受影响的「依赖／操作阶段／故障类型／检测时限／恢复预算／有类型的最终错误／一致性保证／验证方式」。评估连接建立、请求发送、等待响应、重连与会话绑定、状态恢复及资源关闭，不适用项说明原因。
- **及时准确上报**：区分连接拒绝、中断、静默丢包／请求超时、认证／权限失败、限流／服务端繁忙、资源不足及协议错误。保留原始结构化原因、阶段和发送／执行证据；重连失败不能覆盖最初的明确断连原因，Rust／Python 对外边界须保留可识别的分类和恢复指导。
- **有界安全恢复**：明确连接、写入排队、发送、响应、重连／重新绑定和整体恢复的时限与次数，按故障类型采用短时有界重试和退避。连接中断应尽早准确返回，不能由固定长退避、多层重试或重复绑定无限放大延迟；同一次恢复共享绝对截止时间。认证／权限或明确不可恢复错误及时失败，只有协议允许安全重放的请求才可重发。
- **责任边界**：本库容忍协议安全范围内的短暂故障，并向上层提供错误和恢复证据；产品扫描识别故障后的约 3 秒防抖属于调用方调度策略，不在本库硬编码，也不将扫描和普通操作混为一套重试策略。静默丢包的检测时间与恢复时间分别约束和报告。
- **一致性与取消**：响应丢失不能被当作服务端未执行；保留修改操作的不确定结果及会话／重放证据。重连后按协议验证句柄、OPEN、锁、session、layout 和缓存状态，不能仅凭 TCP 恢复判定可继续。取消或部分帧发送失败时收束在途请求、隔离不可复用传输并释放自有资源；Rust Future 和 Python 任务的取消、关闭和后台完成行为必须有明确契约。
- **故障验证**：对受影响协议与 Rust／Python 接口注入闪断、持续断连、静默丢包／超时、认证／权限失败、服务端错误，以及响应丢失／未知写结果、重连绑定失败和故障期间取消。验证错误分类、检测与恢复／退出耗时、安全重放、最终数据、状态／句柄清理、资源有界和正常性能／内存开销。确定性测试覆盖预算和边界，受控真实服务验证适用链路；明确未测环境、跳过项和能力限制。

这些是设计和验收约束，不代表各协议均已完成对应实现或实测；结论必须附当前代码与验证证据。

## 验证、review 和发布

- 按影响范围执行固定工具链的 fmt、clippy、Rust 回归与文档测试。修改 Python／PyO3 时，用当前源码构建并安装独立 wheel，以 NFS_RS_TEST_INSTALLED=1 验证真实 Rust 适配层，并执行现有类型／stub 检查。仅修改指引文档时检查文档、链接及 diff，不要求无关运行时测试。
- 真实服务测试遵守 [tests/lab/README.md](tests/lab/README.md) 的允许范围、独立运行身份、共享锁和无条件清理；凭据及本机证据保留在仓库外。分别报告确定性测试与实际运行的物理环境。
- 性能变更使用现有 benchmark 清单及对应环境基线，报告吞吐、延迟和内存；保持验收阈值，不能复用不同端点／协议的基线或用补充测量替代正式验收。
- review 关注协议正确性、数据完整性、重试／取消安全、状态清理、Rust／Python 契约一致及外部异常的验证证据。对外行为变化同步更新文档与 changelog。
- 发布依照 [.github/workflows/release.yml](.github/workflows/release.yml) 和现有发布流程，保持 crate、Python 元数据、changelog 与 tag 版本一致；发布准确验证过的同一份产物。测试支持 wheel 不得发布，tag 和已发布版本保持不可变。只有发布工作流成功且 crates.io／PyPI 可见后才能宣告完成。
- 完成报告说明改动、职责归属、实际验证、未测环境及剩余风险；不能把跳过项或能力缺失写成通过。
