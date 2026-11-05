# Codex 项目指令

本文件是统一 AI 开发入口，子目录 AGENTS.md 可补充相应范围。协议、编码、验证和发布约束按任务读 [开发规范](docs/development.md)，公共 Python 行为读 [Python API](docs/python-api.md)。

- 职责：本库维护 NFS 协议、传输及 Rust／Python 接口；按版本和操作证明重放安全，恢复连接后重新核验 session／OPEN／layout 等状态。网络输入、并发和缓冲区必须有界；相关变更按开发规范的验收表执行。
- 修改前检查 git status，明确行为、协议／接口边界和验证目标，保护共享目录中的无关工作，仅提交本次任务文件；结构重构与行为修复分开。
- 协议字段、状态、错误或恢复变更先查对应 RFC；维护 Rust／Python 契约，保留结构化原因与执行证据，不能用字符串抹去错误分类或不确定结果。
- 涉及网络 I/O、协议恢复或 Rust／Python 生命周期的设计／开发／测试／review，必须读 [故障约束](docs/external-dependency-failures.md)：覆盖本层 NFS、协议服务及网络等实际依赖异常；恢复有界、可取消、安全重放，连接错误及时准确上报。产品扫描的 3 秒防抖、ClickHouse 和产品 callback 由上层负责。
- 按影响范围执行确定性测试及适用真实链路故障验证；Python／PyO3 改动验证当前源码构建并安装的 wheel，不能仅测 Python 替身；仅改文档检查内容、链接及 diff。
- 合入前 review 最终改动，报告真实证据、未测环境和风险；发布严格依现有流程验证同一产物，tag／已发布版本不可变，测试支持产物不得发布。
- 技术资料按任务显式查阅，不依赖 Claude 自动加载或专属 agent；接口、版本和能力以当前源码、公开文档与构建配置为准。

<!-- CODEGRAPH_START -->
## CodeGraph

仓库存在 .codegraph/ 时，理解或定位代码先用 codegraph_explore（MCP），或在仓库根执行 `codegraph explore "<符号或问题>"`；需要时再用 rg。未索引则正常使用 rg，不自行建索引。
<!-- CODEGRAPH_END -->
