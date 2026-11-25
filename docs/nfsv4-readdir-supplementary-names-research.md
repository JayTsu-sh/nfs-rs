# ONTAP NFSv4 遍历遗漏四字节 UTF-8 名称

2026-11-25 实测。OTS 的 ONTAP 9.12.1P4 在 `C.UTF-8` 卷上能创建并按全名访问包含 Unicode 补充平面字符的文件和子目录。NFSv4 `READDIR` 返回成功和 `eof=true`，却省略这些条目。OTS 管理面的 `wafl.bagofbits.name` 事件确认了服务端的省略原因。FAS2750 表现相同，证据覆盖其 NFS 响应和同卷 NFSv3 对照，卷语言与 EMS 尚未独立回查。

## 实机对照

每组在独立测试目录中经 NFSv4 创建九项，并按完整名称读回各自内容。ASCII、双字节 `Å`、三字节 `中` 和三字节变体选择符留在列表中。五个省略项分别是含 U+10000、U+20000（汉字）、U+1D11E（乐谱符号）、U+1F9EA（🧪）的四个文件，以及含 U+1F9EA 的一个子目录。已知子目录原名时，可进入并读取其子文件。

| 环境与导出 | 协议与路径 | 枚举结果 |
| --- | --- | --- |
| FAS2750 `/nfsrs_v40_test` | Linux 内核 NFSv4.0、v4.1、v4.2；原始 RPC NFSv4.0 | 每组 4/9 |
| FAS2750 `/nfstest` | 同一批对象，Linux 内核和原始 RPC NFSv4.0；Linux 内核与 nfs-rs Python NFSv3 | v4.0 4/9，v3 9/9 |
| OTS `/nfsrs_pnfs_a`（FlexGroup） | Linux 内核 NFSv4.0、v4.1、v4.2；原始 RPC NFSv4.0 | 每组 4/9 |
| OTS `/nfsrs_v40_test`（FlexVol） | Linux 内核和原始 RPC NFSv4.0 | 4/9 |
| OTS `/nfs_rs_test`（FlexVol） | 同一批对象，Linux 内核和原始 RPC NFSv4.0；Linux 内核 NFSv3 | v4.0 4/9，v3 9/9 |
| DXN `/tmp3_xuyh`、Linux nfsd `/srv/nfs/v4/ci` | Linux 内核和原始 RPC NFSv4.0 | 各 9/9 |

原始 NFSv4.0 探针直接请求 `PUTROOTFH → LOOKUP → READDIR`，用服务端返回的 cookie 和 verifier 遍历到 EOF。省略发生在 `maxcount` 为 512、4096 和 65536 的请求中，覆盖单页和三页，以及空属性位图和 `fileid + rdattr_error` 两种请求。在 FAS2750 与 OTS 的试验目录内，将已知四字节名称的文件临时改为 ASCII 名后，NFSv4 立即列出该文件，内容和同协议下的 inode 保持一致。

在支持双协议的两个导出中，NFSv3 按完全相同的名称字节枚举，且逐项读回 NFSv4 写入的独立内容。FAS2750 的 9/9 结果也通过已安装 wheel 的 `Client.listdir` 验证。FAS2750 `/nfsrs_v40_test` 拒绝 NFSv3 挂载，OTS `/nfsrs_pnfs_a` 的导出策略仅允许 NFSv4。只改动了探针新建的条目，试验目录与挂载已清理。

## 服务端原因

OTS 只读管理查询显示，SVM `svm0` 和上述三个卷的语言均为 `C.UTF-8`。SVM 查询无 `utf8mb4` 对照卷。复现时的 EMS 事件 `wafl.bagofbits.name` 报告 `nfsrs_pnfs_a` 的 constituent 上有条目无法提供给 NFSv4 客户端，原因涉及 WAFL 名称兼容存储且没有 NFS 替代名。NetApp 的 TR-4067 解释了旧名称路径如何用 *bag of bits* 存储四字节特殊字符，以及 `utf8mb4` 卷语言如何原生保存它们。这里的输入本身是合法 UTF-8，EMS 的描述针对 ONTAP 内部目录项。 [ONTAP 9.12.1 EMS](https://docs.netapp.com/us-en/ontap-ems-9121/wafl-bagofbits-events.html#wafl-bagofbits-name) · [NetApp TR-4067](https://www.netapp.com/pdf.html?item=%2Fmedia%2F10720-tr-4067.pdf&v=2021114178) · [NetApp 多字节名称文档](https://docs.netapp.com/us-en/ontap/nfs-admin/multi-byte-file-directory-qtree-names-concept.html)

RFC 的 `READDIR` 回复由服务端提供名称、续读 cookie 和 EOF。`rdattr_error` 随已返回的条目报告读取属性失败。省略整个条目后，回复里没有该名称或对应错误可供客户端处理。nfs-rs 从回复缓冲区直接切出名称字节。相同缺失无需经过本库即可复现。 [RFC 7530 §16.24.4](https://www.rfc-editor.org/rfc/rfc7530.html#section-16.24.4) · [RFC 8881 §18.23.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.23.3) · [库的目录项字节解码](../src/directory_buffer.rs)

## 保留原名的处理方式

- 双协议导出可以显式选择 NFSv3 遍历，实测能取得全部原始名称。在需要 NFSv4 专有操作时，可用已经取得的原始名称在同一授权导出的 NFSv4 连接上按路径定位。各连接分别解析自身的句柄和状态。
- 仅提供 NFSv4 的导出需要另有可信名称清单，才能按完整路径访问遗漏项。正常的 `eof=true` 回包无法作为自动降级或判断枚举不完整的信号。
- 对新卷，NetApp 从 ONTAP 9.5 开始提供 `utf8mb4`。厂商指出，单独切换现有卷的语言不会回填已写入 *bag of bits* 的历史名称。保留现存名称的迁移方案需要拿这些历史条目做实机验收。本轮使用现有卷和策略，未做 `utf8mb4` 迁移对照。 [NetApp 多字节名称文档](https://docs.netapp.com/us-en/ontap/nfs-admin/multi-byte-file-directory-qtree-names-concept.html) · [现有卷语言转换限制](https://kb.netapp.com/on-prem/ontap/da/NAS/NAS-KBs/Can_the_volume_language_be_changed_after_creation_in_ONTAP)
