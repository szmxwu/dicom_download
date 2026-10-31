# H217受限研究导出：阶段1交付与使用

2026-10-30。用户已批准修改、同步193并重启。当前实现并部署的是“已有结果的受控发布与下载”；新病例PACS获取、原生DICOM导出和Pi自动批准均未开放。唯一上线数据是无临床来源的合成联调包。设计见[完整设计](H217_RESTRICTED_RESEARCH_EXPORT_DESIGN_20261030.md)。

## 当前部署

| 节点 | 对象 | 身份与边界 |
|---|---|---|
| 193 | 原5005 | 仍是原dicom_download业务；217不能直连 |
| 193 | `https://192.0.0.193:8443` | research-export无登录账号；mTLS、证书摘要绑定授权、manifest与artifact归属校验；无PACS/原5005访问能力 |
| 217 | `research-fetch.service` | 独立无登录账号，密钥仅该服务可读；研究Agent用Unix socket/CLI提交固定ID |
| 217 | `/srv/research/research_exports` | 新24TB盘上的已校验下载；Research-dev可读，*.partial在校验前不可读 |
| 187 | `research-export-217-193-8443` | 仅217→193 TCP8443，ether2→ether1，先于原193 TCP拒绝；5005保持拒绝 |
| 193/217 | 专用egress服务 | 出口UID禁止新出站；217仅fetch UID可连193:8443，fetch UID也不能改走代理/其他地址 |

192.0.0.222代理的内网拒绝保持。角色隔离不防御宿主root/完整sudo维护者；同一研究UID的不同Agent也不是独立安全主体。需要进一步按项目隔离Agent时，应使用不同UID和客户端身份/审批范围，不能用脚本名冒充进程隔离。

193 runtime在`/opt/research-export`，217 runtime在`/opt/research-fetch`，均为root所有。网关和217服务仅用标准库；可信发布工具使用193现有依赖的选定副本，固定在root管理的`/opt/research-export/vendor`，不向原生产环境安装或升级依赖。临时pytest工具仅在wmx私有部署暂存目录，未加入生产PYTHONPATH。

## 使用217客户端

以`research-dev`登录217：

```bash
cat /srv/research/research_exports/COMMISSIONING.json
research-fetch manifest GRANT_ID
research-fetch download GRANT_ID ARTIFACT_ID
research-fetch wait GRANT_ID JOB_ID
```

替换为返回的实际ID。download立即返回job_id，wait轮询本地作业；失败返回有限错误码（例如ACCESS_DENIED_OR_NOT_READY、VERSION_MISMATCH、JOB_LIMIT），不回传上游异常正文或私有路径；支持服务拥有的partial文件续传，最终总SHA256一致后才原子发布。CLI不接受URL、证书路径、目标目录、上游Host或命令。

多日批次新增：`research-fetch batch GRANT_ID`一次入队该授权manifest全部文件，返回JOB_ID；用`research-fetch job GRANT_ID JOB_ID`查总数/完成数，用`research-fetch wait GRANT_ID JOB_ID`等待。关闭终端不影响后台。原`download`也改为持久入队，首次回复可以是QUEUED。原`src/cli/download.py`不能仅更换SERVER_URL使用新出口。

合成联调授权有效期24小时、1例/4个产物、16MiB传输额度、1MiB/s，仅用于验证机制。失效后需可信端新建授权；已下载文件不能通过撤销远程收回。该grant不是明文Bearer凭据，单独知道ID不能替代mTLS。原先用作撤权测试的授权已撤销，COMMISSIONING.json记录当前新授权。

下载任务串行，单分钟最多新建30项，SQLite队列最多50000条记录；单个manifest最多10000产物，超限需可信维护者分批或归档终态记录。批次按文件展开，任务、进度及重试时间保存在`/var/lib/research-fetch/queue.sqlite`，同步提交后才回复入队成功。重启自动恢复未完成新任务，重复提交在途同一授权/产物或批次返回原任务。阶段1旧JSON历史仍可查；旧RUNNING记录缺少artifact_id，只标INTERRUPTED，不盲目重启。只对本机批准调用UID提供服务；通过命名ACL适配已登录research-dev，无需为使用代理而增加sudo或让旧会话获取密钥组。

## 发布真实研究数据的可信端步骤

必须先具备具体研究范围及源图像审核记录；本交付不自动将193现有ZIP变成可公开数据。

1. 在193可信管理域选择已完成结果，为每个文件登记源相对路径、源SHA256、case_ref（仅在193保留）、series_number及像素复核证据。例：

```json
{
  "policy_version": "research-v1",
  "reviewer": "authorized-reviewer",
  "files": [
    {
      "path": "approved_case/series.nii.gz",
      "sha256": "SOURCE_SHA256",
      "case_ref": "TRUSTED_CASE_REFERENCE",
      "series_number": 1,
      "pixel_review": {"status": "approved", "evidence": "HASH_BOUND_REVIEW_RECORD"},
      "quality_review": {"status": "approved", "evidence": "HASH_BOUND_CONVERSION_AND_QC_RECORD"}
    }
  ]
}
```

像素复核记录应由可信流程给出，不能让217自己填写approved即获得发布权。CLI必须由root运行。review文件和原始关联仅留193；不要把实际患者标识或含临床信息的路径贴进聊天/Agent日志。

2. 在193 root维护终端执行：

```bash
research-export-admin publish --source /path/to/approved/input --review /path/to/review.json --project PROJECT_ID
```

新包只含NII/NPZ/PNG及按白名单重建的Excel，每个Excel只能对应一个检查，跨sheet也不允许混合身份。输入不得为链接/多硬链接文件，源摘要不符、像素或质量证据未批准、未知格式或未知元数据进入拒绝/隔离；不能发布原生.dcm。

发布前要求至少24GiB剩余空间，单次产物总量限20GiB；NIfTI单卷解码上限8GiB、NPZ2GiB、Excel64MiB解压数据。大于初始限制应先审查资源及策略，不能让217通过参数自行解除。可信维护者应串行发布并核对193负载；这个手工CLI不是后续阶段的自动生产worker。成功目录是只读root所有的版本，失败在quarantine，不改变原结果/原缓存。

NIfTI清空文本头与扩展，拒绝非有限值和常量卷，只复制预期体素payload，保持类型、缩放和空间几何；未知尾随内容拒绝。NPZ在分配数组前核对NPY声明形状、类型与ZIP成员长度，限制总数组字节；仅数值数组白名单，禁pickle/object/未知键和常量图像。常量PNG预览拒绝发布但不判定原DICOM为空白、更不删除原件。PNG保留受支持模式的数值像素并清除文本/EXIF。Excel所有可见sheet按严格数值/枚举列重建，去掉标识、绝对日期、路径和自由文本；只输出项目域HMAC生成的研究检查ID。MR高级清洗列、原文件路径及绝对日期不在v1出口白名单中，不能声称与原Excel完全同schema。研究检查ID不是已实现的跨检查患者随访ID。

当前没有自动OCR/像素文字清理，也没有面部去识别流程；已有转换成功不能代替这些证据。不满足适用模态/设备与像素复核要求的数据不应获批发布；原生DICOM及新PACS获取仍走设计中的后续独立profile。

3. 由可信维护者核对`/etc/research-export/registered_clients.json`中的注册客户端证书摘要，再授权：

```bash
research-export-admin grant --export-id EXPORT_ID --certificate-sha256 REGISTERED_CLIENT_FP --days 7 --max-cases 100 --max-bytes 21474836480 --rate 5242880
```

该动作不自动批准其他检查、格式或产物。总额需覆盖文件总字节、manifest读取与合理的重试余量，不能按净文件大小恰好分配。得到grant_id后，由root维护者在217 `/etc/research-fetch/config.json` 的grants映射登记`grant_id: export_id`（原子更新、保留root:research-fetch 0640），再将不含私钥的授权/manifest信息交给研究者。代理每次读取配置，无需重启。

## 授权与撤销

- 多日版：`grant --days 1..14`由可信root一次批准固定export/manifest/病例/总额度，服务器每次重读授权表。10分钟租约由mTLS身份通过固定GET `/v1/exports/{export_id}/lease`自动取得/续领，217约5分钟续领一次，正在传输时也会续领；服务端每块/限速等待均重验租约与root批准。短租约仅保管在代理内存与193私有SQLite，不传给研究Agent；不能修改root策略或重置累计字节。租约失效只尝试重新取得一次，批准失效/撤销/额度耗尽进入BLOCKED。
- 原`--hours 1..24`兼容旧单窗口授权；不自动升级现有授权。两种批准均不赋予全库/API能力。客户端证书初始30天，真实批准前维护者必须确认其剩余有效期覆盖下载窗口；证书/CA异常停止并需维护处理，不自动忽略TLS验证。
- `/etc/research-export/policy.json`绑定client证书SHA256、export/project、manifest SHA256、时间窗、病例上限、累计传输额度、速率和一条并发流。清单不符或策略不可读即拒绝。多日总额是整个批次累计额度，不能按每天重置。
- 数据发送每256KiB重查授权/额度，低速等待也至少每秒检查；源文件身份/只读状态、内容摘要和流中元数据一致性均检查。传输块在写socket前预扣额度，写失败与重传也消耗额度，因此计数是保守的尝试发送上界，不等于217已落盘字节；不得在续租/重启时清零。不回退到旧API。
- manifest/读取字节审计在193 `/var/lib/research-export/usage.sqlite`，记录研究授权ID/操作/结果/字节，不记录病例正文或凭据。审批变更由root授权表和维护终端sudo审计对账，不向217开放修改接口。
- 撤销命令：

```bash
research-export-admin revoke GRANT_ID
```

现场验证同一证书/同一授权从200变为404；流中撤销由专项回归验证会中断后续字节。已发出的字节和已存本地副本无法撤回。

## 多日运行与保留策略（2026-10-30增量）

网络错误、短读、上游429/503自动进入RETRY_WAIT（旧连接尚未释放并发名额也返回429，而非授权拒绝404），间隔5秒起指数增长，最长1小时；磁盘不足每5分钟复查。版本/SHA异常为FAILED；撤销/到期/额度拒绝为BLOCKED，不通过创建新grant绕过。可信端修复原因后，可用`research-fetch resume GRANT_ID JOB_ID`重新检查同一批准；这不是续权或扩大范围。未知服务内部异常使服务退出，由systemd重启并恢复队列，避免工作线程静默死亡。

217默认留20GiB余量，开始文件前还需容纳其剩余字节，每块检查低水位。已下载文件/partial不会为腾空间被自动删除。下载每16MiB或10秒做一次fsync与进度检查点，最终逐文件完整SHA验证；文件身份仍由export/artifact/SHA绑定，Range须带If-Match。机器断电可能损失尚未落盘的尾部，恢复后仍须完整SHA通过。

批准期限按193时钟，客户端租约刷新采用服务端剩余秒数与本地单调时钟。已获知期限会记录在队列；即使网络一直离线也不会无限重试，未获知批准信息的离线排队最长14天。时钟大幅回拨仍需可信授时治理，不能由自动续租替代。

发布物位于独立`/srv/research-export/published`，原5005结果/cache清理不管理此处。当前不设置自动删除期限，未完成的多日下载源保持；到期/撤销只停止访问，不删除源或已下载副本。维护者根据空间与使用结果审核后再决定清理；未实现自动删除/容量回收。发布水位仍为24GiB、单包20GiB，大数据应拆成多个不可变发布包，每包单独批准后入队。逐例结果可在审核完成后分别发布，不必等全部源任务结束；自动PACS获取、转换调度与自动审核发布worker仍未接入。

回退到不支持租约的旧代码前，必须先撤销全部含lease_seconds的多日授权，避免旧代码按长expires_at直接放行；队列与已下载数据保留。新增模块`leases.py`、`queue.py`；增量部署`deploy/research_export/upgrade_multiday.py`只重启两个研究服务，不重启原5005。

## 阶段1生产重启与验证（历史回执）

- 原5005：精确停止旧PID313858，确认死亡后启动新PID2166351。重启前running=0、pending=0，未发现打开的ZIP/Excel/图像结果传输句柄；运行环境在193内存中保留，未输出或导出环境凭据。
- 原源码备份：`/home/wmx/work/dicom_download_prod/backups/pre_research_export_20261030_132733/legacy_src.tar.gz`及restart_receipt.json。原app.py/packaging.py保持原始SHA，新增模块不改变原转换/过滤/QC行为。旧业务任务历史属于其原有内存模型，重启不保证内存历史继续存在，原结果与缓存不执行主动清理。
- 本机完整回归：230 passed / 3 skipped；193生产解释器完整回归：210 passed（远端既有测试文件集合比本机少20项）；其中35项新增出口/发布/客户端专项。三个本机跳过项为既有环境专项，不能当作已测。
- 现场：217 research-dev经代理下载xlsx/png/nii.gz/npz四类合成产物，全部SHA一致；普通研究进程直接连接8443被拒绝、5005超时、私钥PermissionError；出口身份访问本机5005连接被拒绝。
- 现场mTLS：无证书TLS失败，错误授权/跨artifact/原管理路由404，写方法405；实际撤销后404。验证不使用真实病例，不声称已有临床脱敏样本验收。
- 新服务active/enabled，重启后持久性尚未通过整机重启实测。193当前时钟比211约快30秒且报告未NTP同步，授权由193时钟判定；不宣称其时间同步已验收，生产可信授时仍需后续维护核验。

审阅源码：`src/research_export/{publish,gateway,fetch,admin,synthetic}.py`；部署模板：`deploy/research_export/`；回归：`test/test_research_export.py`。机器回执/版本SHA、联调清单与下载记录在211私有`/home/ubuntu/research_export_deploy_20261030/`；不把PKI私钥复制到仓库或文档。

## 停用与回退

可以先在193将相关grant置REVOKED或撤销187精确8443放行，然后停止新服务；原5005服务不依赖新出口。回退时仅操作本方案命名对象：

- 187按注释`research-export-217-193-8443`删除该单条允许规则；
- 193撤销来源217的8443 UFW规则，停用research-export及其egress服务；
- 217撤销RESEARCH_EXPORT_193_TLS对应UFW出站规则，停用research-fetch及其egress服务；
- 两主机仅从OUTPUT删除本方案链跳转，再清理`REXP_GW_OUT`/`REXP_FETCH_OUT`，不flush全局链，不清理其他运维策略。

已停止oneshot服务不会自动清除iptables链；为免误开权限，停用后由维护者按上述定点步骤处理，并核验既有SSH/代理/迁移仍正常。发布包、源数据和审计先保留，不把停用服务当作数据删除授权。

## 多日增量验收状态（2026-10-30）

**193与217均已部署并通过合成故障验收**，两端运行源码SHA与本机一致，研究服务active/enabled。原5005 PID2166351保持，本增量未重启它，也未改变其转换/过滤或193生产任务。新增租约只用于已发布产物，真实病例发布数仍为0。

- 本机`python3 -m pytest test/ -q`：249 passed / 3 skipped；193隔离测试树用生产解释器`/opt/ia-runtime/bin/python3.12 -m pytest test/ -q`（测试依赖PYTHONPATH固定到私有testdeps，使用原dcm2niix）：229 passed；其中54项研究出口专项。三个本机跳过项仍是既有环境专项。隔离树最初漏复制dcm2niix，补入原二进制后全过；未改转换行为。
- 217真实research-fetch进程收到SIGKILL后由systemd重启，partial从786432增长到1048576字节；无需重新提交任务。包含NII/NPZ/PNG/Excel的批次最终4/4成功，全部SHA一致。
- 193真实研究网关在另一个正在传输的合成文件中重启，最终版本约0.33秒恢复8443监听；217自动重试Range续传，完整SHA一致。网关的SO_REUSEADDR仅处理TIME_WAIT重绑，专项验证第二个同时监听仍失败，未启用SO_REUSEPORT。
- 现场故障已修复并回归：连接名额尚未释放应返回429，不能返回404误触发BLOCKED；成功任务清除之前的错误字段，CLI不再误报失败。旧版本禁用地址复用导致重启遇EADDRINUSE，已修复。
- 现场安全：无短租约404，合法租约/manifest200，无证书TLS失败，错误授权/跨产物/原管理路由404，写方法405；撤销后同一证书、已缓存短租约的代理仍得到ACCESS_DENIED_OR_NOT_READY。故障测试两个grant均已撤销，已下载合成文件保留。
- 普通research-dev仍不能直连8443或5005、不能读取私钥；CLI拒绝URL/路径/扩权字段。现有防火墙规则未放宽。当前可用非临床联调授权信息在217 `/srv/research/research_exports/MULTIDAY_COMMISSIONING.json`（14天、1合成例、64MiB、512KiB/s），不是实际研究病例批准。

本次管理连接在211直连217/187 SSH握手异常后，改用222现有允许的SSH转发，严格匹配217原主机密钥；未修改222/187访问策略。直连SSH根因仍待独立检查，不声称该网络问题已修复。

回执与版本/故障/SHA证据在211私有`/home/ubuntu/research_export_deploy_20261030/MULTIDAY_RECEIPT.json`、`multiday_fault_receipt.json`、`multiday_gateway_restart_receipt.json`、`multiday_live_tls.json`、`multiday_revocation.json`。193root和217root备份路径见同目录multiday_upgrade_receipts.json。这里记录的是实际进程终止/网关重启及加速边界回归，尚未完成整机重启或连续14天的现场耐久测试；193可信授时问题仍见上方说明。
