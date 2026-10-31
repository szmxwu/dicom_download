# KNOWN_ISSUES.md — 已知问题与环境约束

未解决的问题、环境约束、以及已修复但值得保留的事故模式。每条带日期与出处；修复后标注修复 commit，条目保留。

## 未解决 / 待复核

### K1. Windows 下运行一段时间后自动崩溃（未确诊）
- 首次记录：2026-07 前；2026-07-07 已加崩溃监控日志（commit aea551b）。
- 现象：136（Windows 单机）运行一段时间后进程退出，难以定位。
- 已排除方向：队列槽位泄漏已修（D17，但那是并发减少不是崩溃）；Windows emoji 输出崩溃已修（commit 7418c22）。
- 下次发生时：查 `logs/app.log` 末尾、`faulthandler` 输出、Windows 事件查看器；重点怀疑 eventlet/线程/文件句柄。

### K2. Completed 但 ZIP 404 —— 已修复（2026-09-08）
- 报告：2026-08，136 上 3933963、Z19121200215 任务状态 Completed 但 `/api/download/{task_id}/zip` 稳定 404（ZIP 未生成）。
- **根因确认**：这两个 accession 反复出现 → 每次走缓存命中。`copy_from_cache` 用硬链接把缓存的 `result.zip` 链到 `results/result_{task_id}.zip`；**Windows NTFS 建硬链不刷新 mtime/ctime**（沿用缓存原件的数天旧值，`shutil.copy2` 回退同样保留旧 mtime）。缓存命中任务秒级 Completed 后，磁盘紧张时清理守护进程把这条"旧" ZIP 当作超出 `CLEANUP_MIN_AGE_MINUTES` 保护窗的文件立即删除 → 404。Linux 的 `link(2)` 会刷 ctime，故只在 Windows 复发。
- **修复**：`copy_from_cache` 链接/复制目标 ZIP 后显式 `os.utime(target_zip, None)` 刷新时间戳（同时 `os.utime(cache_dir)` 刷新缓存条目 atime，保证 LRU 顺序准确）。已用 10 天前旧时间戳的模拟缓存验证：修复前目标 zip mtime 保持 10 天前，修复后为当前时刻。

### K3. Windows 缓存 LRU 淘汰偶发 WinError
- 2026-08：136 缓存超 20GB 限额时 LRU 清理报 WinError 3/5（文件被占用/路径不存在）。已做：淘汰改异步后台线程、失败条目 30 分钟冷却、清理不再触碰 `results/cache/`。不影响新下载，但老旧 accession 缓存可能失效，重复下载需重新走 PACS。

### K4. 厚层序列三视图的 Z 轴比例 —— 已复核排除（2026-09-08）
- 2026-08：排查缺 preamble 文件时自绘三视图曾忽略层厚导致冠状/矢状位压扁或拉长——那是临时脚本的 bug；**项目自身预览即三视图设计未被改动**。
- 2026-09-08 复核：`preview.py:_generate_3d_triplane_preview` 有完整的体素各向异性修正（`_scale_height()` 按 PixelSpacing 拉伸面内高度，矢状/冠状面板按 SliceThickness 修正层间距；nii 路径用 header zooms，npz 路径用 DICOM tag）。合成数据（19 层×5mm → 高度 95px）与真实数据（`MRplusDownloads/0000000099/004_t1_tse_sag_384`，dz=4.9mm）双验证通过，重新生成与存量预览一致。**项目三视图无此 bug**。若预览出现 Z 轴比例异常，先确认是否真是项目预览输出。

### K10. 单张转换部分失败后删除失败原件 —— 已修复（2026-09-13，未 commit）
- 审查实证：两张 DX 输入，模拟第二张转换失败，旧代码仍返回 success=True 且删除两张 DICOM；Python 回退分支同样只检查 success_count > 0。
- 修复：逐张转换必须全部成功才删除原件；部分失败保留整组 DICOM，允许完整 Python 回退；最终失败由整理层传递到工作流，禁止作为成功检查写入缓存。既有缓存中的历史缺片结果不会自动修复。
- 回归：`test/test_conversion_failures.py` 覆盖两种转换器、内存返回模式、完整回退、串行整理和并行工作流失败传播。仅用合成 DICOM，无 PACS 连接。

### K11. Windows/eventlet 子进程管道阻塞 —— 已修复路径，待 Windows 原生复核（2026-09-13）
- 审查实证：eventlet 0.41.0 + 标准库 Windows 管道读取逻辑的 Linux 模拟中，50ms 超时仍阻塞事件循环约 572ms。
- 修复：dcm2niix 执行及 `-h` 探测统一有超时，在真实线程内使用原生 subprocess/threading/time；输出写临时文件，避免 Windows PIPE 读取线程被绿化；原生串行锁只在真实线程内获取。Windows 超时先 taskkill 整棵树，再兜底 kill/wait。
- 回归：`test/test_conversion_failures.py` 验证输出解码、超时、启动失败、eventlet 心跳及嵌套并发。不能据此认定已解决 K1 进程自动退出；仍需用户手动同步并在 Windows 服务器复核。

### K12. Windows 启动环境与离线安装环境分离 —— 已修复（2026-09-13，未 commit）
- 代码实证：旧 `run_on_windows.bat` 创建新 venv 并联网安装，安装失败仍启动；`offline_packages/install.ps1:11` 的 `2&1` 为错误重定向。
- 修复：启动脚本使用当前已激活环境，显示解释器路径，依赖/导入检查失败即退出，不安装包；PowerShell 重定向改为 `2>&1`。离线 wheel 和 requirements 未调整，用户自行补齐。

## 环境约束（长期有效，不是 bug）

### K5. `.env` 只由 web app 加载
`src/web/app.py` 在 import 时 `load_dotenv()`。脚本/测试直接实例化 `DICOMDownloadClient` 必须自己加载 `.env`，否则静默回退到 `unified.py` 硬编码默认值（错的 CALLING_PORT/CALLING_AET → PACS 无法回连 C-STORE → C-MOVE "成功"但 0 文件，status 0xCA36）。

### K6. eventlet 三大纪律
见 DECISIONS D19。排查工具：193 上有 `~/py-spy`（需 sudo，ptrace_scope=1）可 dump 运行中进程全线程栈定位冻结点。

### K7. 无包安装 / 测试现实
不能 `pip install -e .`，必须从仓库根目录运行（PYTHONPATH 含根）。`pytest test/` 覆盖 MR clean、方位修正、转换失败和子进程回归；eventlet 专项在未安装 eventlet 时跳过。`test.py` 是上传流程集成测试且硬编码只测 nifti。

### K8. `offline_packages/` 版本可能与 requirements.txt 不一致
离线安装时以 requirements.txt 为准逐个人工核对。

### K9. 双实例与端口
见 DECISIONS D20。重启纪律：先杀旧进程（精确 `kill <pid>`），再启动。

## 已修复的事故模式（保留作参考）

| 日期 | 事故 | 修复 |
|------|------|------|
| 2026-08-27 | 缺 preamble 的隐式 VR 文件被 dcm2niix 拒收 → 整序列走 50s 慢速路径 | 398731f（D9） |
| 2026-08-27 | 缺 IOP 的 2D DR 布局不稳定 + 少数 CT 反转 | 6d9d93e（D11） |
| 2026-08-27 | 双实例抢端口撕裂任务状态 | 单实例锁（D20） |
| 2026-08-26 | tpool offload 内碰 green 锁 → lost wakeup → 全站假死 | 77e8c6d（D19③） |
| 2026-08-26 | 缓存淘汰机制 bug / 清理删掉 38GB 缓存并竞争 | 2d58c4d + D10 |
| 2026-08-29 | Mindray DR 诊断序列 ImageType=DERIVED 被误杀，136 失败率 85% | X 光宽容（D4） |
| 2026-08-31 | KEY_IMAGES PR 无像素对象泄漏进结果包（nonce 关键词拆防线） | d67c171（D3/D7/D21） |
| 2026-08-31 | Mindray MONO1 预览纯白/反转/对比度极差 | 7418c22/0b1278a/a1970bb（D14） |
| 2026-08-31 | QC 边框-中心启发式反转健康胸部 CT | 判据退役（D12），存量 33364 个 nii 已批量恢复并验证 |
| 2026-08-31 | 监控页 failed 任务耗时无限增长 | d67c171（D18） |
| 2026-09-05 | 卡死任务取消后并发通道永久 3→2 | 2b6a2e4（D17） |
| 2026-09-13 | C-STORE SCP 启动失败（CALLING_PORT 被占）异常在 try 之外 → `_cmove_lock` 永不释放 → 全部任务永久卡在 acquire；线程静默死亡无任何错误日志 | SCP 启动移入 try（finally 兜底 shutdown+释放锁）+ `_download_worker` 补 except 日志（未 commit） |
| 2026-09-17 | 超大老序列（单层 1711 文件、6.4GB）metadata 提取超 600s 超时 → `success` 依赖 excel_file → 下载转换全部成功却报 "Unknown error during process"（193 上一批 2019 年 CT 全部因此失败） | metadata 变为可选产物：超时/失败仅记 warning，success=True；超时改 `METADATA_TIMEOUT_SEC`（默认 1800）（未 commit） |
| 2026-10-02 | 拼装序列（printingSeries/shukunlist，混合矩阵尺寸）dcm2niix+python-libs 双失败 → `Series conversion failed` 株连整个任务不出 zip（136 一天 20 次）；几何头损坏的 CT 触发 nibabel SVD 崩溃；K13 严格门禁拒绝的 CT 同样株连 | D24：任务级部分成功 + 混合 shape 多数派容错 + `_safe_as_closest_canonical` 全面替换裸调用（未 commit） |
| 2026-10-01 | 队列满拒绝路径调用 `update_status('failed', error=...)` 但该方法无 error 形参 → TypeError → 兜底 except 里再调一次又炸 → 客户端收到 500 而非 503 → 礼貌退避失效，客户端狂轰滥炸（136 实证 3033 次 ERROR、队列持续爆满）；附带监控接口裸迭代 `processing_tasks` 偶发 dictionary changed size | `update_status` 增加 `error=None` 形参；所有 `processing_tasks` 迭代改为 `list(...)` 快照（未 commit） |
| 2026-09-20 | tpool 真实线程在 `logging` 里触发 eventlet green `current_thread()` → `patch_function` → `imp.acquire_lock()`，导入锁被主线程挂起 greenlet 持有 → worker 永久阻塞 → 任务持 `_cmove_lock` 不放 → 全站假死 24h+（HTTP 200 但零处理，py-spy 实证） | `fix_logging_locks_for_eventlet()` 增加 `logging.threading = original('threading')`（未 commit） |
| 2026-09-18 | GE 乳腺 DCE 减影序列（ImageType `DERIVED\SECONDARY\OTHER\SUBTRACT`）被衍生过滤误杀：pydicom `MultiValue` 非 `list/tuple` 子类 → 白名单对 ImageType 其余值从未生效 | 白名单加 `SUB`；`is_derived_series` 与 `organize._is_derived_series` 入口统一 MultiValue→list（未 commit） |
| 2026-05-14 | '3D' 关键词误杀原始 MR 序列 | 17ee220 移除 |
| 2026-05-14 | QueueWatchdog 误报 | e8da48b |
| 2026-05-15 | GE Propeller (RM) 序列 MR_clean 不识别 | d2530db |

## 存量数据修复记录（一次性行动，勿重复执行）

- **Y 轴翻转 nii 批量恢复（2026-08-30 完成）**：33364 个受影响文件全部恢复并逐像素 GT 验证（corr=1.0000），header 标记 `REVERTED_2026-08-28`。脚本：`test/verify_restore.py`、`test/backfill_marker.py`、`test/gt_check.py`；清单：`test/output/xray_affected.json`。
- **预览图全量重生（2026-08-31 完成）**：35014 个序列目录，34926 ok / 88 SKIP / 0 ERR，用 D14 最终代码覆盖全部旧预览。日志 `logs/regen_all.log`。
- **PR 垃圾目录清理（2026-09-01 完成）**：75 个 `*_KEY_IMAGES_PR` 目录删除，75 个 xlsx 清除 152 行 PR 记录（备份 `.bak_pr_clean`）。脚本：`test/clean_pr_junk.py`。
### K13. 长CT超过1,024层跳过几何计算及历史校验放行缺口（2026-10-02，本地修复、生产待部署）

CT_YOLO第17项用户提供2,432层PACS客户端导出，与历史C-MOVE的Series/SOP UID及HU不同，保留来源差异。导出IPP步进−0.45mm、SliceThickness=0.9mm、SpacingBetweenSlices=0.45mm。直接跑本项目捆绑dcm2niix独立生成+0.45mm错误头；全部637,534,208源HU体素与本次转换精确相等，原2,432文件SHA未变。此前历史NIfTI派生修正版与历史旧图像素全等，不能称作等于新导出。

已定位执行文件触发边界：1,024层正确，1,025层开始错误。上游2025-05-19 f6b6cda在sliceTimingCore加入dim3>kMaxEPI3D的early-return，跳过headerDcm2Nii2；2026-05-13 1cd1620修复。仅加/删该条件的编译实验能切换6项合成序列的故障；带guard构建与捆绑执行文件的Z列/原点/层序/q-s冲突全部一致。版本横幅v1.0.20250505不能唯一证明构建commit，绑定执行文件SHA与行为。C++float层距仍能积累约0.178mm误差，最终出口按全部IPP重建及角点位置校验。

本地v3修复：源像素无法匹配则CT拒绝成功、保留DICOM；全部源头可读、Study/Series/Frame UID一致、SOP唯一、矩阵维度一致及非多帧才可建立当前真值几何；缺身份或头子集不得默许。preferred affine正确但有效q/s冲突也修正；shear无法由qform表达时禁用qform、保留精确sform。物理角点误差>0.05mm即修正，真实HU精确保留。缓存几何策略20261002-source-pixel-geometry-v3，阻止旧NIfTI缓存静默复用。此前O(N)排序、Python行列/有符号步进、wrapper output_format传参、可选metadata成功语义及所有校验状态留痕修复保留。

最终补丁对完整真实17导出转换重放通过，全部637,534,208体素保持原值与次序，q/s一致且匹配源几何。历史Python防线究竟为何没有修正那次旧下载，具体运行时分支仍不能唯一恢复；不把故障注入当历史诊断。没有技师或FFS错误证据。

本地python3 -m pytest test/ -q：46通过、2eventlet专项因未安装环境跳过。生产136/193未部署、未重启，Windows实证待复核；193于2026-10-02核对3运行/17排队/3并发上限，不能为本补丁中断其他下载。独立193环境验证目录使用生产解释器及同SHA转换器48通过，包含2项eventlet专项、无跳过；不属于生产部署，原进程及生产源码未动。

全CTA1,831序列重新审计风险：1,138序列超过1,024层，不是1,138均已污染；16几何候选人工12颠倒/4头脚正常，全部继续隔离。旧53/65拉伸分别有约2倍/1.64倍q/s间距冲突。新CTA门禁1123待源复核/16隔离/692头相容，整库源像素验证未闭环。详../CT_YOLO/docs/CTA017_DIRECTION_INCIDENT_20261002.md。

存量修复检查点：本项目test/output/cta_slice_count_v1、cta_slice_count_guard_injected_v1、cta_slice_count_no_earlyreturn_v1为无患者信息的可恢复根因实验；真实源审计和独立修正版在CT_YOLO/derived/anatomy_sensor_v3/20261001_phase0/direction_incident_v2。未覆盖原下载目录或批量修改1,831源图。

- 2026-10-02：生产几何v3剩余验收已移交[具体交接清单](CTA_GEOMETRY_V3_PRODUCTION_ACCEPTANCE_HANDOFF_20261002.md)，下载空闲时实施；不阻塞CT_YOLO存量数据清理与主线。

**2026-10-22补充：K13历史存量四肢范围（非本次生产代码故障复现）**。CT_YOLO对2609＋supplement索引7051原生NIfTI核验，43初始header／metadata／解剖冲突及4同检查保守隔离，47原源held。5项q/s冲突有限计划中4项通过同次重建物理对齐与元数据端点佐证，发布保留头后载荷的独立修正版，原源SHA不变；PJT151为旧+0.9mm／qform约+0.45mm而端点及厚重建支持−0.45mm，兼有倒置与两倍拉伸。1项未达冻结门槛不放行。并非246个>1024层序列全部有错。

PJT059原1025层与153层厚重建在原物理框架下对齐但共享解剖方向疑点，不能单因>1024盲修；PJT124仅691层但xlsx数量／面内间距／IPP与图像身份冲突，需匹配原始DICOM。FFS／mirror_relayout均不足以唯一归因。PJT203用户撤回，为正常上举摆位。当前无证据证明新生产转换路径仍有同一缺陷，未部署／重启或修改本项目生产代码。技术恢复角色不解封临床测试。

可复用入口为兄弟项目[治理报告](../../CT_YOLO/docs/LIMB_NATIVE_GEOMETRY_AUDIT_AND_REPAIR_20261022.md)、`configs/native_geometry_holds.json`与222独立`derived/native_geometry_consumer_release_20261022_v1/`。4份修正仅为同次重建／元数据佐证等级，非完整原DICOM像素验证；剩余原源保持隔离，原始DICOM补齐后按源探针路径继续，不在QC／预览层盲改源CT。

### K14. PACS 返回重复序列条目 → 二次 organize 崩溃并覆盖首次成功结果（2026-10-09，本地已修复、193 待重启）

- 事故：M26052802565 任务整体失败，报 `Series conversion failed (all series): cannot access local variable 'pydicom'`。
- 完整链条（193 日志实证）：
  1. C-FIND 返回的序列列表中 Series 401/402 各出现**两次**（同一 accession 下多 study 复用相同 SeriesNumber+SeriesDescription，或 PACS 重复返回）。`_query_series_metadata` 不去重。
  2. 下载循环对重复条目各发一次 C-MOVE（日志：Series 402 完成于 16:12:00 和 16:12:58 两次），`on_series_downloaded` 回调也触发两次。
  3. 第一次 organize：无缓存 → `if not modality:` 分支内 `import pydicom` 正常执行 → 写 metadata 缓存（含 modality）→ 转换成功。
  4. 第二次 organize：缓存命中使 modality 非空 → 条件分支被跳过 → `import pydicom` 未执行 → 函数末尾 `isinstance(sample_dcm, pydicom.Dataset)` 抛 `UnboundLocalError`（HEAD 快照复现验证通过）。异常被 organize worker 记为 conversion_error，**覆盖了第一次的成功结果**。
  5. 附带数据风险：若重复条目是**不同 SeriesInstanceUID**（多 study 同 SeriesNumber），两序列文件会混入同一 `{num}_{desc}` 目录，转换出混合体积。
- 修复（三层）：
  1. `organize.py`：`import pydicom` 提升为无条件导入（消除崩溃本身）。回归 `test/test_organize_cached_modality.py`。
  2. `unified.py:_query_series_metadata`：按 SeriesInstanceUID 去重，重复条目记 warning 丢弃（消除重复下载/回调）。
  3. `unified.py:download_study`：新增 `_resolve_series_dir()`，检测目录名冲突——不同 UID 映射到同一 `{num}_{desc}` 目录名时追加 UID 后缀（防止数据混合）；同一 UID 重复解析（主流程/重试）返回同一目录。
- 教训：目录命名键（SeriesNumber+Description）不是唯一键，只有 SeriesInstanceUID 是；任何"以缓存存在与否分支"的代码路径都要保证分支两侧的局部变量一致可用。

### K15. RF 模态未列入 2D X-ray 家族导致预览三视图化 + nii 布局未归一 —— 已修复（2026-10-19，待同步服务器）

- 现象：X-ray_YOLO 侧发现 RF（胃肠机）胸片预览为 8838×2946（3× 宽黑画布，仅左 1/3 有图像），平台 nii 视图旋转 90°。实证 accession 521112900030DR。
- 根因（两处同源遗漏）：
  1. `convert.py` dcm2niix/python-libs 的逐文件 2D 分支 `modality in ['DR','MG','DX','CR']` 不含 RF → RF 落入"整序列单 nii"的 3D 分支，`normalize_2d_nifti_display` 探针归一化未执行，nii 保持 dcm2niix 原始 (cols,rows) 布局。
  2. `preview.py is_2d_xray` 同样不含 RF → 单文件 2D RF 序列走进 `_generate_3d_triplane_preview`，三面板横拼（3×宽），仅 axial 面板有内容。
- 修复（2026-10-19）：5 处 2D X-ray 模态清单统一补 'RF'（convert.py 两分支、preview.py is_2d_xray、metadata.py read_all ×2、unified.py read_all）。`pytest test/` 139 passed。
- 未纳入 XA/US：XA 常见多帧造影（cine），逐文件 2D 化可能误拆 multi-frame；如出现需求再专项评估。
- 存量数据：修复前下载的 RF 序列 nii 布局与预览均不可信，需重下；X-ray_YOLO 侧以 preview/nii 长宽比一致性扫描圈定（geometry_audit）。
- static/src 为旧快照，本次未同步。

### 2026-10-23 K13续：v3部署状态已更新，v4完整CT输入门禁新增修复

10月9日HANDOFF已记录136／193部署，2026-10-23对现193 P3650生产目录只读核SHA确认v3，旧“生产待部署”系10月2日历史。研究双3090已迁188。新审计实际复现Python读头／读像素丢末片、混矩阵保留多数片、缺源身份后仍success，以及单文件多帧CT绕过门禁。新增v4本地修复且普通单层保持通过；全套152通过／2 eventlet环境跳过，24层真实HU逐值全等。当前生产仍v3，未部署或重启，不能宣布新缺口已在生产消失。具体补丁与空闲验收见[完整交接](CT_GEOMETRY_V4_COMPLETENESS_HANDOFF_20261023.md)，现场解释器独立验证证据亦在交接链接。

转换阶段保留原件，不等于10月18日最终交付清理后永久保存原件；本轮不更改D24。源标签自身错误、均匀缺失输入和探针外像素损坏仍是证据边界。

2026-10-23补充验收：当前生产Python3.12／eventlet／实际生产转换器副本的独立快照全集154通过、0跳过、0失败，源码绑定一致且生产4文件SHA前后未变；无全局包安装、部署或重启。这取代“仅本地152／2跳过”作为候选补丁运行环境证据，~~生产切换及Windows实证仍pending~~。

**2026-10-23 生产切换已完成（193）**：队列空闲窗口部署 convert.py/constants.py（md5 与本地补丁一致），`backups/pre_v4_20261023_convert_constants.tar.gz` 留存 v3 备份；生产解释器对部署后源码跑核心回归（test_ct_source_completeness/orientation_fix/conversion_failures/d24_delivery/organize_cached_modality/mr_clean_exclusions）**131 通过／0 失败／0 跳过**（eventlet 专项真实执行）。旧 PID 544624 精确 SIGTERM 后确认退出，新实例 PID 2223784 监听 5005，HTTP 200、PACS connected，`CONVERSION_GEOMETRY_VERSION='20261023-complete-ct-source-geometry-v4'` 生效（缓存指纹自动失配旧产物）。unified/metadata/preview 的 RF 补充此前已在产（SHA 核对一致）。**136 仍待用户手动更新**，正常生产下载路径验收（≤1024/>1024 层、斜位、缺片拒绝等）随真实任务持续观察。

### K16. 多任务大体积 QC/预览的整卷副本导致内存墙风险 —— 已修复并部署193（2026-10-24）

- 现场：193（46.8 GiB）运行多个服务，可用内存约 2.75–4.8 GiB、2 GiB swap 已满，但当前无持续 swap I/O；可查日志未见 OOM/MemoryError。DICOM 服务 RSS 约 1.5–2.5 GiB、VmHWM 5.2 GiB，任务间回落，未证明存在泄漏。
- 根因：单序列 QC 先 `get_fdata()` 生成 float64 整卷，再转 float32、做全量 `np.isfinite` 和索引复制；三视图 canonical 后又复制 float32 整卷。默认 3 任务 × 每任务 2 converters，峰值可在阶段重叠时叠加。`MAX_PENDING_SERIES` 只管目录队列，不能限制这些数组。
- 修复：见 D25。QC 延迟缩放并先抽样（浮点仍先过滤非有限值），三视图用懒重定向仅读取切片；全局重处理默认 2 槽并带低内存防叠加/单工作防饿死。代码审查曾发现 `np.asanyarray(dataobj)`、直接 canonical、NaN 抽样顺序及循环轴排列问题，均已补真实回归后解除。
- 验证：166 passed / 2 skipped；48 方向逐一对照；512³ 非单位仿射基准 QC 2326→663 MiB、预览 920→168 MiB，p98/切片值一致。193 已同步全部改动并核对 md5，生产解释器 145 项回归全过后于 2026-10-24 重启（PID 2002530）；HTTP 200、PACS connected，客户端已自动重试原任务。136 未同步，生产内存峰值仍需随真实大体积任务持续观察。

### K17. 下载恢复水位高于清理能力，C-MOVE 持锁永久等待 —— 已修复并部署193（2026-10-24）

- 现象：重启后 3 个 running、6 个 pending，最早任务超过 2 小时停在 40% `Downloading DICOM files`，无 C-MOVE 网络连接；C-ECHO 正常、SCP 1235 在监听、HTTP 正常。
- 根因：任务 519102102092CT 于 16:09:28 读到 `Disk free space low (129.65GB < 130.0GB). Pausing downloads until free >= 160.0GB...`，随后在 `_wait_for_disk_low()` 内持 `_cmove_lock` 等待。实际配置为下载暂停 130/恢复 160，但自动清理只在空闲 <60 或 results ≥180 时触发，且清理目标最多 70，不可能满足 160 的恢复条件。等待只记录一次日志，表现为“卡死”；与新增内存背压无关。
- 处置：用户选择不删除文件，将 193 的 `DOWNLOAD_LOW_WATERMARK_GB` 改为 100、`DOWNLOAD_HIGH_WATERMARK_GB` 改为 120（当前空闲约 129.7，重启即恢复）；`_wait_for_disk_low` 增加每 5 分钟等待日志。`test/test_disk_backpressure.py` 覆盖恢复线和周期日志。
- 验证：本地 168 passed / 2 skipped，生产解释器 147 passed；193 新 PID 2676150。重启后 C-MOVE、转换和 ZIP 均恢复，M20091202427 已完成，其他任务继续推进。D10 已记录水位一致性要求；136 未同步。

### K18. 136 旧版 X-ray 全黑/全白/XY 颠倒 —— 当前193未复现，已补交付门禁（2026-10-24）

- 用户报告：Z21050200397 的 V04_0016/V04_0014 全黑，Z24031801632 与 Z21051000117 胸部横向/XY 颠倒，Z21052001013 全白。`logs/app (4).log` 未覆盖这些检查，无法从该文件还原旧处理链。
- 从193重下四个检查并逐张读取预览与主 NIfTI：均为正常直立胸片，灰度分布非恒定，方向无横向；旧问题在当前版本未复现。Z21051000117 目录中的 `*_ROI1.nii.gz` 是 ROI 而非主图，不能误作主图比较。
- 决策见 D26：真正空白源图放弃、不可信归一化回退、最终失败不交付、NPZ/源文件递归清理且 fail closed。新增回归 175 passed / 2 skipped，终审无阻断项。193 已完成同步、生产解释器 154 项回归并重启（PID 3086791），新缓存/水位配置同时生效，重启后已有 3 个任务完成。136 仍是旧版，需用户手动同步后才能获得这些保证。

### K19. 缓存达到180GB后普通清理无法恢复下载水位 —— 已修复（2026-10-25）

- 现象：3 个 running 超过 1 小时，24 个 pending；磁盘空闲约 39.95GB，下载等待恢复到 60GB。普通清理反复报告“所有文件都属于活跃任务”，仅删除少量非缓存旧结果，无法达到恢复线。
- 根因：缓存上限扩至 180GB 后占满空间，LRU 只在保存后按容量淘汰，普通结果清理排除 cache；磁盘水位不足时下载持锁等待，客户端断连也无法解除。不是 PACS 故障，也不是新增内存闸门故障。
- 修复：见 D27。LRU 同时按容量与 `CACHE_MIN_FREE_GB` 淘汰，并在启动及普通清理 `finally` 中异步执行；真实空闲重采样处理硬链接；save/copy 与淘汰互斥，锁为 eventlet 原生 OS 锁。运行中同 accession+options 复用任务，避免断连重试制造重复下载。
- 验证：本地 193 passed / 3 skipped；193 生产解释器 173 passed（含 eventlet 专项）。新进程 PID 4066592，启动 LRU 淘汰 107 个旧缓存条目，空闲恢复至 60GB 以上（复核时 81.84GB），C-MOVE 与完成任务继续推进。真实重复提交复用原 task_id；M23110801983 重连后 `Completed (from cache)`，ZIP 下载 234,724,037 字节成功。

### K20. 单序列几何标签为空导致 `float(NoneType)` —— 属源数据拒绝，非转换器故障（2026-10-25）

- 案例：Z23063000083 的唯一序列 101_Unknown。dcm2niix 已生成 NIfTI，但 CT 几何核验在 `build_dicom_truth_affine()` 的单层步进计算中读取 `SliceThickness=None`，直接 `float(None)` 报错，最终按 `CT geometry not verified` 拒绝。
- C-FIND 同样未返回 SliceThickness/PixelSpacing/IOP/IPP 等可选几何标签（PACS 不保证这些字段完整），说明属于源数据几何信息缺失/不完整，不能靠 dcm2niix 成功输出反推坐标可信。失败序列按隐私策略删除，整检查因唯一序列全部失败而失败，无 ZIP。
- 阻断语义：部分序列失败不会株连整检查；有任一成功序列时任务部分成功，失败清单写入 `conversion_errors`。只有全部序列失败（本例仅 1 个）才使整个检查失败。若需处理此类数据，应补充可靠几何标签或明确人工可接受的回退规则，不能静默假定层厚。

### K21. 结果 ZIP 权限异常导致 HTTP 回传 Permission denied —— 已修复（2026-10-27）

- 现象：其他 agent 报告部分结果为 200（只写不可读），日志确认 Z24030601798、M21053102971 的下载请求连续 `PermissionError: [Errno 13] Permission denied`。排查时异常文件已于 15:27 被外部改为 600，279 个现存 ZIP 均可读，无法从日志唯一确定 200 的初始来源；进程 umask=0002，不是当前进程默认权限导致。
- 修复：`create_result_zip` 完成后显式 `chmod 0o640`；缓存保存和缓存命中复制也显式设置 0o640，避免依赖 umask 或历史权限。新增生成与缓存权限回归。
- 验证：本地 195 passed / 3 skipped，193 生产解释器 175 passed；重启 PID 313858。重新注册两个原失败检查后均 `Completed (from cache)`，HTTP HEAD 200（约 887MB/935MB），新结果 ZIP 权限均为 0o640。

### 2026-10-30：217研究导出缺少独立权限与发布门（设计阶段）

本机app.py的任务、下载、配置等路由未见按客户端/数据集隔离；状态包含logs/result/error，元数据Excel默认含PatientID/AccessionNumber/StudyDate。现有网络阻止217访问193，不能据此把原API或原ZIP视为安全研究出口。候选方案见H217_RESTRICTED_RESEARCH_EXPORT_DESIGN_20261030.md，未部署；生产逐文件版本一致性未在本次重验。

2026-10-30更新：上方217导出缺口已通过独立出口阶段1解决，详H217_RESEARCH_EXPORT_DEPLOYMENT_20261030.md。5005原API未作全局认证改造，旧Excel仍含原字段；仅新出口使用净化后发布物。原生DICOM/新PACS获取/OCR与面部处理未实现，真实病例须可信端逐文件像素证据，未做临床数据发布验收。193时钟当前未NTP同步；不能把证书/授权正常联调视为可信授时验收。新服务启用后的整机重启持久性也尚未实测。

2026-10-30 多日传输补齐已部署193/217：固定批准最长14天、600秒自动续租，持久队列/退避/断点/重启恢复及磁盘背压；249本机通过/3跳过，193生产解释器229通过，54研究专项。真实SIGKILL与网关重启后续传/SHA、安全撤权均通过。并发冲突404误判、成功后残留error和网关TIME_WAIT重绑失败均已修复；证据详部署说明。原5005 PID2166351保持；新PACS获取未接入、整机重启/14天现场耐久未测。211直连217/187管理SSH仍异常，本次通过222既有SSH转发完成且不改防火墙；根因待独立查验。
