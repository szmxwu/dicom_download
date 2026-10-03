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
