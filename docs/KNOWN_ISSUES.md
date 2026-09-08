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

## 环境约束（长期有效，不是 bug）

### K5. `.env` 只由 web app 加载
`src/web/app.py` 在 import 时 `load_dotenv()`。脚本/测试直接实例化 `DICOMDownloadClient` 必须自己加载 `.env`，否则静默回退到 `unified.py` 硬编码默认值（错的 CALLING_PORT/CALLING_AET → PACS 无法回连 C-STORE → C-MOVE "成功"但 0 文件，status 0xCA36）。

### K6. eventlet 三大纪律
见 DECISIONS D19。排查工具：193 上有 `~/py-spy`（需 sudo，ptrace_scope=1）可 dump 运行中进程全线程栈定位冻结点。

### K7. 无包安装 / 测试现实
不能 `pip install -e .`，必须从仓库根目录运行（PYTHONPATH 含根）。无正式测试套件；`pytest test/` 目前 14 个用例（MR clean、方位修正等），`test.py` 是上传流程集成测试且硬编码只测 nifti。

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
| 2026-05-14 | '3D' 关键词误杀原始 MR 序列 | 17ee220 移除 |
| 2026-05-14 | QueueWatchdog 误报 | e8da48b |
| 2026-05-15 | GE Propeller (RM) 序列 MR_clean 不识别 | d2530db |

## 存量数据修复记录（一次性行动，勿重复执行）

- **Y 轴翻转 nii 批量恢复（2026-08-30 完成）**：33364 个受影响文件全部恢复并逐像素 GT 验证（corr=1.0000），header 标记 `REVERTED_2026-08-28`。脚本：`test/verify_restore.py`、`test/backfill_marker.py`、`test/gt_check.py`；清单：`test/output/xray_affected.json`。
- **预览图全量重生（2026-08-31 完成）**：35014 个序列目录，34926 ok / 88 SKIP / 0 ERR，用 D14 最终代码覆盖全部旧预览。日志 `logs/regen_all.log`。
- **PR 垃圾目录清理（2026-09-01 完成）**：75 个 `*_KEY_IMAGES_PR` 目录删除，75 个 xlsx 清除 152 行 PR 记录（备份 `.bak_pr_clean`）。脚本：`test/clean_pr_junk.py`。
