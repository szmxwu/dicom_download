# DECISIONS.md — 架构与设计决策

每条决策记录：日期、背景、决策、理由。推翻旧决策时不删除，移到 `DEAD_ENDS.md`。

## 过滤体系

### D1. 三阶段序列过滤共用同一判定函数（2026-05 起，2026-08 强化）
查询（SERIES 级 C-FIND 一次往返取回 Modality/ImageType/SeriesDescription/SliceThickness/实例数）→ 接收（`handle_store` 用首文件头部再判一次，拒收序列不落盘）→ 整理（按实际文件兜底）。统一规则是 `src/core/constants.py:is_derived_series()`。
**理由**：C-FIND 返回可能不全（缺 ImageType/SeriesDescription），单靠查询阶段会漏网；三阶段共用同一函数避免逻辑漂移。
**注意**：`organize.py` 还有一个自己的 `_is_derived_series` 副本，与 constants 版有历史漂移（organize 版对 ORIGINAL 直接放行不做关键词匹配）——改规则时两边都要改。

### D2. 接收端拒收序列时 C-STORE 仍返回 0x0000（2026-05）
**理由**：返回失败码会导致部分 PACS 中止整个 C-MOVE，宁可接收后丢弃。
**副作用**：被整序列拒收的检查会以 0 文件落盘 → 任务失败（`Download failed, workflow terminated`），这是有意为之。

### D3. 非图像模态无条件过滤（2026-08-31）
PR/KO/SR/OT/DOC/REG/FID/RWV（`constants.NON_IMAGE_MODALITIES`）在三阶段无条件丢弃，与 `exclude_derived` 开关无关；接收端额外检查首文件无 PixelData。
**理由**：这些对象无像素数据、无法转换，下载下来只是几 KB 垃圾 dcm 混进结果包。实证：Carestream DR 的 `KEY_IMAGES PR` 序列（客户端用 nonce 关键词覆盖默认过滤后漏网，见 D7）。

### D4. 2D X 光模态的 DERIVED 宽容（2026-08-29）
DR/DX/CR/XR/RF/XA/MG 等模态（`XRAY_DERIVED_TOLERANT_MODALITIES`）仅凭 `ImageType[0]=DERIVED` 不判衍生，仍需 SeriesDescription 命中关键词；CT/MR 行为不变。
**理由**：DR 探测器输出的 for-presentation 诊断图像常规就是 `DERIVED\PRIMARY/SECONDARY`。实证：Mindray DR 唯一诊断序列被误杀 → 136 上任务失败率 85%。

### D5. 衍生关键词例外表 + ImageType 白名单（2026-08 厂商命名调研）
`DERIVED_KEYWORD_EXCEPTIONS`（iDose≠剂量报告、MRCP≠厚层MPR、veLOCity≠定位像、KEYHOLE≠关键图像…）与 `DERIVED_IMAGE_TYPE_WHITELIST`（ADC/Dixon/DWI + DR 全脊柱/全长下肢拼合 SPINE/STITCH/LONGLEG）。
**理由**：各厂商迭代/深度学习重建（iDose/IMR、ASIR、SAFIRE/ADMIRE、AIDR/AiCE、KARL/DELTA、ClearView、IPV、iDream）均为 ORIGINAL 诊断序列，命名与过滤关键词存在大量子串碰撞；短子串关键词（REF/LOC/KEY/SUM/VR）有误伤面。实证事故：Philips iCT iDose 被整体剔除只剩定位像（2026-08）；Philips DR 全脊柱拼合 520010500096DR 无法下载（2026-08）。
**规则**：新增关键词前先排查是否命中真实诊断序列命名；关键词匹配时两侧都 upper 归一化（历史上混入小写 'nodule' 成为从未生效的死代码）。

### D6. 过滤规则指纹并入缓存键（2026-08）
`get_filter_rules_fingerprint()` 覆盖关键词+例外表+白名单+行为版本标志，任何规则修改使旧缓存条目自动失配。
**理由**：规则修复后（如 iDose 误过滤、无全脊柱白名单、X 光 DERIVED 误杀），旧缓存会拉回错误过滤的结果，必须自动失效，不能靠人工清缓存。

### D7. 任务级关键词覆盖保持"替换"语义，追加走 `extra_derived_keywords`（2026-08-31）
`options.derived_keywords` 整体替换默认列表（web 页面关键词编辑器依赖此语义删除关键词）；API 客户端应用 `extra_derived_keywords` 追加。服务端 INFO 级审计日志记录每次提交的 IP + options 全文。
**理由/事故**：192.0.0.211 的 agent 传 `derived_keywords: ['XRAY_GAPFILL_NONCE_20260827']`（不可能命中的 nonce 词）变相关闭全部 48 个默认关键词 → KEY_IMAGES PR 垃圾下载。替换语义不能改（web 编辑器需求），只能加审计 + 提供安全的追加通道 + skill 红线。

## 下载与传输

### D8. C-MOVE 全局串行（`_cmove_lock` 类级锁）
**理由**：每个 C-MOVE 需要在同一本地端口启动 C-STORE SCP，并发会冲突。
**边界**：僵尸任务只要不持有该锁，其他任务下载不受影响（2026-09-05 卡死任务实证）。

### D9. C-STORE 落盘原样序列化 + preamble 回填（2026-08-27, commit 398731f）
`dataset.save_as()` 不重编码（pydicom 3.x 默认 `enforce_file_format=False`），写前必须补 128 字节 preamble（`getattr(dataset,'preamble',None) is None → b"\x00"*128`），整个写盘走 `run_cpu_bound()`。
**理由**：网络接收的 dataset 无文件 preamble，原样保存缺 DICM 魔数——显式 VR 文件 dcm2niix 能启发式嗅探，隐式 VR LE（老 Philips，UID 前缀 1.2.840.113704）直接拒收 → 整序列回退 ~50s 的 python-libs 慢速路径。pynetdicom 解码的 Dataset 可能没有 preamble 属性，直接访问会 AttributeError，必须 getattr 且在 try 内。

### D10. 磁盘背压与清理策略
下载暂停/恢复按 `.env` 水位线；清理按 max(mtime, ctime) 排序（**不用 atime**——relatime 下清理自身的遍历会刷新 atime，Windows NTFS 常不更新 atime）；`CLEANUP_MIN_AGE_MINUTES`（默认 60）保护新生成的 ZIP；`results/cache/` 不参与清理（LRU 自管理——历史上清理删掉 38GB 缓存并与淘汰竞争 → WinError 3/5）。
**理由/事故**：磁盘清理曾把新下载的批量任务 ZIP 清掉（2026-08）；Windows 上 atime 不可信。
**配套陷阱（2026-09-08，K2）**：缓存命中用硬链接把缓存文件链入 `results/`，**Windows NTFS 建硬链不刷新 mtime/ctime**——链出的 ZIP 带着缓存原件的旧时间戳，会被 min-age 保护漏掉而遭立即清理。`copy_from_cache` 必须在链接/复制后显式 `os.utime` 刷新目标 ZIP（并顺手刷新缓存条目 atime 保证 LRU 准确）。任何"从旧文件硬链/copy2 出新结果文件"的路径都要想到这一点。

## 转换、QC 与预览

### D11. 方位/灰度修正在转换时探针验证，不在 QC 时盲改（2026-08-27 起）
`convert.normalize_2d_nifti_display()` 对 2D 缺 IOP 图像在 8 个二面体变换上探针验证源 DICOM 像素；`verify_and_fix_orientation()` 校验内容-仿射一致性、纠正左手系镜像、强负相关时灰度再反转（3D CT/MR 同样适用，布局探针本来就跑，零额外成本）。日志 `ORIENTATION_MIRROR_CORRECTED`。
**理由**：dcm2niix 对缺 IOP 的 2D DR 输出布局不稳定（观测到 `(cols,rows)+flipud` 和 `(rows,cols)+fliplr` 两族）；QC 阶段的盲翻转（`detect_nifti_orientation_error`）会对已归一化的数组二次翻转，已退役。见 DEAD_ENDS。

### D12. QC 不做灰度反转启发式判定（2026-08-31 强化）
`GRAYSCALE_INVERTED` 的边框-vs-中心判据已退役。
**理由**：胸部 CT 解剖学上恒成立（边框=胸壁软组织 > 中心=肺野空气），曾把健康体数据静默反转（儿童胸 CT 实证：62% 体素被背景规则排除 → 动态范围被压 → 阈值失效 → 误判反转 → `max+min-v` 把好数据反转）。真实反转（含 PI 标签说谎）由转换时探针捕获（D11）。MR 偶尔存在的反转也由探针路径处理，不能简单禁止 3D 反转修正。

### D13. 预览图保持原始长宽比，`PREVIEW_TARGET_SIZE` 只限最大边（2026-08-19, commit 14c1146）
不用居中画布（`normalize_2d_preview` 的缩放+居中设计已移除）——居中画布破坏长宽比信息；DR 原始分辨率高，仅需限制最大径线。预览 `_load_image_2d` 对 2D 直接用原始数组，**永远不要在预览层加 canonical/翻转逻辑**。

### D14. 预览窗宽窗位不信任校验（2026-08-31, commits 7418c22/0b1278a/a1970bb）
`apply_windowing()`：窗口覆盖率 <1% 或**2D X 光数据中位数落在窗口外** → 判定窗口不可信 → 回退数据百分位 p1–p99.5 自适应窗；回退时对 2D X 光模态忽略 MONOCHROME1 标签、一律按 MONO2 诊断极性渲染。
**理由**：Mindray DX 的 MONO1 元数据两层说谎（窗宽窗位与像素完全不匹配 + PI 标签假，实际按 MONO2 极性存储）；LandWind 部分序列窗口开在数据范围外；按位深全范围渲染会导致解剖集中在低端时对比度极差。覆盖率判据存在中间地带漏网（coverage=29% 但 71% 像素堆在窗下方），中位数判据补齐。泛化验证：LandWind/SIEMENS/Mindray/EOS 四厂家抽样正确。仅影响预览渲染，不改像素数据。

### D15. 元数据 JSON 缓存（`dicom_metadata_cache.json`）
转换可安全删除源 DICOM，Excel 生成走缓存。

### D16. MR 序列分类规则在 `mr_clean_config.json`，不在代码里
改规则编辑 JSON；`mr_clean.py` 只做解释执行。

## 并发与运行时（eventlet/Windows）

### D17. 任务队列：闸门计数 + executor 冗余 + 超时看门狗（2026-09-05, commit 2b6a2e4）
`_running_task_count` 闸门限 3 并发；`task_executor` 线程数 = 闸门 + 2 冗余吸收僵尸线程；取消运行中任务时 `_release_task_slot()` 主动归还槽位（`slot_released` 防双减）；`_task_timeout_watchdog` 每 5 分钟检查，运行超 `MAX_TASK_DURATION_HOURS`（3h，app.py 常量）自动取消并释放槽位；取消/看门狗触发时 dump 卡死 worker 调用栈（`_dump_worker_stack`）。
**理由/事故**：标志位取消是协作式的，worker 卡在不可中断阻塞调用（无超时网络 recv）时永远不到检查点 → 闸门计数和 pool 线程永久泄漏 → 并发通道 3→2 不可恢复（136 实证，卡死任务不持 `_cmove_lock`，否则全站冻结）。

### D18. `update_status` 终态自动补设 `end_time`（2026-08-31, commit d67c171）
**理由/事故**：`process_single_task` 的 "No result returned" 失败分支漏设 `end_time` → `get_duration()` 退化为 now-start_time → 监控页耗时无限增长（实际 2.4 分钟的任务显示 11+ 分钟）。

### D19. eventlet 三大纪律
① `Thread.join(timeout)` 在 monkey-patch 下抛 `BaseException`（eventlet.Timeout），统一走 `_safe_thread_join()`，任务处理兜底 catch `BaseException`；② CPU 密集/阻塞磁盘 I/O 必须走 `run_cpu_bound()`（tpool → 真实线程，可重入），否则饿死事件循环（py-spy 实证 python-libs 转换曾在 hub 线程冻结全站 7-10s/次；GB 级 ZIP 打包/缓存写盘同理）；③ tpool 真实线程内禁止碰 green 锁/队列（lost wakeup → `greenlet.error` → C-MOVE 永不返回 → 全站假死），logging 锁已由 `fix_logging_locks_for_eventlet()` 替换为真实锁。

### D20. 单实例锁（`logs/app.lock`，posix flock / windows msvcrt）
eventlet 下两进程可同时 LISTEN 同一端口（SO_REUSEPORT 语义），任务状态与 C-STORE SCP 被撕裂 → 僵尸任务（2026-08-27 实测）。**重启纪律**：先确认旧进程已死再启动；远程重启用 `kill <pid>` 精确杀（`pkill -f` 会匹配到 ssh 远程 shell 自己的命令行导致会话自杀）。

## 可观测性

### D21. 提交审计日志（2026-08-31）
`/api/process/single` 和 `/api/process/batch` 以 INFO 记录来源 IP + options 全文；任务级关键词覆盖打 WARNING 并打印完整列表。
**理由**：D7 事故在 DEBUG 级日志下无法溯源。

### D22. Windows 崩溃监控日志（2026-07-07, commit aea551b）
Windows 下存在运行一段时间后自动崩溃的未定位问题，已增强日志记录便于事后定位。根本原因仍未确诊——见 KNOWN_ISSUES。
