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
**补充（2026-09-18）**：白名单新增 `SUB`（乳腺 DCE/血管减影，GE `SUBTRACT`、Siemens `_SUB` 命名）。**陷阱**：pydicom 的 `MultiValue` 不是 `list/tuple` 子类（是 `ConstrainedList`），`isinstance(image_type, (list, tuple))` 为 False 会落入 str 分支，白名单只查 SeriesDescription——ImageType 其余值（SUBTRACT/SPINE）形同虚设，此前 SPINE 案例只是侥幸靠描述里的 "LongSpine" 命中。`is_derived_series` 与 `organize._is_derived_series` 入口已统一把 MultiValue 转 list。实证：GE SIGNA Voyager 乳腺减影 `DERIVED\SECONDARY\OTHER\SUBTRACT` 被误杀（/mnt/h/dicom/breat/）。

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

**水位一致性（2026-10-24，K17）**：`DOWNLOAD_LOW_WATERMARK_GB`（暂停）、`DOWNLOAD_HIGH_WATERMARK_GB`（恢复）与 `CLEANUP_MIN_FREE_GB`（清理触发/目标）必须作为一组审查；若清理不会在暂停线附近触发，或清理后的空闲量达不到恢复线，C-MOVE 会永久等待且持锁阻塞全部任务。193 的 130/160 与清理 60 即此配置事故，已按用户选择改为 100/120 且不删除文件。`_wait_for_disk_low()` 等待时每 5 分钟重复记录当前空闲量和恢复线，禁止只记录一次后静默数小时。

**193 缓存扩容（2026-10-24）**：用户确认该机不再部署其他大型软件，`CACHE_MAX_GB=180` 利用剩余空间；配套下载暂停/恢复为 40/60，`CLEANUP_MIN_FREE_GB=50`（清理目标 60），保留 60GB 恢复缓冲并避免缓存扩张再次形成 K17 死锁。不得只提高 CACHE_MAX_GB 而不同步审查水位；`results/cache/` 仍只由 LRU 自管理。

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
① `Thread.join(timeout)` 在 monkey-patch 下抛 `BaseException`（eventlet.Timeout），统一走 `_safe_thread_join()`，任务处理兜底 catch `BaseException`；② CPU 密集/阻塞磁盘 I/O 必须走 `run_cpu_bound()`（tpool → 真实线程，可重入），否则饿死事件循环（py-spy 实证 python-libs 转换曾在 hub 线程冻结全站 7-10s/次；GB 级 ZIP 打包/缓存写盘同理）；③ tpool 真实线程内禁止碰 green 锁/队列（lost wakeup → `greenlet.error` → C-MOVE 永不返回 → 全站假死），logging 由 `fix_logging_locks_for_eventlet()` 双重加固：锁替换为真实锁，且 `logging.threading` 重绑原生 threading 模块——py3.13 `LogRecord.__init__` 的 `threading.current_thread()` 在 green 版下会经 `patch_function` 抢 CPython 导入锁，被挂起 greenlet 持锁即永久死锁（2026-09-20 全站假死 24h 实证）。

### D20. 单实例锁（`logs/app.lock`，posix flock / windows msvcrt）
eventlet 下两进程可同时 LISTEN 同一端口（SO_REUSEPORT 语义），任务状态与 C-STORE SCP 被撕裂 → 僵尸任务（2026-08-27 实测）。**重启纪律**：先确认旧进程已死再启动；远程重启用 `kill <pid>` 精确杀（`pkill -f` 会匹配到 ssh 远程 shell 自己的命令行导致会话自杀）。

### D24. 任务级部分成功 + 混合尺寸容错 + SVD 崩溃安全化（2026-10-02，用户决策）
单序列转换失败不再株连整个任务：只要有序列成功转换，任务部分成功，失败清单记入 `results['conversion_errors']` 并在任务日志显性提示；全部失败仍判任务失败。**修订 D23 的序列级传播**（D23 的序列内逐张完整性约束不变）。
**理由/事故**：136 一天 20 次任务失败中，多数是打印排版（printingSeries）/数坤 AI 报告（shukunlist）等拼装序列毁掉了整个检查——这些任务由客户端以 `exclude_derived: False` 主动提交，过滤不是答案。
配套：① python-libs 回退路径对混合 shape 序列保留多数派尺寸切片、丢弃少数派并记录数量（原 `np.stack` 整体崩溃）；② 所有裸 `nib.as_closest_canonical` 改为 `_safe_as_closest_canonical`——affine 含 NaN/inf（几何头损坏）时 nibabel `io_orientation` 的 SVD 直接崩溃（"SVD did not converge"），安全版退化为可分解 affine 不抛异常，CT 序列随后仍由几何验证门禁兜底。

**2026-10-18 修订（用户决策，隐私）**：失败序列的原始 DICOM **不再随 zip 交付**——DICOM 含完整患者头信息，随结果包下发有隐私泄露风险。`_accept_series_results` 汇总失败清单后调用 `_drop_failed_series_files` 删除失败序列目录中的全部数据文件（原始 DICOM + 转换残留的部分 nii/npz），仅保留 `dicom_metadata_cache.json` 供 Excel 记录失败信息；任务失败（全部序列失败）时同样清理。旧口径"kept as raw DICOM"（PR #1 统一进 `_accept_series_results`）作废，测试契约同步反转（`test_d24_delivery.py: assert_zip` 断言失败序列的 dcm 不在 zip 中）。

### D23. 转换完整性与原生子进程边界（2026-09-13）
逐张转换仅在全部输入成功后删除原始 DICOM；部分失败保留整组输入以支持完整回退，最终失败传递到工作流。理由：旧代码在 1/2 成功时删除失败原件且报告成功，合成 DX 故障注入已复现（K10）。

dcm2niix 的原生串行锁、创建进程、等待和清理均位于真实线程；eventlet hub 只派发任务。原生 subprocess 的 threading/time 引用也必须为原生模块。输出使用临时文件而非 PIPE，Windows 超时先终止进程树再回收父进程。理由：Windows 管道读取在绿化线程下可阻塞整个 hub，导致超时失效（K11）；禁止把包含 green 锁/队列的整个转换流程直接卸载（D19）。

## 可观测性

### D21. 提交审计日志（2026-08-31）
`/api/process/single` 和 `/api/process/batch` 以 INFO 记录来源 IP + options 全文；任务级关键词覆盖打 WARNING 并打印完整列表。
**理由**：D7 事故在 DEBUG 级日志下无法溯源。

### D22. Windows 崩溃监控日志（2026-07-07, commit aea551b）
Windows 下存在运行一段时间后自动崩溃的未定位问题，已增强日志记录便于事后定位。根本原因仍未确诊——见 KNOWN_ISSUES。
### D24. CT转换源像素几何未验证时禁止放行（2026-10-02）

多层CT转换必须取得源DICOM几何与像素布局证据；skipped/not_verifiable返回失败并保留原件，不得用文件存在清除处理异常。保留所有校验结果和策略版本，NIfTI缓存指纹纳入几何版本；FFS不作为翻转规则。affine修复必须保持物理像素值，不引入整数header自动量化。源DICOM几何一致不自动证明解剖方向真值；源标签自身错误仍需独立图像/人工证据。依据：CTA017源DICOM与旧NIfTI Z符号冲突及故障注入复现，见K13。本次只完成本地代码与验证，未部署生产服务。

## D23：长CT最终坐标必须独立验证，不由转换器成功或版本横幅担保（2026-10-02）

CTA017真实完整DICOM及1,024/1,025层边界实验确认dcm2niix超1,024层提前返回可跳过方向及q/s更新；加/删同一guard可切换故障。转换器returncode=0、q/s相等及FFS均不能作为坐标正确证据。最终CT出口以完整源几何和源像素布局重建实际步进；未验证失败且保留原件，源头子集/身份混合/有效q-s冲突不得默许，缓存包含几何策略版本。对长体积按物理角点检查累积误差，不只比较局部置换容差；shear无法由qform精确表达时仅启用源证明的sform。生产部署与源修复须检查运行任务、保留备份和逐项检查点。本地46测试及637,534,208体素真实重放通过；生产/Windows复核不由本地结果代替。见KNOWN_ISSUES K13及CT_YOLO事件报告。

### D25. QC/预览先消除整卷副本，再以全局重处理闸门限制叠加峰值（2026-10-24）

193 多服务共存时可用内存仅约 2.75–4.8 GiB、swap 已满；服务未见 OOM 或持续泄漏，但旧 QC 的 `get_fdata()`、float32 转换、全量有限值掩码与索引复制会在抽样前叠加整卷副本，三视图也会为三张切片 canonical 整卷。决定：
- QC 用 `get_unscaled()` 保留原始 dtype；整数直接抽样，浮点仍先过滤 NaN/Inf 再抽样以保持旧口径；slope/intercept 只应用到样本和分割检测中间层。
- NIfTI 三视图恒等方向直接按 `dataobj` 取切片；非恒等方向用 `_LazyReorientedData` 把 canonical 索引映射回源数据，按 nibabel 的源轴翻转 + `argsort` 转置语义仅重排小切片，禁止用整卷 `as_closest_canonical()` 充当低内存方案。
- `heavy_processing_slot()` 在串行整理、单序列转换和元数据提取的外层工作流边界限制全局重处理并发（默认 2）；可用内存低于 `MEMORY_MIN_AVAILABLE_GB`（默认 2 GiB）时暂缓附加工作，但始终允许唯一工作防饿死。不得在 `run_cpu_bound()` 真实线程内取闸；PACS 接收不等待，序列队列继续背压。

验证：166 passed / 2 skipped；48 种空间方向的 shape/affine/zooms/三视图值与 nibabel 全部一致；512³、非单位仿射合成 NIfTI 的 QC p98 不变，峰值 2326→663 MiB，预览切片值不变，峰值 920→168 MiB。193 于 2026-10-24 完成源码同步、生产解释器 145 项回归并重启（PID 2002530），HTTP 200、PACS connected；136 未同步。

### D26. X-ray 只交付源像素可信的图像；空白、不可信输出和残留源文件不得入包（2026-10-24）

136 旧版本出现全黑/全白/XY 颠倒报告后，从 193 当前版本重下 Z21050200397、Z24031801632、Z21051000117、Z21052001013，四个目标序列的预览和主 NIfTI 均为正常直立胸片，证明旧问题在当前代码未复现。质量规则明确为：
- 仅当源像素真正恒定、全为 NaN/Inf 时判空白并放弃；窗口错误造成的全黑/全白预览不等于空白源图，禁止据此删除有效诊断图像。合法小 RescaleSlope 不得用绝对动态范围阈值误杀。
- dcm2niix 的 2D 归一化若非 `normalized/inverted_normalized/ok`，该输出不计成功并触发 Python 源像素回退；最终失败按 D24 不交付。
- 成功路径也必须递归清理源 DICOM（任意后缀/无后缀及 temp_* 副本）；NPZ 交付前清理所有中间 NIfTI，包括曾被拒绝的输出。任何删除失败均 fail closed，不能打包。
- `_drop_failed_series_files` 同步递归并 fail closed，避免“只删顶层、失败仍打包”的隐私与质量缺口。

回归覆盖空白、小 slope、不可信归一化、锁定拒绝输出、无后缀源文件、NPZ 回退与递归清理，全套 175 passed / 2 skipped，独立终审无 Critical/Important。193 已部署并重启（PID 3086791），生产解释器 154 项回归全过。

### D27. 客户端断连不影响服务端续跑；任务去重与缓存自愈（2026-10-25）

保持现有无状态客户端模型：服务端工作流不依赖轮询连接，完成后按 accession+options 保存缓存；客户端重跑时，运行中的同工作负载复用 task_id，完成后命中缓存，ZIP 下载中断不回滚缓存。`process_single` 在 `_task_lock` 临界区内原子查找并登记，pending/running 且未 `slot_released` 才复用，响应 `status='existing'`、`reused=True`，无需客户端协议修改或持久化数据库。

缓存淘汰同时约束 `CACHE_MAX_GB` 和 `CACHE_MIN_FREE_GB`（默认跟随下载恢复线）：任一越线均按 atime 淘汰，直到容量与真实空闲均达标。删除后必须重新 `disk_usage` 采样，硬链接不能按逻辑大小虚增空闲量。save/copy 与 LRU 按 cache key 互斥，冲突时回退 PACS 或下轮重试；互斥锁在 eventlet 下必须来自 `original('threading')`，只允许微秒级状态操作。普通结果清理的 `finally` 和服务启动都会异步调度缓存维护，避免“没有普通可删项”阻断恢复。

options 指纹补入 `auto_metadata/auto_organize/rename_mr_series/extra_derived_keywords`，防止不同交付语义错误合并；该变更使旧缓存键失配一次。回归覆盖并发重连、options 差异、容量/空闲双阈值、硬链接、活跃 IO 与 eventlet 原生锁竞争，全套 193 passed / 3 skipped。


### D28. MR 属性证据与交付分类分离（2026-10-31）

依据 main `062ff9e` 的源码及 `test/test_mr_semantics.py` 合成反例，移除 T1+对比剂字段强制增强、重复采集自动 PRE/POST、Dixon 全输出压脂等路径。保留旧分类/命名接口，新增 MR_Cleaned_v2 逐属性证据、未知/冲突状态与监督掩码；历史 `_FAT` 必须从原始证据重新判定，不批量迁移。多帧/多回波/多 b 值 header 索引通过 MR_FRAME_INDEX_ENABLED 显式启用，默认不增加生产全量头部 I/O；候选体积与转换输出的映射保留 UNVERIFIED。结果缓存包含语义版本/配置/索引开关。理由：交付便利与可用训练真值是不同契约，缺失或重复采集不能自动成为阴性或增强证据。边界与真实队列后续验证见 [MR_SEMANTICS_V2.md](MR_SEMANTICS_V2.md)。此次没有部署生产。
