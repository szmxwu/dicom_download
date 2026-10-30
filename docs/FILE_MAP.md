# FILE_MAP.md — 目录职责与高风险区域

## 2026-10-23当前：v4已部署193生产

生产当前为`wmx@192.0.0.193:/home/wmx/work/dicom_download_prod`，解释器`/opt/ia-runtime/bin/python3.12`，5005；研究双3090迁188。旧下方yusi／work2路径及“尚未部署v3”是历史。**v4完整CT源集合补丁已于2026-10-23部署193并重启**（PID 2223784，队列空闲窗口，v3备份在`backups/pre_v4_20261023_convert_constants.tar.gz`；部署后生产解释器核心回归131通过／0失败／0跳过含eventlet专项），详见[K13续](KNOWN_ISSUES.md)与[v4交接](CT_GEOMETRY_V4_COMPLETENESS_HANDOFF_20261023.md)。**136仍待手动更新**。

## 源码布局

```
src/
├── __init__.py              # 导出 ClientConfig, SeriesInfo, WorkflowResult, DICOMDownloadClient
├── models.py                # 数据类
├── client/unified.py        # DICOMDownloadClient 主编排器（PACS 操作/工作流/线程管理）
├── core/
│   ├── organize.py          # 按序列整理 DICOM（含自己的 _is_derived_series 副本，见 DECISIONS D1）
│   ├── convert.py           # DICOM → NIfTI/NPZ（dcm2niix + python 回退；转换时方位/灰度探针修正）
│   ├── metadata.py          # DICOM tag 提取 → Excel
│   ├── qc.py                # 图像质量控制（模态阈值；方位/灰度启发式已退役，见 D11/D12）
│   ├── fix_nifti.py         # NIfTI 方位/灰度自动修正
│   ├── preview.py           # PNG 预览（保持长宽比；窗口不信任校验，见 D13/D14）
│   ├── mr_clean.py          # MR 序列分类（规则在 mr_clean_config.json）
│   └── constants.py         # 过滤关键词/例外表/白名单/非图像模态/模态同义词/规则指纹
├── web/app.py               # Flask + SocketIO：任务队列（闸门3并发+冗余线程+3h看门狗）、REST API、清理守护
├── cli/download.py          # HTTP API 批量客户端（4 级流水线，串行模式重叠提交与本地下载）
├── cli/download_batch.py    # 批量客户端（轮询看到终态即释放 inflight 槽）
├── cli/copy_files.py        # 多线程 PNG 拷贝（mask 过滤）
└── utils/
    ├── offload.py           # run_cpu_bound（eventlet tpool 卸载）+ fix_logging_locks_for_eventlet
    ├── memory.py            # 跨任务转换/预览/QC 重内存闸门（green 工作流边界，D25）
    ├── packaging.py         # 结果 ZIP 打包
    └── cache.py             # PACS 结果磁盘缓存（LRU，自管理，清理守护不得触碰）
```

## 配置文件

- `.env` — PACS 连接、QC 阈值、磁盘水位、HTTPS（gitignored，含凭据）
- `dicom_tags/{ct,dx,mg,mr}.json` — 各模态元数据 tag 列表
- `mr_clean_config.json` — MR 分类规则（改规则改这里，不改代码）
- `keywords.json` — 默认 tag 列表（当前同 mr.json）

## 高风险改动区域（改动前必读对应 DECISIONS 条目）

| 文件/区域 | 风险 | 必读 |
|-----------|------|------|
| `constants.py` 过滤规则 | 误杀诊断序列（iDose/全脊柱/Mindray DERIVED 事故） | D1–D7, X1–X4 |
| `unified.py` C-STORE 落盘 | preamble 缺失 → 整序列慢速回退；失败码 → PACS 中止 C-MOVE | D2, D9, X3 |
| `unified.py` 任何 `join(timeout)` / CPU 密集段 | eventlet BaseException / hub 冻结 | D19 |
| `app.py` 任务队列 | 槽位泄漏、end_time 遗漏、双实例 | D17, D18, D20 |
| `preview.py` 窗口/极性/懒重定向 | 厂商标签说谎；轴交换+翻转时读错切片；整卷 canonical 内存峰值 | D13, D14, D25, X10–X13, X19 |
| `qc.py` 方位/灰度/抽样 | 盲改会产生静默数据破坏；NaN 抽样顺序和缩放影响 QC 口径 | D11, D12, D25, X8, X9, X14, X19 |
| `memory.py` / 重处理边界 | green 锁跨真实线程、槽位泄漏、低内存叠加或饿死 | D19, D25 |
| `cache.py` / 磁盘清理 | 38GB 缓存误删、新任务 ZIP 被清 | D10, X5, X6 |

## 运维要点速查

- 2026-10-02方向事件补丁：已复现超过1,024层的转换器提前返回；本地`convert.py`、`unified.py`、`metadata.py`与v3缓存几何版本及完整真实17导出重放已验证，本地46通过/2环境专项跳过、193独立运行环境48通过/0跳过，**136/193生产尚未部署**。见KNOWN_ISSUES K13；不要把本地修复误报为生产已生效。

- 生产 193：`yusi@192.0.0.193:~/work2/dicom_download`，conda `/home/yusi/anaconda3`，rsync 同步 + 精确 kill 重启（D20）
- 生产 136：Windows 单机，只能用户手动更新，curl 需 `--noproxy '*'`
- 排障：193 上 `~/py-spy`（sudo）dump 全线程栈定位冻结点
- 日志：`logs/app.log`（INFO 级；提交审计含 IP+options）

- 2026-10-02：生产几何v3剩余验收已移交[具体交接清单](CTA_GEOMETRY_V3_PRODUCTION_ACCEPTANCE_HANDOFF_20261002.md)，下载空闲时实施；不阻塞CT_YOLO存量数据清理与主线。


## MR 语义扩展（2026-10-31）

- `src/core/mr_semantics.py`：统一多值解析、逐属性证据/冲突、规则版本与监督掩码。
- `src/core/mr_index.py`：增强 MR 功能组来源解析、可选 header 帧索引及 Excel 摘要；不声称转换体积已对齐。
- `test/test_mr_semantics.py`：语义反例、标准/增强 DICOM、缓存与 Excel 集成回归。
- `docs/MR_SEMANTICS_V2.md`：输出契约、默认开关、兼容性与未完成的实证验证。
