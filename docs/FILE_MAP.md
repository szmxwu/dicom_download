# FILE_MAP.md — 目录职责与高风险区域

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
| `preview.py` 窗口/极性逻辑 | 厂商标签说谎（Mindray），启发式误判 | D13, D14, X10–X13 |
| `qc.py` 方位/灰度判定 | 盲改会产生静默数据破坏 | D11, D12, X8, X9, X14 |
| `cache.py` / 磁盘清理 | 38GB 缓存误删、新任务 ZIP 被清 | D10, X5, X6 |

## 运维要点速查

- 生产 193：`yusi@192.0.0.193:~/work2/dicom_download`，conda `/home/yusi/anaconda3`，rsync 同步 + 精确 kill 重启（D20）
- 生产 136：Windows 单机，只能用户手动更新，curl 需 `--noproxy '*'`
- 排障：193 上 `~/py-spy`（sudo）dump 全线程栈定位冻结点
- 日志：`logs/app.log`（INFO 级；提交审计含 IP+options）
