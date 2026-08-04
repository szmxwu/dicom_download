# MR_Clean 更新说明（2026-07-15）

本文档说明对 `dicom_process/src/core/mr_clean.py` 的修复和完善，供引用同一资源的其他项目参考。

## 修改背景

MR_Clean 是 MRI 序列清洗与分类模块，输出 `sequenceClass`、`standardOrientation`、`isFatSuppressed` 等标准化字段。本次修改解决以下问题：

1. **UNKNOWN 比例过高**：旧版本有 2.1% 的序列被标记为 UNKNOWN，其中大部分可通过规则修复
2. **DWI 误分类**：当 `b_value` 缺失且 `ScanningSequence` 含 `SE/RM` 时，DWI 被错误分类为 T2_TSE/T2_SE
3. **MRCP 识别缺失**：不支持 MRCP（磁共振胰胆管成像）序列识别
4. **空元数据未过滤**：ExamCard、空 SeriesDescription 等应排除的行被标记为 UNKNOWN 而非 EXCLUDED
5. **sequenceFamily 字段缺失**：新计划需要按序列族（T2_anatomic、DWI 等）进行分层分析

## 修改内容

### 1. 配置文件 `mr_clean_config.json`

#### 1.1 新增 `excluded_patterns` 配置

```json
"excluded_patterns": {
    "exam_card_keywords": ["examcard", "exam_card"],
    "empty_series_description": true,
    "derived_screen_save_keywords": ["screen save", "processed images", "filming", "inline_vf_results"],
    "unknown_protocol_names": ["____"]
}
```

用于过滤空元数据、ExamCard 和派生图。

#### 1.2 新增 `dwi_name_keywords` 配置

```json
"dwi_name_keywords": ["dwi", "dwibs", "diff", "trace", "resolve_diff", "dwiblack"]
```

用于在 `b_value` 缺失时通过关键词识别 DWI 序列。

#### 1.3 新增 MRCP 规则

```json
"MRCP": {"protocol_keywords": ["mrcp", "cholangio", "biliary"]}
```

在 `classification.ruleA` 中添加 MRCP 识别规则。

#### 1.4 新增 `sequence_family_map` 映射表

```json
"sequence_family_map": {
    "T2_TSE": "T2_anatomic",
    "T2_STIR": "T2_fat_suppressed",
    "T1_TSE": "T1_anatomic",
    "DWI": "DWI",
    "ADC": "ADC",
    "MRCP": "MRCP",
    ...
}
```

将 `sequenceClass` 映射为更高层的 `sequenceFamily`，用于新计划的序列族分层。

### 2. 代码文件 `mr_clean.py`

#### 2.1 新增 `_is_excluded_row()` 函数

```python
def _is_excluded_row(row, cfg: dict) -> bool:
```

检查一行是否应被排除（空元数据、ExamCard、派生图等）。被排除的行标记为 `EXCLUDED` 而非 `UNKNOWN`。

#### 2.2 新增 `_get_sequence_family()` 函数

```python
def _get_sequence_family(sequence_class: str, cfg: dict) -> str:
```

根据 `sequenceClass` 和配置中的映射表返回 `sequenceFamily`。

#### 2.3 修改 `classify_sequence()` - DWI 关键词检查

在规则 B 的形态学判断**之前**增加 DWI 关键词检查：

```python
# 物理规则0: DWI关键词检查（优先于b_value检查，因为b_value可能缺失）
dwi_name_keywords = [str(x).lower() for x in classification_cfg.get('dwi_name_keywords', [...])]
if any(k in combined_name for k in dwi_name_keywords):
    base_class = 'DWI'
```

**原因**：部分扫描仪（如 GE Propeller）的 DWI 序列 `ScanningSequence` 为 `RM` 或 `SE`，且 DICOM 中可能缺少 `b_value`。旧版本会将其误判为 T2_TSE。

#### 2.4 修改 `classify_sequence()` - MRCP 识别

在规则 A 中添加 MRCP 检查：

```python
elif _check_keywords('MRCP', ['mrcp', 'cholangio', 'biliary']):
    base_class = 'MRCP'
```

#### 2.5 修改 `process_mri_dataframe()` - 新增 Stage 0 和 Stage 3c

- **Stage 0**：调用 `_is_excluded_row()` 过滤空元数据
- **Stage 3c**：调用 `_get_sequence_family()` 计算 `sequenceFamily`

### 3. 输出字段变更

| 字段 | 变更 |
|------|------|
| `sequenceClass` | 新增 `EXCLUDED` 值（原为 `UNKNOWN`） |
| `sequenceFamily` | **新增字段**，取值见下表 |

#### sequenceFamily 取值

| sequenceFamily | 包含的 sequenceClass |
|----------------|---------------------|
| T2_anatomic | T2_TSE, T2_SE, T2_FLAIR, T2_SE_SingleShot, T2_TSE_MC, T2_MAP, ... |
| T2_fat_suppressed | T2_STIR, T2_TSE_FAT |
| T1_anatomic | T1_TSE, T1_SE, T1_GRE, T1_GRE_SPOILED, T1_GRE_STEADY_STATE, T1_MAP, ... |
| T1_fat_suppressed | T1_TSE_FAT, T1_GRE_STEADY_STATE_FAT |
| T1_in_phase | T1_TSE_INPHASE, T1_GRE_STEADY_STATE_INPHASE |
| T1_out_phase | T1_TSE_OUTPHASE, T1_GRE_STEADY_STATE_OUTPHASE |
| PD | PD_TSE, PD_SE, PD_GRE, PD_TSE_FAT, PD_TSE_WATER |
| DWI | DWI, DTI |
| ADC | ADC |
| MRA | MRA |
| SWI | SWI |
| PWI | PWI |
| MRS | MRS |
| MRCP | MRCP |
| fMRI | fMRI_BOLD |
| LOCALIZER | LOCALIZER |
| DERIVED | DERIVED, SUBTRACTION, MIP, FA_MAP |
| OTHER | BREATH MOVEMENT |
| UNKNOWN | UNKNOWN, EXCLUDED |

## 兼容性说明

- **向后兼容**：`classify_sequence()` 函数签名不变，仍返回 `sequenceClass` 字符串
- **新增字段**：`sequenceFamily` 仅在 `process_mri_dataframe()` 的输出中添加
- **EXCLUDED 替代 UNKNOWN**：空元数据行从 `UNKNOWN` 改为 `EXCLUDED`，统计时需注意

## 使用方式

### 运行 mr_clean 更新 MR_Cleaned sheet

```bash
# 全量更新（8 workers，约3分钟）
PYTHONPATH=src python3 scripts/update_mr_cleaned.py --workers 8

# 测试模式（不保存）
PYTHONPATH=src python3 scripts/update_mr_cleaned.py --dry-run --limit 10

# 限制处理数量
PYTHONPATH=src python3 scripts/update_mr_cleaned.py --workers 8 --limit 100
```

### 更新 series_context_index.parquet

```bash
# 更新 parquet 文件，添加 sequence_family 列
PYTHONPATH=src python3 scripts/update_series_context.py

# 测试模式
PYTHONPATH=src python3 scripts/update_series_context.py --dry-run
```

## 已知问题

1. **49 个文件更新失败**：错误为 `can only concatenate str (not "float") to str`，原因是部分 DICOM 元数据字段为 NaN（float 类型），与字符串拼接时出错。这些文件的 MR_Cleaned sheet 未更新。

2. **旧版本 UNKNOWN 残留**：如果仅更新 `series_context_index.parquet` 而不重新运行 `update_mr_cleaned.py`，则 `sequence_class=UNKNOWN` 的序列仍为 UNKNOWN（约 1601 条）。

## 依赖文件

- `dicom_process/mr_clean_config.json` - 配置文件
- `dicom_process/src/core/mr_clean.py` - 核心代码
- `scripts/update_mr_cleaned.py` - 批量更新脚本（新建）
- `scripts/update_series_context.py` - parquet 更新脚本（新建）
