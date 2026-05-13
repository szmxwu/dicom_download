# 解决方案：MR 3D 原始采集序列被误判为衍生序列的过滤问题

## 问题现象

乳腺 MR 检查中的 `Ax 3D VIBRANT Mph+C` 序列（T1WI 增强，原始采集）在 `dicom_download` 的 `organize_dicom_files` 阶段被错误过滤，日志输出：

```
🚫 Filtered derived series (organize stage): 'Ax 3D VIBRANT Mph+C' (T1_FS)
Series count: 0
```

这导致该序列的 DICOM 文件**未进行 NIfTI 转换**，后续质量评估无法获取评分。

---

## 根因分析

### 1. `3D` 关键词匹配了正常的 3D 采集序列

`dicom_download/src/core/constants.py` 中的衍生序列关键词列表包含 `3D`：

```python
DEFAULT_DERIVED_SERIES_KEYWORDS = [
    'MPR', 'MIP', 'MINIP', 'SSD', 'VRT', 'VR',
    'CPR', 'CURVED', '3D', 'THICK',   # ← '3D' 在此
    'SCOUT', 'TOPOGRAM', 'SURVEY',
    ...
]
```

序列描述 `Ax 3D VIBRANT Mph+C` 包含子串 `"3D"`，触发了匹配。

### 2. `_is_derived_series()` 逻辑缺陷

`dicom_download/src/core/organize.py` 中的判断逻辑：

```python
def _is_derived_series(series_desc: str, image_type=None) -> bool:
    if image_type:
        # 检查 ImageType[0] == 'DERIVED'
        if isinstance(image_type, (list, tuple)):
            first_val = str(image_type[0]).upper().strip() if image_type else ''
            if first_val == 'DERIVED':
                return True        # 确实是衍生，正确
        else:
            if 'DERIVED' in str(image_type).upper():
                return True
    
    # ⚠️ 问题：即使 ImageType[0] == 'ORIGINAL'，这里仍然执行！
    if series_desc:
        desc_upper = series_desc.upper()
        for keyword in get_derived_keywords():
            if keyword in desc_upper:
                return True        # 被 '3D' 关键词误伤
    return False
```

DICOM 标准中 `ImageType[0]` 的取值含义：
- `ORIGINAL` = 从原始采集数据生成（非衍生）
- `DERIVED` = 从其他图像计算生成（衍生，如 MPR、MIP、VRT）

该序列的实际 `ImageType`：
```python
['ORIGINAL', 'SECONDARY', 'OTHER']
```

`ImageType[0] == 'ORIGINAL'` 明确表明这是**原始采集**，但 `_is_derived_series()` 在确认 `ORIGINAL` 后，仍然继续执行 `SeriesDescription` 关键词匹配，导致被 `3D` 误伤。

---

## 修复方案

### 修改 `dicom_download/src/core/organize.py` 中的 `_is_derived_series()`

**核心原则**：`ImageType[0]` 是 DICOM 标准中判断像素来源的金标准，应优先于 `SeriesDescription` 关键词匹配。

```python
def _is_derived_series(series_desc: str, image_type=None) -> bool:
    """检查是否为衍生序列（MPR/MIP/3D重建等）。

    优先级：
      1. ImageType[0] == 'ORIGINAL' → 明确不是衍生，直接返回 False
      2. ImageType[0] == 'DERIVED'  → 明确是衍生，直接返回 True
      3. ImageType 缺失/不明         → 退回到 SeriesDescription 关键词匹配
    """
    if image_type:
        if isinstance(image_type, (list, tuple)):
            first_val = str(image_type[0]).upper().strip() if image_type else ''
            if first_val == 'ORIGINAL':
                # 明确为原始采集，SeriesDescription 中的关键词（如 3D）不能覆盖此判断
                return False
            if first_val == 'DERIVED':
                return True
        else:
            itype_upper = str(image_type).upper()
            if 'DERIVED' in itype_upper:
                return True
            if 'ORIGINAL' in itype_upper and 'DERIVED' not in itype_upper:
                return False

    # ImageType 缺失或无法明确判断时，退回到 SeriesDescription 关键词匹配
    if series_desc:
        desc_upper = series_desc.upper()
        for keyword in get_derived_keywords():
            if keyword in desc_upper:
                return True
    return False
```

### 修复要点

| 修改点 | 说明 |
|--------|------|
| `ImageType[0] == 'ORIGINAL'` → `return False` | 明确为原始采集时，不再检查 `SeriesDescription` |
| `ImageType[0] == 'DERIVED'` → `return True` | 明确为衍生时，直接过滤（原逻辑保留） |
| `ImageType` 缺失/不明 → 关键词回退 | PACS 数据不完整时的兜底策略 |

---

## 验证方法

### 1. 确认 DICOM 的 ImageType

```python
import pydicom
dcm = pydicom.dcmread("path/to/dcm", force=True, stop_before_pixels=True)
print(dcm.ImageType)        # ['ORIGINAL', 'SECONDARY', 'OTHER']
print(dcm.SeriesDescription) # 'Ax 3D VIBRANT Mph+C'
```

### 2. 单元测试修复后的函数

```python
from dicom_download.src.core.organize import _is_derived_series

# 原始采集 + 含 3D 的序列描述 → 应返回 False（不再被过滤）
assert _is_derived_series(
    "Ax 3D VIBRANT Mph+C",
    ['ORIGINAL', 'SECONDARY', 'OTHER']
) == False

# 衍生序列（ImageType 明确为 DERIVED）→ 应返回 True
assert _is_derived_series(
    "Axial MPR",
    ['DERIVED', 'SECONDARY']
) == True

# 无 ImageType 时，纯关键词匹配仍工作 → 应返回 True
assert _is_derived_series(
    "Axial MPR",
    None
) == True

# 无 ImageType + 无关键词 → 应返回 False
assert _is_derived_series(
    "Ax T1WI",
    None
) == False
```

### 3. 端到端验证

运行 `organize_dicom_files` 处理包含 3D 采集序列的 MR DICOM 目录，确认：
- 序列**不再被过滤**
- `Series count` 正常（非 0）
- dcm2niix 转换成功执行
- NIfTI 文件生成

---

## 额外建议（可选）

### 从 `DEFAULT_DERIVED_SERIES_KEYWORDS` 中移除 `3D`

`3D` 在 MR 模态中是极为常见的**采集参数**（如 `3D T1 MPRAGE`、`3D FLAIR`、`3D VIBE`），而不是衍生序列标识。真正的 3D 重建序列（如 `3D VRT`、`3D MPR`）其 `ImageType[0]` 通常为 `DERIVED`，会被 `ImageType` 检查正确过滤。

如果移除 `3D`：
```python
DEFAULT_DERIVED_SERIES_KEYWORDS = [
    'MPR', 'MIP', 'MINIP', 'SSD', 'VRT', 'VR',
    'CPR', 'CURVED', 'THICK',   # 移除 '3D'
    'SCOUT', 'TOPOGRAM', 'SURVEY',
    ...
]
```

**注意**： `_is_derived_series()` 的修复已经能正确处理 `3D` 关键词，因此移除 `3D` 是**锦上添花**，不是必需。但如果项目中有大量 MR 数据，建议同时移除 `3D`，避免 `ImageType` 缺失时的潜在误伤。

---

## 应用到另一个 dicom_download 项目

将上述修复同步到目标目录：

```bash
# 1. 备份原文件
cp /path/to/other/dicom_download/src/core/organize.py \
   /path/to/other/dicom_download/src/core/organize.py.bak

# 2. 替换 _is_derived_series 函数（手动或使用 patch）
# 参考上面的"修复方案"代码块

# 3. 清除 Python 字节码缓存，确保修改生效
find /path/to/other/dicom_download -name "*.pyc" -delete
find /path/to/other/dicom_download -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null

# 4. 验证
python -c "
import sys
sys.path.insert(0, '/path/to/other/dicom_download')
sys.path.insert(0, '/path/to/other/dicom_download/src')
from src.core.organize import _is_derived_series
print('ORIGINAL + 3D:', _is_derived_series('Ax 3D VIBRANT', ['ORIGINAL']))
print('DERIVED + MPR:', _is_derived_series('Axial MPR', ['DERIVED']))
"
```

---

## 相关文件

| 文件 | 修改内容 |
|------|----------|
| `dicom_download/src/core/organize.py` | `_is_derived_series()` 函数逻辑 |
| `dicom_download/src/core/constants.py` | （可选）`DEFAULT_DERIVED_SERIES_KEYWORDS` 移除 `3D` |

---

## 参考

- DICOM PS3.3 C.7.6.1.1.2 Image Type 定义：`ImageType[0]` = `ORIGINAL` | `DERIVED`
- 乳腺 MR 序列示例：`Ax 3D VIBRANT Mph+C`（GE Healthcare，3D 增强 T1WI，原始采集）
