# -*- coding: utf-8 -*-
"""
核心常量定义模块

集中管理跨模块共享的常量，避免重复定义。
"""

# 衍生序列关键词列表（默认值）
# 用于在 PACS 查询阶段和整理阶段过滤 MPR/MIP/3D 重建等衍生序列
DEFAULT_DERIVED_SERIES_KEYWORDS = [
    'MPR', 'MIP', 'MINIP', 'SSD', 'VRT', 'VR',
    'CPR', 'CURVED', '3D', 'THICK',
    'SCOUT', 'TOPOGRAM', 'SURVEY',
    'REF', 'REFERENCE', 'LOC', 'BATCH',
    'AVERAGE', 'SUM', 'REFORMAT',
    'PROJECTION', 'RAYSUM', 'KEY', 'ROI','DOSE',
    'TRACKER'
]

# 运行时可修改的过滤关键词（模块级可变状态）
# 通过 get_derived_keywords() / set_derived_keywords() 访问
_runtime_derived_keywords = list(DEFAULT_DERIVED_SERIES_KEYWORDS)


def get_derived_keywords():
    """获取当前生效的衍生序列过滤关键词列表。"""
    return _runtime_derived_keywords


def set_derived_keywords(keywords):
    """设置衍生序列过滤关键词列表（立即生效）。

    Args:
        keywords: 关键词字符串列表
    """
    global _runtime_derived_keywords
    validated = []
    for k in keywords:
        if isinstance(k, str) and k.strip():
            validated.append(k.strip().upper())
    # 去重并保持顺序
    seen = set()
    unique = []
    for k in validated:
        if k not in seen:
            seen.add(k)
            unique.append(k)
    _runtime_derived_keywords = unique


def reset_derived_keywords():
    """重置过滤关键词为系统默认值。"""
    global _runtime_derived_keywords
    _runtime_derived_keywords = list(DEFAULT_DERIVED_SERIES_KEYWORDS)


# 向后兼容：DERIVED_SERIES_KEYWORDS 指向当前运行时列表
DERIVED_SERIES_KEYWORDS = _runtime_derived_keywords


# ========== 模态同义词映射（解决 PACS 命名不规范问题）==========
#
# PACS 中同一模态可能存在多种写法，例如 2D X-ray 可能标记为
# DR、DX、CR、XR 等。建立同义词组后，当用户指定一个标准模态
# 时，系统会自动展开为组内所有别名进行匹配。
#
# 用法示例：
#   用户输入 modality_filter="DR" → 实际匹配 {DR, DX, CR, XR}
#   用户输入 modality_filter="CT" → 实际匹配 {CT, PETCT, CTPET}
#
MODALITY_SYNONYMS = {
    'DR': {'DR', 'DX', 'CR', 'XR', 'RF'},           # 2D X-ray / 普放
    'CT': {'CT', 'PETCT', 'CTPET', 'PET-CT'},       # CT / PET-CT
    'MR': {'MR', 'MRI', 'MRPT'},                    # 磁共振
    'MG': {'MG', 'MAMMO', 'MAMMOGRAPHY', 'BCT'},    # 乳腺钼靶
    'PT': {'PT', 'PET', 'PETCT', 'CTPET'},          # PET
    'NM': {'NM', 'NUC', 'NUCLEAR'},                 # 核医学
    'US': {'US', 'ULT', 'ULTRASOUND'},              # 超声
    'XR': {'XR', 'XRAY', 'DX', 'CR', 'DR', 'RF'},   # X-ray 通用别名
}


def expand_modality_filter(modality_filter: str) -> list:
    """展开模态过滤字符串，自动包含同义词组内的所有别名。

    例如输入 "MR,DR" 会返回 ['MR','MRI','MRPT','DR','DX','CR','XR','RF']。
    输入 "DX" 也会自动展开为 DR 同义词组（因为 DX 是 DR 组的成员）。
    如果某个输入模态不在任何同义词组中，则保留原值进行精确匹配。

    Args:
        modality_filter: 逗号分隔的模态字符串，如 "MR,CT"

    Returns:
        去重后的大写模态列表
    """
    if not modality_filter:
        return []
    raw = [m.strip().upper() for m in modality_filter.split(',') if m.strip()]
    expanded = set()
    for m in raw:
        # 1) 直接命中同义词组的键
        if m in MODALITY_SYNONYMS:
            expanded.update(MODALITY_SYNONYMS[m])
            continue
        # 2) 命中某个同义词组的成员（如输入 DX → 展开为 DR 组）
        found_group = False
        for group_name, synonyms in MODALITY_SYNONYMS.items():
            if m in synonyms:
                expanded.update(synonyms)
                found_group = True
                break
        if not found_group:
            # 3) 不在任何已知组中，保留原值精确匹配
            expanded.add(m)
    return sorted(list(expanded))
