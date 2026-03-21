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
