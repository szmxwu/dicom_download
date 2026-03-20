# -*- coding: utf-8 -*-
"""
核心常量定义模块

集中管理跨模块共享的常量，避免重复定义。
"""

# 衍生序列关键词列表
# 用于在 PACS 查询阶段和整理阶段过滤 MPR/MIP/3D 重建等衍生序列
DERIVED_SERIES_KEYWORDS = [
    'MPR', 'MIP', 'MINIP', 'SSD', 'VRT', 'VR',
    'CPR', 'CURVED', '3D', 'THICK',
    'SCOUT', 'TOPOGRAM', 'SURVEY',
    'REF', 'REFERENCE', 'LOC', 'BATCH',
    'AVERAGE', 'SUM', 'REFORMAT',
    'PROJECTION', 'RAYSUM', 'KEY', 'ROI'
]
