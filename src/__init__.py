# -*- coding: utf-8 -*-
"""
DICOM 下载与处理系统

本模块提供 DICOM 文件的下载、组织、转换、元数据提取等功能。
支持 PACS 查询下载和本地文件上传两种工作流程。
"""

# 若安装了 eventlet，则尽早 monkey_patch，避免 Web 模式下 RLock 未绿化警告。
# 对未安装 eventlet 的环境（如 Windows 本地开发）自动降级，不影响 CLI。
try:
    import eventlet
    eventlet.monkey_patch()
except ImportError:
    pass

__version__ = "1.0.0"
__author__ = "DICOM Team"

# 导出主要类
from src.models import ClientConfig, SeriesInfo, WorkflowResult
from src.client.unified import DICOMDownloadClient

__all__ = [
    "ClientConfig",
    "SeriesInfo",
    "WorkflowResult",
    "DICOMDownloadClient",
]
