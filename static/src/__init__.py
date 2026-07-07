# -*- coding: utf-8 -*-
"""
DICOM 下载与处理系统

本模块提供 DICOM 文件的下载、组织、转换、元数据提取等功能。
支持 PACS 查询下载和本地文件上传两种工作流程。
"""

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
