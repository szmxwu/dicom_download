# -*- coding: utf-8 -*-
"""
DICOM 下载与处理系统

本模块提供 DICOM 文件的下载、组织、转换、元数据提取等功能。
支持 PACS 查询下载和本地文件上传两种工作流程。
"""

import sys

# Windows GBK 控制台无法编码 print/日志中的 emoji（✅❌ 等），默认 strict
# 错误处理会让任何一条 emoji print 抛 UnicodeEncodeError 直接杀死进程
# （136 实测：_load_keywords 失败时 print ❌ 崩在异常处理里，掩盖真实错误）。
# errors='replace' 兜底：控制台 emoji 显示为 '?'，日志文件始终 UTF-8 不受影响。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors='replace')
    except Exception:
        pass

# 若安装了 eventlet，则尽早 monkey_patch，避免 Web 模式下 RLock 未绿化警告。
# 对未安装 eventlet 的环境（如 Windows 本地开发）自动降级，不影响 CLI。
try:
    import eventlet
    eventlet.monkey_patch()
    # logging 必须使用真实 OS 锁：green 锁被 tpool 真实线程触碰会导致
    # greenlet.error（跨线程切换）且唤醒丢失 → greenlet 永久卡死
    from src.utils.offload import fix_logging_locks_for_eventlet
    fix_logging_locks_for_eventlet()
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
