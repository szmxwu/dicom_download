# -*- coding: utf-8 -*-
"""
数据模型模块

定义 DICOM 处理流程中使用的核心数据类。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional


@dataclass
class ClientConfig:
    """
    PACS 客户端配置类

    Attributes:
        pacs_ip: PACS 服务器 IP 地址
        pacs_port: PACS 服务器端口
        calling_aet: 本机 AET (Application Entity Title)
        called_aet: 目标 PACS 的 AET
        calling_port: 本机监听端口
        tags_dir: DICOM 标签配置文件目录
        work_dir: 工作目录路径
    """
    pacs_ip: str = "127.0.0.1"
    pacs_port: int = 11112
    calling_aet: str = "PYTHON_CLIENT"
    called_aet: str = "PACS_SERVER"
    calling_port: int = 11113
    tags_dir: str = "dicom_tags"
    work_dir: str = "work"


@dataclass
class SeriesInfo:
    """
    DICOM 序列信息类

    Attributes:
        study_uid: 研究实例 UID
        series_uid: 序列实例 UID
        modality: 模态类型 (MR/CT/DR 等)
        description: 序列描述
        dicom_dir: DICOM 文件目录
        organized_dir: 组织后的目录
        nifti_dir: NIfTI 文件目录
        files: DICOM 文件路径列表
    """
    study_uid: str = ""
    series_uid: str = ""
    modality: str = ""
    description: str = ""
    dicom_dir: str = ""
    organized_dir: str = ""
    nifti_dir: str = ""
    files: List[str] = field(default_factory=list)


@dataclass
class WorkflowResult:
    """
    工作流结果类

    Attributes:
        success: 处理是否成功
        organized_dir: 组织后的目录路径
        excel_file: 生成的 Excel 文件路径
        result_zip: 结果 ZIP 文件路径
        series_info: 序列信息字典
        errors: 错误信息列表
    """
    success: bool = False
    organized_dir: Optional[str] = None
    excel_file: Optional[str] = None
    result_zip: Optional[str] = None
    series_info: Dict[str, SeriesInfo] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
