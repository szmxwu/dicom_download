# -*- coding: utf-8 -*-
"""
DICOM 核心处理模块

包含 DICOM 文件的查询、下载、组织、转换、元数据提取等功能。
"""

from src.core.organize import organize_dicom_files
from src.core.convert import convert_dicom_to_nifti, convert_to_npz
from src.core.metadata import extract_dicom_metadata
from src.core.qc import (
    assess_series_quality_converted,
    ImageQualityResult,
    QualityReasons,
    REASON_DESCRIPTIONS,
    QCConfig,
    get_qc_config,
    reset_qc_config,
)
from src.core.preview import generate_series_preview
from src.core.mr_clean import process_mri_dataframe

__all__ = [
    "organize_dicom_files",
    "convert_dicom_to_nifti",
    "convert_to_npz",
    "extract_dicom_metadata",
    "assess_series_quality_converted",
    "ImageQualityResult",
    "QualityReasons",
    "REASON_DESCRIPTIONS",
    "QCConfig",
    "get_qc_config",
    "reset_qc_config",
    "generate_series_preview",
    "process_mri_dataframe",
]
