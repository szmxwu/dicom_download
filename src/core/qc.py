# -*- coding: utf-8 -*-
"""
图像质量控制模块

提供 DICOM 和转换后图像（NIfTI/NPZ）的质量评估功能，
包括过曝、欠曝、对比度检查等。

支持基于模态（Modality）的可配置阈值，从环境变量读取配置。
"""

import json
import os
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

import numpy as np
import nibabel as nib


# Quality issue reasons (English)
class QualityReasons:
    """Low quality reason constants"""
    NO_PIXEL_DATA = "no_pixel_data"
    EMPTY_DATA = "empty_data"
    DYNAMIC_RANGE_INVALID = "dynamic_range_invalid"
    DYNAMIC_RANGE_LOW = "dynamic_range_low"
    CONTRAST_LOW = "contrast_low"
    COMPLEXITY_LOW = "complexity_low"
    UNDER_EXPOSED = "under_exposed"
    OVER_EXPOSED = "over_exposed"
    INVERTED_BORDER = "inverted_border"
    READ_ERROR = "read_error"
    FILE_NOT_FOUND = "file_not_found"
    UNSUPPORTED_FORMAT = "unsupported_format"
    NIFTI_ORIENTATION_ERROR = "nifti_orientation_error"


# Human readable descriptions for reasons
REASON_DESCRIPTIONS = {
    QualityReasons.NO_PIXEL_DATA: "No pixel data available",
    QualityReasons.EMPTY_DATA: "Empty pixel data",
    QualityReasons.DYNAMIC_RANGE_INVALID: "Invalid dynamic range",
    QualityReasons.DYNAMIC_RANGE_LOW: "Low dynamic range",
    QualityReasons.CONTRAST_LOW: "Low contrast",
    QualityReasons.COMPLEXITY_LOW: "Low complexity",
    QualityReasons.UNDER_EXPOSED: "Under-exposed",
    QualityReasons.OVER_EXPOSED: "Over-exposed",
    QualityReasons.INVERTED_BORDER: "Potential inverted border",
    QualityReasons.READ_ERROR: "Error reading image data",
    QualityReasons.FILE_NOT_FOUND: "File not found",
    QualityReasons.UNSUPPORTED_FORMAT: "Unsupported file format",
    QualityReasons.NIFTI_ORIENTATION_ERROR: "NIfTI orientation error (dcm2niix bug detected)",
}


class QCConfig:
    """
    质量控制的阈值配置类
    
    从环境变量读取配置，支持按模态设置不同的阈值。
    配置格式：QC_{MODALITY}_{THRESHOLD_NAME}
    """
    
    # 默认阈值
    DEFAULT_THRESHOLDS = {
        'dynamic_range_min': 20.0,
        'std_min': 5.0,
        'unique_ratio_min': 0.01,
        'low_ratio_threshold': 0.6,
        'high_ratio_threshold': 0.6,
        'percentile_low': 2.0,
        'percentile_high': 98.0,
        'series_low_quality_ratio': 0.3,
    }
    
    # 各模态特定的默认值（在环境变量未设置时使用）
    MODALITY_DEFAULTS = {
        'DX': {
            'dynamic_range_min': 10.0,
            'std_min': 3.0,
            'unique_ratio_min': 0.001,
        },
        'DR': {
            'dynamic_range_min': 10.0,
            'std_min': 3.0,
            'unique_ratio_min': 0.001,
        },
        'MG': {
            'dynamic_range_min': 10.0,
            'std_min': 3.0,
            'unique_ratio_min': 0.001,
        },
        'CR': {
            'dynamic_range_min': 10.0,
            'std_min': 3.0,
            'unique_ratio_min': 0.001,
        },
        'CT': {
            'dynamic_range_min': 20.0,
            'std_min': 5.0,
            'unique_ratio_min': 0.01,
        },
        'MR': {
            'dynamic_range_min': 15.0,
            'std_min': 5.0,
            'unique_ratio_min': 0.008,
        },
    }
    
    def __init__(self):
        """初始化配置，从环境变量读取"""
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Dict[str, float]]:
        """从环境变量加载配置"""
        config = {'DEFAULT': self.DEFAULT_THRESHOLDS.copy()}
        
        # 为已知模态创建配置
        for modality in ['CT', 'MR', 'DX', 'DR', 'MG', 'CR', 'US', 'PT', 'NM', 'XA', 'RF']:
            config[modality] = {}
        
        # 从环境变量读取配置
        for key, value in os.environ.items():
            if key.startswith('QC_') and not key.startswith('QC_DEFAULT'):
                # 解析格式：QC_{MODALITY}_{THRESHOLD}
                parts = key.split('_')
                if len(parts) >= 3:
                    modality = parts[1]
                    threshold_name = '_'.join(parts[2:]).lower()
                    try:
                        if modality not in config:
                            config[modality] = {}
                        config[modality][threshold_name] = float(value)
                    except ValueError:
                        pass  # 忽略无效数值
            elif key.startswith('QC_DEFAULT_'):
                # 解析格式：QC_DEFAULT_{THRESHOLD}
                threshold_name = '_'.join(key.split('_')[2:]).lower()
                try:
                    config['DEFAULT'][threshold_name] = float(value)
                except ValueError:
                    pass
        
        return config
    
    def get_threshold(self, modality: str, threshold_name: str) -> float:
        """
        获取指定模态的阈值
        
        Args:
            modality: 模态代码 (CT, MR, DX, etc.)
            threshold_name: 阈值名称
            
        Returns:
            float: 阈值数值
        """
        modality = modality.upper() if modality else 'DEFAULT'
        
        # 1. 从环境变量配置查找
        if modality in self._config and threshold_name in self._config[modality]:
            return self._config[modality][threshold_name]
        
        # 2. 从硬编码的模态默认值查找
        if modality in self.MODALITY_DEFAULTS and threshold_name in self.MODALITY_DEFAULTS[modality]:
            return self.MODALITY_DEFAULTS[modality][threshold_name]
        
        # 3. 使用全局默认值
        return self._config['DEFAULT'].get(threshold_name, self.DEFAULT_THRESHOLDS.get(threshold_name, 0.0))
    
    def get_all_thresholds(self, modality: str) -> Dict[str, float]:
        """
        获取指定模态的所有阈值
        
        Args:
            modality: 模态代码
            
        Returns:
            Dict[str, float]: 所有阈值的字典
        """
        result = self.DEFAULT_THRESHOLDS.copy()
        modality = modality.upper() if modality else 'DEFAULT'
        
        # 应用硬编码的模态默认值
        if modality in self.MODALITY_DEFAULTS:
            result.update(self.MODALITY_DEFAULTS[modality])
        
        # 应用环境变量配置
        if modality in self._config:
            result.update(self._config[modality])
        
        return result


# 全局配置实例
_qc_config = QCConfig()


def get_qc_config() -> QCConfig:
    """获取全局QC配置实例"""
    return _qc_config


def reset_qc_config():
    """重置QC配置（主要用于测试）"""
    global _qc_config
    _qc_config = QCConfig()


@dataclass
class ImageQualityResult:
    """Single image quality assessment result"""
    is_low_quality: bool = False
    reasons: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    def __int__(self) -> int:
        """Backward compatibility: return 0/1"""
        return 1 if self.is_low_quality else 0
    
    def __bool__(self) -> bool:
        """Support direct boolean check"""
        return self.is_low_quality
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'is_low_quality': self.is_low_quality,
            'quality_status': 'low_quality' if self.is_low_quality else 'normal',
            'low_quality_reason': '; '.join(self.reasons) if self.reasons else '',
            'metrics': self.metrics
        }
    
    def get_reason_description(self) -> str:
        """Get human readable reason description"""
        if not self.reasons:
            return "Normal"
        descriptions = [REASON_DESCRIPTIONS.get(r, r) for r in self.reasons]
        return "; ".join(descriptions)


def _apply_rescale(pixel_data: np.ndarray, dcm) -> np.ndarray:
    """
    应用像素值重缩放变换

    Args:
        pixel_data: 原始像素数据
        dcm: pydicom Dataset 对象

    Returns:
        np.ndarray: 重缩放后的像素数据
    """
    try:
        slope = float(getattr(dcm, 'RescaleSlope', 1.0))
        intercept = float(getattr(dcm, 'RescaleIntercept', 0.0))
        return pixel_data.astype(np.float32) * slope + intercept
    except Exception:
        return pixel_data.astype(np.float32)


def _apply_photometric(pixel_data: np.ndarray, dcm) -> np.ndarray:
    """
    应用光度学变换（处理 MONOCHROME1 反色）

    Args:
        pixel_data: 原始像素数据
        dcm: pydicom Dataset 对象

    Returns:
        np.ndarray: 变换后的像素数据
    """
    try:
        photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
        if photometric == 'MONOCHROME1':
            max_val = np.nanmax(pixel_data)
            return max_val - pixel_data
    except Exception:
        pass
    return pixel_data


def assess_image_quality(dcm, modality: Optional[str] = None) -> ImageQualityResult:
    """
    评估 DICOM 图像质量

    Args:
        dcm: pydicom Dataset 对象
        modality: 模态代码 (CT, MR, DX, etc.)，可选，自动从 dcm 读取

    Returns:
        ImageQualityResult: 质量评估结果
    """
    try:
        if not hasattr(dcm, 'pixel_array'):
            return ImageQualityResult(
                is_low_quality=True,
                reasons=[QualityReasons.NO_PIXEL_DATA],
                metrics={}
            )

        # 自动获取模态
        if modality is None:
            modality = getattr(dcm, 'Modality', '')

        pixel_data = dcm.pixel_array.astype(np.float32)
        pixel_data = _apply_rescale(pixel_data, dcm)
        pixel_data = _apply_photometric(pixel_data, dcm)
        return assess_image_quality_from_array(pixel_data, modality)
    except Exception as e:
        return ImageQualityResult(
            is_low_quality=True,
            reasons=[QualityReasons.READ_ERROR],
            metrics={'error': str(e)}
        )


def assess_image_quality_from_array(
    pixel_data: np.ndarray,
    modality: Optional[str] = None
) -> ImageQualityResult:
    """
    从像素数组评估图像质量

    评估指标包括：
    - 动态范围：像素值的分布范围
    - 标准差：像素值的离散程度
    - 唯一值比例：图像复杂度
    - 过曝/欠曝检测
    - 边缘反转检测

    Args:
        pixel_data: 像素数据数组
        modality: 模态代码 (CT, MR, DX, etc.)，用于选择不同阈值

    Returns:
        ImageQualityResult: 质量评估结果
    """
    reasons = []
    metrics = {}
    
    # 获取当前模态的阈值
    config = get_qc_config()
    modality = modality.upper() if modality else 'DEFAULT'
    thresholds = config.get_all_thresholds(modality)
    
    try:
        if pixel_data is None:
            return ImageQualityResult(
                is_low_quality=True,
                reasons=[QualityReasons.NO_PIXEL_DATA],
                metrics=metrics
            )

        pixel_data = np.asarray(pixel_data, dtype=np.float32)
        flat = pixel_data[np.isfinite(pixel_data)].ravel()
        
        if flat.size == 0:
            return ImageQualityResult(
                is_low_quality=True,
                reasons=[QualityReasons.EMPTY_DATA],
                metrics=metrics
            )

        # 大数据集时进行采样以提高性能
        if flat.size > 200000:
            flat = flat[:: max(1, flat.size // 200000)]

        # 获取百分位阈值
        p_low = thresholds.get('percentile_low', 2.0)
        p_high = thresholds.get('percentile_high', 98.0)
        
        p2, p98 = np.percentile(flat, [p_low, p_high])
        dynamic_range = p98 - p2
        std = float(np.std(flat))
        unique_ratio = len(np.unique(flat)) / max(1, flat.size)
        mean_val = float(np.mean(flat))
        range_eps = max(dynamic_range, 1e-6)
        
        metrics = {
            'dynamic_range': round(dynamic_range, 2),
            'std': round(std, 2),
            'unique_ratio': round(unique_ratio, 4),
            'mean_val': round(mean_val, 2),
            'p2': round(p2, 2),
            'p98': round(p98, 2),
            'modality': modality,
        }

        if dynamic_range <= 0:
            reasons.append(QualityReasons.DYNAMIC_RANGE_INVALID)
            return ImageQualityResult(
                is_low_quality=True,
                reasons=reasons,
                metrics=metrics
            )

        # 质量判定规则（使用模态特定阈值）
        dynamic_range_min = thresholds.get('dynamic_range_min', 20.0)
        std_min = thresholds.get('std_min', 5.0)
        unique_ratio_min = thresholds.get('unique_ratio_min', 0.01)
        
        if dynamic_range < dynamic_range_min:
            reasons.append(QualityReasons.DYNAMIC_RANGE_LOW)
            metrics['dynamic_range_threshold'] = dynamic_range_min

        if std < std_min:
            reasons.append(QualityReasons.CONTRAST_LOW)
            metrics['std_threshold'] = std_min

        if unique_ratio < unique_ratio_min:
            reasons.append(QualityReasons.COMPLEXITY_LOW)
            metrics['unique_ratio_threshold'] = unique_ratio_min

        # 检测过曝和欠曝
        low_thresh = p2 + 0.01 * range_eps
        high_thresh = p98 - 0.01 * range_eps
        low_ratio = float(np.mean(flat <= low_thresh))
        high_ratio = float(np.mean(flat >= high_thresh))
        
        metrics['low_ratio'] = round(low_ratio, 4)
        metrics['high_ratio'] = round(high_ratio, 4)

        low_ratio_threshold = thresholds.get('low_ratio_threshold', 0.6)
        high_ratio_threshold = thresholds.get('high_ratio_threshold', 0.6)

        under_exposed = mean_val < (p2 + 0.1 * range_eps) or low_ratio > low_ratio_threshold
        over_exposed = mean_val > (p98 - 0.1 * range_eps) or high_ratio > high_ratio_threshold

        if under_exposed:
            reasons.append(QualityReasons.UNDER_EXPOSED)
        
        if over_exposed:
            reasons.append(QualityReasons.OVER_EXPOSED)

        # 边缘反转检测（检查边框与中心的差异）
        slice_data = pixel_data
        if slice_data.ndim > 2:
            mid = slice_data.shape[-1] // 2
            slice_data = slice_data[..., mid]
        if slice_data.ndim == 2:
            h, w = slice_data.shape
            border = max(1, int(min(h, w) * 0.1))
            border_mask = np.zeros((h, w), dtype=bool)
            border_mask[:border, :] = True
            border_mask[-border:, :] = True
            border_mask[:, :border] = True
            border_mask[:, -border:] = True
            center_mask = ~border_mask
            border_vals = slice_data[border_mask]
            center_vals = slice_data[center_mask]
            if border_vals.size > 0 and center_vals.size > 0:
                border_mean = float(np.mean(border_vals))
                center_mean = float(np.mean(center_vals))
                inverted_like = border_mean - center_mean > 0.1 * range_eps
                if inverted_like:
                    reasons.append(QualityReasons.INVERTED_BORDER)
                    metrics['border_mean'] = round(border_mean, 2)
                    metrics['center_mean'] = round(center_mean, 2)

        return ImageQualityResult(
            is_low_quality=len(reasons) > 0,
            reasons=reasons,
            metrics=metrics
        )
        
    except Exception as e:
        return ImageQualityResult(
            is_low_quality=True,
            reasons=[QualityReasons.READ_ERROR],
            metrics={'error': str(e), 'modality': modality}
        )


def detect_nifti_orientation_error(
    img: nib.Nifti1Image, 
    modality: Optional[str] = None,
    dicom_metadata: Optional[Dict[str, Any]] = None
) -> bool:
    """
    检测 NIfTI 文件是否存在方向错误（dcm2niix bug）
    
    对于2D X-ray图像（DX/DR/CR），当原始DICOM缺少ImageOrientationPatient(IOP)时，
    dcm2niix有时会生成Y轴翻转的NIfTI文件，导致图像上下颠倒。
    
    检测逻辑：
    1. 只对2D X-ray模态（DX/DR/CR/RF）进行检测
    2. 检查是否为2D或伪3D图像（第三维为1）
    3. 如果有DICOM元数据，检查是否缺少IOP
    4. 使用启发式方法检测NIfTI数据是否可能被翻转
    
    Args:
        img: nibabel Nifti1Image 对象
        modality: 模态代码 (DX, DR, CR, etc.)
        dicom_metadata: 可选的DICOM元数据字典，用于辅助判断
        
    Returns:
        bool: 是否存在方向错误
    """
    try:
        modality = (modality or '').upper()
        
        # 只对2D X-ray模态进行检测
        if modality not in ['DX', 'DR', 'CR', 'RF']:
            return False
        
        data = img.get_fdata()
        
        # 只检查2D或伪3D（第三维为1）的图像
        if data.ndim == 2:
            pass
        elif data.ndim == 3 and data.shape[2] == 1:
            pass
        else:
            return False
        
        # 如果有DICOM元数据，检查是否缺少ImageOrientationPatient
        # 这是dcm2niix产生方向错误的主要原因
        if dicom_metadata is not None:
            iop = dicom_metadata.get('ImageOrientationPatient')
            if iop is None:
                # 缺少IOP时，dcm2niix可能使用默认方向，导致翻转
                return True
        
        # 启发式检测：分析NIfTI的affine矩阵和数据布局
        # 对于典型的X-ray，如果affine的Y轴方向与数据存储不匹配，
        # 可能存在方向问题
        
        affine = img.affine
        
        # 检查affine的Y轴（第二列）在图像平面内的投影
        # 对于典型的2D X-ray，Y轴应该主要沿图像的垂直方向
        y_axis_x = abs(affine[0, 1])  # Y轴在X方向的投影
        y_axis_y = abs(affine[1, 1])  # Y轴在Y方向的投影
        
        # 如果Y轴主要在水平方向（X方向），说明坐标系可能有问题
        if y_axis_x > y_axis_y:
            return True
        
        # 数据驱动的启发式检测：
        # 对于典型的骨盆/胸部X-ray，图像中心区域通常比边框亮
        # 如果检测到数据分布异常，可能存在方向问题
        
        # 获取2D切片
        slice_2d = data if data.ndim == 2 else data[:, :, 0]
        h, w = slice_2d.shape
        
        # 检查上下边框的像素值分布
        border_size = max(1, h // 20)  # 5%的边框
        top_border = slice_2d[:border_size, :]
        bottom_border = slice_2d[-border_size:, :]
        
        top_mean = np.mean(top_border)
        bottom_mean = np.mean(bottom_border)
        center_mean = np.mean(slice_2d[h//3:2*h//3, w//3:2*w//3])
        
        # 如果上下边框都比中心亮很多，可能存在异常
        # 但这种检测不太可靠，容易产生假阳性
        
        return False
        
    except Exception:
        return False


def assess_converted_file_quality(
    filepath: str, 
    modality: Optional[str] = None, 
    check_orientation: bool = True,
    dicom_metadata: Optional[Dict[str, Any]] = None
) -> ImageQualityResult:
    """
    评估转换后文件（NIfTI/NPZ）的质量

    Args:
        filepath: 文件路径
        modality: 模态代码 (CT, MR, DX, etc.)，可选
        check_orientation: 是否检查NIfTI方向错误
        dicom_metadata: 可选的DICOM元数据字典，用于方向错误检测

    Returns:
        ImageQualityResult: 质量评估结果
    """
    try:
        if not os.path.exists(filepath):
            return ImageQualityResult(
                is_low_quality=True,
                reasons=[QualityReasons.FILE_NOT_FOUND],
                metrics={'filepath': filepath}
            )
        
        # 如果未提供 dicom_metadata，尝试从 cache 文件加载
        if dicom_metadata is None:
            file_dir = os.path.dirname(filepath)
            cache_path = os.path.join(file_dir, "dicom_metadata_cache.json")
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache = json.load(f)
                    dicom_metadata = cache.get('sample_tags', {})
                except Exception:
                    pass
        
        if filepath.endswith('.npz'):
            with np.load(filepath) as npz:
                if 'data' in npz.files:
                    data = npz['data']
                elif npz.files:
                    data = npz[npz.files[0]]
                else:
                    return ImageQualityResult(
                        is_low_quality=True,
                        reasons=[QualityReasons.NO_PIXEL_DATA],
                        metrics={'filepath': filepath}
                    )
            # NPZ 文件不受 dcm2niix bug 影响
            orientation_error = False
            
        elif filepath.endswith(('.nii', '.nii.gz')):
            img = nib.load(filepath)
            data = img.get_fdata()
            
            # 检测NIfTI方向错误（dcm2niix bug）
            orientation_error = False
            if check_orientation:
                orientation_error = detect_nifti_orientation_error(img, modality, dicom_metadata)
        else:
            return ImageQualityResult(
                is_low_quality=True,
                reasons=[QualityReasons.UNSUPPORTED_FORMAT],
                metrics={'filepath': filepath, 'extension': os.path.splitext(filepath)[1]}
            )

        result = assess_image_quality_from_array(data, modality)
        
        # 如果检测到方向错误，添加到结果中
        if orientation_error:
            result.is_low_quality = True
            if QualityReasons.NIFTI_ORIENTATION_ERROR not in result.reasons:
                result.reasons.append(QualityReasons.NIFTI_ORIENTATION_ERROR)
            result.metrics['nifti_orientation_error'] = True
            result.metrics['nifti_orientation_note'] = (
                "dcm2niix generated NIfTI with flipped Y-axis. "
                "Preview generator will auto-correct this."
            )
        
        return result
        
    except Exception as e:
        return ImageQualityResult(
            is_low_quality=True,
            reasons=[QualityReasons.READ_ERROR],
            metrics={'error': str(e), 'filepath': filepath}
        )


def _summarize_reasons(results: List[ImageQualityResult], total_files: int) -> str:
    """
    汇总多个文件的质量原因
    
    Args:
        results: 质量结果列表
        total_files: 总文件数
        
    Returns:
        str: 原因汇总字符串
    """
    all_reasons = []
    for r in results:
        all_reasons.extend(r.reasons)
    
    if not all_reasons:
        return "Normal"
    
    reason_counter = Counter(all_reasons)
    low_count = sum(1 for r in results if r.is_low_quality)
    ratio = low_count / max(1, len(results))
    
    # Get top 3 most common reasons
    top_reasons = reason_counter.most_common(3)
    reason_parts = [f"{REASON_DESCRIPTIONS.get(r, r)}:{count}" for r, count in top_reasons]
    
    return f"{ratio*100:.0f}% files with issues - " + ", ".join(reason_parts)


def assess_series_quality_converted(
    converted_files: List[str],
    modality: Optional[str] = None,
    series_dir: Optional[str] = None
) -> Dict[str, Any]:
    """
    评估转换后序列的质量

    Args:
        converted_files: 转换后的文件路径列表
        modality: 模态代码 (CT, MR, DX, etc.)，可选
        series_dir: 序列目录路径，用于查找dicom_metadata_cache.json

    Returns:
        Dict: 包含以下字段的字典：
            - low_quality: 是否低质量 (0/1)
            - low_quality_reason: 低质量原因汇总描述
            - low_quality_details: 每个文件的详细质量结果
            - low_quality_ratio: 低质量文件比例
            - qc_mode: 质检模式 ('full'/'sample'/'none'/'error')
            - qc_sample_indices: 抽样检查的索引列表
    """
    try:
        total = len(converted_files)
        if total == 0:
            return {
                'low_quality': 1,
                'low_quality_reason': REASON_DESCRIPTIONS[QualityReasons.NO_PIXEL_DATA],
                'low_quality_details': [],
                'low_quality_ratio': 1.0,
                'qc_mode': 'none',
                'qc_sample_indices': []
            }

        # 根据文件数量决定质检模式
        if total <= 200:
            sample_indices = list(range(total))
            qc_mode = 'full'
        else:
            mid = total // 2
            sample_indices = [i for i in range(mid - 3, mid + 4) if 0 <= i < total]
            qc_mode = 'sample'

        # 尝试加载DICOM元数据缓存（用于方向错误检测）
        dicom_metadata = None
        if series_dir:
            cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache = json.load(f)
                    dicom_metadata = cache.get('sample_tags', {})
                except Exception as e:
                    pass

        file_results = []
        for idx in sample_indices:
            try:
                # DEBUG: Check dicom_metadata
                # print(f"DEBUG assess_series_quality_converted: dicom_metadata is None = {dicom_metadata is None}")
                result = assess_converted_file_quality(
                    converted_files[idx], 
                    modality, 
                    check_orientation=True,
                    dicom_metadata=dicom_metadata
                )
                # DEBUG: Check result
                # print(f"DEBUG assess_series_quality_converted: result.is_low_quality = {result.is_low_quality}, reasons = {result.reasons}")
                file_results.append({
                    'file_index': idx,
                    'file_name': os.path.basename(converted_files[idx]),
                    'is_low_quality': result.is_low_quality,
                    'reasons': result.reasons.copy(),
                    'metrics': result.metrics
                })
            except Exception as e:
                file_results.append({
                    'file_index': idx,
                    'file_name': os.path.basename(converted_files[idx]),
                    'is_low_quality': True,
                    'reasons': [QualityReasons.READ_ERROR],
                    'metrics': {'error': str(e)}
                })

        low_count = sum(1 for r in file_results if r['is_low_quality'])
        ratio = low_count / max(1, len(sample_indices))
        
        # 获取模态特定的系列阈值
        config = get_qc_config()
        series_threshold = config.get_threshold(modality or 'DEFAULT', 'series_low_quality_ratio')
        is_low_quality = ratio > series_threshold
        
        # 收集所有原因（包括方向错误等非质量问题的警告）
        all_reasons = []
        orientation_error_count = 0
        for r in file_results:
            if r['is_low_quality']:
                all_reasons.extend(r['reasons'])
            # 单独统计方向错误（即使不标记为低质量也要报告）
            if QualityReasons.NIFTI_ORIENTATION_ERROR in r['reasons']:
                orientation_error_count += 1
        
        # Generate reason summary
        if is_low_quality or all_reasons:
            reason_summary = _summarize_reasons(
                [ImageQualityResult(r['is_low_quality'], r['reasons'], {}) for r in file_results],
                len(sample_indices)
            )
        else:
            reason_summary = "Normal"
        
        # 如果检测到方向错误，添加到报告（即使系列质量正常）
        if orientation_error_count > 0:
            if reason_summary == "Normal":
                reason_summary = ""
            else:
                reason_summary += "; "
            reason_summary += f"NIfTI orientation error (dcm2niix bug) detected in {orientation_error_count} file(s)"

        return {
            'low_quality': 1 if is_low_quality else 0,
            'low_quality_reason': reason_summary,
            'low_quality_details': file_results,
            'low_quality_ratio': ratio,
            'qc_mode': qc_mode,
            'qc_sample_indices': sample_indices
        }
        
    except Exception as e:
        return {
            'low_quality': 1,
            'low_quality_reason': f"{REASON_DESCRIPTIONS[QualityReasons.READ_ERROR]}: {str(e)}",
            'low_quality_details': [],
            'low_quality_ratio': 1.0,
            'qc_mode': 'error',
            'qc_sample_indices': []
        }


def assess_series_quality(
    dicom_files: List[str],
    dcmread,
    modality: Optional[str] = None
) -> Dict[str, Any]:
    """
    评估 DICOM 序列的质量

    Args:
        dicom_files: DICOM 文件路径列表
        dcmread: pydicom.dcmread 函数
        modality: 模态代码 (CT, MR, DX, etc.)，可选

    Returns:
        Dict: 质量评估结果字典
    """
    try:
        total = len(dicom_files)
        if total == 0:
            return {
                'low_quality': 1,
                'low_quality_reason': REASON_DESCRIPTIONS[QualityReasons.NO_PIXEL_DATA],
                'low_quality_details': [],
                'low_quality_ratio': 1.0,
                'qc_mode': 'none',
                'qc_sample_indices': []
            }

        if total <= 200:
            sample_indices = list(range(total))
            qc_mode = 'full'
        else:
            mid = total // 2
            sample_indices = [i for i in range(mid - 3, mid + 4) if 0 <= i < total]
            qc_mode = 'sample'

        file_results = []
        for idx in sample_indices:
            try:
                dcm = dcmread(dicom_files[idx], force=True)
                # 如果未提供模态，尝试从DICOM读取
                file_modality = modality
                if file_modality is None:
                    file_modality = getattr(dcm, 'Modality', None)
                result = assess_image_quality(dcm, file_modality)
                file_results.append({
                    'file_index': idx,
                    'file_name': os.path.basename(dicom_files[idx]),
                    'is_low_quality': result.is_low_quality,
                    'reasons': result.reasons.copy(),
                    'metrics': result.metrics
                })
            except Exception as e:
                file_results.append({
                    'file_index': idx,
                    'file_name': os.path.basename(dicom_files[idx]),
                    'is_low_quality': True,
                    'reasons': [QualityReasons.READ_ERROR],
                    'metrics': {'error': str(e)}
                })

        low_count = sum(1 for r in file_results if r['is_low_quality'])
        ratio = low_count / max(1, len(sample_indices))
        
        # 获取模态特定的系列阈值
        config = get_qc_config()
        series_threshold = config.get_threshold(modality or 'DEFAULT', 'series_low_quality_ratio')
        is_low_quality = ratio > series_threshold
        
        # Generate reason summary
        if is_low_quality:
            reason_summary = _summarize_reasons(
                [ImageQualityResult(r['is_low_quality'], r['reasons'], {}) for r in file_results],
                len(sample_indices)
            )
        else:
            reason_summary = "Normal"

        return {
            'low_quality': 1 if is_low_quality else 0,
            'low_quality_reason': reason_summary,
            'low_quality_details': file_results,
            'low_quality_ratio': ratio,
            'qc_mode': qc_mode,
            'qc_sample_indices': sample_indices
        }
        
    except Exception as e:
        return {
            'low_quality': 1,
            'low_quality_reason': f"{REASON_DESCRIPTIONS[QualityReasons.READ_ERROR]}: {str(e)}",
            'low_quality_details': [],
            'low_quality_ratio': 1.0,
            'qc_mode': 'error',
            'qc_sample_indices': []
        }
