# -*- coding: utf-8 -*-
"""
DICOM 转换模块。

该模块提供 DICOM 医学影像文件转换为 NIfTI 和 NPZ 格式的功能。
主要功能包括：
- 使用 dcm2niix 工具进行高效转换
- 使用 Python 库（pydicom + nibabel）进行纯 Python 转换
- 处理不同模态（CT、MR、DR、MG、DX 等）的影像数据
- 像素值重缩放和光度解释处理
- 从 DICOM 构建仿射变换矩阵

典型用法：
    from src.core.convert import convert_dicom_to_nifti, convert_to_npz
    
    result = convert_dicom_to_nifti(client, "/path/to/series", "series_name")
    result = convert_to_npz(client, "/path/to/series", "series_name")
"""

from __future__ import annotations

import logging
import os
import json
import shutil
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import nibabel as nib
import numpy as np
import pydicom
from pydicom.dataset import FileDataset

if TYPE_CHECKING:
    from src.client.unified import DICOMDownloadClient as DicomClient


logger = logging.getLogger('DICOMApp')

# 全局锁用于保护 dcm2niix 调用，避免 Windows 下多进程/多线程并发问题
dcm2niix_global_lock = threading.Lock()


def _build_conversion_entry(output_file: str, dcm: FileDataset, file_index: Optional[int] = None, source_file: Optional[str] = None) -> Dict[str, str]:
    entry: Dict[str, str] = {
        'output_file': output_file
    }
    if file_index is not None:
        entry['FileIndex'] = str(file_index)
    if source_file:
        entry['SourceFile'] = str(source_file)

    for attr in [
        'Rows', 'Columns', 'SOPInstanceUID', 'InstanceNumber',
        'PhotometricInterpretation', 'ImageLaterality',
        'WindowCenter', 'WindowWidth', 'RescaleSlope', 'RescaleIntercept'
    ]:
        try:
            value = getattr(dcm, attr, None)
            if value is not None:
                entry[attr] = str(value)
        except Exception:
            continue
    return entry


def _write_conversion_map(series_dir: str, entries: List[Dict[str, str]]) -> None:
    if not entries:
        return
    cache_path = os.path.join(series_dir, 'dicom_metadata_cache.json')
    cache: Dict[str, object] = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache = json.load(f) or {}
        except Exception:
            cache = {}

    conversion_map = cache.get('conversion_map', {})
    if not isinstance(conversion_map, dict):
        conversion_map = {}

    for entry in entries:
        output_file = entry.get('output_file')
        if output_file:
            conversion_map[output_file] = entry

    cache['conversion_map'] = conversion_map
    try:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        return


def apply_rescale(pixel_data: np.ndarray, dcm: FileDataset) -> np.ndarray:
    """
    对像素数据应用 DICOM 重缩放变换。

    根据 DICOM 标签 RescaleSlope 和 RescaleIntercept 对原始像素值进行线性变换：
    输出值 = 像素值 × RescaleSlope + RescaleIntercept

    参数:
        pixel_data: 原始像素数据数组
        dcm: pydicom Dataset 对象

    返回:
        应用重缩放后的 float32 类型像素数据
    """
    try:
        slope = float(getattr(dcm, 'RescaleSlope', 1.0))
        intercept = float(getattr(dcm, 'RescaleIntercept', 0.0))
        return pixel_data.astype(np.float32) * slope + intercept
    except Exception:
        return pixel_data.astype(np.float32)


def apply_photometric(pixel_data: np.ndarray, dcm: FileDataset) -> np.ndarray:
    """
    应用 DICOM 光度解释转换。

    当 PhotometricInterpretation 为 MONOCHROME1 时，将像素值反转
    （用最大值减去每个像素值），将图像从高像素值表示低密度转换为
    常规的高像素值表示高密度。

    参数:
        pixel_data: 原始像素数据数组
        dcm: pydicom Dataset 对象

    返回:
        应用光度解释转换后的像素数据
    """
    try:
        photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
        if photometric == 'MONOCHROME1':
            max_val = np.nanmax(pixel_data)
            return max_val - pixel_data
    except Exception:
        pass
    return pixel_data


def build_affine_from_dicom(
    dcm: FileDataset,
    slice_spacing: float = 1.0,
    slice_cosines: Optional[np.ndarray] = None
) -> np.ndarray:
    """
    从 DICOM 数据构建 NIfTI 仿射变换矩阵。

    使用 ImageOrientationPatient、ImagePositionPatient 和 PixelSpacing
    构建从体素坐标到世界坐标的 4×4 变换矩阵。将 LPS（左-后-上）坐标系
    转换为 RAS（右-前-上）坐标系。
    
    当缺少 IOP/IPP 时，对于 2D 图像使用 _build_2d_xray_affine 作为回退方案。

    参数:
        dcm: pydicom Dataset 对象，包含方向、位置和像素间距信息
        slice_spacing: 切片间距（毫米），默认为 1.0
        slice_cosines: 切片方向余弦向量（可选），默认通过叉积计算

    返回:
        4×4 仿射变换矩阵（RAS 坐标系）
    """
    try:
        iop = getattr(dcm, 'ImageOrientationPatient', None)
        ipp = getattr(dcm, 'ImagePositionPatient', None)
        pixel_spacing = getattr(dcm, 'PixelSpacing', [1.0, 1.0])
        
        if iop is None or ipp is None:
            logger.warning("Missing ImageOrientationPatient or ImagePositionPatient in DICOM, using 2D fallback")
            # 对于缺少 IOP/IPP 的 2D 图像，使用专门的 2D X-ray 仿射矩阵
            modality = getattr(dcm, 'Modality', '')
            if modality in ['DR', 'MG', 'DX', 'CR', 'US', 'XA', 'RF']:
                return _build_2d_xray_affine(dcm)
            else:
                # 对于 3D 图像，仍使用单位矩阵作为最后的回退
                return np.eye(4, dtype=np.float64)

        row_cosine = np.array([float(i) for i in iop[:3]], dtype=np.float64)
        col_cosine = np.array([float(i) for i in iop[3:6]], dtype=np.float64)
        if slice_cosines is None:
            slice_cosines = np.cross(row_cosine, col_cosine)

        row_spacing = float(pixel_spacing[0])
        col_spacing = float(pixel_spacing[1])

        affine_lps = np.eye(4, dtype=np.float64)
        # DICOM pixel_array is (Rows, Columns) stacked as (Rows, Columns, Slices)
        # dim 0 = Rows (vertical, top to bottom) = -Y in patient coordinates (since row 0 is top)
        # dim 1 = Columns (horizontal, left to right) = X in patient coordinates
        # So: X axis = column direction, Y axis = -row direction
        affine_lps[:3, 0] = col_cosine * col_spacing  # x-axis = column direction
        affine_lps[:3, 1] = -row_cosine * row_spacing  # y-axis = negative row direction (flip vertical)
        affine_lps[:3, 2] = slice_cosines * float(slice_spacing)
        affine_lps[:3, 3] = np.array([float(i) for i in ipp], dtype=np.float64)

        lps_to_ras = np.diag([-1.0, -1.0, 1.0, 1.0])
        affine_ras = lps_to_ras @ affine_lps
        return affine_ras
    except Exception as e:
        logger.warning("Failed to build affine from DICOM: %s, using fallback", e)
        # 尝试使用 2D 回退方案
        modality = getattr(dcm, 'Modality', '')
        if modality in ['DR', 'MG', 'DX', 'CR', 'US', 'XA', 'RF']:
            return _build_2d_xray_affine(dcm)
        return np.eye(4, dtype=np.float64)


def _build_decomposable_affine(img: Any) -> np.ndarray:
    """
    为不可分解 affine 构建一个可分解的回退 affine。

    该矩阵仅保留体素尺寸、主方向符号与平移分量，以确保
    nib.as_closest_canonical 能够进行方向变换。
    """
    affine = np.array(getattr(img, 'affine', np.eye(4)), dtype=np.float64)
    if affine.shape != (4, 4):
        affine = np.eye(4, dtype=np.float64)

    fallback = np.eye(4, dtype=np.float64)

    try:
        zooms = tuple(float(v) for v in img.header.get_zooms()[:3])
    except Exception:
        zooms = (1.0, 1.0, 1.0)

    for axis in range(3):
        col = affine[:3, axis]
        norm = float(np.linalg.norm(col))

        voxel_size = zooms[axis] if axis < len(zooms) else 1.0
        if not np.isfinite(voxel_size) or voxel_size <= 0:
            voxel_size = norm if (np.isfinite(norm) and norm > 0) else 1.0

        sign = 1.0
        if np.all(np.isfinite(col)) and norm > 0:
            dominant = float(col[int(np.argmax(np.abs(col)))])
            sign = -1.0 if dominant < 0 else 1.0

        fallback[axis, axis] = sign * float(voxel_size)

    translation = affine[:3, 3]
    if np.all(np.isfinite(translation)):
        fallback[:3, 3] = translation

    return fallback


def _safe_as_closest_canonical(img: Any) -> Any:
    """
    安全执行 canonical 方向转换。

    若原始 affine 不可分解导致 as_closest_canonical 失败，则重建可分解 affine 后重试。
    """
    try:
        return nib.as_closest_canonical(img)
    except Exception as first_error:
        logger.warning(
            "as_closest_canonical failed, retrying with decomposable affine: %s",
            first_error
        )

    repaired_affine = _build_decomposable_affine(img)
    repaired_img = nib.Nifti1Image(np.asanyarray(img.dataobj), repaired_affine, header=img.header.copy())
    repaired_img.set_qform(repaired_affine, code=1)
    repaired_img.set_sform(repaired_affine, code=1)

    try:
        return nib.as_closest_canonical(repaired_img)
    except Exception as second_error:
        logger.warning(
            "canonical retry failed after affine repair, using repaired image directly: %s",
            second_error
        )
        return repaired_img


def normalize_and_save_npz(nii_path: str, npz_path: str) -> None:
    """
    归一化并保存 NIfTI 数据为 NPZ 压缩格式。

    加载 NIfTI 文件，将其转换为标准方向（canonical），
    对数据在各轴上进行翻转，并重新排列维度顺序为 (Z, Y, X)，
    最后以 float32 类型压缩保存为 NPZ 格式。

    参数:
        nii_path: 输入 NIfTI 文件路径
        npz_path: 输出 NPZ 文件路径
    """
    # 加载 NIfTI 文件，返回一个 Nifti1Image 对象（包含数据和头信息）
    img = nib.load(nii_path)
    # 将图像转换为最接近的标准方向（canonical），以统一轴向（通常为 RAS）
    # 对于 affine 不可分解的文件，先重建可分解 affine 再重试。
    img_canonical = _safe_as_closest_canonical(img)
    # 从 Nifti 对象中获取数据数组（通常为 float64），形状如 (X, Y, Z[, T])
    data = img_canonical.get_fdata()

    if data.ndim < 3:
        raise ValueError(f"NIfTI data must be at least 3D, got shape={data.shape}")

    # 对数据在每个轴上做反转。具体含义：
    # - 第一个索引 `[::-1]` 代表在第 0 轴（X）上反转
    # - 第二个索引 `[::-1]` 代表在第 1 轴（Y）上反转
    # - 第三个索引 `[::-1]` 代表在第 2 轴（Z）上反转
    # 这样做通常用于将 NIfTI 的内部存储方向调整为期望的显示/处理方向
    data = np.flip(data, axis=(0, 1, 2))
    # 重新排列轴顺序：把空间维从 (X, Y, Z) 变为 (Z, Y, X)，
    # 若存在第 4 维（如多参数/时间维），则保持在后续维度不变。
    transpose_axes = [2, 1, 0] + list(range(3, data.ndim))
    data = np.transpose(data, transpose_axes)

    # 将数据转换为 float32（节省空间）并以压缩的 npz 格式写入磁盘
    np.savez_compressed(npz_path, data=data.astype(np.float32))


def convert_dicom_to_nifti(
    client: "DicomClient",
    series_dir: str,
    series_name: str
) -> Dict[str, Union[bool, str, int, List[str]]]:
    """
    将 DICOM 序列转换为 NIfTI 格式。

    首先尝试使用 dcm2niix 工具进行转换，如果失败则回退到 Python 库。
    支持处理 DR、MG、DX 等特殊模态（单文件单独转换）。
    转换成功后生成预览图和元数据缓存。

    参数:
        client: DICOM 客户端实例，提供辅助方法
        series_dir: DICOM 序列目录路径
        series_name: 序列名称

    返回:
        包含转换结果的字典：
        - success: 是否成功
        - method: 使用的转换方法
        - output_file(s): 输出文件路径
        - modality: 影像模态
        - error: 错误信息（失败时）
    """
    try:
        print(f"   🔄 Converting {series_name} to NIfTI...")

        sample_dcm, modality = client._get_series_sample_dicom(series_dir)
        dicom_files: List[str] = []
        try:
            for file in os.listdir(series_dir):
                filepath = os.path.join(series_dir, file)
                if os.path.isfile(filepath) and client._is_dicom_file(filepath):
                    dicom_files.append(filepath)
        except Exception:
            dicom_files = []

        if dicom_files:
            client._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
            client._write_minimal_cache(
                series_dir,
                series_name,
                modality,
                sample_dcm=sample_dcm,
                file_count=len(dicom_files)
            )

        nifti_result = convert_with_dcm2niix(client, series_dir, series_name)
        if nifti_result and nifti_result.get('success'):
            logger.info(
                "dcm2niix转换成功: series=%s, output=%s",
                series_name,
                nifti_result.get('output_files') or nifti_result.get('output_file')
            )
            print("   ✅ dcm2niix conversion succeeded.")
            client._generate_series_preview(series_dir, series_name, nifti_result, sample_dcm, modality)
            cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
            if not os.path.exists(cache_path) and sample_dcm is not None:
                record = client._build_metadata_record_from_sample(
                    series_name,
                    sample_dcm,
                    len(dicom_files),
                    modality
                )
                payload = {
                    'modality': modality,
                    'records': [record],
                    'sample_tags': client._build_sample_tags(sample_dcm)
                }
                try:
                    import json
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
            return nifti_result
        logger.warning(
            "dcm2niix转换失败: series=%s, error=%s",
            series_name,
            nifti_result.get('error') if isinstance(nifti_result, dict) else 'unknown'
        )

        print("   ⚠️  dcm2niix not available, trying Python libraries...")
        nifti_result = convert_with_python_libs(client, series_dir, series_name)
        if nifti_result and nifti_result.get('success'):
            client._generate_series_preview(series_dir, series_name, nifti_result, sample_dcm, modality)
            cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
            if not os.path.exists(cache_path) and sample_dcm is not None:
                record = client._build_metadata_record_from_sample(
                    series_name,
                    sample_dcm,
                    len(dicom_files),
                    modality
                )
                payload = {
                    'modality': modality,
                    'records': [record],
                    'sample_tags': client._build_sample_tags(sample_dcm)
                }
                try:
                    import json
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
        return nifti_result

    except Exception as e:
        print(f"   ❌ NIfTI conversion failed: {e}")
        return {'success': False, 'error': str(e)}


def convert_to_npz(
    client: "DicomClient",
    series_dir: str,
    series_name: str
) -> Dict[str, Union[bool, str, int, float, List[str], List[int]]]:
    """
    将 DICOM 序列转换为归一化的 NPZ 格式。

    首先将 DICOM 转换为 NIfTI，然后将 NIfTI 数据归一化并保存为 NPZ 格式。
    支持批量转换多文件序列。转换完成后进行质量控制评估。

    参数:
        client: DICOM 客户端实例，提供辅助方法
        series_dir: DICOM 序列目录路径
        series_name: 序列名称

    返回:
        包含转换结果的字典：
        - success: 是否成功
        - method: 'npz_normalized'
        - output_files: 输出文件列表
        - low_quality: 低质量文件数量
        - low_quality_ratio: 低质量比例
        - qc_mode: 质量控制模式
        - qc_sample_indices: 质检采样索引
        - error: 错误信息（失败时）
    """
    try:
        print(f"   🔄 Converting {series_name} to NPZ (Normalized)...")

        sample_dcm, modality = client._get_series_sample_dicom(series_dir)

        nifti_res = convert_with_dcm2niix(client, series_dir, series_name)
        if not (nifti_res and nifti_res.get('success')):
            nifti_res = convert_with_python_libs(client, series_dir, series_name)

        if not (nifti_res and nifti_res.get('success')):
            return {'success': False, 'error': 'Failed to generate base volume for NPZ'}

        output_files: List[str] = []
        if nifti_res.get('conversion_mode') == 'individual':
            for nii_file in nifti_res.get('output_files', []):
                nii_path = os.path.join(series_dir, nii_file)
                npz_file = nii_file.replace('.nii.gz', '.npz').replace('.nii', '.npz')
                npz_path = os.path.join(series_dir, npz_file)

                normalize_and_save_npz(nii_path, npz_path)
                output_files.append(npz_file)
                if os.path.exists(nii_path):
                    os.remove(nii_path)
        else:
            nii_file = nifti_res.get('output_file')
            nii_path = os.path.join(series_dir, nii_file)
            npz_file = nii_file.replace('.nii.gz', '.npz').replace('.nii', '.npz')
            npz_path = os.path.join(series_dir, npz_file)

            normalize_and_save_npz(nii_path, npz_path)
            output_files.append(npz_file)
            if os.path.exists(nii_path):
                os.remove(nii_path)

        qc_summary = client._assess_series_quality_converted(
            [os.path.join(series_dir, f) for f in output_files],
            modality=modality,
            series_dir=series_dir
        )
        print(
            f"   🧪 QC({qc_summary['qc_mode']}): "
            f"low_ratio={qc_summary['low_quality_ratio']:.2f}, "
            f"low_quality={qc_summary['low_quality']}"
        )

        try:
            client._generate_series_preview(
                series_dir,
                series_name,
                {
                    'success': True,
                    'conversion_mode': 'individual' if len(output_files) > 1 else 'series',
                    'output_files': output_files
                },
                sample_dcm,
                modality
            )
        except Exception as e:
            print(f"   ⚠️  Preview generation failed: {e}")

        # If the cache already has mapping for NIfTI outputs, clone it for NPZ names.
        try:
            cache_path = os.path.join(series_dir, 'dicom_metadata_cache.json')
            if os.path.exists(cache_path):
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache = json.load(f) or {}
                conversion_map = cache.get('conversion_map', {})
                if isinstance(conversion_map, dict):
                    updated = False
                    for npz_file in output_files:
                        nifti_candidate = npz_file.replace('.npz', '.nii.gz')
                        if nifti_candidate not in conversion_map:
                            nifti_candidate = npz_file.replace('.npz', '.nii')
                        entry = conversion_map.get(nifti_candidate)
                        if entry and npz_file not in conversion_map:
                            cloned = dict(entry)
                            cloned['output_file'] = npz_file
                            conversion_map[npz_file] = cloned
                            updated = True
                    if updated:
                        cache['conversion_map'] = conversion_map
                        with open(cache_path, 'w', encoding='utf-8') as f:
                            json.dump(cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        client._write_minimal_cache(
            series_dir,
            series_name,
            modality,
            sample_dcm=sample_dcm,
            file_count=len(output_files)
        )

        return {
            'success': True,
            'method': 'npz_normalized',
            'output_files': output_files,
            'low_quality': qc_summary.get('low_quality', 0),
            'low_quality_ratio': qc_summary.get('low_quality_ratio', 0.0),
            'qc_mode': qc_summary.get('qc_mode', 'none'),
            'qc_sample_indices': qc_summary.get('qc_sample_indices', [])
        }

    except Exception as e:
        print(f"   ❌ NPZ conversion failed: {e}")
        return {'success': False, 'error': str(e)}


def convert_with_dcm2niix(
    client: "DicomClient",
    series_dir: str,
    series_name: str
) -> Dict[str, Union[bool, str, int, List[str]]]:
    """
    使用 dcm2niix 工具将 DICOM 转换为 NIfTI。

    dcm2niix 是一个高性能的 DICOM 到 NIfTI 转换工具。
    在 Linux 系统上会优先使用项目根目录下的捆绑版本。
    支持 DR、MG、DX 模态的单文件单独转换模式。

    参数:
        client: DICOM 客户端实例
        series_dir: DICOM 序列目录路径
        series_name: 序列名称

    返回:
        包含转换结果的字典：
        - success: 是否成功
        - method: 'dcm2niix'
        - output_file(s): 输出文件路径
        - modality: 影像模态
        - conversion_mode: 'series' 或 'individual'
        - file_count: 成功转换的文件数
        - error: 错误信息（失败时）
    """
    try:
        # Choose dcm2niix command based on platform. Prefer bundled binaries
        # located at the project root when available.
        dcm2niix_cmd = 'dcm2niix'
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        if sys.platform.startswith('linux'):
            bundled = os.path.join(base_dir, 'dcm2niix')
            if os.path.exists(bundled):
                if not os.access(bundled, os.X_OK):
                    try:
                        os.chmod(bundled, 0o755)
                    except Exception:
                        pass
                if os.access(bundled, os.X_OK):
                    dcm2niix_cmd = bundled
        elif sys.platform.startswith('win'):
            bundled = os.path.join(base_dir, 'dcm2niix.exe')
            if os.path.exists(bundled):
                dcm2niix_cmd = bundled

        try:
            subprocess.run([dcm2niix_cmd, '-h'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("dcm2niix not found or not executable: %s", dcm2niix_cmd)
            return {'success': False, 'error': 'dcm2niix not available'}

        # 等待文件系统稳定，并收集 DICOM 文件（最多重试3次）
        dicom_files: List[str] = []
        for attempt in range(3):
            dicom_files = []
            for file in os.listdir(series_dir):
                filepath = os.path.join(series_dir, file)
                if file.endswith('.dcm') and os.path.isfile(filepath):
                    dicom_files.append(filepath)
            
            if dicom_files:
                break
            
            if attempt < 2:
                logger.info("No DICOM files found in %s, waiting 0.5s and retrying... (attempt %d/3)", series_dir, attempt + 1)
                time.sleep(0.5)

        if not dicom_files:
            logger.warning("No DICOM files found in series directory: %s", series_dir)
            return {'success': False, 'error': 'No DICOM files found'}

        modality = ''
        sample_tags = client._load_sample_tags_from_cache(series_dir)
        if isinstance(sample_tags, dict):
            modality = str(sample_tags.get('Modality') or '')
        if not modality:
            first_dcm = pydicom.dcmread(dicom_files[0], force=True)
            modality = getattr(first_dcm, 'Modality', '')

        output_name = client._sanitize_folder_name(series_name)

        if modality in ['DR', 'MG', 'DX', 'CR']:
            logger.info("Detected %s modality, converting each DICOM to NIfTI", modality)

            success_count = 0
            output_files: List[str] = []
            conversion_entries: List[Dict[str, str]] = []

            for idx, dcm_file in enumerate(dicom_files):
                temp_dir: Optional[str] = None
                try:
                    temp_dir = os.path.join(series_dir, f'temp_{idx}')
                    os.makedirs(temp_dir, exist_ok=True)

                    temp_dcm = os.path.join(temp_dir, os.path.basename(dcm_file))
                    shutil.copy2(dcm_file, temp_dcm)

                    file_output_name = f"{output_name}_{idx+1:04d}"

                    cmd = [
                        dcm2niix_cmd,
                        '-m', 'y',
                        '-f', file_output_name,
                        '-o', series_dir,
                        '-z', 'y',
                        '-b', 'n',
                        temp_dir
                    ]

                    # 使用全局锁保护 dcm2niix 调用，避免 Windows 下并发问题
                    # 添加重试机制应对 Windows 文件句柄未释放问题
                    result = None
                    for attempt in range(3):
                        with dcm2niix_global_lock:
                            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                        if result.returncode == 0:
                            break
                        if attempt < 2:
                            logger.warning("dcm2niix failed for %s (attempt %d/3), retrying in 0.5s...", file_output_name, attempt + 1)
                            time.sleep(0.5)

                    if result and result.returncode == 0:
                        nifti_file = f"{file_output_name}.nii.gz"
                        if os.path.exists(os.path.join(series_dir, nifti_file)):
                            output_files.append(nifti_file)
                            success_count += 1
                            try:
                                dcm = pydicom.dcmread(dcm_file, force=True, stop_before_pixels=True)
                                entry = _build_conversion_entry(
                                    nifti_file,
                                    dcm,
                                    file_index=idx + 1,
                                    source_file=os.path.basename(dcm_file)
                                )
                                conversion_entries.append(entry)
                            except Exception:
                                pass
                        else:
                            # dcm2niix returncode 为 0 但没有生成文件，记录详细诊断信息
                            logger.warning("dcm2niix returned 0 but no output file for %s, stdout=%s, stderr=%s", 
                                          file_output_name, 
                                          result.stdout[:300] if result.stdout else 'empty',
                                          result.stderr[:300] if result.stderr else 'empty')
                    else:
                        if result:
                            logger.warning("dcm2niix failed for %s after 3 attempts: stdout=%s, stderr=%s", 
                                          file_output_name, 
                                          result.stdout[:300] if result.stdout else 'empty',
                                          result.stderr[:300] if result.stderr else 'empty')

                    if (idx + 1) % 10 == 0:
                        logger.info("Converted %d/%d files...", idx + 1, len(dicom_files))

                except Exception as e:
                    logger.warning("Failed converting file %d: %s", idx + 1, e)
                finally:
                    if temp_dir and os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)

            if success_count > 0:
                logger.info("dcm2niix conversion succeeded: %d/%d files", success_count, len(dicom_files))

                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
                _write_conversion_map(series_dir, conversion_entries)

                for dcm_file in dicom_files:
                    try:
                        os.remove(dcm_file)
                    except Exception:
                        pass

                return {
                    'success': True,
                    'method': 'dcm2niix',
                    'modality': modality,
                    'conversion_mode': 'individual',
                    'output_files': output_files,
                    'file_count': success_count
                }
            return {'success': False, 'error': 'No files converted successfully'}

        logger.info("%s modality: converting entire series to a single NIfTI file", modality)

        cmd = [
            dcm2niix_cmd,
            '-m', 'y',
            '-f', output_name,
            '-o', series_dir,
            '-z', 'y',
            '-b', 'n',
            series_dir
        ]

        # 使用全局锁保护 dcm2niix 调用，避免 Windows 下并发问题
        # 添加重试机制应对 Windows 文件句柄未释放问题
        result = None
        for attempt in range(3):
            with dcm2niix_global_lock:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                break
            if attempt < 2:
                logger.warning("dcm2niix failed for %s (attempt %d/3), retrying in 0.5s...", output_name, attempt + 1)
                time.sleep(0.5)

        if result and result.returncode == 0:
            nifti_files = [f for f in os.listdir(series_dir) if f.endswith(('.nii.gz', '.nii'))]
            if nifti_files:
                logger.info("   ✅ dcm2niix conversion succeeded: %s", nifti_files[0])

                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)

                for file in os.listdir(series_dir):
                    if file.endswith('.dcm'):
                        try:
                            os.remove(os.path.join(series_dir, file))
                        except Exception:
                            pass

                return {
                    'success': True,
                    'method': 'dcm2niix',
                    'modality': modality,
                    'conversion_mode': 'series',
                    'output_file': nifti_files[0]
                }
            else:
                # dcm2niix returncode 为 0 但没有生成文件，记录详细诊断信息
                logger.warning("dcm2niix returned 0 but no output files for series %s, stdout=%s, stderr=%s", 
                              output_name, 
                              result.stdout[:300] if result.stdout else 'empty',
                              result.stderr[:300] if result.stderr else 'empty')

        if result and result.stderr:
            logger.warning("dcm2niix failed for series %s: stderr=%s", output_name, result.stderr[:300])
        return {'success': False, 'error': result.stderr if result else 'dcm2niix failed'}

    except Exception as e:
        logger.error("dcm2niix conversion failed: %s", e)
        return {'success': False, 'error': str(e)}


def _build_2d_xray_affine(dcm: FileDataset) -> np.ndarray:
    """
    为 2D X-ray 图像构建正确的 RAS 仿射矩阵。
    
    关键：当 DICOM 缺少 ImageOrientationPatient 时，避免使用 as_closest_canonical
    导致的 Y 轴翻转问题。直接构建标准 RAS 方向的仿射矩阵。
    
    RAS 坐标系定义：
    - X: 右 (Right) = 患者右侧
    - Y: 前 (Anterior) = 患者前方
    - Z: 上 (Superior) = 患者头部
    
    对于 2D X-ray (仰卧位，AP/PA 视图)：
    - 图像行方向 (i) 对应患者左右方向 (R/L)
    - 图像列方向 (j) 对应患者头脚方向 (S/I) 或前后方向 (A/P)
    - 标准视图：X 轴从左到右，Y 轴从上到下
    
    参数:
        dcm: pydicom Dataset 对象
        
    返回:
        4x4 仿射变换矩阵（RAS 坐标系）
    """
    # 获取像素间距
    pixel_spacing = getattr(dcm, 'PixelSpacing', None)
    if not pixel_spacing:
        pixel_spacing = getattr(dcm, 'ImagerPixelSpacing', [1.0, 1.0])
    row_spacing = float(pixel_spacing[0])  # 行间距离 (垂直方向)
    col_spacing = float(pixel_spacing[1])  # 列间距离 (水平方向)
    
    rows = int(getattr(dcm, 'Rows', 1))
    cols = int(getattr(dcm, 'Columns', 1))
    
    # 尝试从 IOP 获取方向
    iop = getattr(dcm, 'ImageOrientationPatient', None)
    
    if iop is not None:
        # 有 IOP 的情况：使用 IOP 构建仿射矩阵
        try:
            row_cosine = np.array([float(i) for i in iop[:3]], dtype=np.float64)
            col_cosine = np.array([float(i) for i in iop[3:6]], dtype=np.float64)
            slice_cosine = np.cross(row_cosine, col_cosine)
            
            # 获取图像位置
            ipp = getattr(dcm, 'ImagePositionPatient', None)
            if ipp is not None:
                origin = np.array([float(i) for i in ipp], dtype=np.float64)
            else:
                origin = np.zeros(3, dtype=np.float64)
            
            # 构建 LPS 仿射矩阵
            affine_lps = np.eye(4, dtype=np.float64)
            affine_lps[:3, 0] = row_cosine * row_spacing
            affine_lps[:3, 1] = col_cosine * col_spacing
            affine_lps[:3, 2] = slice_cosine
            affine_lps[:3, 3] = origin
            
            # 转换为 RAS: LPS_to_RAS = diag(-1, -1, 1, 1)
            lps_to_ras = np.diag([-1.0, -1.0, 1.0, 1.0])
            affine_ras = lps_to_ras @ affine_lps
            
            return affine_ras
        except Exception:
            pass  # 回退到默认方向
    
    # 缺少 IOP 的情况：构建标准 2D X-ray 方向矩阵
    # 对于标准 AP/PA X-ray 视图：
    # - 图像第1维 (行, i): 从上到下 -> Y轴从后向前 (患者仰卧)
    # - 图像第2维 (列, j): 从左到右 -> X轴从左到右
    
    # 标准 2D X-ray 方向（仰卧位）：
    # 行方向 (向下): 从患者头部到脚部 -> Y轴方向 (从后到前)
    # 列方向 (向右): 从患者左侧到右侧 -> X轴方向 (从左到右)
    
    # 注意：不使用 as_closest_canonical 以避免 Y 轴翻转
    # 直接构建 RAS 坐标系的仿射矩阵
    affine = np.eye(4, dtype=np.float64)
    
    # 第1维 (行): Y轴 (Anterior方向) - 注意：图像行向下 = Y轴向前
    affine[:3, 0] = [0.0, row_spacing, 0.0]  # 行方向映射到 Y 轴
    
    # 第2维 (列): X轴 (Right方向) - 图像列向右 = X轴向右
    affine[:3, 1] = [col_spacing, 0.0, 0.0]  # 列方向映射到 X 轴
    
    # 第3维 (切片): Z轴 (Superior方向)
    affine[:3, 2] = [0.0, 0.0, 1.0]
    
    # 原点设置为左上角 (RAS 坐标)
    # 左上角在患者坐标系中的位置：左-后-上
    affine[:3, 3] = [0.0, 0.0, 0.0]
    
    return affine


def convert_with_python_libs(
    client: "DicomClient",
    series_dir: str,
    series_name: str
) -> Dict[str, Union[bool, str, int, List[str]]]:
    """
    使用 Python 库（pydicom + nibabel）将 DICOM 转换为 NIfTI。

    当 dcm2niix 不可用时作为回退方案。支持单文件和多文件序列转换，
    自动处理切片排序、像素值重缩放和光度解释。
    
    特殊处理：
    - 2D X-ray (DX/DR/CR/MG)：正确处理缺少 ImageOrientationPatient 的情况，
      避免使用 as_closest_canonical 导致的 Y 轴翻转问题。

    参数:
        client: DICOM 客户端实例
        series_dir: DICOM 序列目录路径
        series_name: 序列名称

    返回:
        包含转换结果的字典：
        - success: 是否成功
        - method: 'python_libs'
        - output_file(s): 输出文件路径
        - modality: 影像模态
        - conversion_mode: 'series' 或 'individual'
        - file_count/slice_count: 文件数或切片数
        - error: 错误信息（失败时）
    """
    try:
        dicom_files: List[str] = []
        for file in os.listdir(series_dir):
            filepath = os.path.join(series_dir, file)
            if client._is_dicom_file(filepath):
                dicom_files.append(filepath)

        if not dicom_files:
            return {'success': False, 'error': 'No DICOM files found'}

        first_dcm = pydicom.dcmread(dicom_files[0], force=True)
        modality = getattr(first_dcm, 'Modality', '')

        if modality in ['DR', 'MG', 'DX', 'CR']:
            logger.info("Detected %s modality; converting each DICOM file to NIfTI (Python libs)", modality)

            success_count = 0
            output_files: List[str] = []
            conversion_entries: List[Dict[str, str]] = []

            for idx, dcm_file in enumerate(dicom_files):
                try:
                    dcm = pydicom.dcmread(dcm_file, force=True)

                    if not hasattr(dcm, 'pixel_array'):
                        logger.warning("File %d has no pixel data: %s", idx + 1, os.path.basename(dcm_file))
                        continue

                    # 获取像素数据并应用重缩放和光度解释
                    pixel_data = dcm.pixel_array
                    pixel_data = apply_rescale(pixel_data, dcm)
                    pixel_data = apply_photometric(pixel_data, dcm)

                    # 确保数据为 3D (添加单切片维度)
                    if len(pixel_data.shape) == 2:
                        pixel_data = pixel_data[:, :, np.newaxis]

                    # 为 2D X-ray 构建正确的仿射矩阵
                    # 关键：不使用 as_closest_canonical 以避免方向问题
                    affine = _build_2d_xray_affine(dcm)

                    # 创建 NIfTI 图像
                    nifti_img = nib.Nifti1Image(pixel_data.astype(np.float32), affine)
                    
                    # 注意：对于缺少 IOP 的 2D X-ray，不使用 as_closest_canonical
                    # 因为这会导致 Y 轴翻转，与 dcm2niix 的问题相同
                    iop = getattr(dcm, 'ImageOrientationPatient', None)
                    if iop is not None:
                        # 有 IOP 时，可以使用 canonical 转换
                        nifti_img = nib.as_closest_canonical(nifti_img)
                    # 否则：保持原方向，_build_2d_xray_affine 已经构建了正确的 RAS 矩阵

                    output_filename = f"{client._sanitize_folder_name(series_name)}_{idx+1:04d}.nii.gz"
                    output_path = os.path.join(series_dir, output_filename)
                    nib.save(nifti_img, output_path)

                    output_files.append(output_filename)
                    success_count += 1

                    # 记录转换信息
                    try:
                        entry = _build_conversion_entry(
                            output_filename,
                            dcm,
                            file_index=idx + 1,
                            source_file=os.path.basename(dcm_file)
                        )
                        conversion_entries.append(entry)
                    except Exception as e:
                        logger.debug("Failed to build conversion entry for file %d: %s", idx + 1, e)

                    if (idx + 1) % 10 == 0:
                        logger.info("Converted %d/%d files...", idx + 1, len(dicom_files))

                except Exception as e:
                    logger.warning("Failed converting file %d (%s): %s", idx + 1, os.path.basename(dcm_file), e)
                    continue

            if success_count > 0:
                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
                _write_conversion_map(series_dir, conversion_entries)
                
                # 清理原始 DICOM 文件
                for dcm_file in dicom_files:
                    try:
                        os.remove(dcm_file)
                    except Exception:
                        pass

                logger.info("   ✅ Python libs conversion succeeded: %d/%d files", success_count, len(dicom_files))
                return {
                    'success': True,
                    'method': 'python_libs',
                    'modality': modality,
                    'conversion_mode': 'individual',
                    'output_files': output_files,
                    'file_count': success_count
                }

            return {'success': False, 'error': 'No files converted successfully'}

        print(f"   ℹ️  {modality} modality: converting entire series to a single NIfTI file")

        if len(dicom_files) == 1:
            dcm = first_dcm
            if not hasattr(dcm, 'pixel_array'):
                return {'success': False, 'error': 'No pixel data'}

            pixel_data = dcm.pixel_array
            pixel_data = apply_rescale(pixel_data, dcm)
            pixel_data = apply_photometric(pixel_data, dcm)
            
            # 确保数据为 3D（添加单切片维度如果需要）
            if len(pixel_data.shape) == 2:
                pixel_data = pixel_data[:, :, np.newaxis]

            slice_thickness = float(getattr(dcm, 'SliceThickness', 1.0))
            affine = build_affine_from_dicom(dcm, slice_spacing=slice_thickness)

            nifti_img = nib.Nifti1Image(pixel_data.astype(np.float32), affine)
            
            # 对于缺少 IOP 的 2D 图像，不使用 as_closest_canonical 以避免方向问题
            iop = getattr(dcm, 'ImageOrientationPatient', None)
            if iop is not None:
                nifti_img = nib.as_closest_canonical(nifti_img)
            # 否则：保持原方向，build_affine_from_dicom 已经构建了正确的矩阵
            
            output_filename = f"{client._sanitize_folder_name(series_name)}.nii.gz"
            output_path = os.path.join(series_dir, output_filename)
            nib.save(nifti_img, output_path)

            client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
            for file in dicom_files:
                try:
                    os.remove(file)
                except Exception:
                    pass

            print(f"   ✅ Python libs conversion succeeded: {output_filename}")
            return {
                'success': True,
                'method': 'python_libs',
                'modality': modality,
                'conversion_mode': 'series',
                'output_file': output_filename
            }

        slice_info: List[Tuple[float, str, FileDataset, Optional[List[float]]]] = []
        for filepath in dicom_files:
            try:
                dcm = pydicom.dcmread(filepath, force=True)
                if hasattr(dcm, 'ImagePositionPatient'):
                    ipp = [float(v) for v in dcm.ImagePositionPatient]
                    z_pos = ipp[2]
                elif hasattr(dcm, 'SliceLocation'):
                    z_pos = float(dcm.SliceLocation)
                    ipp = None
                else:
                    z_pos = 0.0
                    ipp = None
                slice_info.append((z_pos, filepath, dcm, ipp))
            except Exception:
                continue

        if not slice_info:
            return {'success': False, 'error': 'Could not sort slices'}

        slice_info.sort(key=lambda x: x[0])

        slices: List[np.ndarray] = []
        positions: List[np.ndarray] = []
        for _, _, dcm, ipp in slice_info:
            if hasattr(dcm, 'pixel_array'):
                pixel_data = dcm.pixel_array
                pixel_data = apply_rescale(pixel_data, dcm)
                pixel_data = apply_photometric(pixel_data, dcm)
                slices.append(pixel_data)
                if ipp is not None:
                    positions.append(np.array(ipp, dtype=np.float64))

        if not slices:
            return {'success': False, 'error': 'No pixel data found'}

        # DICOM pixel_array is (Rows, Columns)
        # Stack directly: dim 0 = Rows (vertical), dim 1 = Columns (horizontal), dim 2 = Slices
        volume = np.stack(slices, axis=2)

        if len(positions) > 1:
            slice_spacing = float(np.linalg.norm(positions[1] - positions[0]))
        elif len(slice_info) > 1:
            slice_spacing = abs(slice_info[1][0] - slice_info[0][0])
        else:
            slice_spacing = float(getattr(first_dcm, 'SliceThickness', 1.0))

        iop = getattr(first_dcm, 'ImageOrientationPatient', None)
        if iop is not None:
            row_cosine = np.array([float(i) for i in iop[:3]], dtype=np.float64)
            col_cosine = np.array([float(i) for i in iop[3:6]], dtype=np.float64)
            slice_cosines = np.cross(row_cosine, col_cosine)
        else:
            slice_cosines = None

        affine = build_affine_from_dicom(first_dcm, slice_spacing=slice_spacing, slice_cosines=slice_cosines)

        nifti_img = nib.Nifti1Image(volume.astype(np.float32), affine)
        
        # 对于缺少 IOP 的序列，不使用 as_closest_canonical 以避免方向问题
        if iop is not None:
            nifti_img = nib.as_closest_canonical(nifti_img)
        # 否则：保持原方向，build_affine_from_dicom 已经处理了回退方案
        
        output_filename = f"{client._sanitize_folder_name(series_name)}.nii.gz"
        output_path = os.path.join(series_dir, output_filename)
        nib.save(nifti_img, output_path)

        client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
        for file in dicom_files:
            try:
                os.remove(file)
            except Exception:
                pass

        print(f"   ✅ Python libs conversion succeeded: {output_filename} ({len(slices)} slices)")
        return {
            'success': True,
            'method': 'python_libs',
            'modality': modality,
            'conversion_mode': 'series',
            'output_file': output_filename,
            'slice_count': len(slices)
        }

    except Exception as e:
        return {'success': False, 'error': str(e)}
