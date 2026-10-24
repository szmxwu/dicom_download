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
- 转换时方向一致性校验与镜像处方校正（verify_and_fix_orientation）

典型用法：
    from src.core.convert import convert_dicom_to_nifti, convert_to_npz
    
    result = convert_dicom_to_nifti(client, "/path/to/series", "series_name")
    result = convert_to_npz(client, "/path/to/series", "series_name")
"""

from __future__ import annotations

import logging
import locale
import os
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import nibabel as nib
import numpy as np
import pydicom
from pydicom.dataset import FileDataset

from src.utils.offload import run_cpu_bound

if TYPE_CHECKING:
    from src.client.unified import DICOMDownloadClient as DicomClient


logger = logging.getLogger('DICOMApp')

# Resolve before entering tpool; its worker must not import/patch green primitives.
try:
    from eventlet.patcher import original
    _native_subprocess = original('subprocess')
    # original(subprocess) can still refer to monkey-patched dependency modules.
    # Replace references in this private module, not in global threading/time.
    _native_subprocess.threading = original('threading')
    _native_subprocess.time = original('time')
    dcm2niix_global_lock = original('threading').Lock()
except ImportError:
    _native_subprocess = subprocess
    dcm2niix_global_lock = threading.Lock()


def _run_subprocess_with_timeout(cmd, timeout, capture_output=True, text=True):
    def run():
        # Acquire the native lock only in the worker, never on the eventlet hub.
        with dcm2niix_global_lock:
            return _run_native_subprocess(cmd, timeout, capture_output, text)
    return run_cpu_bound(run)


def _run_native_subprocess(cmd, timeout, capture_output=True, text=True):
    """Native process wait in an OS worker; temporary files avoid Windows pipe readers."""
    native = _native_subprocess
    empty = '' if text else b''
    # Unlike PIPE, file handles need no reader threads (which eventlet patches).
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            proc = native.Popen(
                cmd, stdout=stdout if capture_output else None,
                stderr=stderr if capture_output else None
            )
        except OSError as exc:
            error = str(exc) if text else str(exc).encode('utf-8', errors='replace')
            return native.CompletedProcess(cmd, -1, empty, error)

        timed_out = False
        try:
            try:
                proc.wait(timeout=timeout)
            except native.TimeoutExpired:
                timed_out = True
                # Kill the tree before the parent exits, while taskkill can find it.
                if sys.platform.startswith('win'):
                    try:
                        native.run(
                            ['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                            stdout=native.DEVNULL, stderr=native.DEVNULL, timeout=5
                        )
                    except (OSError, native.TimeoutExpired):
                        pass
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)

        stdout.seek(0)
        stderr.seek(0)
        out, err = stdout.read(), stderr.read()
        if text:
            encoding = locale.getpreferredencoding(False)
            out = out.decode(encoding, errors='replace')
            err = err.decode(encoding, errors='replace')
        if timed_out:
            message = f'Timeout after {timeout}s'
            err = (err + '\n' + message) if text else err + b'\n' + message.encode()
        return native.CompletedProcess(cmd, -1 if timed_out else proc.returncode, out, err)


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
        # pixel_array axis0 follows IOP[3:6] at row spacing;
        # axis1 follows IOP[0:3] at column spacing. No display flip belongs here.
        affine_lps[:3, 0] = col_cosine * row_spacing  # pixel_array axis0 = DICOM rows
        affine_lps[:3, 1] = row_cosine * col_spacing  # pixel_array axis1 = DICOM columns
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


def normalize_and_save_npz(nii_path_or_img, npz_path: str) -> None:
    """
    归一化并保存 NIfTI 数据为 NPZ 压缩格式。

    加载 NIfTI 文件（或直接使用传入的 nibabel 图像对象），将其转换为标准方向（canonical），
    对数据在各轴上进行翻转，并重新排列维度顺序为 (Z, Y, X)，
    最后以 float32 类型压缩保存为 NPZ 格式。

    参数:
        nii_path_or_img: 输入 NIfTI 文件路径(str)或 nib.Nifti1Image 对象
        npz_path: 输出 NPZ 文件路径
    """
    # 支持传入文件路径或 nibabel 图像对象（Python-libs 路径跳过中间 NIfTI 写入）
    if hasattr(nii_path_or_img, 'get_fdata') or hasattr(nii_path_or_img, 'dataobj'):
        img = nii_path_or_img
    else:
        img = nib.load(nii_path_or_img)
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

    # 将数据转换为 float32 并以 NPZ 格式写入磁盘。
    # 压缩策略：当前网络为内外100M，savez_compressed 默认级别6 的 CPU 开销（~58s/600MB）
    # 远超传输收益。固定使用级别1：压缩率~37%，写入时间~10s/600MB，
    # 在100M网络下总时间（写+传）最优。可通过环境变量 NPZ_COMPRESS 覆盖。
    #   0 = np.savez（不压缩）
    #   1 = np.savez_compressed 级别1（默认，100M网络推荐）
    #   6 = np.savez_compressed 默认级别6（外网慢速）
    compress_level = os.environ.get('NPZ_COMPRESS', '1')
    if compress_level == '1':
        import zlib
        old_level = zlib.Z_DEFAULT_COMPRESSION
        zlib.Z_DEFAULT_COMPRESSION = 1
        try:
            np.savez_compressed(npz_path, data=data.astype(np.float32))
        finally:
            zlib.Z_DEFAULT_COMPRESSION = old_level
    elif compress_level == '6':
        np.savez_compressed(npz_path, data=data.astype(np.float32))
    else:
        np.savez(npz_path, data=data.astype(np.float32))


# ==============================================================================
# 转换时方向一致性校验与镜像校正 (Orientation Consistency Check & Mirror Fix)
# ==============================================================================
#
# 背景：斜位扫描可能使用镜像处方（IOP 行方向 x 分量与常规反号），
# 转换器若在镜像处方下产出自相矛盾的 内容-仿射 关系（如纯 Python 兜底路径
# 的面内翻转），下游 TotalSegmentator 会把左肱骨标成 humerus_right。
#
# 设计原则：NIfTI/NPZ 的 内容+仿射 必须与 DICOM 头描述的几何严格一致。
# 校验步骤（实测校准，详见 test/test_orientation_fix.py）：
#   1. 从全部 DICOM 头重建真值体素网格几何 dicom_affine（G_d 约定：
#      axis0=rows, axis1=columns, axis2=按规范方向排序的层，RAS 坐标）；
#   2. 相对变换 T = inv(nifti_affine) @ dicom_affine 归一化后应为带符号置换矩阵，
#      否则视为重采样过，不自动改，记 QC 告警；
#   3. 用探针体素（probing）实测内容布局 L_c：比较 NIfTI 数据在 L_c(p) 处
#      与 DICOM 像素在 p 处的相关性，确认内容与仿射是否自洽；
#      - L_c != T → 仿射与内容不自洽（转换器 bug）：修正仿射使二者一致；
#   4. det(layout) < 0（左手性网格 = 镜像处方签名）→ 沿列轴（IOP 行方向，
#      即镜像发生的轴）翻转数据并同步修正仿射（一致性重排，世界几何不变），
#      使体素内容方向恢复正常处方形态。
#
# 实测结论（v1.0.20250505 捆绑 dcm2niix，合成斜位序列验证）：
#   - dcm2niix 对正常/镜像处方均为忠实转换（内容与仿射一致）；
#     镜像处方输出 det(T)<0 是其固定布局惯例，需要第 4 步的重排校正；
#   - 纯 Python 兜底路径存在面内镜像 bug（仿射列方向与内容相反），
#     由第 3 步检测并修正仿射。

# 方向校验结果动作常量（与 qc.QualityReasons 的字符串保持一致）
ORIENTATION_CHECK_OK = 'ok'
ORIENTATION_CHECK_MIRROR_CORRECTED = 'orientation_mirror_corrected'
ORIENTATION_CHECK_NOT_VERIFIABLE = 'orientation_not_verifiable'
ORIENTATION_CHECK_SKIPPED = 'skipped'

# 探针相关阈值
_PROBE_CORR_THRESHOLD = 0.995      # 布局判定的皮尔逊相关系数阈值
_PERM_TOL = 0.02                   # 置换矩阵元素容差
_TRANS_TOL = 0.1                   # 平移整数性容差（体素）


def _canonical_slice_order(positions: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    按规范方向对层位置排序。

    排序方向取层分布主轴（世界坐标 |分量| 最大的轴）的正方向，
    与 IOP 叉积无关——这样左手性（镜像处方）才能体现为
    cross(X,Y) 与排序步进方向的反号。

    参数:
        positions: (N, 3) 的 IPP 数组（LPS 坐标）

    返回:
        (order, step_vec): 排序索引与排序后相邻差分均值向量
    """
    positions = np.asarray(positions, dtype=np.float64)
    if len(positions) < 2:
        return np.arange(len(positions)), np.zeros(3, dtype=np.float64)
    # O(N) memory. The former all-pairs NxNx3 allocation could exhaust memory
    # on long CTA, and its sign depended on input enumeration order.
    spread = np.ptp(positions, axis=0)
    dom = int(np.argmax(np.abs(spread)))
    step_dir = np.zeros(3, dtype=np.float64)
    step_dir[dom] = 1.0
    order = np.argsort(positions @ step_dir, kind='stable')
    sorted_pos = positions[order]
    step_vec = np.diff(sorted_pos, axis=0).mean(axis=0)
    return order, step_vec


def build_dicom_truth_affine(datasets: List[FileDataset]) -> Optional[Tuple[np.ndarray, List[FileDataset]]]:
    """
    从序列全部 DICOM 头重建真值体素网格几何（G_d 约定，RAS 坐标）。

    G_d 体素网格定义：axis0 = rows (r)，axis1 = columns (c)，axis2 = 规范排序后的层 (s)。
    仿射列：Y*row_spacing（IOP[3:6]，r 递增方向）、X*col_spacing（IOP[0:3]，c 递增方向）、
    排序后 IPP 差分估计的层步进、首层 IPP。

    参数:
        datasets: pydicom Dataset 列表（需含 IOP/IPP/PixelSpacing）

    返回:
        (affine_ras, sorted_datasets) 或 None（缺少几何标签时）
    """
    try:
        first = datasets[0]
        iop = getattr(first, 'ImageOrientationPatient', None)
        pixel_spacing = getattr(first, 'PixelSpacing', None)
        if iop is None or pixel_spacing is None:
            return None
        positions = []
        for ds in datasets:
            ipp = getattr(ds, 'ImagePositionPatient', None)
            if ipp is None:
                return None
            positions.append([float(v) for v in ipp])
        positions = np.asarray(positions, dtype=np.float64)

        if not np.isfinite(positions).all():return None
        reference_iop=np.asarray(iop,dtype=float)
        reference_spacing=np.asarray(pixel_spacing,dtype=float)
        if reference_iop.shape!=(6,) or reference_spacing.shape!=(2,) or (reference_spacing<=0).any():return None
        if not (np.isclose(np.linalg.norm(reference_iop[:3]),1,atol=.01) and
                np.isclose(np.linalg.norm(reference_iop[3:]),1,atol=.01) and
                abs(float(reference_iop[:3]@reference_iop[3:]))<.01):return None
        for ds in datasets:
            if not np.allclose(getattr(ds,'ImageOrientationPatient',[]),reference_iop,atol=1e-4):return None
            if not np.allclose(getattr(ds,'PixelSpacing',[]),reference_spacing,atol=1e-4):return None
            if (int(getattr(ds, 'Rows', 0)), int(getattr(ds, 'Columns', 0))) != (
                    int(getattr(first, 'Rows', 0)), int(getattr(first, 'Columns', 0))):return None
            if int(getattr(ds, 'NumberOfFrames', 1)) != 1:return None
        # Matching geometry does not make different source series one volume.
        # Compare identity strings without silently normalizing/replacing UIDs.
        for tag in ('StudyInstanceUID', 'SeriesInstanceUID', 'FrameOfReferenceUID'):
            if len({str(getattr(ds, tag, '')) for ds in datasets}) != 1:return None
        sops = [str(getattr(ds, 'SOPInstanceUID', '')) for ds in datasets]
        if any(sops) and (not all(sops) or len(set(sops)) != len(sops)):return None

        order, step_vec = _canonical_slice_order(positions)
        if len(datasets)>1:
            increments=np.diff(positions[order],axis=0)
            if np.linalg.norm(step_vec)<1e-6 or not np.allclose(increments,step_vec,atol=.05,rtol=.01):
                return None  # duplicates/missing slices/nonuniform grids are not one affine
        sorted_datasets = [datasets[i] for i in order]
        if len(datasets) == 1:
            # 单层：步进方向用叉积法向 * 层厚
            row_cos = np.array([float(v) for v in iop[:3]], dtype=np.float64)
            col_cos = np.array([float(v) for v in iop[3:6]], dtype=np.float64)
            thickness = float(getattr(first, 'SliceThickness', 1.0))
            step_vec = np.cross(row_cos, col_cos) * thickness

        row_cos = np.array([float(v) for v in iop[:3]], dtype=np.float64)  # X：c 递增方向
        col_cos = np.array([float(v) for v in iop[3:6]], dtype=np.float64)  # Y：r 递增方向
        row_spacing = float(pixel_spacing[0])  # Δr
        col_spacing = float(pixel_spacing[1])  # Δc

        affine_lps = np.eye(4, dtype=np.float64)
        affine_lps[:3, 0] = col_cos * row_spacing   # axis0 = rows：沿 IOP[3:6]
        affine_lps[:3, 1] = row_cos * col_spacing   # axis1 = columns：沿 IOP[0:3]
        affine_lps[:3, 2] = step_vec
        affine_lps[:3, 3] = positions[order[0]]

        lps_to_ras = np.diag([-1.0, -1.0, 1.0, 1.0])
        return lps_to_ras @ affine_lps, sorted_datasets
    except Exception as e:
        logger.warning("Failed to build DICOM truth affine: %s", e)
        return None


def _ct_source_set_error(datasets, expected_count):
    """Do not certify a readable subset or unsupported enhanced CT as a series."""
    if not datasets or len(datasets) != expected_count:
        return 'CT source header set incomplete; original DICOM retained'
    if any(not str(getattr(ds, tag, '')) for ds in datasets
           for tag in ('StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID')):
        return 'CT source identity missing; original DICOM retained'
    if any(int(getattr(ds, 'NumberOfFrames', 1)) != 1 for ds in datasets):
        return 'Multiframe CT per-frame geometry unsupported; original DICOM retained'
    return None


def _analyze_signed_permutation(T: np.ndarray) -> Optional[Tuple[np.ndarray, np.ndarray]]:
    """
    判断相对变换 T 是否为带符号置换矩阵（归一化后元素为 0/±1，平移为整数体素）。

    返回:
        (perm_matrix, translation) 或 None（不是置换矩阵，说明被重采样/插值过）
    """
    M = np.array(T[:3, :3], dtype=np.float64)
    for j in range(3):
        norm = float(np.linalg.norm(M[:, j]))
        if norm <= 0:
            return None
        M[:, j] /= norm
    rounded = np.round(M)
    if not np.allclose(M, rounded, atol=_PERM_TOL, rtol=0):
        return None
    if not (np.all(np.abs(rounded).sum(axis=0) == 1) and np.all(np.abs(rounded).sum(axis=1) == 1)):
        return None
    translation = T[:3, 3]
    if not np.allclose(translation, np.round(translation), atol=_TRANS_TOL, rtol=0):
        return None
    return rounded.astype(np.int64), np.round(translation).astype(np.int64)


def _layout_matrix(perm: Tuple[int, int, int], flips: Tuple[bool, bool, bool],
                   gd_shape: Tuple[int, int, int]) -> np.ndarray:
    """
    由布局描述构造 4x4 映射矩阵：nifti_idx = L @ gd_idx。

    布局约定：np.transpose(V, perm) 后按 flips 翻转对应轴，
    即 G_d 轴 perm[a] 映射到 NIfTI 轴 a，翻转时平移 dim-1。
    """
    L = np.eye(4, dtype=np.float64)
    for a in range(3):
        b = perm[a]
        L[a, :] = 0.0
        if flips[a]:
            L[a, b] = -1.0
            L[a, 3] = float(gd_shape[b] - 1)
        else:
            L[a, b] = 1.0
    return L


def _probe_content_layout(
    data: np.ndarray,
    sorted_datasets: List[FileDataset],
    max_probe_slices: int = 5,
    slice_loader=None
) -> Optional[Tuple[np.ndarray, float]]:
    """
    用探针体素实测 NIfTI 内容布局。

    在 G_d 网格上采样探针点（若干层的 (r,c) 网格），对全部 48 种带符号置换
    候选布局，计算 NIfTI 数据在 L(p) 处与 DICOM 像素在 p 处的皮尔逊相关系数，
    按绝对值最大选取相关度达标的布局矩阵。

    返回的相关系数带符号：强正 = 内容一致；强负 = 几何布局正确但灰度被
    转换器反转（调用方据此还原灰度）。

    参数:
        data: NIfTI 体数据（3D）
        sorted_datasets: 按 G_d 规范排序的 DICOM 数据集（与真值仿射一致）
        max_probe_slices: 最多抽取的层数
        slice_loader: 可选，按序号返回含像素的 Dataset（默认直接用 sorted_datasets）

    返回:
        (layout_matrix, correlation) 或 None（无法判定：空白数据或无候选达标）
    """
    import itertools

    n_slices = len(sorted_datasets)
    if n_slices == 0:
        return None
    if slice_loader is None:
        slice_loader = lambda i: sorted_datasets[i]
    rows = int(getattr(sorted_datasets[0], 'Rows', data.shape[0]))
    cols = int(getattr(sorted_datasets[0], 'Columns', data.shape[1]))
    gd_shape = (rows, cols, n_slices)
    if data.ndim != 3:
        return None

    # 探针点：均匀抽取若干层，每层 6x6 的 (r,c) 网格（内缩 15% 避开边缘）
    slice_indices = np.linspace(0, n_slices - 1, min(max_probe_slices, n_slices)).astype(int)
    r_grid = np.linspace(int(rows * 0.15), int(rows * 0.85), 6).astype(int)
    c_grid = np.linspace(int(cols * 0.15), int(cols * 0.85), 6).astype(int)

    probe_pts: List[Tuple[int, int, int]] = []
    probe_vals: List[float] = []
    for s in slice_indices:
        try:
            ds = slice_loader(int(s))
            pixels = ds.pixel_array
        except Exception:
            return None
        pixels = apply_rescale(pixels, ds)
        pixels = apply_photometric(pixels, ds)
        for r in r_grid:
            for c in c_grid:
                probe_pts.append((int(r), int(c), int(s)))
                probe_vals.append(float(pixels[int(r), int(c)]))
    probe_vals_arr = np.asarray(probe_vals, dtype=np.float64)
    if probe_vals_arr.std() < 1e-6:
        # 空白/均匀数据，无法探针判定
        return None
    pts = np.asarray(probe_pts, dtype=np.float64)  # (N, 3)

    best_layout = None
    best_corr = 0.0  # 带符号最佳相关：按绝对值最大选取；强负 = 内容灰度被反转
    for perm in itertools.permutations(range(3)):
        expected_shape = tuple(gd_shape[perm[a]] for a in range(3))
        if expected_shape != tuple(data.shape[:3]):
            continue
        for flips in itertools.product([False, True], repeat=3):
            L = _layout_matrix(perm, flips, gd_shape)
            q = (L[:3, :] @ np.hstack([pts, np.ones((len(pts), 1))]).T)[:3, :]
            qi = np.round(q).astype(np.int64)
            if (qi < 0).any():
                continue
            if any(qi[a].max() >= data.shape[a] for a in range(3)):
                continue
            gathered = data[qi[0], qi[1], qi[2]].astype(np.float64)
            if gathered.std() < 1e-6:
                continue
            corr = float(np.corrcoef(gathered, probe_vals_arr)[0, 1])
            if abs(corr) > abs(best_corr):
                best_corr = corr
                best_layout = L
    if best_layout is not None and abs(best_corr) >= _PROBE_CORR_THRESHOLD:
        return best_layout, best_corr
    return None


def _flip_image_axis(data: np.ndarray, affine: np.ndarray, axis: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    沿指定体素轴翻转数据并同步修正仿射（一致性重排，世界几何不变）。
    """
    flipped = np.flip(data, axis=axis)
    F = np.eye(4, dtype=np.float64)
    F[axis, axis] = -1.0
    F[axis, 3] = float(data.shape[axis] - 1)
    return flipped, affine @ F


def _rebuild_nifti(img: nib.Nifti1Image, data: np.ndarray, affine: np.ndarray) -> nib.Nifti1Image:
    """以新数据/仿射重建 NIfTI 图像，保留原 header 并同步 qform/sform。"""
    header = img.header.copy() if img.header is not None else None
    values=np.asanyarray(data)
    target_dtype=img.get_data_dtype()
    # ArrayProxy has already applied slope/intercept. Letting nibabel write
    # float HU through the old integer header auto-quantizes them to a new
    # slope. Keep exact integer HU when representable; otherwise keep the
    # actual float dtype instead of introducing a second quantization.
    if np.issubdtype(target_dtype,np.integer) and np.issubdtype(values.dtype,np.floating):
        limits=np.iinfo(target_dtype);exact_integer=True
        for chunk in np.nditer(values,flags=['external_loop','buffered'],buffersize=1024*1024):
            if not (np.isfinite(chunk).all() and (chunk>=limits.min).all() and (chunk<=limits.max).all() and np.equal(chunk,np.rint(chunk)).all()):
                exact_integer=False;break
        if exact_integer:values=values.astype(target_dtype)
    new_img = nib.Nifti1Image(values, affine, header)
    new_img.set_data_dtype(values.dtype)
    qcode = int(img.header['qform_code']) if img.header is not None else 1
    scode = int(img.header['sform_code']) if img.header is not None else 1
    new_img.set_sform(affine, code=scode if scode > 0 else 1)
    try:
        new_img.set_qform(affine, code=qcode if qcode > 0 else 1, strip_shears=False)
    except nib.spatialimages.HeaderDataError:
        # qform cannot represent shear. Retain the source-proven sform rather
        # than advertise a second, approximated physical geometry as valid.
        new_img.set_qform(None, code=0)
    return new_img


def verify_and_fix_orientation(
    nifti_img: nib.Nifti1Image,
    datasets: List[FileDataset],
    series_name: str = '',
    slice_loader=None
) -> Tuple[nib.Nifti1Image, Dict[str, Any]]:
    """
    校验 NIfTI 内容+仿射与 DICOM 头描述几何的一致性，必要时校正。

    流程：重建真值仿射 → 置换矩阵判定 → 探针实测内容布局 →
    仿射不自洽则修正仿射 → 左手性网格（镜像处方）则沿列轴重排去镜像。

    注意：该校验设计为转换时执行一次。若对已校正的 NIfTI 用同一批 DICOM
    重复执行，去镜像重排会再次翻转（网格 handedness 签名不变），请勿重复调用。

    参数:
        nifti_img: 转换产出的 NIfTI 图像（内存对象）
        datasets: 该序列的 DICOM 数据集列表（顺序不限，可不含像素）
        series_name: 序列名（日志用）
        slice_loader: 可选，按排序后序号返回含像素的 Dataset（探针用）

    返回:
        (fixed_img, result)：result 含 action/det_t/flip_axis/corr/message 等字段，
        action ∈ {ok, orientation_mirror_corrected, orientation_not_verifiable, skipped}
    """
    result: Dict[str, Any] = {
        'action': ORIENTATION_CHECK_OK,
        'fixes': [],
        'det_t': None,
        'det_grid': None,
        'flip_axis': None,
        'probe_corr': None,
        'message': ''
    }
    try:
        data = np.asanyarray(nifti_img.dataobj)
        if data.ndim != 3:
            result['action'] = ORIENTATION_CHECK_SKIPPED
            result['message'] = f'non-3D data (ndim={data.ndim}), check skipped'
            return nifti_img, result

        truth = build_dicom_truth_affine(datasets)
        if truth is None:
            result['action'] = ORIENTATION_CHECK_SKIPPED
            result['message'] = 'DICOM geometry missing, inconsistent, or nonuniform; verification skipped'
            return nifti_img, result
        affine_d, sorted_datasets = truth

        nifti_affine = np.array(nifti_img.affine, dtype=np.float64)
        T = np.linalg.inv(nifti_affine) @ affine_d
        perm = _analyze_signed_permutation(T)
        if perm is not None:
            perm_matrix, _ = perm
            result['det_t'] = float(np.linalg.det(perm_matrix))

        # 探针实测内容布局
        probe = _probe_content_layout(data, sorted_datasets, slice_loader=slice_loader)
        layout_matrix = None
        if probe is not None:
            layout_matrix, corr = probe
            result['probe_corr'] = corr
            if corr <= -_PROBE_CORR_THRESHOLD:
                # 正确布局下相关强负 = 转换器把体数据灰度反转了
                # （如 dcm2niix 对 MONOCHROME1 处理不当的已知 bug；MR 偶发反转实证）。
                # 以源 DICOM 像素为真值还原，再做后续仿射/镜像校验。
                # max+min-v 在转换器纯反转模型（v'=C-v）下是精确还原。
                data = float(data.max()) + float(data.min()) - data
                nifti_img = _rebuild_nifti(nifti_img, data, nifti_affine)
                result['fixes'].append('grayscale_inversion')
                logger.warning(
                    "[ORIENTATION] %s: grayscale inversion detected (probe corr=%.4f), "
                    "content inverted back to match DICOM pixels",
                    series_name, corr
                )
            # Check physical corner positions too: a tiny spacing error on a
            # long CT can accumulate beyond the voxel-layout tolerance.
            fixed_affine = affine_d @ np.linalg.inv(layout_matrix)
            import itertools
            corners = np.array([(*point, 1.) for point in itertools.product(
                *[(0., float(size-1)) for size in data.shape])])
            errors = (corners @ (nifti_affine-fixed_affine).T)[:, :3]
            result['max_source_geometry_error_mm'] = float(np.linalg.norm(errors, axis=1).max())
            # 内容与仿射是否自洽：仿射隐含的布局（T）应等于实测内容布局
            if (not np.allclose(layout_matrix, T, atol=_PERM_TOL, rtol=0) or
                    result['max_source_geometry_error_mm'] > .05):
                # 不自洽（转换器 bug，如纯 Python 路径面内镜像）：修正仿射匹配实测布局
                nifti_img = _rebuild_nifti(nifti_img, data, fixed_affine)
                nifti_affine = fixed_affine
                result['fixes'].append('affine_mismatch')
                logger.warning(
                    "[ORIENTATION] %s: content-affine mismatch (probe corr=%.4f), affine corrected",
                    series_name, corr
                )
        else:
            # Header agreement is insufficient. Preserve raw DICOM on failure;
            # never relayout content based only on handedness or FFS.
            result['action']=ORIENTATION_CHECK_NOT_VERIFIABLE
            result['message']='source pixel layout could not be verified'
            return nifti_img,result

        # The preferred affine alone cannot clear contradictory encoded forms.
        # This branch is reached only after independent source pixel probing.
        qform, qcode = nifti_img.get_qform(coded=True)
        sform, scode = nifti_img.get_sform(coded=True)
        forms_mismatch = (not scode or not np.allclose(sform, nifti_affine, atol=1e-3, rtol=0)
                          or (qcode and not np.allclose(qform, nifti_affine, atol=1e-3, rtol=0)))
        if forms_mismatch:
            nifti_img = _rebuild_nifti(nifti_img, data, nifti_affine)
            result['fixes'].append('geometry_forms_mismatch')

        # 左手性网格 = 镜像处方签名：真值网格（规范排序）在 RAS 下 det>0。
        # 该签名只取决于 DICOM 几何，与转换器布局惯例无关（实测：
        # dcm2niix 镜像输出 det(T)<0 即源于此；python 路径仿射修正前后 det 会变，不可用）。
        # 去镜像：沿列轴（IOP 行方向，G_d axis1，即镜像发生的轴）翻转数据并同步仿射。
        det_grid = float(np.linalg.det(affine_d[:3, :3]))
        result['det_grid'] = det_grid
        if det_grid > 0:
            # 列轴（G_d axis1）映射到的 NIfTI 轴
            flip_axis = None
            for a in range(3):
                if abs(layout_matrix[a, 1]) > 0.5:
                    flip_axis = a
                    break
            if flip_axis is None:
                flip_axis = 1
            new_data, new_affine = _flip_image_axis(
                np.asanyarray(nifti_img.dataobj), nifti_affine, flip_axis
            )
            nifti_img = _rebuild_nifti(nifti_img, new_data, new_affine)
            result['fixes'].append('mirror_relayout')
            result['flip_axis'] = int(flip_axis)
            logger.warning(
                "[ORIENTATION] %s: mirrored prescription detected (det(grid)=%.1f), "
                "flipped axis %d to un-mirror content",
                series_name, det_grid, flip_axis
            )

        if result['fixes']:
            result['action'] = ORIENTATION_CHECK_MIRROR_CORRECTED
            result['message'] = ','.join(result['fixes'])
        return nifti_img, result
    except Exception as e:
        logger.warning("[ORIENTATION] %s: orientation check failed: %s", series_name, e)
        result['action'] = ORIENTATION_CHECK_SKIPPED
        result['message'] = f'check error: {e}'
        return nifti_img, result


def verify_and_fix_orientation_file(
    nifti_path: str,
    dicom_files: List[str],
    series_name: str = ''
) -> Dict[str, Any]:
    """
    对已写入磁盘的 NIfTI 文件执行方向一致性校验与校正（dcm2niix 路径）。

    几何标签从全部文件读取（不含像素），像素仅在校验阶段按探针层读取。
    若发生校正，用临时文件+重命名覆盖原文件。

    返回:
        verify_and_fix_orientation 的 result 字典
    """
    try:
        # 先只读头（不含像素）构建几何，探针层按需重读像素，避免大序列全量读像素
        datasets: List[FileDataset] = []
        readable_files: List[str] = []
        for filepath in dicom_files:
            try:
                datasets.append(pydicom.dcmread(filepath, force=True, stop_before_pixels=True))
                readable_files.append(filepath)
            except Exception:
                continue
        if len(datasets) != len(dicom_files) or not datasets:
            return {'action': ORIENTATION_CHECK_SKIPPED, 'fixes': [],
                    'det_t': None, 'det_grid': None, 'flip_axis': None,
                    'probe_corr': None, 'message': 'source DICOM header set incomplete'}
        if any(not str(getattr(ds, tag, '')) for ds in datasets
               for tag in ('StudyInstanceUID', 'SeriesInstanceUID', 'SOPInstanceUID')):
            return {'action': ORIENTATION_CHECK_NOT_VERIFIABLE, 'fixes': [],
                    'probe_corr': None, 'message': 'source DICOM identity missing'}
        # 与 build_dicom_truth_affine 相同的排序，供探针层定位源文件
        file_of_dataset = {id(ds): f for ds, f in zip(datasets, readable_files)}

        img = nib.load(nifti_path)
        # 构造按排序后序号读取探针层像素的 loader；
        # verify_and_fix_orientation 内部用同一 _canonical_slice_order 排序，顺序一致
        with_ipp = [(ds, file_of_dataset[id(ds)]) for ds in datasets
                    if getattr(ds, 'ImagePositionPatient', None) is not None]
        sorted_files = None
        if len(with_ipp) == len(datasets) and len(with_ipp) > 1:
            positions = np.array([[float(v) for v in ds.ImagePositionPatient] for ds, _ in with_ipp])
            order, _ = _canonical_slice_order(positions)
            sorted_files = [with_ipp[i][1] for i in order]

        slice_loader = None
        if len(readable_files) == 1:
            slice_loader = lambda index: pydicom.dcmread(readable_files[index], force=True)
        if sorted_files is not None:
            def slice_loader(sorted_idx):
                return pydicom.dcmread(sorted_files[sorted_idx], force=True)

        fixed_img, result = verify_and_fix_orientation(
            img, datasets, series_name,
            slice_loader=slice_loader
        )
        if result.get('fixes'):
            temp_dir = os.path.dirname(nifti_path)
            fd, temp_path = tempfile.mkstemp(suffix='.nii.gz', dir=temp_dir)
            try:
                os.close(fd)
                nib.save(fixed_img, temp_path)
                original_stat = os.stat(nifti_path)
                shutil.move(temp_path, nifti_path)
                os.chmod(nifti_path, original_stat.st_mode)
            finally:
                if os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                    except Exception:
                        pass
        return result
    except Exception as e:
        logger.warning("[ORIENTATION] %s: file-level orientation check failed: %s", series_name, e)
        return {'action': ORIENTATION_CHECK_SKIPPED, 'fixes': [],
                'det_t': None, 'det_grid': None, 'flip_axis': None,
                'probe_corr': None, 'message': f'check error: {e}'}


def convert_dicom_to_nifti(
    client: "DicomClient",
    series_dir: str,
    series_name: str,
    dicom_files: Optional[List[str]] = None,
    sample_dcm=None,
    modality: Optional[str] = None
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
        dicom_files: 可选，已扫描的 DICOM 文件列表（避免重复扫描）
        sample_dcm: 可选，已读取的样本 DICOM 对象（避免重复读取）
        modality: 可选，已知的影像模态（避免重复读取）

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

        if sample_dcm is None or not modality:
            sample_dcm, modality = client._get_series_sample_dicom(series_dir)

        if dicom_files is None:
            dicom_files = []
            try:
                for file in os.listdir(series_dir):
                    if file.startswith('.'):
                        continue
                    filepath = os.path.join(series_dir, file)
                    if os.path.isfile(filepath) and client._is_dicom_file(filepath):
                        dicom_files.append(filepath)
            except Exception:
                dicom_files = []

        if dicom_files:
            logger.info(f"[CONVERT_CACHE] Caching metadata before conversion: series={series_name}, files={len(dicom_files)}, modality={modality}")
            client._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
            client._write_minimal_cache(
                series_dir,
                series_name,
                modality,
                sample_dcm=sample_dcm,
                file_count=len(dicom_files)
            )
        else:
            logger.warning(f"[CONVERT_CACHE] No dicom_files to cache for {series_name} before conversion")

        nifti_result = convert_with_dcm2niix(
            client, series_dir, series_name,
            dicom_files=dicom_files, sample_dcm=sample_dcm, modality=modality
        )
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
        # CPU 密集（pydicom 批量解析 + numpy 堆叠 + nibabel gzip 写盘），
        # eventlet 下必须卸载到真实线程，否则整个 HTTP/SocketIO 层冻结（页面"卡死"）。
        nifti_result = run_cpu_bound(
            convert_with_python_libs,
            client, series_dir, series_name,
            dicom_files=dicom_files, sample_dcm=sample_dcm, modality=modality
        )
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
    series_name: str,
    dicom_files: Optional[List[str]] = None,
    sample_dcm=None,
    modality: Optional[str] = None
) -> Dict[str, Union[bool, str, int, float, List[str], List[int]]]:
    """
    将 DICOM 序列转换为归一化的 NPZ 格式。

    首先将 DICOM 转换为 NIfTI，然后将 NIfTI 数据归一化并保存为 NPZ 格式。
    支持批量转换多文件序列。转换完成后进行质量控制评估。
    Python-libs 回退路径会跳过中间 NIfTI 文件写入，直接在内存中生成 NPZ。

    参数:
        client: DICOM 客户端实例，提供辅助方法
        series_dir: DICOM 序列目录路径
        series_name: 序列名称
        dicom_files: 可选，已扫描的 DICOM 文件列表（避免重复扫描）
        sample_dcm: 可选，已读取的样本 DICOM 对象（避免重复读取）
        modality: 可选，已知的影像模态（避免重复读取）

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

        if sample_dcm is None or not modality:
            sample_dcm, modality = client._get_series_sample_dicom(series_dir)

        nifti_res = convert_with_dcm2niix(
            client, series_dir, series_name,
            dicom_files=dicom_files, sample_dcm=sample_dcm, modality=modality
        )
        intermediate_nifti_files = []
        if isinstance(nifti_res, dict):
            intermediate_nifti_files.extend(nifti_res.get('output_files') or [])
            if nifti_res.get('output_file'):
                intermediate_nifti_files.append(nifti_res['output_file'])
        used_python_libs_memory = False
        if not (nifti_res and nifti_res.get('success')):
            # Python-libs 回退：跳过中间 NIfTI 写入，直接在内存中生成 NPZ
            nifti_res = convert_with_python_libs(
                client, series_dir, series_name,
                dicom_files=dicom_files, sample_dcm=sample_dcm, modality=modality,
                save_to_disk=False
            )
            used_python_libs_memory = True

        if not (nifti_res and nifti_res.get('success')):
            return {'success': False, 'error': 'Failed to generate base volume for NPZ'}

        output_files: List[str] = []
        if nifti_res.get('conversion_mode') == 'individual':
            if used_python_libs_memory and 'output_images' in nifti_res:
                # 内存对象路径：直接传入 nibabel image 生成 NPZ
                for nifti_img, nii_file in nifti_res.get('output_images', []):
                    npz_file = nii_file.replace('.nii.gz', '.npz').replace('.nii', '.npz')
                    npz_path = os.path.join(series_dir, npz_file)
                    normalize_and_save_npz(nifti_img, npz_path)
                    output_files.append(npz_file)
            else:
                # dcm2niix 路径：从磁盘读取中间 NIfTI
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
            npz_file = nii_file.replace('.nii.gz', '.npz').replace('.nii', '.npz')
            npz_path = os.path.join(series_dir, npz_file)

            if used_python_libs_memory and 'nifti_img' in nifti_res:
                normalize_and_save_npz(nifti_res['nifti_img'], npz_path)
            else:
                nii_path = os.path.join(series_dir, nii_file)
                normalize_and_save_npz(nii_path, npz_path)
                if os.path.exists(nii_path):
                    os.remove(nii_path)
            output_files.append(npz_file)

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

        # NPZ 是唯一需要交付的图像格式。Python 内存回退不会在转换阶段
        # 清理源文件，因此必须在这里递归删除源 DICOM 和中间 NIfTI；
        # 删除失败由外层返回失败，禁止把含患者头信息的文件打入 ZIP。
        _remove_intermediate_nifti(series_dir)
        _remove_source_dicoms(series_dir, dicom_files)

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
    series_name: str,
    dicom_files: Optional[List[str]] = None,
    sample_dcm=None,
    modality: Optional[str] = None
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
        dicom_files: 可选，已扫描的 DICOM 文件列表（避免重复扫描）
        sample_dcm: 可选，已读取的样本 DICOM 对象（避免重复读取）
        modality: 可选，已知的影像模态（避免重复读取）

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
            probe = _run_subprocess_with_timeout([dcm2niix_cmd, '-h'], timeout=10)
            if probe.returncode != 0:
                return {'success': False, 'error': 'dcm2niix not available: ' + str(probe.stderr)}
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("dcm2niix not found or not executable: %s", dcm2niix_cmd)
            return {'success': False, 'error': 'dcm2niix not available'}

        # 等待文件系统稳定，并收集 DICOM 文件（最多重试3次）
        if dicom_files is None:
            dicom_files = []
            for attempt in range(3):
                dicom_files = []
                for file in os.listdir(series_dir):
                    if file.startswith('.'):
                        continue
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

        if modality:
            pass  # 使用传入的 modality
        else:
            sample_tags = client._load_sample_tags_from_cache(series_dir)
            if isinstance(sample_tags, dict):
                modality = str(sample_tags.get('Modality') or '')
            if not modality:
                first_dcm = sample_dcm if sample_dcm is not None else pydicom.dcmread(dicom_files[0], force=True)
                modality = getattr(first_dcm, 'Modality', '')

        output_name = client._sanitize_folder_name(series_name)

        if modality in ['DR', 'MG', 'DX', 'CR', 'RF']:
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
                        result = _run_subprocess_with_timeout(cmd, timeout=60)
                        if result.returncode == 0:
                            break
                        if attempt < 2:
                            logger.warning("dcm2niix failed for %s (attempt %d/3), retrying in 0.5s...", file_output_name, attempt + 1)
                            time.sleep(0.5)

                    if result and result.returncode == 0:
                        nifti_file = f"{file_output_name}.nii.gz"
                        nifti_path = os.path.join(series_dir, nifti_file)
                        if os.path.exists(nifti_path):
                            try:
                                dcm = pydicom.dcmread(dcm_file, force=True)
                                # 只有源像素证明布局可信的文件才可交付；不可信/空白结果
                                # 会计入转换失败并触发 Python 源像素回退，最终失败则放弃。
                                normalization = run_cpu_bound(
                                    normalize_2d_nifti_display, nifti_path, dcm, series_name
                                )
                                action = normalization.get('action')
                                if action not in ('normalized', 'inverted_normalized', 'ok'):
                                    logger.warning(
                                        "[2D-NORMALIZE] %s: reject %s (%s)",
                                        series_name, nifti_file,
                                        normalization.get('message') or action
                                    )
                                    os.remove(nifti_path)
                                    continue

                                output_files.append(nifti_file)
                                success_count += 1
                                try:
                                    entry = _build_conversion_entry(
                                        nifti_file,
                                        dcm,
                                        file_index=idx + 1,
                                        source_file=os.path.basename(dcm_file)
                                    )
                                    conversion_entries.append(entry)
                                except Exception:
                                    pass
                            except Exception as exc:
                                logger.warning(
                                    "[2D-NORMALIZE] %s: reject unprocessable output %s: %s",
                                    series_name, nifti_file, exc
                                )
                                try:
                                    os.remove(nifti_path)
                                except OSError:
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

            if success_count != len(dicom_files):
                error = f'Incomplete conversion: {success_count}/{len(dicom_files)} files; original DICOM retained'
                logger.error(error)
                return {
                    'success': False,
                    'error': error,
                    'file_count': success_count,
                    # NPZ 回退成功后需要清除这些中间文件，避免与源 DICOM 一起交付。
                    'output_files': output_files,
                }

            if success_count > 0:
                logger.info("dcm2niix conversion succeeded: %d/%d files", success_count, len(dicom_files))

                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
                _write_conversion_map(series_dir, conversion_entries)

                try:
                    _remove_source_dicoms(series_dir, dicom_files)
                except Exception as exc:
                    return {'success': False, 'error': f'Failed to remove source DICOM after conversion: {exc}'}

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
            result = _run_subprocess_with_timeout(cmd, timeout=300)
            if result.returncode == 0:
                break
            if attempt < 2:
                logger.warning("dcm2niix failed for %s (attempt %d/3), retrying in 0.5s...", output_name, attempt + 1)
                time.sleep(0.5)

        if result and result.returncode == 0:
            nifti_files = [f for f in os.listdir(series_dir) if f.endswith(('.nii.gz', '.nii'))]
            if modality=='CT':
                # Do not select an unrelated/stale NIfTI via os.listdir()[0].
                expected={output_name+'.nii.gz',output_name+'.nii'}
                nifti_files=[f for f in nifti_files if f in expected]
                extras=[f for f in os.listdir(series_dir) if f.startswith(output_name) and f.endswith(('.nii.gz','.nii')) and f not in expected]
                if len(nifti_files)!=1 or extras:
                    return {'success':False,'error':'Ambiguous CT conversion outputs; original DICOM retained'}
            if nifti_files:
                logger.info("   ✅ dcm2niix conversion succeeded: %s", nifti_files[0])

                # 方向一致性校验与镜像校正（需在删除 DICOM 之前，依赖其头与像素）
                # CPU 密集（全量体数据缩放），eventlet 下需卸载到真实线程避免饿死事件循环
                orientation_check = run_cpu_bound(
                    verify_and_fix_orientation_file,
                    os.path.join(series_dir, nifti_files[0]),
                    dicom_files,
                    series_name
                )

                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
                client._record_orientation_check(series_dir, orientation_check)

                if modality=='CT' and orientation_check.get('action') not in (ORIENTATION_CHECK_OK,ORIENTATION_CHECK_MIRROR_CORRECTED):
                    return {'success':False,'error':'CT geometry not verified; original DICOM retained',
                            'orientation_check':orientation_check}

                try:
                    _remove_source_dicoms(series_dir, dicom_files)
                except Exception as exc:
                    return {'success': False, 'error': f'Failed to remove source DICOM after conversion: {exc}'}

                return {
                    'success': True,
                    'method': 'dcm2niix',
                    'modality': modality,
                    'conversion_mode': 'series',
                    'output_file': nifti_files[0],
                    'orientation_check': orientation_check
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


def _has_diagnostic_2d_pixels(pixel_data: np.ndarray) -> bool:
    """排除真正全黑/全白/全非有限值的 2D 源像素，不评价窗口或对比度。"""
    try:
        if pixel_data is None or pixel_data.size == 0:
            return False
        if not bool(np.isfinite(pixel_data).any()):
            return False
        # 只拒绝真正恒定的像素；合法的小 RescaleSlope 不能把非空白图像
        # 压到某个绝对动态范围阈值以下后误杀。
        return bool(np.nanmax(pixel_data) != np.nanmin(pixel_data))
    except Exception:
        return False


def _remove_intermediate_nifti(series_dir: str) -> None:
    """NPZ 交付前删除所有中间 NIfTI，包括被拒绝但初次删除失败的输出。"""
    for root, _, files in os.walk(series_dir):
        for filename in files:
            if filename.lower().endswith(('.nii', '.nii.gz')):
                os.remove(os.path.join(root, filename))


def _remove_source_dicoms(series_dir: str, dicom_files=None) -> None:
    """成功转换后删除源 DICOM；任何删除失败都向上传播以防误交付。"""
    if dicom_files is not None:
        for filepath in dicom_files:
            if os.path.exists(filepath):
                os.remove(filepath)

    for root, dirs, files in os.walk(series_dir, topdown=False):
        for filename in files:
            filepath = os.path.join(root, filename)
            if filename.lower().endswith('.dcm'):
                os.remove(filepath)
            elif dicom_files is None:
                # 无后缀/其他后缀的输入按内容识别，不能只靠扩展名。
                try:
                    with open(filepath, 'rb') as source:
                        has_preamble = source.read(132)[128:132] == b'DICM'
                    if not has_preamble:
                        try:
                            pydicom.dcmread(filepath, force=True, stop_before_pixels=True)
                        except Exception:
                            continue
                    os.remove(filepath)
                except FileNotFoundError:
                    pass
        for dirname in dirs:
            if dirname.startswith('temp_'):
                shutil.rmtree(os.path.join(root, dirname))


def normalize_2d_nifti_display(
    nifti_path: str,
    dcm: FileDataset,
    series_name: str = '',
    corr_threshold: float = 0.98
) -> Dict[str, Any]:
    """
    将 2D X-ray NIfTI 归一化为 DICOM 显示布局（探针实测，不猜约定）。

    dcm2niix 在缺少 IOP 时的输出布局随图像内容/尺寸而变化（实测同一家医院
    出现 (cols,rows)+行翻转 与 (rows,cols)+列镜像 两种家族），任何硬编码
    转置/翻转都会顾此失彼。本函数用源 DICOM 像素做真值，对 8 种二面体
    变换（转置 × 上下/左右翻转）逐一计算相关系数，选取达标者重写 NIfTI，
    使数组与 DICOM 像素逐点一致（行,列,1）。

    顺带检测灰度反转：最佳布局的相关为强负（≤ -threshold）时说明转换器
    反了灰度（tag 与像素不一致的场景），一并反转修正。

    归一化后仿射统一为 _build_2d_xray_affine 约定，下游（预览/QC）
    可直接使用原始数组，无需再做任何翻转。

    返回:
        result 字典：action ∈ {normalized, inverted_normalized, ok, not_verifiable, error}
    """
    result: Dict[str, Any] = {'action': 'ok', 'transform': None, 'corr': None, 'message': ''}
    try:
        img = nib.load(nifti_path)
        data = np.asanyarray(img.dataobj).astype(np.float32)
        while data.ndim > 2:
            data = data[..., 0] if data.shape[-1] == 1 else data[..., data.shape[-1] // 2]

        ref = dcm.pixel_array.astype(np.float32)
        ref = apply_rescale(ref, dcm)
        ref = apply_photometric(ref, dcm)
        rows, cols = ref.shape
        if not _has_diagnostic_2d_pixels(ref):
            result['action'] = 'not_verifiable'
            result['message'] = 'blank DICOM pixels, reject output'
            logger.warning("[2D-NORMALIZE] %s: rejecting truly blank DICOM pixels", series_name)
            return result

        # 子采样网格（≤96 点/维），相关系数对线性灰度变换不变
        sr = max(1, rows // 96)
        sc = max(1, cols // 96)
        ref_sub = ref[::sr, ::sc].ravel()

        candidates = {
            'identity': data,
            'flipud': data[::-1, :],
            'fliplr': data[:, ::-1],
            'rot180': data[::-1, ::-1],
            'transpose': data.T,
            'transpose_flipud': data.T[::-1, :],
            'transpose_fliplr': data.T[:, ::-1],
            'transpose_rot180': data.T[::-1, ::-1],
        }
        best_name, best_corr, best_arr = None, 0.0, None
        for name, cand in candidates.items():
            if cand.shape != ref.shape:
                continue
            cand_sub = cand[::sr, ::sc].ravel()
            if float(cand_sub.std()) < 1e-6:
                continue
            corr = float(np.corrcoef(cand_sub, ref_sub)[0, 1])
            if abs(corr) > abs(best_corr):
                best_name, best_corr, best_arr = name, corr, cand

        if best_name is None or abs(best_corr) < corr_threshold:
            result['action'] = 'not_verifiable'
            result['corr'] = round(best_corr, 4) if best_name else None
            result['message'] = f'no dihedral transform reached threshold (best={best_name}, corr={best_corr:.4f})'
            logger.warning(
                "[2D-NORMALIZE] %s: %s — keeping dcm2niix layout",
                series_name, result['message']
            )
            return result

        normalized = best_arr
        action = 'normalized'
        if best_corr < 0:
            # 强负相关：转换器输出了灰度反转的像素，一并修正
            normalized = float(normalized.max()) + float(normalized.min()) - normalized
            action = 'inverted_normalized'

        if best_name == 'identity' and best_corr > 0:
            # 已经是 DICOM 布局，无需重写文件
            result['corr'] = round(best_corr, 4)
            result['transform'] = 'identity'
            result['message'] = 'already in DICOM display layout'
            return result

        new_data = normalized.astype(np.float32)[:, :, np.newaxis]
        new_affine = _build_2d_xray_affine(dcm)
        new_img = nib.Nifti1Image(new_data, new_affine)
        try:
            new_img.set_qform(new_affine, code=1)
            new_img.set_sform(new_affine, code=1)
        except Exception:
            pass
        nib.save(new_img, nifti_path)

        result.update({
            'action': action,
            'transform': best_name,
            'corr': round(best_corr, 4),
            'message': f'{best_name} corr={best_corr:.4f}'
        })
        logger.warning(
            "[2D-NORMALIZE] %s: %s -> DICOM layout (transform=%s, corr=%.4f%s)",
            series_name, os.path.basename(nifti_path), best_name, best_corr,
            ', grayscale inverted' if action == 'inverted_normalized' else ''
        )
        return result
    except Exception as e:
        logger.warning("[2D-NORMALIZE] %s: normalize failed for %s: %s",
                       series_name, os.path.basename(nifti_path), e)
        result['action'] = 'error'
        result['message'] = str(e)
        return result


def convert_with_python_libs(
    client: "DicomClient",
    series_dir: str,
    series_name: str,
    dicom_files: Optional[List[str]] = None,
    sample_dcm=None,
    modality: Optional[str] = None,
    save_to_disk: bool = True
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
        dicom_files: 可选，已扫描的 DICOM 文件列表（避免重复扫描）
        sample_dcm: 可选，已读取的样本 DICOM 对象（避免重复读取）
        modality: 可选，已知的影像模态（避免重复读取）
        save_to_disk: 是否写入 NIfTI 文件；False 时返回内存中的 nibabel 对象

    返回:
        包含转换结果的字典：
        - success: 是否成功
        - method: 'python_libs'
        - output_file(s): 输出文件路径（save_to_disk=True 时）
        - output_images: [(nifti_img, filename), ...]（save_to_disk=False 且 2D 时）
        - nifti_img: nibabel 图像对象（save_to_disk=False 且 3D 时）
        - modality: 影像模态
        - conversion_mode: 'series' 或 'individual'
        - file_count/slice_count: 文件数或切片数
        - error: 错误信息（失败时）
    """
    try:
        if dicom_files is None:
            dicom_files = []
            for file in os.listdir(series_dir):
                filepath = os.path.join(series_dir, file)
                if client._is_dicom_file(filepath):
                    dicom_files.append(filepath)

        if not dicom_files:
            return {'success': False, 'error': 'No DICOM files found'}

        if sample_dcm is not None and modality:
            first_dcm = sample_dcm
        else:
            first_dcm = pydicom.dcmread(dicom_files[0], force=True)
            modality = modality or getattr(first_dcm, 'Modality', '')

        if modality in ['DR', 'MG', 'DX', 'CR', 'RF']:
            logger.info("Detected %s modality; converting each DICOM file to NIfTI (Python libs)", modality)

            success_count = 0
            output_files: List[str] = []
            output_images: List[Tuple] = []
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
                    if not _has_diagnostic_2d_pixels(pixel_data):
                        logger.warning(
                            "Rejecting truly blank 2D X-ray file %d: %s",
                            idx + 1, os.path.basename(dcm_file)
                        )
                        continue

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
                        # 有 IOP 时可以使用 canonical 转换；_safe 版在 affine
                        # 含 NaN/inf 时退化为可分解 affine，不会 SVD 崩溃
                        nifti_img = _safe_as_closest_canonical(nifti_img)
                    # 否则：保持原方向，_build_2d_xray_affine 已经构建了正确的 RAS 矩阵

                    output_filename = f"{client._sanitize_folder_name(series_name)}_{idx+1:04d}.nii.gz"

                    if save_to_disk:
                        output_path = os.path.join(series_dir, output_filename)
                        nib.save(nifti_img, output_path)
                        output_files.append(output_filename)
                    else:
                        output_images.append((nifti_img, output_filename))

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

            if success_count != len(dicom_files):
                error = f'Incomplete conversion: {success_count}/{len(dicom_files)} files; original DICOM retained'
                logger.error(error)
                return {'success': False, 'error': error, 'file_count': success_count}

            if success_count > 0:
                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
                _write_conversion_map(series_dir, conversion_entries)
                
                # 清理原始 DICOM 文件；删除失败不得作为成功交付。
                if save_to_disk:
                    try:
                        _remove_source_dicoms(series_dir, dicom_files)
                    except Exception as exc:
                        return {'success': False, 'error': f'Failed to remove source DICOM after conversion: {exc}'}

                logger.info("   ✅ Python libs conversion succeeded: %d/%d files", success_count, len(dicom_files))
                result = {
                    'success': True,
                    'method': 'python_libs',
                    'modality': modality,
                    'conversion_mode': 'individual',
                    'file_count': success_count
                }
                if save_to_disk:
                    result['output_files'] = output_files
                else:
                    result['output_images'] = output_images
                return result

            return {'success': False, 'error': 'No files converted successfully'}

        print(f"   ℹ️  {modality} modality: converting entire series to a single NIfTI file")

        if modality == 'CT' and len(dicom_files) == 1:
            # Read actual current source rather than trusting a caller's sample.
            first_dcm = pydicom.dcmread(dicom_files[0], force=True)
            error = _ct_source_set_error([first_dcm], 1)
            if error:
                return {'success': False, 'error': error}

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
            
            # 对于缺少 IOP 的 2D 图像，不使用 canonical 转换以避免方向问题
            iop = getattr(dcm, 'ImageOrientationPatient', None)
            if iop is not None:
                nifti_img = _safe_as_closest_canonical(nifti_img)
            # 否则：保持原方向，build_affine_from_dicom 已经构建了正确的矩阵

            # 方向一致性校验与镜像校正（内存中完成后再写盘）
            nifti_img, orientation_check = verify_and_fix_orientation(
                nifti_img, [dcm], series_name
            )
            if modality == 'CT' and orientation_check.get('action') not in (ORIENTATION_CHECK_OK, ORIENTATION_CHECK_MIRROR_CORRECTED):
                return {'success': False, 'error': 'CT geometry not verified; original DICOM retained',
                        'orientation_check': orientation_check}

            output_filename = f"{client._sanitize_folder_name(series_name)}.nii.gz"

            if save_to_disk:
                output_path = os.path.join(series_dir, output_filename)
                nib.save(nifti_img, output_path)

                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
                client._record_orientation_check(series_dir, orientation_check)
                try:
                    _remove_source_dicoms(series_dir, dicom_files)
                except Exception as exc:
                    return {'success': False, 'error': f'Failed to remove source DICOM after conversion: {exc}'}

                print(f"   ✅ Python libs conversion succeeded: {output_filename}")
            else:
                print(f"   ✅ Python libs conversion succeeded (in-memory): {output_filename}")

            result = {
                'success': True,
                'method': 'python_libs',
                'modality': modality,
                'conversion_mode': 'series',
                'output_file': output_filename,
                'orientation_check': orientation_check
            }
            if not save_to_disk:
                result['nifti_img'] = nifti_img
            return result

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
        if modality == 'CT':
            error = _ct_source_set_error([entry[2] for entry in slice_info], len(dicom_files))
            if error:
                return {'success': False, 'error': error}

        if all(item[3] is not None for item in slice_info):
            order,_=_canonical_slice_order(np.asarray([item[3] for item in slice_info]))
            slice_info=[slice_info[int(i)] for i in order]
        else:
            slice_info.sort(key=lambda x: x[0])

        slices: List[np.ndarray] = []
        valid_info: List[Tuple[float, str, FileDataset, Optional[List[float]]]] = []
        for entry in slice_info:
            _, _, dcm, ipp = entry
            if hasattr(dcm, 'pixel_array'):
                pixel_data = dcm.pixel_array
                pixel_data = apply_rescale(pixel_data, dcm)
                pixel_data = apply_photometric(pixel_data, dcm)
                slices.append(pixel_data)
                valid_info.append(entry)

        if not slices:
            return {'success': False, 'error': 'No pixel data found'}

        if modality == 'CT' and len(valid_info) != len(dicom_files):
            return {'success': False, 'error': 'CT source pixel set incomplete; original DICOM retained'}

        # 同一序列混入不同矩阵尺寸的图像（打印排版/AI 报告等拼装序列）时，
        # np.stack 会整体崩溃。保留多数派 shape 的切片，丢弃少数派并记录数量。
        shape_counts = Counter(s.shape for s in slices)
        if len(shape_counts) > 1:
            if modality == 'CT':
                return {'success': False, 'error': 'Mixed CT pixel matrices; original DICOM retained'}
            modal_shape = shape_counts.most_common(1)[0][0]
            keep_idx = [i for i, s in enumerate(slices) if s.shape == modal_shape]
            logger.warning(
                f"   ⚠️  {series_name}: mixed pixel shapes {dict(shape_counts)}, "
                f"keeping {len(keep_idx)}/{len(slices)} slices with modal shape {modal_shape}"
            )
            slices = [slices[i] for i in keep_idx]
            valid_info = [valid_info[i] for i in keep_idx]

        positions: List[np.ndarray] = [
            np.array(entry[3], dtype=np.float64) for entry in valid_info if entry[3] is not None
        ]

        # DICOM pixel_array is (Rows, Columns)
        # Stack directly: dim 0 = Rows (vertical), dim 1 = Columns (horizontal), dim 2 = Slices
        volume = np.stack(slices, axis=2)

        if len(positions) > 1:
            slice_spacing = float(np.linalg.norm(positions[1] - positions[0]))
        elif len(valid_info) > 1:
            slice_spacing = abs(valid_info[1][0] - valid_info[0][0])
        else:
            slice_spacing = float(getattr(first_dcm, 'SliceThickness', 1.0))

        iop = getattr(first_dcm, 'ImageOrientationPatient', None)
        if iop is not None:
            row_cosine = np.array([float(i) for i in iop[:3]], dtype=np.float64)
            col_cosine = np.array([float(i) for i in iop[3:6]], dtype=np.float64)
            slice_cosines = np.cross(row_cosine, col_cosine)
        else:
            slice_cosines = None

        # Stack and affine must share the same first slice and signed IPP step.
        # The old first_dcm came from unsorted input and cross(IOP)*abs(spacing)
        # could describe the opposite direction or a different origin.
        first_dcm=valid_info[0][2]
        if len(positions)>1:
            actual_step=positions[1]-positions[0]
            slice_cosines=actual_step/np.linalg.norm(actual_step)
        affine = build_affine_from_dicom(first_dcm, slice_spacing=slice_spacing, slice_cosines=slice_cosines)

        nifti_img = nib.Nifti1Image(volume.astype(np.float32), affine)

        # 对于缺少 IOP 的序列，不使用 canonical 转换以避免方向问题。
        # affine 含 NaN/inf（几何头损坏）时 nibabel io_orientation 的 SVD 会直接
        # 崩溃（"SVD did not converge"）；_safe 版本退化为可分解 affine，不抛异常
        if iop is not None:
            nifti_img = _safe_as_closest_canonical(nifti_img)
        # 否则：保持原方向，build_affine_from_dicom 已经处理了回退方案

        # 方向一致性校验与镜像校正（内存中完成后再写盘）
        nifti_img, orientation_check = verify_and_fix_orientation(
            nifti_img, [dcm for _, _, dcm, _ in valid_info], series_name
        )

        if modality=='CT' and orientation_check.get('action') not in (ORIENTATION_CHECK_OK,ORIENTATION_CHECK_MIRROR_CORRECTED):
            return {'success':False,'error':'CT geometry not verified; original DICOM retained',
                    'orientation_check':orientation_check}

        output_filename = f"{client._sanitize_folder_name(series_name)}.nii.gz"

        if save_to_disk:
            output_path = os.path.join(series_dir, output_filename)
            nib.save(nifti_img, output_path)

            client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
            client._record_orientation_check(series_dir, orientation_check)
            try:
                _remove_source_dicoms(series_dir, dicom_files)
            except Exception as exc:
                return {'success': False, 'error': f'Failed to remove source DICOM after conversion: {exc}'}

            print(f"   ✅ Python libs conversion succeeded: {output_filename} ({len(slices)} slices)")
        else:
            print(f"   ✅ Python libs conversion succeeded (in-memory): {output_filename} ({len(slices)} slices)")

        result = {
            'success': True,
            'method': 'python_libs',
            'modality': modality,
            'conversion_mode': 'series',
            'output_file': output_filename,
            'slice_count': len(slices),
            'orientation_check': orientation_check
        }
        if not save_to_disk:
            result['nifti_img'] = nifti_img
        return result

    except Exception as e:
        return {'success': False, 'error': str(e)}
