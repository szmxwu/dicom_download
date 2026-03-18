# -*- coding: utf-8 -*-
"""
预览图生成模块

提供 DICOM 和 NIfTI/NPZ 文件的预览图生成功能，
支持窗宽窗位调整和纵横比校正。
"""

import os
import re
import json
import numpy as np
import nibabel as nib
from PIL import Image, ImageDraw, ImageFont
from typing import Tuple, Optional, Callable, List
from dotenv import load_dotenv


def _get_project_root() -> str:
    try:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    except Exception:
        return os.getcwd()


def get_preview_target_size(default: int = 896) -> int:
    """
    使用 python-dotenv 从项目根目录的 .env 读取 PREVIEW_TARGET_SIZE，失败则返回默认值
    """
    try:
        env_path = os.path.join(_get_project_root(), '.env')
        # load .env if present (python-dotenv handles nonexistent paths)
        load_dotenv(env_path)
        val = os.getenv('PREVIEW_TARGET_SIZE', str(default))
        try:
            return int(val)
        except Exception:
            return default
    except Exception:
        return default


def get_window_params(dcm) -> Tuple[Optional[float], Optional[float]]:
    """
    获取窗宽窗位参数

    Args:
        dcm: pydicom Dataset 对象

    Returns:
        Tuple: (窗位 WindowCenter, 窗宽 WindowWidth)
    """
    try:
        if dcm is None:
            return None, None
        wc = getattr(dcm, 'WindowCenter', None)
        ww = getattr(dcm, 'WindowWidth', None)
        if wc is None or ww is None:
            return None, None

        # 处理多值情况
        if hasattr(wc, '__len__') and not isinstance(wc, str):
            wc = float(wc[0])
        else:
            wc = float(wc)

        if hasattr(ww, '__len__') and not isinstance(ww, str):
            ww = float(ww[0])
        else:
            ww = float(ww)

        if ww <= 1e-6:
            return None, None

        return wc, ww
    except Exception:
        return None, None


def _get_mr_sequence_type(dcm) -> str:
    """
    根据 DICOM 参数判断 MR 序列类型
    
    简化版本的序列分类，基于 TR/TE/TI 和扫描序列类型
    
    Args:
        dcm: pydicom Dataset 对象
        
    Returns:
        str: 序列类型 ('T1', 'T2', 'FLAIR', 'STIR', 'DWI', 'PD', 'UNKNOWN')
    """
    try:
        if dcm is None:
            return 'UNKNOWN'
        
        # 获取关键参数
        tr = getattr(dcm, 'RepetitionTime', None)
        te = getattr(dcm, 'EchoTime', None)
        ti = getattr(dcm, 'InversionTime', None)
        scanning_seq = str(getattr(dcm, 'ScanningSequence', '')).upper()
        seq_variant = str(getattr(dcm, 'SequenceVariant', '')).upper()
        protocol_name = str(getattr(dcm, 'ProtocolName', '')).lower()
        image_type = str(getattr(dcm, 'ImageType', '')).lower()
        
        # 转换为数值
        tr = float(tr) if tr is not None else None
        te = float(te) if te is not None else None
        ti = float(ti) if ti is not None else None
        
        # 1. 首先检查特殊序列（基于关键词）
        if any(k in protocol_name for k in ['localizer', 'survey', 'scout']):
            return 'LOCALIZER'
        
        if 'flair' in protocol_name or 'tse_dark_fluid' in protocol_name:
            return 'FLAIR'
        
        if 'stir' in protocol_name:
            return 'STIR'
        
        if any(k in protocol_name for k in ['dwi', 'diff']):
            return 'DWI'
        
        if 'adc' in image_type or 'adc' in protocol_name:
            return 'ADC'
        
        # 2. 基于 TI 判断反转恢复序列
        if ti is not None:
            if ti >= 2000:  # 典型 FLAIR 的 TI
                return 'FLAIR'
            elif 100 <= ti <= 250:  # 典型 STIR 的 TI
                return 'STIR'
        
        # 3. 基于 TR/TE 判断 T1/T2/PD
        if tr is not None and te is not None:
            # T2 加权：长 TE
            if te > 80:
                return 'T2'
            # T1 加权：短 TR 且短 TE
            elif tr < 800 and te < 30:
                return 'T1'
            # PD 加权：长 TR 且短 TE
            elif tr > 2000 and te < 30:
                return 'PD'
        
        # 4. 基于序列名称兜底
        if 't1' in protocol_name:
            return 'T1'
        elif 't2' in protocol_name:
            return 'T2'
        elif 'pd' in protocol_name:
            return 'PD'
        
        return 'UNKNOWN'
    except Exception:
        return 'UNKNOWN'


def _estimate_window_params(dcm, image_2d: np.ndarray, modality: str) -> Tuple[float, float]:
    """
    根据模态和 DICOM 参数估算窗宽窗位
    
    Args:
        dcm: pydicom Dataset 对象
        image_2d: 2D 图像数组
        modality: 模态类型 (CT, MR, DX, etc.)
        
    Returns:
        Tuple: (low, high) 窗位范围
    """
    img = image_2d[np.isfinite(image_2d)]
    
    if len(img) == 0:
        return 0.0, 1.0
    
    modality = (modality or '').upper()
    
    try:
        if modality == 'CT':
            # CT 默认使用腹窗 (Abdomen Window)
            # WL=40, WW=400
            wl, ww = 40.0, 400.0
            low = wl - ww / 2.0
            high = wl + ww / 2.0
            
        elif modality == 'MR':
            # MR 根据序列类型选择不同的窗宽窗位策略
            seq_type = _get_mr_sequence_type(dcm)
            
            # 计算图像统计信息
            img_mean = np.mean(img)
            img_std = np.std(img)
            img_min = np.min(img)
            img_max = np.max(img)
            
            if seq_type == 'T1':
                # T1: 使用较窄的窗宽突出解剖结构
                # 基于均值 ± 2*标准差，但限制最小窗宽
                ww = max(img_std * 4, img_max - img_min)
                wl = img_mean
            elif seq_type == 'T2':
                # T2: 液体信号高，使用较宽的窗宽
                ww = max(img_std * 5, img_max - img_min)
                wl = img_mean
            elif seq_type in ['FLAIR', 'STIR']:
                # FLAIR/STIR: 抑制液体信号，使用中等窗宽
                ww = max(img_std * 4, img_max - img_min)
                wl = img_mean
            elif seq_type == 'DWI':
                # DWI: 扩散受限区域信号高，使用较宽窗宽
                ww = max(img_std * 5, img_max - img_min)
                wl = img_mean
            elif seq_type == 'LOCALIZER':
                # 定位像：通常范围较大
                ww = img_max - img_min
                wl = (img_max + img_min) / 2
            else:
                # 未知 MR 序列：使用自适应方法
                # 排除极端值后计算窗宽窗位
                p5, p95 = np.percentile(img, [5, 95])
                ww = p95 - p5
                wl = (p5 + p95) / 2
            
            low = wl - ww / 2.0
            high = wl + ww / 2.0
            
        elif modality in ['DX', 'DR', 'CR', 'RF']:
            # X-ray 数字成像
            # 基于位深估算窗宽窗位
            if dcm is not None:
                bits_stored = getattr(dcm, 'BitsStored', 12)
                # 假设有效范围是完整动态范围的一部分
                # 对于 X-ray，通常使用较宽的窗宽
                high = (2 ** bits_stored) - 1
                low = 0
            else:
                # 基于图像实际范围
                p1, p99 = np.percentile(img, [1, 99])
                low = p1
                high = p99
                
        elif modality == 'MG':
            # 乳腺钼靶：高对比度需求
            # 使用较窄的窗宽突出组织细节
            img_mean = np.mean(img)
            img_std = np.std(img)
            ww = img_std * 3  # 较窄的窗宽
            wl = img_mean
            low = wl - ww / 2.0
            high = wl + ww / 2.0
            
        else:
            # 其他模态：使用自适应百分位数方法
            # 使用 5%-95% 范围，比原来的 1%-99% 更稳健
            low, high = np.percentile(img, [5, 95])
    
    except Exception:
        # 回退到稳健的百分位数方法
        try:
            low, high = np.percentile(img, [5, 95])
        except Exception:
            low, high = np.min(img), np.max(img)
    
    # 确保窗宽有效
    if high <= low:
        high = low + 1.0
    
    return low, high


def apply_windowing(image_2d: np.ndarray, dcm, modality: str = None) -> np.ndarray:
    """
    应用窗宽窗位变换
    
    优先使用 DICOM 标签中的 WindowCenter/WindowWidth，
    如果不存在则根据模态类型估算经验值。

    Args:
        image_2d: 2D 图像数组
        dcm: pydicom Dataset 对象（可选）
        modality: 模态类型（CT/MR/DX 等），用于估算默认窗宽窗位

    Returns:
        np.ndarray: 8位灰度图像数组
    """
    img = image_2d.astype(np.float32)
    wc, ww = get_window_params(dcm)
    
    if wc is not None and ww is not None:
        # 使用 DICOM 标签中的窗宽窗位
        low = wc - ww / 2.0
        high = wc + ww / 2.0
    else:
        # 根据模态估算窗宽窗位
        # 如果未提供 modality，尝试从 DICOM 读取
        if modality is None and dcm is not None:
            modality = getattr(dcm, 'Modality', None)
        low, high = _estimate_window_params(dcm, img, modality)

    if high <= low:
        high = low + 1.0

    img = np.clip(img, low, high)
    img = (img - low) / (high - low)
    img = (img * 255.0).astype(np.uint8)

    # 处理 MONOCHROME1 反色
    try:
        if dcm is not None:
            photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
            if photometric == 'MONOCHROME1':
                img = 255 - img
    except Exception:
        pass

    return img


def resize_with_aspect(img: np.ndarray, aspect_ratio: Optional[float]) -> np.ndarray:
    """
    按纵横比调整图像大小

    Args:
        img: 输入图像数组
        aspect_ratio: 目标纵横比

    Returns:
        np.ndarray: 调整后的图像
    """
    try:
        if aspect_ratio is None or aspect_ratio <= 0:
            return img
        height, width = img.shape[:2]
        target_height = max(1, int(round(height * aspect_ratio)))
        if target_height == height:
            return img
        pil_img = Image.fromarray(img)
        pil_img = pil_img.resize((width, target_height), resample=Image.BILINEAR)
        return np.array(pil_img)
    except Exception:
        return img


def normalize_2d_preview(img: np.ndarray, target_size: int = 896) -> np.ndarray:
    """
    标准化 2D 预览图尺寸

    将图像缩放并居中放置到目标尺寸的画布上

    Args:
        img: 输入图像数组
        target_size: 目标画布尺寸

    Returns:
        np.ndarray: 标准化后的图像
    """
    try:
        if img is None:
            return img

        h, w = img.shape[:2]
        if h <= 0 or w <= 0:
            return img

        scale = float(target_size) / max(h, w)
        new_w = max(1, int(round(w * scale)))
        new_h = max(1, int(round(h * scale)))

        pil_img = Image.fromarray(img)
        pil_img = pil_img.resize((new_w, new_h), resample=Image.BILINEAR)
        resized = np.array(pil_img)

        # 创建画布并居中放置
        canvas = np.zeros((target_size, target_size), dtype=np.uint8)
        top = max(0, (target_size - new_h) // 2)
        left = max(0, (target_size - new_w) // 2)
        canvas[top:top + new_h, left:left + new_w] = resized
        return canvas
    except Exception:
        return img


def _load_image_2d(preview_file: str, is_3d: bool, preview_idx: int, orientation: str = 'UNKNOWN') -> Optional[np.ndarray]:
    """
    从NIfTI或NPZ文件加载2D图像数据
    
    Args:
        preview_file: 图像文件路径
        is_3d: 是否为3D图像
        preview_idx: 预览索引（用于错误信息）
        orientation: 扫描方位 ('AX', 'SAG', 'COR', 'OBL', 'UNKNOWN')
        
    Returns:
        2D图像数组，失败时返回None
    """
    try:
        if preview_file.endswith('.npz'):
            with np.load(preview_file) as npz:
                if 'data' in npz.files:
                    data = npz['data']
                elif npz.files:
                    data = npz[npz.files[0]]
                else:
                    return None

            if data.ndim == 3 and is_3d:
                # npz data is saved as (Z, Y, X) in convert.py
                # Z: top to bottom, Y: anterior to posterior, X: right to left
                if orientation == 'SAG':
                    mid_x = data.shape[2] // 2
                    image_2d = data[:, :, mid_x]
                elif orientation == 'COR':
                    mid_y = data.shape[1] // 2
                    image_2d = data[:, mid_y, :]
                else: # AX or UNKNOWN
                    mid_z = data.shape[0] // 2
                    image_2d = data[mid_z, :, :]
                
                image_2d = image_2d.astype(np.float32)
            else:
                image_2d = data if data.ndim == 2 else data[0, :, :]

        elif preview_file.endswith(('.nii', '.nii.gz')):
            img = nib.load(preview_file)

            # 对多参数/时间序列（4D及以上）先提取第一个序列，
            # 再进行 canonical 重排，避免 4D 与 3D 方向处理不一致。
            img_for_preview = img
            first_volume_data = None
            if len(img.shape) > 3:
                try:
                    slicer = (slice(None), slice(None), slice(None)) + (0,) * (len(img.shape) - 3)
                    first_volume = np.asarray(img.dataobj[slicer])
                    while first_volume.ndim > 3:
                        first_volume = first_volume[..., 0]
                    first_volume_data = first_volume

                    # 直接用首序列+原 affine 构建 3D 图像；
                    # 若 affine 不可分解，后续 canonical 阶段再做安全降级。
                    img_for_preview = nib.Nifti1Image(first_volume, img.affine)
                except Exception:
                    img_for_preview = img

            try:
                img_canonical = nib.as_closest_canonical(img_for_preview)
                data = img_canonical.get_fdata()
            except Exception:
                # 兜底：部分文件 affine 不可分解（如某轴缩放为0），
                # 构建可分解 affine 后重试 canonical；再失败则回退原数据。
                try:
                    fallback_data = (
                        first_volume_data
                        if first_volume_data is not None
                        else np.asarray(img_for_preview.get_fdata())
                    )

                    orig_affine = np.asarray(getattr(img_for_preview, 'affine', np.eye(4)), dtype=np.float64)
                    try:
                        zooms = tuple(float(z) for z in img_for_preview.header.get_zooms()[:3])
                    except Exception:
                        zooms = (1.0, 1.0, 1.0)
                    safe_zooms = tuple(z if np.isfinite(z) and z > 1e-8 else 1.0 for z in zooms)

                    signs = []
                    for axis in range(3):
                        val = orig_affine[axis, axis] if orig_affine.shape == (4, 4) else 1.0
                        signs.append(-1.0 if np.isfinite(val) and val < 0 else 1.0)

                    safe_affine = np.eye(4, dtype=np.float64)
                    safe_affine[0, 0] = signs[0] * safe_zooms[0]
                    safe_affine[1, 1] = signs[1] * safe_zooms[1]
                    safe_affine[2, 2] = signs[2] * safe_zooms[2]
                    if orig_affine.shape == (4, 4) and np.all(np.isfinite(orig_affine[:3, 3])):
                        safe_affine[:3, 3] = orig_affine[:3, 3]

                    safe_img = nib.Nifti1Image(fallback_data, safe_affine)
                    data = nib.as_closest_canonical(safe_img).get_fdata()
                except Exception:
                    data = (
                        first_volume_data
                        if first_volume_data is not None
                        else np.asarray(img_for_preview.get_fdata())
                    )

            # 防御性处理：若仍为4D及以上，继续默认选取第一个序列
            while data.ndim > 3:
                data = data[..., 0]

            if data.ndim == 3 and is_3d:
                # canonical data is (X, Y, Z)
                # X: Left to Right, Y: Posterior to Anterior, Z: Inferior to Superior
                if orientation == 'SAG':
                    mid_x = data.shape[0] // 2
                    slice_yz = data[mid_x, :, :]
                    # Y: Posterior to Anterior -> Anterior to Posterior (flip)
                    # Z: Inferior to Superior -> Superior to Inferior (flip)
                    image_2d = np.transpose(slice_yz, (1, 0))[::-1, ::-1]
                elif orientation == 'COR':
                    mid_y = data.shape[1] // 2
                    slice_xz = data[:, mid_y, :]
                    # X: Left to Right -> Right to Left (flip)
                    # Z: Inferior to Superior -> Superior to Inferior (flip)
                    image_2d = np.transpose(slice_xz, (1, 0))[::-1, ::-1]
                else: # AX or UNKNOWN
                    mid_z = data.shape[2] // 2
                    slice_xy = data[:, :, mid_z]
                    # nibabel array is (X, Y) where X=horizontal, Y=vertical
                    # Image display: row=Y (top to bottom), column=X (left to right)
                    # Transpose to get (Y, X) = (row, col) for image display
                    # For radiological convention: patient right on image left, anterior on top
                    # Flip X to make patient right on image left (radiological convention)
                    # Flip Y to make Anterior (breast) on top
                    image_2d = np.transpose(slice_xy, (1, 0))[::-1, ::-1]
                
                image_2d = image_2d.astype(np.float32)
            else:
                # 2D图像 (如DX X光片)
                # NIfTI存储为 (Columns, Rows)，DICOM为 (Rows, Columns)，需要转置
                # as_closest_canonical将X轴从L->R变为R->L，需要水平翻转
                # Y轴方向也需要垂直翻转以匹配标准医学图像显示
                image_2d = data if data.ndim == 2 else data[:, :, 0]
                image_2d = np.transpose(image_2d, (1, 0))[::-1, ::-1]
        else:
            return None
        
        return image_2d
    except Exception as e:
        print(f"❌ Error loading image for preview: {preview_file} (index {preview_idx}) - {str(e)}")
        return None


def normalize_orientation(orientation: str, dcm=None) -> str:
    """
    标准化方位名称，将斜位映射到最接近的标准方位

    处理以下情况：
    1. 拼写变体：'OBL' -> 标准方位, 'OLB'/'OB'等 -> 标准方位
    2. 斜位映射：根据法向量的最大分量，将OBL映射到最接近的AX/SAG/COR
    3. 未知/空值处理：返回'AX'作为默认值

    Args:
        orientation: 原始方位名称
        dcm: pydicom Dataset对象（用于斜位时计算最接近的标准方位）

    Returns:
        str: 标准化后的方位名称 ('AX', 'SAG', 'COR')，不会出现'OBL'或'UNKNOWN'
    """
    if orientation is None:
        orientation = 'UNKNOWN'

    orientation = str(orientation).upper().strip()

    # 处理拼写变体和常见错误
    correction_map = {
        'OLB': 'OBL',      # 常见拼写错误
        'OB': 'OBL',
        'OBLIQUE': 'OBL',
        'SAGITTAL': 'SAG',
        'CORONAL': 'COR',
        'AXIAL': 'AX',
        'TRANSVERSE': 'AX',
        'TRA': 'AX',
    }
    orientation = correction_map.get(orientation, orientation)

    # 如果是标准方位，直接返回
    if orientation in ('AX', 'SAG', 'COR'):
        return orientation

    # 对于斜位或未知，尝试从DICOM计算最接近的标准方位
    if orientation in ('OBL', 'UNKNOWN', '') and dcm is not None:
        try:
            iop = getattr(dcm, 'ImageOrientationPatient', None)
            if iop is not None and len(iop) == 6:
                row_vec = np.array([float(iop[0]), float(iop[1]), float(iop[2])])
                col_vec = np.array([float(iop[3]), float(iop[4]), float(iop[5])])
                normal = np.cross(row_vec, col_vec)

                # 即使被判定为斜位，法向量的最大分量仍指示最接近的方位
                main_axis = np.argmax(np.abs(normal))
                if main_axis == 0:
                    return 'SAG'
                elif main_axis == 1:
                    return 'COR'
                else:
                    return 'AX'
        except Exception:
            pass

    # 尝试从协议名推断（备用方案）
    if dcm is not None:
        try:
            protocol_name = str(getattr(dcm, 'ProtocolName', '')).lower()
            if any(k in protocol_name for k in ['sag', 'sg']):
                return 'SAG'
            elif any(k in protocol_name for k in ['cor', 'cr']):
                return 'COR'
            elif any(k in protocol_name for k in ['ax', 'tra', 'trans']):
                return 'AX'
        except Exception:
            pass

    # 默认返回轴位（最常见的扫描方位）
    return 'AX'


def _get_orientation_from_dcm(dcm) -> str:
    """
    从DICOM对象中获取扫描方位（原始值，包含斜位识别）

    通过ImageOrientationPatient(IOP)计算法向量来确定方位。
    能够精确区分轴位(AX)，矢状位(SAG)，冠状位(COR)，并能识别斜位(OBL)。

    Args:
        dcm: pydicom Dataset 对象

    Returns:
        str: 方位名称 ('AX', 'SAG', 'COR', 'OBL', 'UNKNOWN')
    """
    try:
        if dcm is None:
            return 'UNKNOWN'

        # 获取ImageOrientationPatient
        iop = getattr(dcm, 'ImageOrientationPatient', None)
        if iop is None or len(iop) != 6:
            return 'UNKNOWN'

        # 转换为numpy数组
        row_vec = np.array([float(iop[0]), float(iop[1]), float(iop[2])])
        col_vec = np.array([float(iop[3]), float(iop[4]), float(iop[5])])

        # 计算法向量
        normal = np.cross(row_vec, col_vec)

        # 检查是否为斜位：如果没有一个轴占绝对主导，则为斜位
        # 判断依据：主轴分量的平方是否小于向量模长平方的 0.9
        oblique_ratio = 0.9
        if np.max(np.abs(normal))**2 < oblique_ratio * np.sum(normal**2):
            return 'OBL'

        # 根据法向量的主轴判断方位
        main_axis = np.argmax(np.abs(normal))
        if main_axis == 0:
            return 'SAG'  # 法向量主轴为X
        elif main_axis == 1:
            return 'COR'  # 法向量主轴为Y
        elif main_axis == 2:
            return 'AX'   # 法向量主轴为Z

        return 'UNKNOWN'
    except Exception:
        return 'UNKNOWN'


def _correct_image_orientation(
    image_2d: np.ndarray,
    preview_file: str,
    preview_idx: int,
    is_3d: bool,
    series_dir: str,
    sample_dcm
) -> np.ndarray:
    """
    校正图像方向（处理行列颠倒的情况）
    
    结合DICOM元数据中的尺寸信息和扫描方位信息进行校正。
    对于冠状位(COR)图像，使用特殊的校正逻辑以确保方向正确。
    
    Args:
        image_2d: 输入图像
        preview_file: 图像文件路径
        preview_idx: 预览索引
        is_3d: 是否为3D图像
        series_dir: 序列目录
        sample_dcm: 样本DICOM对象
        
    Returns:
        校正后的图像
    """
    try:
        h, w = image_2d.shape[:2]
        
        # 从DICOM缓存或对象中获取Rows和Columns
        rows = None
        cols = None
        
        if not is_3d:
            cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache = json.load(f)
                    records = cache.get('records') or []
                    conversion_map = cache.get('conversion_map') or {}
                except Exception:
                    records = []
                    conversion_map = {}

                if conversion_map:
                    try:
                        conv_entry = conversion_map.get(os.path.basename(preview_file))
                        if isinstance(conv_entry, dict):
                            rows = conv_entry.get('Rows')
                            cols = conv_entry.get('Columns')
                    except Exception:
                        pass

                if records and (rows is None or cols is None):
                    basename = os.path.basename(preview_file)
                    match = re.search(r"_(\d{1,6})(?:\.nii(?:\.gz)?|\.npz)$", basename)
                    if match:
                        file_index = int(match.group(1))
                    else:
                        file_index = preview_idx + 1

                    for record in records:
                        try:
                            if int(record.get('FileIndex', -1)) == file_index:
                                rows = record.get('Rows')
                                cols = record.get('Columns')
                                break
                        except Exception:
                            continue

        # 如果缓存中没有，从sample_dcm获取
        if (rows is None or cols is None) and sample_dcm is not None:
            rows = getattr(sample_dcm, 'Rows', None)
            cols = getattr(sample_dcm, 'Columns', None)
        
        # 对于3D图像，_load_image_2d 已经处理了正确的方向，不需要再转置
        if is_3d:
            return image_2d

        # 基于尺寸判断是否转置（行列颠倒）
        needs_transpose = False
        if rows and cols and int(rows) != int(cols):
            if h == int(cols) and w == int(rows):
                needs_transpose = True
        
        # 应用校正：只需要处理行列颠倒的情况
        if needs_transpose:
            image_2d = image_2d.T
        
        return image_2d
    except Exception:
        return image_2d


def _get_file_dcm_info(
    preview_file: str,
    preview_idx: int,
    series_dir: str,
    sample_dcm
):
    """
    获取特定文件对应的DICOM信息
    
    对于2D序列中的每个文件，尝试从conversion_map或records中获取
    该文件特定的DICOM元数据（如窗宽窗位）
    
    Args:
        preview_file: 图像文件路径
        preview_idx: 预览索引
        series_dir: 序列目录
        sample_dcm: 样本DICOM对象（作为fallback）
        
    Returns:
        DICOM对象或包含元数据的字典
    """
    try:
        cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            conversion_map = cache.get('conversion_map') or {}
            
            # 尝试从conversion_map获取
            conv_entry = conversion_map.get(os.path.basename(preview_file))
            if isinstance(conv_entry, dict):
                # 创建一个简单对象来保存元数据
                class DcmInfo:
                    pass
                
                dcm_info = DcmInfo()
                # 复制基本属性
                for attr in ['Rows', 'Columns', 'WindowCenter', 'WindowWidth', 
                             'PhotometricInterpretation', 'PixelSpacing',
                             'RescaleSlope', 'RescaleIntercept']:
                    if attr in conv_entry:
                        value = conv_entry[attr]
                        # 尝试转换数值
                        try:
                            if attr in ['Rows', 'Columns']:
                                value = int(value)
                            elif attr in ['WindowCenter', 'WindowWidth', 'RescaleSlope', 'RescaleIntercept']:
                                value = float(value)
                            elif attr == 'PixelSpacing':
                                # 可能是字符串表示的列表
                                if isinstance(value, str):
                                    value = [float(x.strip()) for x in value.strip('[]').split(',')]
                        except Exception:
                            pass
                        setattr(dcm_info, attr, value)
                
                # 对于缺失的属性，使用sample_dcm的
                if sample_dcm is not None:
                    for attr in ['WindowCenter', 'WindowWidth', 'PhotometricInterpretation',
                                 'PixelSpacing', 'RescaleSlope', 'RescaleIntercept']:
                        if not hasattr(dcm_info, attr) or getattr(dcm_info, attr) is None:
                            val = getattr(sample_dcm, attr, None)
                            if val is not None:
                                setattr(dcm_info, attr, val)
                
                return dcm_info
        
        return sample_dcm
    except Exception:
        return sample_dcm


def _generate_single_preview(
    preview_file: str,
    preview_idx: int,
    is_3d: bool,
    series_dir: str,
    series_name: str,
    sample_dcm,
    modality: str,
    sanitize_folder_name: Callable[[str], str],
    output_suffix: str = ""
) -> Optional[str]:
    """
    生成单张预览图
    
    Args:
        preview_file: 输入图像文件路径
        preview_idx: 预览索引
        is_3d: 是否为3D图像
        series_dir: 输出目录
        series_name: 序列名称
        sample_dcm: 样本DICOM对象
        modality: 模态类型
        sanitize_folder_name: 文件夹名称清理函数
        output_suffix: 输出文件名后缀
        
    Returns:
        生成的预览图路径，失败时返回None
    """
    # 获取扫描方位
    # 获取并标准化方位（将斜位映射到最接近的标准方位）
    raw_orientation = _get_orientation_from_dcm(sample_dcm)
    orientation = normalize_orientation(raw_orientation, sample_dcm)

    # 加载图像
    image_2d = _load_image_2d(preview_file, is_3d, preview_idx, orientation)
    if image_2d is None:
        return None
    
    # 校正方向
    image_2d = _correct_image_orientation(
        image_2d, preview_file, preview_idx, is_3d, series_dir, sample_dcm
    )
    
    # 获取该文件特定的DICOM信息（用于窗宽窗位等）
    file_dcm = _get_file_dcm_info(preview_file, preview_idx, series_dir, sample_dcm)
    
    # 应用窗宽窗位
    image_2d = apply_windowing(image_2d, file_dcm, modality)
    
    # 计算纵横比（仅对2D X射线图像需要调整）
    # 对于从3D volume提取的切片，像素本身已经反映真实物理尺寸，无需调整
    aspect_ratio = None
    if not is_3d:
        try:
            dcm_for_spacing = file_dcm if file_dcm is not None else sample_dcm
            if dcm_for_spacing is not None:
                pixel_spacing = getattr(dcm_for_spacing, 'PixelSpacing', None)
                if pixel_spacing and len(pixel_spacing) >= 2:
                    # 2D图像：PixelSpacing = [row_spacing, col_spacing]
                    # 如果 row_spacing != col_spacing，需要调整纵横比
                    aspect_ratio = float(pixel_spacing[0]) / max(float(pixel_spacing[1]), 1e-6)
        except Exception:
            aspect_ratio = None
    
    # 应用纵横比调整
    image_2d = resize_with_aspect(image_2d, aspect_ratio)
    
    # 2D 图像标准化尺寸（从 .env 读取 PREVIEW_TARGET_SIZE）
    if not is_3d:
        target_size = get_preview_target_size(default=896)
        image_2d = normalize_2d_preview(image_2d, target_size=target_size)
    
    # 保存预览图
    base_name = sanitize_folder_name(series_name)
    if output_suffix:
        preview_name = f"{base_name}_{output_suffix}_preview.png"
    else:
        preview_name = f"{base_name}_preview.png"
    preview_path = os.path.join(series_dir, preview_name)
    
    Image.fromarray(image_2d).save(preview_path)
    return preview_path


def _get_slice_thickness(dcm) -> Optional[float]:
    """
    从DICOM对象获取层厚信息

    Args:
        dcm: pydicom Dataset 对象

    Returns:
        float: 层厚(mm)，失败时返回 None
    """
    try:
        if dcm is None:
            return None

        # 优先使用 SliceThickness
        thickness = getattr(dcm, 'SliceThickness', None)
        if thickness is not None:
            return float(thickness)

        # 备选：使用 SpacingBetweenSlices
        spacing = getattr(dcm, 'SpacingBetweenSlices', None)
        if spacing is not None:
            return float(spacing)

        return None
    except Exception:
        return None


def _draw_text_on_image(
    img: np.ndarray,
    text: str,
    position: Tuple[int, int],
    font_size: int = 20,
    color: Tuple[int, int, int] = (255, 255, 255),
    shadow_color: Tuple[int, int, int] = (0, 0, 0)
) -> np.ndarray:
    """
    在图像上绘制文字（带阴影效果）

    Args:
        img: 输入图像数组
        text: 要绘制的文字
        position: 文字位置 (x, y)
        font_size: 字体大小
        color: 文字颜色
        shadow_color: 阴影颜色

    Returns:
        np.ndarray: 绘制文字后的图像
    """
    try:
        pil_img = Image.fromarray(img).convert('RGB')
        draw = ImageDraw.Draw(pil_img)

        # 尝试加载字体，失败则使用默认字体
        try:
            # 尝试常见字体路径
            font_paths = [
                '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
                '/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
                '/System/Library/Fonts/Helvetica.ttc',
                'C:/Windows/Fonts/arial.ttf',
            ]
            font = None
            for fp in font_paths:
                if os.path.exists(fp):
                    font = ImageFont.truetype(fp, font_size)
                    break
            if font is None:
                font = ImageFont.load_default()
        except Exception:
            font = ImageFont.load_default()

        x, y = position

        # 绘制阴影
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                if dx != 0 or dy != 0:
                    draw.text((x + dx, y + dy), text, font=font, fill=shadow_color)

        # 绘制主文字
        draw.text((x, y), text, font=font, fill=color)

        return np.array(pil_img)
    except Exception:
        return img


def _load_3d_volume(
    preview_file: str,
    modality: str,
    sample_dcm
) -> Optional[np.ndarray]:
    """
    加载3D体积数据

    Args:
        preview_file: 图像文件路径
        modality: 模态类型
        sample_dcm: 样本DICOM对象

    Returns:
        3D体积数据 (Z, Y, X) 或 (X, Y, Z)，失败时返回None
    """
    try:
        if preview_file.endswith('.npz'):
            with np.load(preview_file) as npz:
                if 'data' in npz.files:
                    data = npz['data']
                elif npz.files:
                    data = npz[npz.files[0]]
                else:
                    return None
            return data.astype(np.float32) if data.ndim == 3 else None

        elif preview_file.endswith(('.nii', '.nii.gz')):
            img = nib.load(preview_file)

            # 处理4D+数据，提取第一个volume
            if len(img.shape) > 3:
                try:
                    slicer = (slice(None), slice(None), slice(None)) + (0,) * (len(img.shape) - 3)
                    data = np.asarray(img.dataobj[slicer])
                    while data.ndim > 3:
                        data = data[..., 0]
                    img = nib.Nifti1Image(data, img.affine)
                except Exception:
                    pass

            try:
                img_canonical = nib.as_closest_canonical(img)
                data = img_canonical.get_fdata()
            except Exception:
                # 兜底处理
                try:
                    data = np.asarray(img.get_fdata())
                except Exception:
                    return None

            # 防御性处理
            while data.ndim > 3:
                data = data[..., 0]

            return data.astype(np.float32) if data.ndim == 3 else None

        return None
    except Exception:
        return None


def _extract_orthogonal_slices(
    data: np.ndarray,
    orientation: str = 'AX'
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    从3D体积提取三个正交切片的中间层面

    Args:
        data: 3D体积数据
        orientation: 扫描方位

    Returns:
        Tuple: (axial_slice, sagittal_slice, coronal_slice)
    """
    try:
        if data.ndim != 3:
            # 返回空白图像
            blank = np.zeros((128, 128), dtype=np.float32)
            return blank, blank, blank

        # 获取中间层面索引
        mid_x = data.shape[0] // 2
        mid_y = data.shape[1] // 2
        mid_z = data.shape[2] // 2

        # 提取三个正交切片
        # 注意：根据数据格式不同，切片方向可能需要调整

        # Axial (横断位): 通常沿Z轴
        axial = data[:, :, mid_z]

        # Sagittal (矢状位): 通常沿X轴
        sagittal = data[mid_x, :, :]

        # Coronal (冠状位): 通常沿Y轴
        coronal = data[:, mid_y, :]

        return axial, sagittal, coronal
    except Exception:
        blank = np.zeros((128, 128), dtype=np.float32)
        return blank, blank, blank


def _generate_3d_triplane_preview(
    preview_file: str,
    series_dir: str,
    series_name: str,
    sample_dcm,
    modality: str,
    sanitize_folder_name: Callable[[str], str]
) -> Optional[str]:
    """
    生成3D三视图预览图（横断、矢状、冠状位）

    Args:
        preview_file: 输入图像文件路径
        series_dir: 输出目录
        series_name: 序列名称
        sample_dcm: 样本DICOM对象
        modality: 模态类型
        sanitize_folder_name: 文件夹名称清理函数

    Returns:
        生成的预览图路径，失败时返回None
    """
    try:
        # 加载3D体积数据
        data = _load_3d_volume(preview_file, modality, sample_dcm)
        if data is None:
            return None

        # 获取扫描方位
        raw_orientation = _get_orientation_from_dcm(sample_dcm)
        orientation = normalize_orientation(raw_orientation, sample_dcm)

        # 加载原始3D数据用于提取三视图（使用nibabel canonical格式）
        if preview_file.endswith(('.nii', '.nii.gz')):
            img = nib.load(preview_file)
            if len(img.shape) > 3:
                try:
                    slicer = (slice(None), slice(None), slice(None)) + (0,) * (len(img.shape) - 3)
                    first_volume = np.asarray(img.dataobj[slicer])
                    while first_volume.ndim > 3:
                        first_volume = first_volume[..., 0]
                    img = nib.Nifti1Image(first_volume, img.affine)
                except Exception:
                    pass

            try:
                img_canonical = nib.as_closest_canonical(img)
                volume = img_canonical.get_fdata().astype(np.float32)
            except Exception:
                volume = np.asarray(img.get_fdata()).astype(np.float32)

            while volume.ndim > 3:
                volume = volume[..., 0]

            # canonical volume is (X, Y, Z)
            # X: Left->Right, Y: Posterior->Anterior, Z: Inferior->Superior
            mid_x = volume.shape[0] // 2
            mid_y = volume.shape[1] // 2
            mid_z = volume.shape[2] // 2

            # 提取三个正交切片的中间层面
            # Axial (横断位): Z轴中间
            slice_axial = volume[:, :, mid_z]
            slice_axial = np.transpose(slice_axial, (1, 0))[::-1, ::-1]

            # Sagittal (矢状位): X轴中间
            slice_sagittal = volume[mid_x, :, :]
            slice_sagittal = np.transpose(slice_sagittal, (1, 0))[::-1, ::-1]

            # Coronal (冠状位): Y轴中间
            slice_coronal = volume[:, mid_y, :]
            slice_coronal = np.transpose(slice_coronal, (1, 0))[::-1, ::-1]

        elif preview_file.endswith('.npz'):
            with np.load(preview_file) as npz:
                if 'data' in npz.files:
                    volume = npz['data'].astype(np.float32)
                elif npz.files:
                    volume = npz[npz.files[0]].astype(np.float32)
                else:
                    return None

            # npz data is saved as (Z, Y, X)
            mid_z = volume.shape[0] // 2
            mid_y = volume.shape[1] // 2
            mid_x = volume.shape[2] // 2

            # 提取三个正交切片的中间层面
            slice_axial = volume[mid_z, :, :]
            slice_sagittal = volume[:, :, mid_x]
            slice_coronal = volume[:, mid_y, :]
        else:
            return None

        # 获取总层数
        total_slices = volume.shape[2] if preview_file.endswith(('.nii', '.nii.gz')) else volume.shape[0]

        # 应用窗宽窗位
        slice_axial = apply_windowing(slice_axial, sample_dcm, modality)
        slice_sagittal = apply_windowing(slice_sagittal, sample_dcm, modality)
        slice_coronal = apply_windowing(slice_coronal, sample_dcm, modality)

        # 获取各方向的实际层数，用于判断原始采集方向
        if preview_file.endswith(('.nii', '.nii.gz')):
            n_axial = volume.shape[2]  # Z方向层数
            n_sagittal = volume.shape[0]  # X方向层数
            n_coronal = volume.shape[1]  # Y方向层数
        else:  # npz
            n_axial = volume.shape[0]  # Z方向层数
            n_sagittal = volume.shape[2]  # X方向层数
            n_coronal = volume.shape[1]  # Y方向层数

        # 判断原始采集方向：层数最少的方向是扫描层厚方向（原始采集方向）
        layer_counts = {'axial': n_axial, 'sagittal': n_sagittal, 'coronal': n_coronal}
        original_orientation = min(layer_counts, key=layer_counts.get)
        original_slices = layer_counts[original_orientation]

        # 从NIfTI header获取体素尺寸
        try:
            voxel_sizes = img.header.get_zooms()[:3]  # (dx, dy, dz) in mm
            dx, dy, dz = voxel_sizes[0], voxel_sizes[1], voxel_sizes[2]
        except Exception:
            # 如果无法获取，从层厚推断
            st = slice_thickness if slice_thickness else 1.0
            dx = dy = dz = st

        # 计算各方向的物理尺寸 (mm)
        phys_x = n_sagittal * dx  # 左右方向
        phys_y = n_coronal * dy   # 前后方向（可能是层厚方向）
        phys_z = n_axial * dz     # 头足方向

        # 调整纵横比：只对重建方向（非原始方向）进行调整
        def adjust_aspect_ratio(slice_img, phys_width, phys_height):
            """根据物理尺寸调整图像纵横比"""
            h, w = slice_img.shape
            current_aspect = w / h
            target_aspect = phys_width / phys_height

            # 如果纵横比接近，不需要调整
            if abs(current_aspect - target_aspect) / target_aspect < 0.1:
                return slice_img

            # 计算调整后的尺寸，保持物理纵横比
            if current_aspect < target_aspect:
                new_w = int(h * target_aspect)
                new_h = h
            else:
                new_w = w
                new_h = int(w / target_aspect)

            pil_img = Image.fromarray(slice_img)
            pil_img = pil_img.resize((new_w, new_h), Image.BILINEAR)
            return np.array(pil_img)

        # 注意：经过transpose后，各方向切片已经呈现真实物理纵横比
        # Axial (横断位): 宽=phys_x, 高=phys_y (对于冠状位扫描，phys_y很小，所以图像很扁)
        # Sagittal (矢状位): 宽=phys_y, 高=phys_z (对于冠状位扫描，phys_y很小，所以图像很窄)
        # Coronal (冠状位): 宽=phys_x, 高=phys_z (原始采集方向，通常是正方形)
        # 不需要额外调整纵横比，直接显示真实比例

        # 转换为RGB用于绘制彩色文字
        slice_axial_rgb = np.stack([slice_axial, slice_axial, slice_axial], axis=2)
        slice_sagittal_rgb = np.stack([slice_sagittal, slice_sagittal, slice_sagittal], axis=2)
        slice_coronal_rgb = np.stack([slice_coronal, slice_coronal, slice_coronal], axis=2)

        # 获取层厚信息
        slice_thickness = _get_slice_thickness(sample_dcm)
        thickness_text = f"ST: {slice_thickness:.2f}mm" if slice_thickness else "ST: N/A"
        count_text = f"Slices: {original_slices}"

        # 绘制标签：只在原始方向标注层数和层厚
        def draw_orientation_label(img, orientation_name, is_original):
            """绘制方位标签"""
            img[:85, :, :] = 0
            if is_original:
                img = _draw_text_on_image(img, f"{orientation_name} ({original_slices})",
                                          (10, 10), font_size=20, color=(255, 255, 0))
                img = _draw_text_on_image(img, thickness_text, (10, 40),
                                          font_size=16, color=(255, 255, 255))
                img = _draw_text_on_image(img, count_text, (10, 65),
                                          font_size=16, color=(255, 255, 255))
            else:
                img = _draw_text_on_image(img, orientation_name, (10, 10),
                                          font_size=20, color=(255, 255, 0))
            return img

        slice_axial_rgb = draw_orientation_label(slice_axial_rgb, "AXIAL",
                                                  original_orientation == 'axial')
        slice_sagittal_rgb = draw_orientation_label(slice_sagittal_rgb, "SAGITTAL",
                                                     original_orientation == 'sagittal')
        slice_coronal_rgb = draw_orientation_label(slice_coronal_rgb, "CORONAL",
                                                    original_orientation == 'coronal')

        # 统一三个视图的大小
        max_h = max(slice_axial_rgb.shape[0], slice_sagittal_rgb.shape[0], slice_coronal_rgb.shape[0])
        max_w = max(slice_axial_rgb.shape[1], slice_sagittal_rgb.shape[1], slice_coronal_rgb.shape[1])

        def resize_to_canvas(img, target_h, target_w):
            """将图像缩放到目标尺寸并居中放置"""
            h, w = img.shape[:2]
            # 计算缩放比例，保持纵横比
            scale = min(target_h / h, target_w / w)
            new_h, new_w = int(h * scale), int(w * scale)

            pil_img = Image.fromarray(img)
            pil_img = pil_img.resize((new_w, new_h), Image.BILINEAR)
            resized = np.array(pil_img)

            # 创建画布并居中放置
            canvas = np.zeros((target_h, target_w, 3), dtype=np.uint8)
            top = (target_h - new_h) // 2
            left = (target_w - new_w) // 2
            canvas[top:top + new_h, left:left + new_w] = resized
            return canvas

        slice_axial_resized = resize_to_canvas(slice_axial_rgb, max_h, max_w)
        slice_sagittal_resized = resize_to_canvas(slice_sagittal_rgb, max_h, max_w)
        slice_coronal_resized = resize_to_canvas(slice_coronal_rgb, max_h, max_w)

        # 水平拼接三个视图
        triplane = np.concatenate([slice_axial_resized, slice_sagittal_resized, slice_coronal_resized], axis=1)

        # 保存预览图
        base_name = sanitize_folder_name(series_name)
        preview_name = f"{base_name}_preview.png"
        preview_path = os.path.join(series_dir, preview_name)

        Image.fromarray(triplane).save(preview_path)
        return preview_path

    except Exception as e:
        print(f"❌ Error generating 3D triplane preview: {str(e)}")
        return None


def generate_series_preview(
    series_dir: str,
    series_name: str,
    conversion_result: dict,
    sample_dcm,
    modality: str,
    sanitize_folder_name: Callable[[str], str]
) -> Optional[str]:
    """
    生成序列预览图

    对于2D模态（DR/DX/CR/MG），为每个文件生成预览图；
    对于3D模态，从中间层提取并生成单张预览图。

    Args:
        series_dir: 序列目录路径
        series_name: 序列名称
        conversion_result: 转换结果字典
        sample_dcm: 样本 DICOM 对象（用于获取窗宽窗位）
        modality: 模态类型（MR/CT/DR等）
        sanitize_folder_name: 文件夹名称清理函数

    Returns:
        Optional[str]: 预览图文件路径（主预览图），失败时返回 None
    """
    try:
        if not (conversion_result and conversion_result.get('success')):
            return None

        # 获取输出文件列表
        output_files = []
        if conversion_result.get('conversion_mode') == 'individual':
            output_files = conversion_result.get('output_files', [])
        else:
            output_file = conversion_result.get('output_file')
            if output_file:
                output_files = [output_file]

        if not output_files:
            output_files = conversion_result.get('output_files', [])

        if not output_files:
            return None

        # 构建完整路径并过滤不存在文件
        output_files = [os.path.join(series_dir, f) for f in output_files]
        output_files = [f for f in output_files if os.path.exists(f)]
        if not output_files:
            return None

        modality = (modality or '').upper()

        # 判断是否为2D X射线模态
        is_2d_xray = modality in ['DR', 'MG', 'DX', 'CR']
        
        generated_previews = []
        
        if is_2d_xray and len(output_files) > 1:
            # 2D X射线模态且有多张图像：为每张生成预览图
            for idx, preview_file in enumerate(output_files):
                # 生成序号后缀（如 0001, 0002）
                file_idx = idx + 1
                suffix = f"{file_idx:04d}"
                
                preview_path = _generate_single_preview(
                    preview_file=preview_file,
                    preview_idx=idx,
                    is_3d=False,
                    series_dir=series_dir,
                    series_name=series_name,
                    sample_dcm=sample_dcm,
                    modality=modality,
                    sanitize_folder_name=sanitize_folder_name,
                    output_suffix=suffix
                )
                
                if preview_path:
                    generated_previews.append(preview_path)
            
            # 返回第一张作为主预览图
            return generated_previews[0] if generated_previews else None
            
        else:
            # 3D模态或单张2D图像
            if is_2d_xray or len(output_files) > 1:
                # 2D X射线多张图像：使用单张预览
                preview_idx = len(output_files) // 2
                preview_file = output_files[preview_idx]
                is_3d = False

                preview_path = _generate_single_preview(
                    preview_file=preview_file,
                    preview_idx=preview_idx,
                    is_3d=is_3d,
                    series_dir=series_dir,
                    series_name=series_name,
                    sample_dcm=sample_dcm,
                    modality=modality,
                    sanitize_folder_name=sanitize_folder_name,
                    output_suffix=""
                )
                return preview_path
            else:
                # 3D模态：生成三视图预览
                preview_file = output_files[0]

                preview_path = _generate_3d_triplane_preview(
                    preview_file=preview_file,
                    series_dir=series_dir,
                    series_name=series_name,
                    sample_dcm=sample_dcm,
                    modality=modality,
                    sanitize_folder_name=sanitize_folder_name
                )
                return preview_path
            
    except Exception:
        return None
