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
from PIL import Image
from typing import Tuple, Optional, Callable
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
            img_canonical = nib.as_closest_canonical(img)
            data = img_canonical.get_fdata()

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
                    # X: Left to Right -> Right to Left (flip)
                    # Y: Posterior to Anterior -> Anterior to Posterior (flip)
                    image_2d = np.transpose(slice_xy, (1, 0))[::-1, ::-1]
                
                image_2d = image_2d.astype(np.float32)
            else:
                image_2d = data if data.ndim == 2 else data[:, :, 0]
                image_2d = image_2d[::-1, :]
        else:
            return None
        
        return image_2d
    except Exception:
        return None


def _get_orientation_from_dcm(dcm) -> str:
    """
    从DICOM对象中获取扫描方位
    
    通过ImageOrientationPatient(IOP)计算法向量来确定方位。
    能够精确区分轴位(AX)，矢状位(SAG)，冠状位(COR)，并能识别斜位(OBL)。
    
    Args:
        dcm: pydicom Dataset 对象
        
    Returns:
        str: 标准化的方位名称 ('AX', 'SAG', 'COR', 'OBL', 'UNKNOWN')
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
    orientation = _get_orientation_from_dcm(sample_dcm)

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
    
    # 计算纵横比
    aspect_ratio = None
    try:
        dcm_for_spacing = file_dcm if file_dcm is not None else sample_dcm
        if dcm_for_spacing is not None:
            pixel_spacing = getattr(dcm_for_spacing, 'PixelSpacing', None)
            spacing_between = getattr(dcm_for_spacing, 'SpacingBetweenSlices', None)
            slice_thickness = getattr(dcm_for_spacing, 'SliceThickness', None)
            if modality == 'MR':
                slice_spacing = max(spacing_between, slice_thickness)
            else:
                slice_spacing = float(slice_thickness + spacing_between or 1.0)
            if pixel_spacing and len(pixel_spacing) >= 2:
                pixel_spacing = [float(pixel_spacing[0]), float(pixel_spacing[1])]
                if is_3d:
                    # 因为我们总是提取与原始DICOM相同方位的切片，
                    # 所以切片的面内像素间距总是由 PixelSpacing 决定。
                    # 无论是 AX, COR 还是 SAG，纵横比都应该是 PixelSpacing 的比值。
                    # DICOM PixelSpacing 是 [row_spacing, col_spacing]
                    # 对应于图像的 [height_spacing, width_spacing]
                    aspect_ratio = pixel_spacing[0] / max(pixel_spacing[1], 1e-6)
                else:
                    # 2D图像：直接使用像素间距计算
                    aspect_ratio = pixel_spacing[0] / max(pixel_spacing[1], 1e-6)
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
            # 3D模态或单张2D图像：只生成一张预览图
            if is_2d_xray or len(output_files) > 1:
                preview_idx = len(output_files) // 2
                preview_file = output_files[preview_idx]
                is_3d = False
            else:
                preview_idx = 0
                preview_file = output_files[0]
                is_3d = True
            
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
            
    except Exception:
        return None
