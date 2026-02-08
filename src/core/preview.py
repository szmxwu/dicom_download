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


def apply_windowing(image_2d: np.ndarray, dcm) -> np.ndarray:
    """
    应用窗宽窗位变换

    Args:
        image_2d: 2D 图像数组
        dcm: pydicom Dataset 对象（可选）

    Returns:
        np.ndarray: 8位灰度图像数组
    """
    img = image_2d.astype(np.float32)
    wc, ww = get_window_params(dcm)
    if wc is not None and ww is not None:
        low = wc - ww / 2.0
        high = wc + ww / 2.0
    else:
        low, high = np.percentile(img[np.isfinite(img)], [1, 99])

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


def _load_image_2d(preview_file: str, is_3d: bool, preview_idx: int) -> Optional[np.ndarray]:
    """
    从NIfTI或NPZ文件加载2D图像数据
    
    Args:
        preview_file: 图像文件路径
        is_3d: 是否为3D图像
        preview_idx: 预览索引（用于错误信息）
        
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
                mid_y = data.shape[1] // 2
                image_2d = data[:, mid_y, :]
                image_2d = image_2d.astype(np.float32)
            else:
                image_2d = data if data.ndim == 2 else data[0, :, :]

        elif preview_file.endswith(('.nii', '.nii.gz')):
            img = nib.load(preview_file)
            img_canonical = nib.as_closest_canonical(img)
            data = img_canonical.get_fdata()

            if data.ndim == 3 and is_3d:
                mid_y = data.shape[1] // 2
                slice_xz = data[:, mid_y, :]
                image_2d = np.transpose(slice_xz, (1, 0))
                image_2d = image_2d[::-1, ::-1].astype(np.float32)
            else:
                image_2d = data if data.ndim == 2 else data[:, :, 0]
                image_2d = image_2d[::-1, :]
        else:
            return None
        
        return image_2d
    except Exception:
        return None


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

        if (rows is None or cols is None) and sample_dcm is not None:
            rows = getattr(sample_dcm, 'Rows', None)
            cols = getattr(sample_dcm, 'Columns', None)

        # 如果尺寸颠倒，进行转置校正
        if rows and cols:
            h, w = image_2d.shape[:2]
            if h == int(cols) and w == int(rows):
                image_2d = image_2d.T
                image_2d = image_2d[::-1, :]
        
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
    # 加载图像
    image_2d = _load_image_2d(preview_file, is_3d, preview_idx)
    if image_2d is None:
        return None
    
    # 校正方向
    image_2d = _correct_image_orientation(
        image_2d, preview_file, preview_idx, is_3d, series_dir, sample_dcm
    )
    
    # 获取该文件特定的DICOM信息（用于窗宽窗位等）
    file_dcm = _get_file_dcm_info(preview_file, preview_idx, series_dir, sample_dcm)
    
    # 应用窗宽窗位
    image_2d = apply_windowing(image_2d, file_dcm)
    
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
                    aspect_ratio = slice_spacing / max(pixel_spacing[1], 1e-6)
                else:
                    aspect_ratio = pixel_spacing[0] / max(pixel_spacing[1], 1e-6)
    except Exception:
        aspect_ratio = None
    
    # 应用纵横比调整
    image_2d = resize_with_aspect(image_2d, aspect_ratio)
    
    # 2D 图像标准化尺寸
    if not is_3d:
        image_2d = normalize_2d_preview(image_2d, target_size=896)
    
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
