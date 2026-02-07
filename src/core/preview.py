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

    从转换后的 NIfTI/NPZ 文件中提取中间切片，
    应用窗宽窗位变换后生成 PNG 预览图。

    Args:
        series_dir: 序列目录路径
        series_name: 序列名称
        conversion_result: 转换结果字典
        sample_dcm: 样本 DICOM 对象（用于获取窗宽窗位）
        modality: 模态类型（MR/CT/DR等）
        sanitize_folder_name: 文件夹名称清理函数

    Returns:
        Optional[str]: 预览图文件路径，失败时返回 None
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

        # 根据模态选择预览策略
        if modality in ['DR', 'MG', 'DX'] or len(output_files) > 1:
            preview_idx = len(output_files) // 2
            preview_file = output_files[preview_idx]
            is_3d = False
        else:
            preview_idx = 0
            preview_file = output_files[0]
            is_3d = True

        # 加载图像数据
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
                image_2d=image_2d[::-1, :]
        else:
            return None

        # 尺寸校正：检查是否需要转置
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
        except Exception:
            pass

        # 应用窗宽窗位
        image_2d = apply_windowing(image_2d, sample_dcm)

        # 计算纵横比
        aspect_ratio = None
        try:
            if sample_dcm is not None:
                pixel_spacing = getattr(sample_dcm, 'PixelSpacing', None)
                spacing_between = getattr(sample_dcm, 'SpacingBetweenSlices', None)
                slice_thickness = getattr(sample_dcm, 'SliceThickness', None)
                if modality == 'MR':
                    slice_spacing = max(spacing_between,slice_thickness)
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
        preview_name = f"{sanitize_folder_name(series_name)}_preview.png"
        preview_path = os.path.join(series_dir, preview_name)

        Image.fromarray(image_2d).save(preview_path)
        return preview_path
    except Exception:
        return None
