# -*- coding: utf-8 -*-
"""Preview image helpers."""

import os
import re
import numpy as np
import nibabel as nib
from PIL import Image
import json

def get_window_params(dcm):
    try:
        if dcm is None:
            return None, None
        wc = getattr(dcm, 'WindowCenter', None)
        ww = getattr(dcm, 'WindowWidth', None)
        if wc is None or ww is None:
            return None, None

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


def apply_windowing(image_2d, dcm):
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

    try:
        if dcm is not None:
            photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
            if photometric == 'MONOCHROME1':
                img = 255 - img
    except Exception:
        pass

    return img


def resize_with_aspect(img, aspect_ratio):
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


def normalize_2d_preview(img, target_size=896):
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

        canvas = np.zeros((target_size, target_size), dtype=np.uint8)
        top = max(0, (target_size - new_h) // 2)
        left = max(0, (target_size - new_w) // 2)
        canvas[top:top + new_h, left:left + new_w] = resized
        return canvas
    except Exception:
        return img


def generate_series_preview(series_dir, series_name, conversion_result, sample_dcm, modality, sanitize_folder_name):
    try:
        if not (conversion_result and conversion_result.get('success')):
            return None

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

        output_files = [os.path.join(series_dir, f) for f in output_files]
        output_files = [f for f in output_files if os.path.exists(f)]
        if not output_files:
            return None

        modality = (modality or '').upper()

        if modality in ['DR', 'MG', 'DX'] or len(output_files) > 1:
            preview_idx = len(output_files) // 2
            preview_file = output_files[preview_idx]
            is_3d = False
        else:
            preview_idx = 0
            preview_file = output_files[0]
            is_3d = True

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
                slice_xz = data[:,mid_y, :]
                image_2d = np.transpose(slice_xz, (1, 0))
                image_2d = image_2d[::-1, :]
            else:
                image_2d = data if data.ndim == 2 else data[:, :, 0]
        else:
            return None

        # 校正：2D 图像不能只依赖 sample_dcm，优先用每张图的元数据做 Rows/Columns 校验
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
                    except Exception:
                        records = []

                    if records:
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

            if rows and cols:
                h, w = image_2d.shape[:2]
                # 如果当前图像尺寸与 DICOM 的 Rows/Columns 正好颠倒，则转置修正
                if h == int(cols) and w == int(rows):
                    image_2d = image_2d.T
                    image_2d = image_2d[::-1, :]
        except Exception:
            pass

        image_2d = apply_windowing(image_2d, sample_dcm)

        aspect_ratio = None
        try:
            if sample_dcm is not None:
                pixel_spacing = getattr(sample_dcm, 'PixelSpacing', None)
                spacing_between = getattr(sample_dcm, 'SpacingBetweenSlices', None)
                slice_thickness = getattr(sample_dcm, 'SliceThickness', None)
                slice_spacing = float(slice_thickness + spacing_between  or 1.0)
                if pixel_spacing and len(pixel_spacing) >= 2:
                    pixel_spacing = [float(pixel_spacing[0]), float(pixel_spacing[1])]
                    if is_3d:
                        aspect_ratio = slice_spacing / max(pixel_spacing[1], 1e-6)
                    else:
                        aspect_ratio = pixel_spacing[0] / max(pixel_spacing[1], 1e-6)
        except Exception:
            aspect_ratio = None

        image_2d = resize_with_aspect(image_2d, aspect_ratio)

        if not is_3d:
            image_2d = normalize_2d_preview(image_2d, target_size=896)

        preview_name = f"{sanitize_folder_name(series_name)}_preview.png"
        preview_path = os.path.join(series_dir, preview_name)

        Image.fromarray(image_2d).save(preview_path)
        return preview_path
    except Exception:
        return None
