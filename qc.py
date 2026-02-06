# -*- coding: utf-8 -*-
"""Quality control helpers."""

import numpy as np
import nibabel as nib


def _apply_rescale(pixel_data, dcm):
    try:
        slope = float(getattr(dcm, 'RescaleSlope', 1.0))
        intercept = float(getattr(dcm, 'RescaleIntercept', 0.0))
        return pixel_data.astype(np.float32) * slope + intercept
    except Exception:
        return pixel_data.astype(np.float32)


def _apply_photometric(pixel_data, dcm):
    try:
        photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
        if photometric == 'MONOCHROME1':
            max_val = np.nanmax(pixel_data)
            return max_val - pixel_data
    except Exception:
        pass
    return pixel_data


def assess_image_quality(dcm):
    try:
        if not hasattr(dcm, 'pixel_array'):
            return 1

        pixel_data = dcm.pixel_array.astype(np.float32)
        pixel_data = _apply_rescale(pixel_data, dcm)
        pixel_data = _apply_photometric(pixel_data, dcm)
        return assess_image_quality_from_array(pixel_data)
    except Exception:
        return 1


def assess_image_quality_from_array(pixel_data):
    try:
        if pixel_data is None:
            return 1

        pixel_data = np.asarray(pixel_data, dtype=np.float32)
        flat = pixel_data[np.isfinite(pixel_data)].ravel()
        if flat.size == 0:
            return 1

        if flat.size > 200000:
            flat = flat[:: max(1, flat.size // 200000)]

        p2, p98 = np.percentile(flat, [2, 98])
        dynamic_range = p98 - p2
        std = float(np.std(flat))
        unique_ratio = len(np.unique(flat)) / max(1, flat.size)

        if dynamic_range <= 0:
            return 1

        range_eps = max(dynamic_range, 1e-6)
        mean_val = float(np.mean(flat))

        low_thresh = p2 + 0.01 * range_eps
        high_thresh = p98 - 0.01 * range_eps
        low_ratio = float(np.mean(flat <= low_thresh))
        high_ratio = float(np.mean(flat >= high_thresh))

        under_exposed = mean_val < (p2 + 0.1 * range_eps) or low_ratio > 0.6
        over_exposed = mean_val > (p98 - 0.1 * range_eps) or high_ratio > 0.6

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
            else:
                inverted_like = False
        else:
            inverted_like = False

        if dynamic_range < 20 or std < 5 or unique_ratio < 0.01:
            return 1

        if under_exposed or over_exposed or inverted_like:
            return 1

        return 0
    except Exception:
        return 1


def assess_converted_file_quality(filepath):
    try:
        if filepath.endswith('.npz'):
            with np.load(filepath) as npz:
                if 'data' in npz.files:
                    data = npz['data']
                elif npz.files:
                    data = npz[npz.files[0]]
                else:
                    return 1
        elif filepath.endswith(('.nii', '.nii.gz')):
            img = nib.load(filepath)
            data = img.get_fdata()
        else:
            return 1

        return assess_image_quality_from_array(data)
    except Exception:
        return 1


def assess_series_quality_converted(converted_files):
    try:
        total = len(converted_files)
        if total == 0:
            return {
                'low_quality': 1,
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

        low_count = 0
        for idx in sample_indices:
            try:
                low_count += int(assess_converted_file_quality(converted_files[idx]))
            except Exception:
                low_count += 1

        ratio = low_count / max(1, len(sample_indices))
        low_quality = 1 if ratio > 0.3 else 0

        return {
            'low_quality': low_quality,
            'low_quality_ratio': ratio,
            'qc_mode': qc_mode,
            'qc_sample_indices': sample_indices
        }
    except Exception:
        return {
            'low_quality': 1,
            'low_quality_ratio': 1.0,
            'qc_mode': 'error',
            'qc_sample_indices': []
        }


def assess_series_quality(dicom_files, dcmread):
    try:
        total = len(dicom_files)
        if total == 0:
            return {
                'low_quality': 1,
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

        low_count = 0
        for idx in sample_indices:
            try:
                dcm = dcmread(dicom_files[idx], force=True)
                low_count += int(assess_image_quality(dcm))
            except Exception:
                low_count += 1

        ratio = low_count / max(1, len(sample_indices))
        low_quality = 1 if ratio > 0.3 else 0

        return {
            'low_quality': low_quality,
            'low_quality_ratio': ratio,
            'qc_mode': qc_mode,
            'qc_sample_indices': sample_indices
        }
    except Exception:
        return {
            'low_quality': 1,
            'low_quality_ratio': 1.0,
            'qc_mode': 'error',
            'qc_sample_indices': []
        }
