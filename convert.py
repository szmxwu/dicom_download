# -*- coding: utf-8 -*-
"""Conversion helpers for DICOM -> NIfTI/NPZ."""

import os
import shutil
import numpy as np
import nibabel as nib
import pydicom


def apply_rescale(pixel_data, dcm):
    try:
        slope = float(getattr(dcm, 'RescaleSlope', 1.0))
        intercept = float(getattr(dcm, 'RescaleIntercept', 0.0))
        return pixel_data.astype(np.float32) * slope + intercept
    except Exception:
        return pixel_data.astype(np.float32)


def apply_photometric(pixel_data, dcm):
    try:
        photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
        if photometric == 'MONOCHROME1':
            max_val = np.nanmax(pixel_data)
            return max_val - pixel_data
    except Exception:
        pass
    return pixel_data


def build_affine_from_dicom(dcm, slice_spacing=1.0, slice_cosines=None):
    try:
        iop = getattr(dcm, 'ImageOrientationPatient', None)
        ipp = getattr(dcm, 'ImagePositionPatient', None)
        pixel_spacing = getattr(dcm, 'PixelSpacing', [1.0, 1.0])
        if iop is None or ipp is None:
            raise ValueError("Missing orientation/position")

        row_cosine = np.array([float(i) for i in iop[:3]], dtype=np.float64)
        col_cosine = np.array([float(i) for i in iop[3:6]], dtype=np.float64)
        if slice_cosines is None:
            slice_cosines = np.cross(row_cosine, col_cosine)

        row_spacing = float(pixel_spacing[0])
        col_spacing = float(pixel_spacing[1])

        affine_lps = np.eye(4, dtype=np.float64)
        affine_lps[:3, 0] = row_cosine * row_spacing
        affine_lps[:3, 1] = col_cosine * col_spacing
        affine_lps[:3, 2] = slice_cosines * float(slice_spacing)
        affine_lps[:3, 3] = np.array([float(i) for i in ipp], dtype=np.float64)

        lps_to_ras = np.diag([-1.0, -1.0, 1.0, 1.0])
        affine_ras = lps_to_ras @ affine_lps
        return affine_ras
    except Exception:
        return np.eye(4, dtype=np.float64)


def normalize_and_save_npz(nii_path, npz_path):
    # 加载 NIfTI 文件，返回一个 Nifti1Image 对象（包含数据和头信息）
    img = nib.load(nii_path)
    # 将图像转换为最接近的标准方向（canonical），以统一轴向（通常为 RAS）
    img_canonical = nib.as_closest_canonical(img)
    # 从 Nifti 对象中获取数据数组（通常为 float64），形状如 (X, Y, Z[, T])
    data = img_canonical.get_fdata()

    # 对数据在每个轴上做反转。具体含义：
    # - 第一个索引 `[::-1]` 代表在第 0 轴（X）上反转
    # - 第二个索引 `[::-1]` 代表在第 1 轴（Y）上反转
    # - 第三个索引 `[::-1]` 代表在第 2 轴（Z）上反转
    # 这样做通常用于将 NIfTI 的内部存储方向调整为期望的显示/处理方向
    data = data[::-1, ::-1, ::-1]
    # 重新排列轴顺序：把数据从 (X, Y, Z) 变为 (Z, Y, X)
    # 这样第一维表示切片索引（slice），第二维为行，第三维为列，便于按切片处理或与其他工具兼容
    data = np.transpose(data, (2, 1, 0))

    # 将数据转换为 float32（节省空间）并以压缩的 npz 格式写入磁盘
    np.savez_compressed(npz_path, data=data.astype(np.float32))


def convert_dicom_to_nifti(client, series_dir, series_name):
    try:
        print(f"   🔄 Converting {series_name} to NIfTI...")

        sample_dcm, modality = client._get_series_sample_dicom(series_dir)
        dicom_files = []
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
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        import json
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
            return nifti_result

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
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        import json
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
        return nifti_result

    except Exception as e:
        print(f"   ❌ NIfTI conversion failed: {e}")
        return {'success': False, 'error': str(e)}


def convert_to_npz(client, series_dir, series_name):
    try:
        print(f"   🔄 Converting {series_name} to NPZ (Normalized)...")

        sample_dcm, modality = client._get_series_sample_dicom(series_dir)

        nifti_res = convert_with_dcm2niix(client, series_dir, series_name)
        if not (nifti_res and nifti_res.get('success')):
            nifti_res = convert_with_python_libs(client, series_dir, series_name)

        if not (nifti_res and nifti_res.get('success')):
            return {'success': False, 'error': 'Failed to generate base volume for NPZ'}

        output_files = []
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
            [os.path.join(series_dir, f) for f in output_files]
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


def convert_with_dcm2niix(client, series_dir, series_name):
    try:
        import subprocess

        import sys

        # Choose dcm2niix command based on platform. On Linux prefer the
        # bundled dcm2niix executable (linux build) located next to this file.
        dcm2niix_cmd = 'dcm2niix'
        if sys.platform.startswith('linux'):
            bundled = os.path.join(os.path.dirname(__file__), 'dcm2niix')
            if os.path.exists(bundled) and os.access(bundled, os.X_OK):
                dcm2niix_cmd = bundled

        try:
            subprocess.run([dcm2niix_cmd, '-h'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            return {'success': False, 'error': 'dcm2niix not available'}

        dicom_files = []
        for file in os.listdir(series_dir):
            filepath = os.path.join(series_dir, file)
            if file.endswith('.dcm') and os.path.isfile(filepath):
                dicom_files.append(filepath)

        if not dicom_files:
            return {'success': False, 'error': 'No DICOM files found'}

        modality = ''
        sample_tags = client._load_sample_tags_from_cache(series_dir)
        if isinstance(sample_tags, dict):
            modality = str(sample_tags.get('Modality') or '')
        if not modality:
            first_dcm = pydicom.dcmread(dicom_files[0], force=True)
            modality = getattr(first_dcm, 'Modality', '')

        output_name = client._sanitize_folder_name(series_name)

        if modality in ['DR', 'MG', 'DX']:
            print(f"   ℹ️  Detected {modality} modality, converting each DICOM to NIfTI")

            success_count = 0
            output_files = []

            for idx, dcm_file in enumerate(dicom_files):
                temp_dir = None
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

                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

                    if result.returncode == 0:
                        nifti_file = f"{file_output_name}.nii.gz"
                        if os.path.exists(os.path.join(series_dir, nifti_file)):
                            output_files.append(nifti_file)
                            success_count += 1

                    if (idx + 1) % 10 == 0:
                        print(f"      Converted {idx + 1}/{len(dicom_files)} files...")

                except Exception as e:
                    print(f"      ⚠️  Failed converting file {idx+1}: {e}")
                finally:
                    if temp_dir and os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)

            if success_count > 0:
                print(f"   ✅ dcm2niix conversion succeeded: {success_count}/{len(dicom_files)} files")

                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)

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

        print(f"   ℹ️  {modality} modality: converting entire series to a single NIfTI file")

        cmd = [
            dcm2niix_cmd,
            '-m', 'y',
            '-f', output_name,
            '-o', series_dir,
            '-z', 'y',
            '-b', 'n',
            series_dir
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            nifti_files = [f for f in os.listdir(series_dir) if f.endswith(('.nii.gz', '.nii'))]
            if nifti_files:
                print(f"   ✅ dcm2niix conversion succeeded: {nifti_files[0]}")

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

        return {'success': False, 'error': result.stderr}

    except Exception as e:
        return {'success': False, 'error': str(e)}


def convert_with_python_libs(client, series_dir, series_name):
    try:
        dicom_files = []
        for file in os.listdir(series_dir):
            filepath = os.path.join(series_dir, file)
            if client._is_dicom_file(filepath):
                dicom_files.append(filepath)

        if not dicom_files:
            return {'success': False, 'error': 'No DICOM files found'}

        import pydicom
        first_dcm = pydicom.dcmread(dicom_files[0], force=True)
        modality = getattr(first_dcm, 'Modality', '')

        if modality in ['DR', 'MG', 'DX']:
            print(f"   ℹ️  Detected {modality} modality; converting each DICOM file to NIfTI")

            success_count = 0
            output_files = []

            for idx, dcm_file in enumerate(dicom_files):
                try:
                    dcm = pydicom.dcmread(dcm_file, force=True)

                    if not hasattr(dcm, 'pixel_array'):
                        print(f"      ⚠️  File {idx+1} has no pixel data")
                        continue

                    pixel_data = dcm.pixel_array
                    pixel_data = apply_rescale(pixel_data, dcm)
                    pixel_data = apply_photometric(pixel_data, dcm)

                    slice_thickness = float(getattr(dcm, 'SliceThickness', 1.0))
                    affine = build_affine_from_dicom(dcm, slice_spacing=slice_thickness)

                    if len(pixel_data.shape) == 2:
                        pixel_data = pixel_data[:, :, np.newaxis]

                    # 确保数据为 float32，创建 NIfTI 时指定 dtype
                    nifti_img = nib.Nifti1Image(pixel_data.astype(np.float32), affine)
                    # 将图像转换为最接近的 canonical 方向后再保存，保证方向一致性
                    nifti_img = nib.as_closest_canonical(nifti_img)

                    output_filename = f"{client._sanitize_folder_name(series_name)}_{idx+1:04d}.nii.gz"
                    output_path = os.path.join(series_dir, output_filename)
                    nib.save(nifti_img, output_path)

                    output_files.append(output_filename)
                    success_count += 1

                    if (idx + 1) % 10 == 0:
                        print(f"      Converted {idx + 1}/{len(dicom_files)} files...")

                except Exception as e:
                    print(f"      ⚠️  Failed converting file {idx+1}: {e}")
                    continue

            if success_count > 0:
                client._ensure_metadata_cache(series_dir, series_name, dicom_files, modality)
                for dcm_file in dicom_files:
                    try:
                        os.remove(dcm_file)
                    except Exception:
                        pass

                print(f"   ✅ Python libs conversion succeeded: {success_count}/{len(dicom_files)} files")
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

            slice_thickness = float(getattr(dcm, 'SliceThickness', 1.0))
            affine = build_affine_from_dicom(dcm, slice_spacing=slice_thickness)

            nifti_img = nib.Nifti1Image(pixel_data.astype(np.float32), affine)
            nifti_img = nib.as_closest_canonical(nifti_img)
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

        slice_info = []
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
                    z_pos = 0
                    ipp = None
                slice_info.append((z_pos, filepath, dcm, ipp))
            except Exception:
                continue

        if not slice_info:
            return {'success': False, 'error': 'Could not sort slices'}

        slice_info.sort(key=lambda x: x[0])

        slices = []
        positions = []
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
        nifti_img = nib.as_closest_canonical(nifti_img)
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
