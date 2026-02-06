# -*- coding: utf-8 -*-
"""DICOM organize stage helpers."""

import os
import shutil
import pydicom


def organize_dicom_files(client, extract_dir, organized_dir=None, output_format='nifti'):
    """Organize DICOM series and optionally convert."""
    if organized_dir is None:
        organized_dir = os.path.join(extract_dir, "organized")

    if output_format is True:
        output_format = 'nifti'
    elif output_format is False:
        output_format = None

    os.makedirs(organized_dir, exist_ok=True)

    print(f"📋 Organizing DICOM files (format: {output_format})...")
    print(f"📂 Source directory: {extract_dir}")
    print(f"📂 Organized directory: {organized_dir}")

    series_info = {}
    processed_files = 0

    for series_folder in os.listdir(extract_dir):
        if series_folder == "organized":
            continue

        series_path = os.path.join(extract_dir, series_folder)
        if not os.path.isdir(series_path):
            continue

        dicom_files = []
        for file in os.listdir(series_path):
            filepath = os.path.join(series_path, file)
            if os.path.isfile(filepath) and client._is_dicom_file(filepath):
                dicom_files.append(filepath)

        if dicom_files:
            processed_files += len(dicom_files)
            sample_dcm = None
            modality = ''
            try:
                sample_dcm = pydicom.dcmread(dicom_files[0], force=True)
                modality = str(getattr(sample_dcm, 'Modality', ''))
            except Exception:
                modality = ''

            client._cache_metadata_for_series(series_path, series_folder, dicom_files, modality)
            client._write_minimal_cache(
                series_path,
                series_folder,
                modality,
                sample_dcm=sample_dcm,
                file_count=len(dicom_files)
            )
            series_info[series_folder] = {
                'path': series_path,
                'file_count': len(dicom_files),
                'files': dicom_files
            }

            if output_format == 'nifti':
                client.convert_dicom_to_nifti(series_path, series_folder)
            elif output_format == 'npz':
                client._convert_to_npz(series_path, series_folder)

    print(f"✅ DICOM organization complete! Processed {processed_files} files")

    for series_folder, info in series_info.items():
        src_path = info['path']
        dst_path = os.path.join(organized_dir, series_folder)
        if src_path != dst_path:
            shutil.move(src_path, dst_path)
            info['path'] = dst_path

    return organized_dir, series_info


def process_single_series(client, series_path, series_folder, organized_dir, output_format='nifti'):
    """Process single series directory and move into organized_dir."""
    if not os.path.isdir(series_path):
        return None

    dicom_files = []
    for file in os.listdir(series_path):
        filepath = os.path.join(series_path, file)
        if os.path.isfile(filepath) and client._is_dicom_file(filepath):
            dicom_files.append(filepath)

    if not dicom_files:
        return None

    sample_dcm = None
    modality = ''
    try:
        sample_dcm = pydicom.dcmread(dicom_files[0], force=True)
        modality = str(getattr(sample_dcm, 'Modality', ''))
    except Exception:
        modality = ''

    client._ensure_metadata_cache(series_path, series_folder, dicom_files, modality)
    client._write_minimal_cache(
        series_path,
        series_folder,
        modality,
        sample_dcm=sample_dcm,
        file_count=len(dicom_files)
    )

    if output_format == 'nifti':
        client.convert_dicom_to_nifti(series_path, series_folder)
    elif output_format == 'npz':
        client._convert_to_npz(series_path, series_folder)

    os.makedirs(organized_dir, exist_ok=True)
    dst_path = os.path.join(organized_dir, series_folder)
    if series_path != dst_path:
        try:
            shutil.move(series_path, dst_path)
        except Exception:
            dst_path = series_path

    return {
        'path': dst_path,
        'file_count': len(dicom_files),
        'files': dicom_files
    }
