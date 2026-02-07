# -*- coding: utf-8 -*-
"""
DICOM 文件组织模块

提供 DICOM 文件的整理、分类和组织功能，支持按序列（Series）组织文件。
"""

import os
import shutil
import time
from typing import Dict, List, Any, Optional, Tuple


def organize_dicom_files(
    client,
    extract_dir: str,
    organized_dir: Optional[str] = None,
    output_format: str = 'nifti'
) -> Tuple[str, Dict[str, Any]]:
    """
    组织 DICOM 文件并按序列分类

    扫描源目录中的 DICOM 文件，按序列（Series）组织到目标目录，
    并可选地进行格式转换。

    Args:
        client: DICOMDownloadClient 实例，用于调用转换方法
        extract_dir: 源目录路径，包含未组织的 DICOM 文件
        organized_dir: 目标组织目录路径，默认为 extract_dir/organized
        output_format: 输出格式，可选 'nifti'、'npz' 或 None

    Returns:
        Tuple[str, Dict]: (组织后的目录路径, 序列信息字典)

    Example:
        >>> organized_dir, series_info = organize_dicom_files(
        ...     client, "/data/extracted", "/data/organized", "nifti"
        ... )
        >>> print(f"处理了 {len(series_info)} 个序列")
    """
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

    series_info: Dict[str, Any] = {}
    processed_files = 0

    for series_folder in os.listdir(extract_dir):
        # 跳过已组织的目录
        if series_folder == "organized":
            continue

        series_path = os.path.join(extract_dir, series_folder)
        if not os.path.isdir(series_path):
            continue

        # 收集当前序列的所有 DICOM 文件
        dicom_files: List[str] = []
        for file in os.listdir(series_path):
            filepath = os.path.join(series_path, file)
            if os.path.isfile(filepath) and client._is_dicom_file(filepath):
                dicom_files.append(filepath)

        if dicom_files:
            processed_files += len(dicom_files)
            sample_dcm = None
            modality = ''
            try:
                import pydicom
                sample_dcm = pydicom.dcmread(dicom_files[0], force=True)
                modality = str(getattr(sample_dcm, 'Modality', ''))
            except Exception:
                modality = ''

            # 缓存序列元数据
            client._cache_metadata_for_series(
                series_path, series_folder, dicom_files, modality
            )
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

            # 执行格式转换
            if output_format == 'nifti':
                client.convert_dicom_to_nifti(series_path, series_folder)
            elif output_format == 'npz':
                client._convert_to_npz(series_path, series_folder)

    print(f"✅ DICOM organization complete! Processed {processed_files} files")

    # 将处理后的序列移动到目标目录
    for series_folder, info in series_info.items():
        src_path = info['path']
        dst_path = os.path.join(organized_dir, series_folder)
        if src_path != dst_path:
            shutil.move(src_path, dst_path)
            info['path'] = dst_path

    return organized_dir, series_info


def process_single_series(
    client,
    series_path: str,
    series_folder: str,
    organized_dir: str,
    output_format: str = 'nifti'
) -> Optional[Dict[str, Any]]:
    """
    处理单个序列目录并移动到组织目录

    Args:
        client: DICOMDownloadClient 实例
        series_path: 序列源目录路径
        series_folder: 序列文件夹名称
        organized_dir: 目标组织目录
        output_format: 输出格式，可选 'nifti'、'npz'

    Returns:
        Optional[Dict]: 序列信息字典，处理失败时返回 None
    """
    if not os.path.isdir(series_path):
        return None
    
    # 等待文件系统稳定（确保文件已完全写入磁盘）
    time.sleep(0.2)
    
    # 使用锁文件防止重复处理同一个series（应对多线程竞争条件）
    lock_file = os.path.join(series_path, '.processing_lock')
    try:
        # 如果锁文件已存在且创建时间不超过5分钟，说明可能正在处理或已处理完成
        if os.path.exists(lock_file):
            try:
                lock_mtime = os.path.getmtime(lock_file)
                if time.time() - lock_mtime < 300:  # 5分钟内
                    print(f"   ⚠️ Series {series_folder} is already being processed or was processed recently, skipping")
                    return None
            except Exception:
                pass
        # 创建锁文件
        with open(lock_file, 'w') as f:
            f.write(str(time.time()))
    except Exception:
        pass  # 如果无法创建锁文件，继续处理

    # 收集 DICOM 文件（最多重试3次，应对文件系统延迟）
    dicom_files: List[str] = []
    for attempt in range(3):
        dicom_files = []
        for file in os.listdir(series_path):
            filepath = os.path.join(series_path, file)
            if os.path.isfile(filepath) and client._is_dicom_file(filepath):
                dicom_files.append(filepath)
        
        if dicom_files:
            break
        
        if attempt < 2:
            print(f"   ⚠️ No DICOM files found in {series_folder}, retrying in 0.5s... (attempt {attempt + 1}/3)")
            time.sleep(0.5)

    if not dicom_files:
        return None

    # 读取样本文件获取模态信息
    sample_dcm = None
    modality = ''
    try:
        import pydicom
        sample_dcm = pydicom.dcmread(dicom_files[0], force=True)
        modality = str(getattr(sample_dcm, 'Modality', ''))
    except Exception:
        modality = ''

    # 确保元数据缓存
    client._ensure_metadata_cache(series_path, series_folder, dicom_files, modality)
    client._write_minimal_cache(
        series_path,
        series_folder,
        modality,
        sample_dcm=sample_dcm,
        file_count=len(dicom_files)
    )

    # 执行格式转换
    if output_format == 'nifti':
        client.convert_dicom_to_nifti(series_path, series_folder)
    elif output_format == 'npz':
        client._convert_to_npz(series_path, series_folder)

    # 移动到目标目录
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
