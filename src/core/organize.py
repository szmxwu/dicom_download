# -*- coding: utf-8 -*-
"""
DICOM 文件组织模块

提供 DICOM 文件的整理、分类和组织功能，支持按序列（Series）组织文件。
"""

import logging
import os
import shutil
import time
import json
import hashlib
from typing import Dict, List, Any, Optional, Tuple

# Create logger for organize module - use DICOMApp to match Flask app logging
logger = logging.getLogger('DICOMApp')

# 从常量模块导入衍生序列关键词
from src.core.constants import DERIVED_SERIES_KEYWORDS


def _is_derived_series(series_desc: str, image_type=None) -> bool:
    """检查是否为衍生序列（MPR/MIP/3D重建等）。

    基于实际 DICOM 文件中的 SeriesDescription 和 ImageType 进行判断，
    用于弥补 PACS 查询阶段 SeriesDescription 不完整导致的漏判。
    """
    if image_type:
        image_type_str = ' '.join(image_type) if isinstance(image_type, (list, tuple)) else str(image_type)
        if 'DERIVED' in image_type_str.upper() or 'SECONDARY' in image_type_str.upper():
            return True
    if series_desc:
        desc_upper = series_desc.upper()
        for keyword in DERIVED_SERIES_KEYWORDS:
            if keyword in desc_upper:
                return True
    return False


def compute_file_checksum(filepath: str, algorithm: str = 'md5') -> Optional[str]:
    """计算文件校验和"""
    try:
        hasher = hashlib.new(algorithm)
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return None


def organize_dicom_files(
    client,
    extract_dir: str,
    organized_dir: Optional[str] = None,
    output_format: str = 'nifti',
    min_series_files: Optional[int] = None
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
        min_series_files: 可选，最小文件数验证，实际文件数少于此值则跳过

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

    logger.info(f"📋 Organizing DICOM files (format: {output_format})...")
    logger.info(f"📂 Source directory: {extract_dir}")
    logger.info(f"📂 Organized directory: {organized_dir}")
    logger.info(f"📊 Min series files filter: {min_series_files}")

    # 等待文件系统稳定
    logger.info("⏳ Waiting for file system to stabilize before organizing...")
    time.sleep(1.0)

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
        files_in_dir = os.listdir(series_path)
        logger.info(f"   📂 Series {series_folder}: found {len(files_in_dir)} files")
        for file in files_in_dir:
            filepath = os.path.join(series_path, file)
            if os.path.isfile(filepath) and client._is_dicom_file(filepath):
                normalized_path = filepath
                file_root, file_ext = os.path.splitext(filepath)
                if file_ext != '.dcm':
                    target_path = f"{file_root}.dcm"
                    if target_path != filepath:
                        try:
                            os.rename(filepath, target_path)
                            normalized_path = target_path
                        except Exception:
                            normalized_path = filepath
                dicom_files.append(normalized_path)

        if dicom_files:
            # 整理阶段二次过滤衍生序列（从实际 DICOM 文件验证，PACS 返回的 SeriesDescription 可能不完整）
            try:
                import pydicom
                _hdr = pydicom.dcmread(dicom_files[0], force=True, stop_before_pixels=True)
                _desc = str(getattr(_hdr, 'SeriesDescription', '') or '')
                _itype = getattr(_hdr, 'ImageType', None)
                if _is_derived_series(_desc, _itype):
                    logger.info(f"   🚫 Filtered derived series (organize stage): '{_desc}' ({series_folder})")
                    continue
            except Exception:
                pass

            # 整理阶段验证：检查实际文件数是否满足最小要求
            # 注意：只对3D模态（CT/MR等）应用此验证，2D模态（DX/DR等）跳过
            if min_series_files and min_series_files > 0:
                volume_modalities = {'CT', 'MR', 'MRI', 'PT', 'NM', 'US'}
                # 获取模态用于判断
                check_modality = ''
                try:
                    import pydicom
                    temp_dcm = pydicom.dcmread(dicom_files[0], force=True, stop_before_pixels=True)
                    check_modality = str(getattr(temp_dcm, 'Modality', '')).upper()
                except Exception:
                    check_modality = ''

                if check_modality in volume_modalities or not check_modality:
                    actual_count = len(dicom_files)
                    logger.info(f"   📊 Series {series_folder}: {actual_count} DICOM files, modality={check_modality}")
                    if actual_count < min_series_files:
                        logger.warning(f"   🚫 Series {series_folder} filtered during organize: "
                              f"{actual_count} files < {min_series_files} min "
                              f"(PACS reported count may be unreliable)")
                        continue

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

    # P0: 原地处理 - 不再移动到 organized 子目录
    # 文件已经在按序列组织的目录中，直接返回原目录
    return extract_dir, series_info


def process_single_series(
    client,
    series_path: str,
    series_folder: str,
    organized_dir: str,
    output_format: str = 'nifti',
    use_cached_metadata: bool = True,  # P3: 优先使用缓存的元数据
    min_series_files: Optional[int] = None  # 最小文件数验证
) -> Optional[Dict[str, Any]]:
    """
    处理单个序列目录并移动到组织目录

    Args:
        client: DICOMDownloadClient 实例
        series_path: 序列源目录路径
        series_folder: 序列文件夹名称
        organized_dir: 目标组织目录
        output_format: 输出格式，可选 'nifti'、'npz'
        use_cached_metadata: 是否优先使用下载阶段缓存的元数据
        min_series_files: 可选，最小文件数验证，实际文件数少于此值则跳过

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

    # P3: 尝试从缓存加载元数据（如果 use_cached_metadata=True）
    cached_metadata = None
    if use_cached_metadata:
        cache_file = os.path.join(series_path, "dicom_metadata_cache.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_metadata = json.load(f)
            except Exception:
                pass

    # P3: 如果有缓存的校验和文件，加载它
    checksum_cache = {}
    checksum_file = os.path.join(os.path.dirname(series_path), '.checksums.json')
    if os.path.exists(checksum_file) and use_cached_metadata:
        try:
            with open(checksum_file, 'r', encoding='utf-8') as f:
                checksum_cache = json.load(f)
        except Exception:
            pass

    # 收集 DICOM 文件（最多重试3次，应对文件系统延迟）
    dicom_files: List[str] = []

    # P3: 如果有缓存的文件列表，直接使用
    if cached_metadata and cached_metadata.get('records') and use_cached_metadata:
        # 从缓存重建文件列表
        cached_files = set()
        for record in cached_metadata['records']:
            if 'FileName' in record:
                filepath = os.path.join(series_path, record['FileName'])
                if os.path.exists(filepath):
                    cached_files.add(filepath)

        # 如果缓存的文件都存在，直接使用
        if cached_files and len(cached_files) == len(cached_metadata['records']):
            dicom_files = sorted(list(cached_files))
            print(f"   ✅ Using {len(dicom_files)} files from cached metadata for {series_folder}")

    # 如果没有缓存或缓存不完整，扫描目录
    if not dicom_files:
        for attempt in range(3):
            dicom_files = []
            for file in os.listdir(series_path):
                filepath = os.path.join(series_path, file)
                if os.path.isfile(filepath) and client._is_dicom_file(filepath):
                    normalized_path = filepath
                    file_root, file_ext = os.path.splitext(filepath)
                    if file_ext != '.dcm':
                        target_path = f"{file_root}.dcm"
                        if target_path != filepath:
                            try:
                                os.rename(filepath, target_path)
                                normalized_path = target_path
                            except Exception:
                                normalized_path = filepath
                    dicom_files.append(normalized_path)

            if dicom_files:
                break

            if attempt < 2:
                print(f"   ⚠️ No DICOM files found in {series_folder}, retrying in 0.5s... (attempt {attempt + 1}/3)")
                time.sleep(0.5)

    if not dicom_files:
        return None

    # 整理阶段二次过滤衍生序列（从实际 DICOM 文件验证，PACS 返回的 SeriesDescription 可能不完整）
    try:
        import pydicom as _pd
        _hdr = _pd.dcmread(dicom_files[0], force=True, stop_before_pixels=True)
        _desc = str(getattr(_hdr, 'SeriesDescription', '') or '')
        _itype = getattr(_hdr, 'ImageType', None)
        if _is_derived_series(_desc, _itype):
            logger.info(f"   🚫 Filtered derived series (organize stage): '{_desc}' ({series_folder})")
            try:
                if os.path.exists(lock_file):
                    os.remove(lock_file)
            except Exception:
                pass
            return None
    except Exception:
        pass

    # 整理阶段验证：检查实际文件数是否满足最小要求
    # 注意：只对3D模态（CT/MR等）应用此验证，2D模态（DX/DR等）跳过
    if min_series_files and min_series_files > 0:
        volume_modalities = {'CT', 'MR', 'MRI', 'PT', 'NM', 'US'}

        # 从缓存或文件中获取模态
        check_modality = ''
        if cached_metadata and use_cached_metadata:
            check_modality = cached_metadata.get('modality', '').upper()

        if check_modality in volume_modalities or not check_modality:
            actual_count = len(dicom_files)
            if actual_count < min_series_files:
                print(f"   🚫 Series {series_folder} filtered during organize: "
                      f"{actual_count} files < {min_series_files} min "
                      f"(PACS reported count may be unreliable)")
                # 清理锁文件
                try:
                    if os.path.exists(lock_file):
                        os.remove(lock_file)
                except Exception:
                    pass
                return None

    # P3: 如果有缓存的模态信息，直接使用，避免重新读取DICOM
    sample_dcm = None
    modality = ''
    if cached_metadata and use_cached_metadata:
        # 从缓存获取模态
        cached_modality = cached_metadata.get('modality', '')
        if cached_modality:
            modality = cached_modality
            print(f"   ✅ Using cached modality: {modality}")

        # 从缓存获取样本标签
        sample_tags = cached_metadata.get('sample_tags', {})
        if sample_tags:
            # 创建模拟的DICOM对象（SimpleNamespace）供后续使用
            from types import SimpleNamespace
            sample_dcm = SimpleNamespace(**sample_tags)

    # 如果没有缓存的模态信息，重新读取样本文件
    if not modality:
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
        sample_dcm=sample_dcm if isinstance(sample_dcm, pydicom.Dataset) else None,
        file_count=len(dicom_files)
    )

    # P3: 数据完整性校验（如果缓存中有校验和）
    if checksum_cache and use_cached_metadata:
        verified_count = 0
        corrupted_files = []
        for filepath in dicom_files:
            if filepath in checksum_cache:
                current_checksum = compute_file_checksum(filepath)
                if current_checksum == checksum_cache[filepath]:
                    verified_count += 1
                else:
                    corrupted_files.append(os.path.basename(filepath))

        if corrupted_files:
            print(f"   ⚠️  {len(corrupted_files)} files may be corrupted: {corrupted_files[:5]}...")
        else:
            print(f"   ✅ All {verified_count} files verified with checksums")

    # 执行格式转换
    if output_format == 'nifti':
        client.convert_dicom_to_nifti(series_path, series_folder)
    elif output_format == 'npz':
        client._convert_to_npz(series_path, series_folder)

    # P0: 原地处理 - 不再移动到 organized 子目录
    # 文件已经在正确的位置，直接返回原路径

    return {
        'path': series_path,
        'file_count': len(dicom_files),
        'files': dicom_files
    }
