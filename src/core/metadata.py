# -*- coding: utf-8 -*-
"""
DICOM 元数据提取模块。

该模块提供从 DICOM 医学影像文件中提取元数据并导出为 Excel 的功能。
主要功能包括：
- 遍历序列目录，提取 DICOM 标签信息
- 支持不同模态（CT、MR、DR、MG、DX 等）的特定关键字提取
- 处理缓存机制，避免重复读取 DICOM 文件
- 将提取的元数据导出为结构化的 Excel 文件（包含汇总表和详细表）
- 集成 MR 数据清洗结果到 Excel 报告

典型用法：
    from src.core.metadata import extract_dicom_metadata
    
    excel_path = extract_dicom_metadata(
        organized_dir="/path/to/organized",
        output_excel="/path/to/output.xlsx",
        get_keywords=lambda mod: ["PatientID", "StudyDate", ...],
        ...
    )
"""

from __future__ import annotations

import json
import os
import time
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
import pydicom

from src.core.qc import ImageQualityResult, assess_converted_file_quality as _default_assess_file_qc, assess_series_quality_converted as _default_assess_series_qc


def _extract_quality_value(result: Union[ImageQualityResult, int]) -> Tuple[int, str]:
    """
    从质量结果中提取质量值和原因
    
    Args:
        result: ImageQualityResult 或 int (0/1)
        
    Returns:
        Tuple[int, str]: (质量值 0/1, 原因描述)
    """
    if isinstance(result, ImageQualityResult):
        reason_desc = result.get_reason_description()
        if result.is_low_quality:
            return 1, reason_desc
        else:
            # 如果不是低质量，但原因描述不是 "Normal"（例如包含修复状态），也返回原因
            if reason_desc and reason_desc != "Normal":
                return 0, reason_desc
            return 0, "Normal"
    else:
        # For backward compatibility with int results
        val = int(result)
        return val, "Normal" if val == 0 else ""


def _build_converted_filename(accession_number: str, converted_file_path: str) -> str:
    """
    构建转换后的文件名，格式为: AccessionNumber/filename
    
    Args:
        accession_number: AccessionNumber (访问号)
        converted_file_path: 转换后文件的完整路径
        
    Returns:
        str: 格式为 "AccessionNumber/filename" 的字符串
    """
    if not accession_number:
        accession_number = "Unknown"
    filename = os.path.basename(converted_file_path)
    return f"{accession_number}/{filename}"


def extract_dicom_metadata(
    organized_dir: str,
    output_excel: Optional[str],
    get_keywords: Callable[[str], List[str]],
    get_converted_files: Callable[[str], Tuple[List[str], Optional[str]]],
    assess_converted_file_quality: Callable[[str, Optional[str]], Union[ImageQualityResult, int]],
    assess_series_quality_converted: Callable[[List[str], Optional[str], Optional[str]], Dict],
    append_mr_cleaned_sheet: Callable[[pd.DataFrame, str], None],
) -> Optional[str]:
    """
    从已整理的 DICOM 目录中提取元数据并生成 Excel 报告。

    遍历 organized_dir 下的所有序列文件夹，读取 DICOM 文件的元数据标签，
    根据不同模态提取相应关键字，评估转换后文件的质量，并将结果导出为
    Excel 文件（包含 DICOM_Metadata 和 Series_Summary 两个工作表）。

    参数:
        organized_dir: 已整理的 DICOM 目录路径，每个子文件夹代表一个序列
        output_excel: 输出 Excel 文件路径，若为 None 则自动生成带时间戳的文件名
        get_keywords: 回调函数，接收模态字符串（如 'CT', 'MR'），返回要提取的 DICOM 关键字列表
        get_converted_files: 回调函数，接收序列路径，返回 (转换文件列表, 附加信息) 元组
        assess_converted_file_quality: 回调函数，接收(文件路径, 模态)，返回质量评分（0=正常，1=低质量）或 ImageQualityResult
        assess_series_quality_converted: 回调函数，接收(文件路径列表, 模态, 序列目录)，返回质量汇总字典（包含 low_quality_reason）
        append_mr_cleaned_sheet: 回调函数，接收 DataFrame 和 Excel 路径，用于添加 MR 清洗结果

    返回:
        生成的 Excel 文件路径，提取失败则返回 None
    """
    if output_excel is None:
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        output_excel = os.path.join(os.path.dirname(organized_dir), f"dicom_metadata_{timestamp}.xlsx")

    print("📊 Extracting DICOM metadata...")

    all_metadata: List[Dict] = []

    for series_folder in os.listdir(organized_dir):
        series_path = os.path.join(organized_dir, series_folder)
        if not os.path.isdir(series_path):
            continue

        print(f"📂 Processing series: {series_folder}")

        converted_files, _ = get_converted_files(series_path)

        dicom_files: List[str] = []
        for file in os.listdir(series_path):
            filepath = os.path.join(series_path, file)
            if file.endswith('.dcm') and os.path.isfile(filepath):
                dicom_files.append(filepath)

        if not dicom_files:
            cache_path = os.path.join(series_path, "dicom_metadata_cache.json")
            if os.path.exists(cache_path):
                cache_loaded = False
                cached_records = []
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        cache = json.load(f)
                    cached_records = cache.get('records', [])
                    cached_modality = str(cache.get('modality', '')).upper()
                    sample_tags = cache.get('sample_tags') or {}
                    current_keywords = get_keywords(cached_modality) if cached_modality else []
                    read_all = cached_modality in ['DR', 'MG', 'DX', 'CR']

                    if cached_records:
                        for record in cached_records:
                            for keyword in current_keywords:
                                if keyword not in record:
                                    record[keyword] = str(sample_tags.get(keyword, "")) if sample_tags else ""
                    elif sample_tags:
                        cached_records = [{
                            'SeriesFolder': series_folder,
                            'TotalFilesInSeries': 0,
                            'FilesReadForMetadata': 0,
                            'Modality': cached_modality
                        }]
                        for keyword in current_keywords:
                            cached_records[0][keyword] = str(sample_tags.get(keyword, ""))

                    # Get AccessionNumber from cache or records
                    accession_number = ""
                    if cached_records and 'AccessionNumber' in cached_records[0]:
                        accession_number = cached_records[0]['AccessionNumber']
                    elif sample_tags and 'AccessionNumber' in sample_tags:
                        accession_number = sample_tags['AccessionNumber']
                    
                    # Add QC and fix information - wrap in try/except to not lose tags on QC failure
                    try:
                        if read_all:
                            converted_quality_results = [assess_converted_file_quality(p, cached_modality) for p in converted_files]
                            for idx, record in enumerate(cached_records):
                                quality_val, quality_reason = _extract_quality_value(
                                    converted_quality_results[idx] if idx < len(converted_quality_results) else 1
                                )
                                record['Low_quality'] = quality_val
                                record['Low_quality_reason'] = quality_reason
                                # Add fix information from detailed results
                                if idx < len(converted_quality_results):
                                    detail = converted_quality_results[idx]
                                    # Handle both ImageQualityResult and dict
                                    if hasattr(detail, 'metrics'):
                                        # ImageQualityResult object
                                        metrics = detail.metrics
                                        if metrics.get('fixed'):
                                            record['Fixed'] = 'Yes'
                                            fixes = metrics.get('fixes_applied', [])
                                            if isinstance(fixes, list):
                                                record['Fixes_applied'] = ', '.join(fixes)
                                            else:
                                                record['Fixes_applied'] = str(fixes)
                                    elif isinstance(detail, dict):
                                        # Dict result
                                        if detail.get('fixed'):
                                            record['Fixed'] = 'Yes'
                                            fixes = detail.get('fixes_applied', [])
                                            if isinstance(fixes, list):
                                                record['Fixes_applied'] = ', '.join(fixes)
                                            else:
                                                record['Fixes_applied'] = str(fixes)
                                # Update FileName to converted filename format
                                if idx < len(converted_files):
                                    record['FileName'] = _build_converted_filename(accession_number, converted_files[idx])
                        else:
                            series_quality_result = assess_series_quality_converted(converted_files, cached_modality, series_path)
                            series_quality = series_quality_result.get('low_quality', 1)
                            series_quality_reason = series_quality_result.get('low_quality_reason', '')
                            if cached_records:
                                cached_records[0]['Low_quality'] = series_quality
                                cached_records[0]['Low_quality_reason'] = series_quality_reason
                                # Add fix information
                                fixed_count = series_quality_result.get('fixed_count', 0)
                                if fixed_count > 0:
                                    cached_records[0]['Fixed_count'] = fixed_count
                                    cached_records[0]['Fixed_orientation'] = series_quality_result.get('fixed_orientation_count', 0)
                                    cached_records[0]['Fixed_grayscale'] = series_quality_result.get('fixed_grayscale_count', 0)
                                # Update SampleFileName to first converted filename for 3D series
                                if converted_files:
                                    cached_records[0]['SampleFileName'] = _build_converted_filename(accession_number, converted_files[0])
                    except Exception as e:
                        print(f"     ⚠️  QC failed for {series_folder}, but preserving cached tags: {e}")
                        # Still add the records without QC info
                        for idx, record in enumerate(cached_records):
                            if idx < len(converted_files):
                                record['FileName'] = _build_converted_filename(accession_number, converted_files[idx])
                    
                    # Add all cached records to metadata
                    for record in cached_records:
                        all_metadata.append(record)
                    cache_loaded = True
                    continue
                    
                except Exception as e:
                    print(f"     ⚠️  Failed to load cache for {series_folder}: {e}")
                    # If we have cached_records, still use them even if QC failed
                    if cached_records:
                        for record in cached_records:
                            all_metadata.append(record)
                        cache_loaded = True
                        continue

            # Fallback: only if no cache or cache failed to load
            if not cache_loaded:
                nifti_files = [f for f in os.listdir(series_path) if f.endswith(('.nii.gz', '.nii'))]
                if nifti_files:
                    metadata = {
                        'SeriesFolder': series_folder,
                        'ConvertedToNIfTI': 'Yes',
                        'NIfTIFile': nifti_files[0],
                        'TotalFilesInSeries': 1
                    }
                    all_metadata.append(metadata)
                continue

        try:
            sample_file = dicom_files[0]
            dcm = pydicom.dcmread(sample_file, force=True)
            modality = getattr(dcm, 'Modality', '')
            need_read_all = modality in ['DR', 'MG', 'DX', 'CR']

            current_keywords = get_keywords(modality)

            # Get AccessionNumber from the sample DICOM
            accession_number = getattr(dcm, 'AccessionNumber', '')
            
            if need_read_all:
                print(f"   ℹ️  Detected {modality} modality; will read all {len(dicom_files)} DICOM files")
                records: List[Dict] = []
                for idx, dicom_file in enumerate(dicom_files):
                    try:
                        dcm = pydicom.dcmread(dicom_file, force=True)
                        metadata = {
                            'SeriesFolder': series_folder,
                            'FileName': os.path.basename(dicom_file),
                            'FileIndex': idx + 1,
                            'TotalFilesInSeries': len(dicom_files)
                        }
                        for keyword in current_keywords:
                            try:
                                value = getattr(dcm, keyword, None)
                                if value is not None:
                                    if hasattr(value, '__len__') and not isinstance(value, str):
                                        if len(value) == 1:
                                            value = value[0]
                                        else:
                                            value = str(value)
                                    elif hasattr(value, 'value'):
                                        value = value.value
                                    metadata[keyword] = str(value)
                                else:
                                    metadata[keyword] = ""
                            except Exception:
                                metadata[keyword] = ""
                        records.append(metadata)
                    except Exception:
                        continue

                converted_quality_results = [assess_converted_file_quality(p, modality) for p in converted_files]
                for idx, record in enumerate(records):
                    quality_val, quality_reason = _extract_quality_value(
                        converted_quality_results[idx] if idx < len(converted_quality_results) else 1
                    )
                    record['Low_quality'] = quality_val
                    record['Low_quality_reason'] = quality_reason
                    # Add fix information
                    if idx < len(converted_quality_results):
                        detail = converted_quality_results[idx]
                        # Handle both ImageQualityResult and dict
                        if hasattr(detail, 'metrics'):
                            # ImageQualityResult object
                            metrics = detail.metrics
                            if metrics.get('fixed'):
                                record['Fixed'] = 'Yes'
                                fixes = metrics.get('fixes_applied', [])
                                if isinstance(fixes, list):
                                    record['Fixes_applied'] = ', '.join(fixes)
                                else:
                                    record['Fixes_applied'] = str(fixes)
                        elif isinstance(detail, dict):
                            # Dict result
                            if detail.get('fixed'):
                                record['Fixed'] = 'Yes'
                                fixes = detail.get('fixes_applied', [])
                                if isinstance(fixes, list):
                                    record['Fixes_applied'] = ', '.join(fixes)
                                else:
                                    record['Fixes_applied'] = str(fixes)
                    # Update FileName to converted filename format: AccessionNumber/filename
                    if idx < len(converted_files):
                        record['FileName'] = _build_converted_filename(accession_number, converted_files[idx])
                    all_metadata.append(record)

                    if (idx + 1) % 10 == 0:
                        print(f"      Processed {idx + 1}/{len(records)} files...")
            else:
                print(f"   ℹ️  {modality} modality; reading representative file only")
                metadata = {
                    'SeriesFolder': series_folder,
                    'SampleFileName': os.path.basename(sample_file),
                    'TotalFilesInSeries': len(dicom_files),
                    'FilesReadForMetadata': 1
                }
                for keyword in current_keywords:
                    try:
                        value = getattr(dcm, keyword, None)
                        if value is not None:
                            if hasattr(value, '__len__') and not isinstance(value, str):
                                if len(value) == 1:
                                    value = value[0]
                                else:
                                    value = str(value)
                            elif hasattr(value, 'value'):
                                value = value.value
                            metadata[keyword] = str(value)
                        else:
                            metadata[keyword] = ""
                    except Exception:
                        metadata[keyword] = ""
                series_quality_result = assess_series_quality_converted(converted_files, modality, series_path)
                metadata['Low_quality'] = series_quality_result.get('low_quality', 1)
                metadata['Low_quality_reason'] = series_quality_result.get('low_quality_reason', '')
                # Add fix information
                fixed_count = series_quality_result.get('fixed_count', 0)
                if fixed_count > 0:
                    metadata['Fixed_count'] = fixed_count
                    metadata['Fixed_orientation'] = series_quality_result.get('fixed_orientation_count', 0)
                    metadata['Fixed_grayscale'] = series_quality_result.get('fixed_grayscale_count', 0)
                # Update SampleFileName to first converted filename for 3D series
                if converted_files:
                    metadata['SampleFileName'] = _build_converted_filename(accession_number, converted_files[0])
                all_metadata.append(metadata)

        except Exception as e:
            print(f"     ❌ Failed processing series: {e}")
            continue

    if not all_metadata:
        print("❌ No metadata extracted")
        return None

    try:
        df = pd.DataFrame(all_metadata)

        column_order: List[str] = []
        priority_columns = ['SeriesFolder', 'FileName', 'SampleFileName', 'FileIndex',
                            'TotalFilesInSeries', 'FilesReadForMetadata', 'Low_quality', 'Low_quality_reason']
        for col in priority_columns:
            if col in df.columns:
                column_order.append(col)

        important_fields = ['PatientID', 'AccessionNumber', 'StudyDate', 'Modality',
                            'SeriesNumber', 'SeriesDescription', 'InstanceNumber', 'Rows', 'Columns']
        for field in important_fields:
            if field in df.columns and field not in column_order:
                column_order.append(field)

        for col in df.columns:
            if col not in column_order:
                column_order.append(col)

        df = df[column_order]

        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='DICOM_Metadata', index=False)

            summary_data: List[Dict] = []
            for series_folder in df['SeriesFolder'].unique():
                series_df = df[df['SeriesFolder'] == series_folder]
                summary_row = {
                    'SeriesFolder': series_folder,
                    'FileCount': len(series_df),
                    'Modality': series_df['Modality'].iloc[0] if 'Modality' in series_df.columns else '',
                    'SeriesDescription': series_df['SeriesDescription'].iloc[0] if 'SeriesDescription' in series_df.columns else '',
                    'PatientID': series_df['PatientID'].iloc[0] if 'PatientID' in series_df.columns else '',
                    'AccessionNumber': series_df['AccessionNumber'].iloc[0] if 'AccessionNumber' in series_df.columns else '',
                    'StudyDate': series_df['StudyDate'].iloc[0] if 'StudyDate' in series_df.columns else ''
                }
                summary_data.append(summary_row)

            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Series_Summary', index=False)

            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except Exception:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

        total_files_read = len(df)
        dr_mg_dx_series = df[df['Modality'].isin(['DR', 'MG', 'DX'])]['SeriesFolder'].nunique() if 'Modality' in df.columns else 0

        print("✅ Metadata extraction complete!")
        print(f"📄 Excel file: {output_excel}")
        print(f"📊 Total records: {total_files_read}")
        if dr_mg_dx_series > 0:
            print(f"📋 DR/MG/DX series count: {dr_mg_dx_series} (all files read)")

        append_mr_cleaned_sheet(df, output_excel)
        return output_excel

    except Exception as e:
        print(f"❌ Failed saving Excel file: {e}")
        return None
