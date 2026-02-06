# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
统一版DICOM下载和处理客户端
直接从PACS下载并处理DICOM文件，无需HTTP中间层和ZIP打包
"""

import os
import json
import time
import shutil
import threading
from queue import Queue
from pathlib import Path
import pandas as pd
import pydicom
import numpy as np
from collections import defaultdict
import re
import nibabel as nib
from PIL import Image
from datetime import datetime
import sys
import logging
from pynetdicom import AE, evt, AllStoragePresentationContexts
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove
)
from pydicom.dataset import Dataset

logger = logging.getLogger('DICOMApp')

def get_base_path():
    """获取程序运行时的根目录路径，兼容 PyInstaller 打包"""
    if hasattr(sys, '_MEIPASS'):
        return sys._MEIPASS
    return os.path.abspath(".")

class DICOMDownloadClient:
    """统一版DICOM下载客户端，直接与PACS通信"""
    
    def __init__(self):
        """初始化客户端"""
        # PACS配置（从环境变量加载，提供默认值）
        self.pacs_config = {
            'PACS_IP': os.getenv('PACS_IP', '172.17.250.192'),
            'PACS_PORT': int(os.getenv('PACS_PORT', 2104)),
            'CALLING_AET': os.getenv('CALLING_AET', 'WMX01'),
            'CALLED_AET': os.getenv('CALLED_AET', 'pacsFIR'),
            'CALLING_PORT': int(os.getenv('CALLING_PORT', 1103))
        }
        
        # 初始化AE
        self.ae = AE(ae_title=self.pacs_config['CALLING_AET'])
        self.ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
        self.ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
        self.ae.network_timeout = 300
        self.ae.acse_timeout = 30
        self.ae.dimse_timeout = 300
        
        # 加载DICOM字段列表
        self.modality_keywords = self._load_keywords()
        
        # 兼容性属性
        self.session_id = "dummy_session"
        self.username = os.getenv('DICOM_USERNAME', '')
        self.role = os.getenv('DICOM_ROLE', 'admin')
        # optional progress callback to report MR_clean progress: function(message, stage)
        self.progress_callback = None
    
    def _load_keywords(self, tags_dir="dicom_tags"):
        """加载不同模态的DICOM字段列表"""
        keywords_map = {}
        default_keywords = [
            "Modality", "StudyDate", "StudyInstanceUID", "SeriesInstanceUID",
            "PatientID", "AccessionNumber", "SeriesNumber", "SeriesDescription",
            "BodyPartExamined", "Manufacturer", "ManufacturerModelName"
        ]
        
        try:
            if not os.path.exists(tags_dir):
                # 尝试使用旧的keywords.json作为默认
                keywords_path = os.path.join(get_base_path(), "keywords.json")
                if os.path.exists(keywords_path):
                    with open(keywords_path, 'r', encoding='utf-8') as f:
                        self.tag_mappings['DEFAULT'] = json.load(f)
                    print(f"⚠️  {self.tags_dir} not found, using keywords.json as default")
                else:
                    print(f"⚠️  {tags_dir} not found, using built-in default keywords")
                return {'default': default_keywords}

            # 加载所有JSON文件
            for filename in os.listdir(tags_dir):
                if filename.endswith('.json'):
                    modality = filename.replace('.json', '').upper()
                    try:
                        with open(os.path.join(tags_dir, filename), 'r', encoding='utf-8') as f:
                            keywords_map[modality] = json.load(f)
                        print(f"✅ Loaded {modality} modality keywords ({len(keywords_map[modality])} items)")
                    except Exception as e:
                        print(f"❌ Failed to load {filename}: {e}")
            
            # 确保有默认值
            if 'MR' in keywords_map:
                keywords_map['default'] = keywords_map['MR']
            elif 'DEFAULT' in keywords_map:
                keywords_map['default'] = keywords_map['DEFAULT']
            else:
                keywords_map['default'] = default_keywords
                
            return keywords_map
            
        except Exception as e:
            print(f"❌ Failed to load keywords files: {e}")
            return {'default': default_keywords}
    
    def get_keywords(self, modality):
        """根据模态获取字段列表"""
        # 归一化模态名称
        modality = modality.upper()
        if modality in ['DR', 'DX', 'CR']:
            key = 'DX'
        if "MR" in modality:
            key = 'MR'
        elif modality in self.modality_keywords:
            key = modality
        else:
            key = 'default'
            
        return self.modality_keywords.get(key, self.modality_keywords.get('default', []))

    def login(self, username, password):
        """保持接口兼容性的虚拟登录"""
        self.username = username
        print(f"✅ Login successful: {username} (no actual authentication required)")
        return True
    
    def logout(self):
        """保持接口兼容性的虚拟登出"""
        print(f"✅ Logout successful: {self.username}")
        return True
    
    def check_status(self):
        """检查PACS连接状态"""
        try:
            assoc = self.ae.associate(
                self.pacs_config['PACS_IP'],
                self.pacs_config['PACS_PORT'],
                ae_title=self.pacs_config['CALLED_AET']
            )
            
            if assoc.is_established:
                assoc.release()
                logger.info("PACS connection status: OK")
                return True
            else:
                logger.warning("Unable to connect to PACS")
                return False
        except Exception as e:
            logger.error(f"PACS connection error: {e}")
            return False
    
    def _query_series_metadata(self, accession_number):
        """查询PACS获取Series元数据"""
        series_metadata = []
        
        try:
            assoc = self.ae.associate(
                self.pacs_config['PACS_IP'],
                self.pacs_config['PACS_PORT'],
                ae_title=self.pacs_config['CALLED_AET']
            )
            
            if not assoc.is_established:
                print("❌ Cannot build PACS connection")
                return []
            
            try:
                # 查询Study
                study_ds = Dataset()
                study_ds.QueryRetrieveLevel = "STUDY"
                study_ds.AccessionNumber = accession_number
                study_ds.StudyInstanceUID = ""
                study_ds.PatientID = ""
                study_ds.PatientName = ""
                study_ds.StudyDate = ""
                
                print(f"🔍 Query AccessionNumber: {accession_number}")
                responses = assoc.send_c_find(study_ds, StudyRootQueryRetrieveInformationModelFind)
                
                studies = {}
                for (status, identifier) in responses:
                    if status and status.Status in [0xFF00, 0xFF01]:
                        if identifier and hasattr(identifier, 'StudyInstanceUID'):
                            study_uid = str(identifier.StudyInstanceUID)
                            studies[study_uid] = {
                                'PatientID': str(identifier.PatientID) if hasattr(identifier, 'PatientID') else '',
                                'PatientName': str(identifier.PatientName) if hasattr(identifier, 'PatientName') else '',
                                'StudyDate': str(identifier.StudyDate) if hasattr(identifier, 'StudyDate') else '',
                                'AccessionNumber': accession_number
                            }
                
                if not studies:
                    print(f"⚠️  Can't Find AccessionNumber: {accession_number}")
                    return []
                
                # 查询每个Study的Series
                for study_uid, study_info in studies.items():
                    series_ds = Dataset()
                    series_ds.QueryRetrieveLevel = "SERIES"
                    series_ds.StudyInstanceUID = study_uid
                    series_ds.SeriesInstanceUID = ""
                    series_ds.SeriesNumber = ""
                    series_ds.SeriesDescription = ""
                    series_ds.Modality = ""
                    
                    responses = assoc.send_c_find(series_ds, StudyRootQueryRetrieveInformationModelFind)
                    
                    for (status, identifier) in responses:
                        if status and status.Status in [0xFF00, 0xFF01]:
                            if identifier and hasattr(identifier, 'SeriesInstanceUID'):
                                series_info = dict(study_info)
                                series_info.update({
                                    'StudyInstanceUID': study_uid,
                                    'SeriesInstanceUID': str(identifier.SeriesInstanceUID),
                                    'SeriesNumber': str(identifier.SeriesNumber) if hasattr(identifier, 'SeriesNumber') else '0',
                                    'SeriesDescription': str(identifier.SeriesDescription) if hasattr(identifier, 'SeriesDescription') else 'Unknown',
                                    'Modality': str(identifier.Modality) if hasattr(identifier, 'Modality') else ''
                                })
                                series_metadata.append(series_info)
                
                print(f"📊 Find {len(series_metadata)} Series")
                
            finally:
                assoc.release()
                
        except Exception as e:
            print(f"❌ Query metadata failed: {e}")
        
        return series_metadata
    
    def download_study(self, accession_number, output_dir=".", custom_folder_name=None, on_series_downloaded=None):
        """Download Study data (directly from PACS, no ZIP generation)"""
        print(f"🔍 Downloading AccessionNumber: {accession_number}")
        
        # 查询Series信息
        series_metadata = self._query_series_metadata(accession_number)
        if not series_metadata:
            print(f"❌ No data found for: {accession_number}")
            return None
        
        # 创建输出目录
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        if custom_folder_name:
            output_path = os.path.join(output_dir, custom_folder_name)
        else:
            output_path = os.path.join(output_dir, f"{accession_number}_{timestamp}")
        
        os.makedirs(output_path, exist_ok=True)
        
        # 存储状态
        storage_state = {'current_path': '', 'files_received': 0}
        
        def handle_store(event):
            """处理C-STORE请求"""
            try:
                dataset = event.dataset
                dataset.file_meta = event.file_meta
                
                # 保存文件
                sop_instance_uid = dataset.SOPInstanceUID
                filename = f"{sop_instance_uid}.dcm"
                filepath = os.path.join(storage_state['current_path'], filename)
                
                os.makedirs(storage_state['current_path'], exist_ok=True)
                dataset.save_as(filepath, write_like_original=False)
                
                storage_state['files_received'] += 1
                if storage_state['files_received'] % 10 == 0:
                    print(f"   Received {storage_state['files_received']} files...")
                
                return 0x0000
            except Exception as e:
                print(f"❌ Failed saving DICOM file: {e}")
                return 0xA700
        
        # 启动C-STORE SCP
        ae_scp = AE(ae_title=self.pacs_config['CALLING_AET'])
        ae_scp.supported_contexts = AllStoragePresentationContexts
        ae_scp.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
        
        server = ae_scp.start_server(
            ('', self.pacs_config['CALLING_PORT']),
            block=False,
            evt_handlers=[(evt.EVT_C_STORE, handle_store)]
        )
        
        try:
            # 建立C-MOVE连接
            assoc = self.ae.associate(
                self.pacs_config['PACS_IP'],
                self.pacs_config['PACS_PORT'],
                ae_title=self.pacs_config['CALLED_AET']
            )
            
            if not assoc.is_established:
                print("❌ Unable to establish PACS association")
                return None
            
            try:
                # 下载每个Series
                for i, series in enumerate(series_metadata):
                    series_num = series.get('SeriesNumber', f'Series{i+1}')
                    series_desc = series.get('SeriesDescription', 'Unknown')
                    series_dir = os.path.join(output_path, f"{series_num:0>3}_{self._sanitize_folder_name(series_desc)}")
                    
                    storage_state['current_path'] = series_dir
                    
                    print(f"📥 Downloading series {i+1}/{len(series_metadata)}: {series_num} - {series_desc}")
                    
                    # 发送C-MOVE请求
                    move_ds = Dataset()
                    move_ds.QueryRetrieveLevel = 'SERIES'
                    move_ds.StudyInstanceUID = series['StudyInstanceUID']
                    move_ds.SeriesInstanceUID = series['SeriesInstanceUID']
                    
                    responses = assoc.send_c_move(
                        move_ds,
                        self.pacs_config['CALLING_AET'],
                        query_model=StudyRootQueryRetrieveInformationModelMove
                    )
                    
                    for (status, identifier) in responses:
                        if status and status.Status == 0x0000:
                            pass
                    
                    # 通知外部：该Series下载完成
                    if callable(on_series_downloaded):
                        try:
                            on_series_downloaded(series_dir, series)
                        except Exception as e:
                            print(f"⚠️  Series callback failed: {e}")

                    time.sleep(0.5)  # 短暂延迟
                
            finally:
                assoc.release()
                
        except Exception as e:
            print(f"❌ Download error: {e}")
            return None
        finally:
            server.shutdown()
        
        print(f"✅ Download complete! Received {storage_state['files_received']} files")
        print(f"📁 Files saved to: {output_path}")
        
        return output_path if storage_state['files_received'] > 0 else None
    
    def extract_zip(self, zip_filepath, extract_dir=None):
        """解压zip_filepath到指定目录（兼容接口，实际直接返回原路径）"""
        return zip_filepath
    
    def _is_dicom_file(self, filepath):
        """判断是否为DICOM文件"""
        try:
            with open(filepath, 'rb') as f:
                f.seek(128)
                dicm = f.read(4)
                if dicm == b'DICM':
                    return True
            
            pydicom.dcmread(filepath, force=True, stop_before_pixels=True)
            return True
        except:
            return False
    
    def _sanitize_folder_name(self, name):
        """清理文件夹名称"""
        if not name:
            return "Unknown"
        
        name = re.sub(r'[<>:"/\\|?*]', '_', str(name))
        name = name.strip()
        
        if len(name) > 50:
            name = name[:50]
        
        return name if name else "Unknown"
    
    def organize_dicom_files(self, extract_dir, organized_dir=None, output_format='nifti'):
        """按Series整理DICOM文件并转换为指定格式 (nifti 或 npz)"""
        if organized_dir is None:
            organized_dir = os.path.join(extract_dir, "organized")
        
        # 处理可能的旧版布尔参数兼容性
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
        
        # 遍历已下载的Series目录
        for series_folder in os.listdir(extract_dir):
            if series_folder == "organized":
                continue
                
            series_path = os.path.join(extract_dir, series_folder)
            if not os.path.isdir(series_path):
                continue
            
            # 统计DICOM文件
            dicom_files = []
            for file in os.listdir(series_path):
                filepath = os.path.join(series_path, file)
                if os.path.isfile(filepath) and self._is_dicom_file(filepath):
                    dicom_files.append(filepath)
            
            if dicom_files:
                processed_files += len(dicom_files)
                series_info[series_folder] = {
                    'path': series_path,
                    'file_count': len(dicom_files),
                    'files': dicom_files
                }
                
                # 执行转换
                if output_format == 'nifti':
                    self.convert_dicom_to_nifti(series_path, series_folder)
                elif output_format == 'npz':
                    self._convert_to_npz(series_path, series_folder)
        
        print(f"✅ DICOM organization complete! Processed {processed_files} files")
        
        # 将原目录移动到organized下
        for series_folder, info in series_info.items():
            src_path = info['path']
            dst_path = os.path.join(organized_dir, series_folder)
            if src_path != dst_path:
                shutil.move(src_path, dst_path)
                info['path'] = dst_path
        
        return organized_dir, series_info

    def _process_single_series(self, series_path, series_folder, organized_dir, output_format='nifti'):
        """处理单个Series目录：统计、转换并移动到 organized_dir。"""
        if not os.path.isdir(series_path):
            return None

        # 统计 DICOM 文件
        dicom_files = []
        for file in os.listdir(series_path):
            filepath = os.path.join(series_path, file)
            if os.path.isfile(filepath) and self._is_dicom_file(filepath):
                dicom_files.append(filepath)

        if not dicom_files:
            return None

        # 执行转换
        if output_format == 'nifti':
            self.convert_dicom_to_nifti(series_path, series_folder)
        elif output_format == 'npz':
            self._convert_to_npz(series_path, series_folder)

        # 移动到 organized 目录
        os.makedirs(organized_dir, exist_ok=True)
        dst_path = os.path.join(organized_dir, series_folder)
        if series_path != dst_path:
            try:
                shutil.move(series_path, dst_path)
            except Exception:
                # 如果移动失败（比如文件被占用），保留原路径
                dst_path = series_path

        return {
            'path': dst_path,
            'file_count': len(dicom_files),
            'files': dicom_files
        }
    
    def convert_dicom_to_nifti(self, series_dir, series_name):
        """将DICOM序列转换为NIfTI格式"""
        try:
            print(f"   🔄 Converting {series_name} to NIfTI...")

            sample_dcm, modality = self._get_series_sample_dicom(series_dir)
            
            # 尝试使用dcm2niix
            nifti_result = self._convert_with_dcm2niix(series_dir, series_name)
            if nifti_result and nifti_result.get('success'):
                self._generate_series_preview(series_dir, series_name, nifti_result, sample_dcm, modality)
                return nifti_result
            
            # 使用Python库转换
            print(f"   ⚠️  dcm2niix not available, trying Python libraries...")
            nifti_result = self._convert_with_python_libs(series_dir, series_name)
            if nifti_result and nifti_result.get('success'):
                self._generate_series_preview(series_dir, series_name, nifti_result, sample_dcm, modality)
            return nifti_result
            
        except Exception as e:
            print(f"   ❌ NIfTI conversion failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _convert_to_npz(self, series_dir, series_name):
        """将DICOM序列转换为NPZ格式，并按照要求规范化方向"""
        try:
            print(f"   🔄 Converting {series_name} to NPZ (Normalized)...")

            sample_dcm, modality = self._get_series_sample_dicom(series_dir)
            
            # Step 1: 先生成 NIfTI 作为中间文件，以便利用其成熟的方向处理逻辑
            nifti_res = self._convert_with_dcm2niix(series_dir, series_name)
            if not (nifti_res and nifti_res.get('success')):
                nifti_res = self._convert_with_python_libs(series_dir, series_name)
            
            if not (nifti_res and nifti_res.get('success')):
                return {'success': False, 'error': 'Failed to generate base volume for NPZ'}
            
            # Step 2: 加载 NIfTI 并进行规范化处理
            output_files = []
            if nifti_res.get('conversion_mode') == 'individual':
                # 2D 模态 (DR/DX/MG)
                for nii_file in nifti_res.get('output_files', []):
                    nii_path = os.path.join(series_dir, nii_file)
                    npz_file = nii_file.replace('.nii.gz', '.npz').replace('.nii', '.npz')
                    npz_path = os.path.join(series_dir, npz_file)
                    
                    self._normalize_and_save_npz(nii_path, npz_path)
                    output_files.append(npz_file)
                    if os.path.exists(nii_path): os.remove(nii_path)
            else:
                # 3D 模态 (CT/MR)
                nii_file = nifti_res.get('output_file')
                nii_path = os.path.join(series_dir, nii_file)
                npz_file = nii_file.replace('.nii.gz', '.npz').replace('.nii', '.npz')
                npz_path = os.path.join(series_dir, npz_file)
                
                self._normalize_and_save_npz(nii_path, npz_path)
                output_files.append(npz_file)
                if os.path.exists(nii_path): os.remove(nii_path)
                
            qc_summary = self._assess_series_quality_converted(
                [os.path.join(series_dir, f) for f in output_files]
            )
            print(
                f"   🧪 QC({qc_summary['qc_mode']}): "
                f"low_ratio={qc_summary['low_quality_ratio']:.2f}, "
                f"low_quality={qc_summary['low_quality']}"
            )

            try:
                self._generate_series_preview(
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

    def _normalize_and_save_npz(self, nii_path, npz_path):
        """加载NIfTI，利用DICOM方向信息规范化并保存为NPZ"""
        # 加载 NIfTI
        img = nib.load(nii_path)
        # 转为 RAS (Right, Anterior, Superior) 坐标系，此步骤已综合 DICOM Tag 中的方向信息
        img_canonical = nib.as_closest_canonical(img)
        data = img_canonical.get_fdata()
        
        # 按照用户要求进行翻转:
        # 1. Z轴: Head to Feet (Superior -> Inferior). RAS 中 Z+ 为 Superior，故翻转 axis 2.
        # 2. X,Y轴: 仰卧位横断位 (X: Right->Left, Y: Anterior->Posterior).
        #    - RAS 中 X+ 为 Right，故翻转 axis 0 得到 Right->Left.
        #    - RAS 中 Y+ 为 Anterior，故翻转 axis 1 得到 Anterior->Posterior.
        data = data[::-1, ::-1, ::-1]
        
        # 转置为 [Z, Y, X] 格式 (Depth, Height, Width)
        # 这样 data[0] 是最上层(Head)，且平面内满足仰卧位横断位视角
        data = np.transpose(data, (2, 1, 0))
        
        # 压缩保存
        np.savez_compressed(npz_path, data=data.astype(np.float32))

    def _cache_metadata_for_series(self, series_dir, series_name, dicom_files, modality):
        """缓存DICOM元数据，避免删除后无法提取标签"""
        try:
            if not dicom_files:
                return

            read_all = modality in ['DR', 'MG', 'DX']
            records = self._collect_metadata_from_dicoms(
                dicom_files=dicom_files,
                series_folder=series_name,
                modality=modality,
                read_all=read_all
            )
            if not records:
                return

            cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
            payload = {
                "modality": modality,
                "records": records
            }
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            return

    def _collect_metadata_from_dicoms(self, dicom_files, series_folder, modality, read_all):
        """从DICOM文件提取元数据（不含质控字段）"""
        records = []
        try:
            if not dicom_files:
                return records

            current_keywords = self.get_keywords(modality)

            if read_all:
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
            else:
                sample_file = dicom_files[0]
                dcm = pydicom.dcmread(sample_file, force=True)
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
                records.append(metadata)
        except Exception:
            return []

        return records

    def _get_series_sample_dicom(self, series_dir):
        """读取序列中的样本DICOM用于标签信息"""
        try:
            dicom_files = []
            for file in os.listdir(series_dir):
                filepath = os.path.join(series_dir, file)
                if os.path.isfile(filepath) and self._is_dicom_file(filepath):
                    dicom_files.append(filepath)
            if not dicom_files:
                return None, ''
            dicom_files.sort()
            dcm = pydicom.dcmread(dicom_files[0], force=True)
            modality = getattr(dcm, 'Modality', '')
            return dcm, modality
        except Exception:
            return None, ''

    def _get_window_params(self, dcm):
        """获取窗宽窗位"""
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

    def _apply_windowing(self, image_2d, dcm):
        """应用窗宽窗位并归一化到0-255"""
        img = image_2d.astype(np.float32)
        wc, ww = self._get_window_params(dcm)
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

        # 处理灰度反转
        try:
            if dcm is not None:
                photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
                if photometric == 'MONOCHROME1':
                    img = 255 - img
        except Exception:
            pass

        return img

    def _resize_with_aspect(self, img, aspect_ratio):
        """根据像素间距调整纵横比"""
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

    def _normalize_2d_preview(self, img, target_size=896):
        """2D图像标准化到固定大小的方形画布"""
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

    def _generate_series_preview(self, series_dir, series_name, conversion_result, sample_dcm, modality):
        """为序列生成PNG预览图"""
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

            # 选择用于预览的文件
            if modality in ['DR', 'MG', 'DX'] or len(output_files) > 1:
                preview_file = output_files[len(output_files) // 2]
                is_3d = False
            else:
                preview_file = output_files[0]
                is_3d = True

            # 读取转换后的数据
            if preview_file.endswith('.npz'):
                with np.load(preview_file) as npz:
                    if 'data' in npz.files:
                        data = npz['data']
                    elif npz.files:
                        data = npz[npz.files[0]]
                    else:
                        return None

                if data.ndim == 3 and is_3d:
                    # NPZ: [Z, Y, X], coronal -> 固定Y
                    mid_y = data.shape[1] // 2
                    image_2d = data[:, mid_y, :]
                    image_2d = image_2d.astype(np.float32)
                    # data[0] 为头侧，imshow默认顶部为第0行，无需再翻转
                else:
                    image_2d = data if data.ndim == 2 else data[:, :, 0]

            elif preview_file.endswith(('.nii', '.nii.gz')):
                img = nib.load(preview_file)
                img_canonical = nib.as_closest_canonical(img)
                data = img_canonical.get_fdata()

                if data.ndim == 3 and is_3d:
                    # NIfTI: [X, Y, Z], coronal -> 固定Y, 映射为 [Z, X]
                    mid_y = data.shape[1] // 2
                    slice_xz = data[:, mid_y, :]
                    image_2d = np.transpose(slice_xz, (1, 0))
                    # 使Z+（Superior）在顶部
                    image_2d = image_2d[::-1, :]
                else:
                    image_2d = data if data.ndim == 2 else data[:, :, 0]
            else:
                return None

            # 窗宽窗位
            image_2d = self._apply_windowing(image_2d, sample_dcm)

            # 像素间距/层厚决定纵横比
            aspect_ratio = None
            try:
                if sample_dcm is not None:
                    pixel_spacing = getattr(sample_dcm, 'PixelSpacing', None)
                    spacing_between = getattr(sample_dcm, 'SpacingBetweenSlices', None)
                    slice_thickness = getattr(sample_dcm, 'SliceThickness', None)
                    slice_spacing = float(spacing_between or slice_thickness or 1.0)
                    if pixel_spacing and len(pixel_spacing) >= 2:
                        pixel_spacing = [float(pixel_spacing[0]), float(pixel_spacing[1])]
                        if is_3d:
                            # coronal: vertical=Z, horizontal=X
                            aspect_ratio = slice_spacing / max(pixel_spacing[1], 1e-6)
                        else:
                            # 2D: vertical=Y, horizontal=X
                            aspect_ratio = pixel_spacing[0] / max(pixel_spacing[1], 1e-6)
            except Exception:
                aspect_ratio = None

            image_2d = self._resize_with_aspect(image_2d, aspect_ratio)

            if not is_3d:
                image_2d = self._normalize_2d_preview(image_2d, target_size=896)

            preview_name = f"{self._sanitize_folder_name(series_name)}_preview.png"
            preview_path = os.path.join(series_dir, preview_name)

            Image.fromarray(image_2d).save(preview_path)
            return preview_path
        except Exception:
            return None
    
    def _convert_with_dcm2niix(self, series_dir, series_name):
        """使用dcm2niix工具转换"""
        try:
            import subprocess
            
            # 检查dcm2niix是否可用
            try:
                subprocess.run(['dcm2niix', '-h'], capture_output=True, check=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                return {'success': False, 'error': 'dcm2niix not available'}
            
            # 获取序列中的DICOM文件
            dicom_files = []
            for file in os.listdir(series_dir):
                filepath = os.path.join(series_dir, file)
                if file.endswith('.dcm') and os.path.isfile(filepath):
                    dicom_files.append(filepath)
            
            if not dicom_files:
                return {'success': False, 'error': 'No DICOM files found'}
            
            # 读取第一个文件判断Modality
            first_dcm = pydicom.dcmread(dicom_files[0], force=True)
            modality = getattr(first_dcm, 'Modality', '')
            
            output_name = self._sanitize_folder_name(series_name)
            
            if modality in ['DR', 'MG', 'DX']:
                # DR/MG/DX类型：每个文件单独转换
                print(f"   ℹ️  Detected {modality} modality, converting each DICOM to NIfTI")
                
                success_count = 0
                output_files = []
                
                for idx, dcm_file in enumerate(dicom_files):
                    temp_dir = None  # 初始化临时目录变量
                    try:
                        # 创建临时目录存放单个文件
                        temp_dir = os.path.join(series_dir, f'temp_{idx}')
                        os.makedirs(temp_dir, exist_ok=True)
                        
                        # 复制单个文件到临时目录
                        temp_dcm = os.path.join(temp_dir, os.path.basename(dcm_file))
                        shutil.copy2(dcm_file, temp_dcm)
                        
                        # 为每个文件生成唯一的输出名
                        file_output_name = f"{output_name}_{idx+1:04d}"
                        
                        cmd = [
                            'dcm2niix',
                            '-m', 'y',
                            '-f', file_output_name,
                            '-o', series_dir,
                            '-z', 'y',
                            '-b', 'n',
                            temp_dir
                        ]
                        
                        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                        
                        if result.returncode == 0:
                            # 查找生成的NIfTI文件
                            nifti_file = f"{file_output_name}.nii.gz"
                            if os.path.exists(os.path.join(series_dir, nifti_file)):
                                output_files.append(nifti_file)
                                success_count += 1
                        
                        # 每处理10个文件输出一次进度
                        if (idx + 1) % 10 == 0:
                            print(f"      Converted {idx + 1}/{len(dicom_files)} files...")
                        
                    except Exception as e:
                        print(f"      ⚠️  Failed converting file {idx+1}: {e}")
                    finally:
                        # 清理临时目录
                        if temp_dir and os.path.exists(temp_dir):
                            shutil.rmtree(temp_dir, ignore_errors=True)
                
                if success_count > 0:
                    print(f"   ✅ dcm2niix conversion succeeded: {success_count}/{len(dicom_files)} files")

                    self._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
                    
                    # 删除原始DICOM文件
                    for dcm_file in dicom_files:
                        try:
                            os.remove(dcm_file)
                        except:
                            pass
                    
                    return {
                        'success': True,
                        'method': 'dcm2niix',
                        'modality': modality,
                        'conversion_mode': 'individual',
                        'output_files': output_files,
                        'file_count': success_count
                    }
                else:
                    return {'success': False, 'error': 'No files converted successfully'}
            
            else:
                # 非DR/MG/DX类型：整个序列转换为一个文件（原逻辑）
                print(f"   ℹ️  {modality} modality: converting entire series to a single NIfTI file")
                
                cmd = [
                    'dcm2niix',
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

                        self._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
                        
                        # 删除原始DICOM文件
                        for file in os.listdir(series_dir):
                            if file.endswith('.dcm'):
                                try:
                                    os.remove(os.path.join(series_dir, file))
                                except:
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

    def _apply_rescale(self, pixel_data, dcm):
        """应用Rescale Slope/Intercept"""
        try:
            slope = float(getattr(dcm, 'RescaleSlope', 1.0))
            intercept = float(getattr(dcm, 'RescaleIntercept', 0.0))
            return pixel_data.astype(np.float32) * slope + intercept
        except Exception:
            return pixel_data.astype(np.float32)

    def _apply_photometric(self, pixel_data, dcm):
        """处理Photometric Interpretation (MONOCHROME1/2)"""
        try:
            photometric = str(getattr(dcm, 'PhotometricInterpretation', '')).upper()
            if photometric == 'MONOCHROME1':
                max_val = np.nanmax(pixel_data)
                return max_val - pixel_data
        except Exception:
            pass
        return pixel_data

    def _build_affine_from_dicom(self, dcm, slice_spacing=1.0, slice_cosines=None):
        """基于DICOM方向信息构建NIfTI仿射矩阵 (RAS)"""
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

    def _assess_image_quality(self, dcm):
        """基于直方图/对比度的简单质检，返回0/1"""
        try:
            if not hasattr(dcm, 'pixel_array'):
                return 1

            pixel_data = dcm.pixel_array.astype(np.float32)
            pixel_data = self._apply_rescale(pixel_data, dcm)
            pixel_data = self._apply_photometric(pixel_data, dcm)
            return self._assess_image_quality_from_array(pixel_data)
        except Exception:
            return 1

    def _assess_image_quality_from_array(self, pixel_data):
        """基于直方图/对比度的简单质检，返回0/1（输入为数组）"""
        try:
            if pixel_data is None:
                return 1

            pixel_data = np.asarray(pixel_data, dtype=np.float32)
            flat = pixel_data[np.isfinite(pixel_data)].ravel()
            if flat.size == 0:
                return 1

            # 采样避免超大图像的性能问题
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

            # 曝光异常：过暗/过亮或饱和比例过高
            low_thresh = p2 + 0.01 * range_eps
            high_thresh = p98 - 0.01 * range_eps
            low_ratio = float(np.mean(flat <= low_thresh))
            high_ratio = float(np.mean(flat >= high_thresh))

            under_exposed = mean_val < (p2 + 0.1 * range_eps) or low_ratio > 0.6
            over_exposed = mean_val > (p98 - 0.1 * range_eps) or high_ratio > 0.6

            # 灰度反转检测：边缘背景显著更亮
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

    def _assess_converted_file_quality(self, filepath):
        """基于转换后的NPZ/NIfTI文件做质检，返回0/1"""
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

            return self._assess_image_quality_from_array(data)
        except Exception:
            return 1

    def _assess_series_quality_converted(self, converted_files):
        """对转换后的序列做QC，<=200全量，>200中间±3抽样"""
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
                    low_count += int(self._assess_converted_file_quality(converted_files[idx]))
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

    def _get_converted_files(self, series_path):
        """获取转换后的NPZ/NIfTI文件列表，优先NPZ"""
        try:
            npz_files = sorted([f for f in os.listdir(series_path) if f.endswith('.npz')])
            if npz_files:
                return [os.path.join(series_path, f) for f in npz_files], 'npz'

            nifti_files = sorted([f for f in os.listdir(series_path) if f.endswith(('.nii.gz', '.nii'))])
            if nifti_files:
                return [os.path.join(series_path, f) for f in nifti_files], 'nifti'

            return [], None
        except Exception:
            return [], None

    def _assess_series_quality(self, dicom_files):
        """对序列做QC，<=200全量，>200中间±3抽样"""
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
                    dcm = pydicom.dcmread(dicom_files[idx], force=True)
                    low_count += int(self._assess_image_quality(dcm))
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


    def _convert_with_python_libs(self, series_dir, series_name):
        """使用Python库转换DICOM到NIfTI"""
        try:
            dicom_files = []
            for file in os.listdir(series_dir):
                filepath = os.path.join(series_dir, file)
                if self._is_dicom_file(filepath):
                    dicom_files.append(filepath)
            
            if not dicom_files:
                return {'success': False, 'error': 'No DICOM files found'}
            
            # 读取第一个文件判断Modality
            first_dcm = pydicom.dcmread(dicom_files[0], force=True)
            modality = getattr(first_dcm, 'Modality', '')
            
            if modality in ['DR', 'MG', 'DX']:
                # DR/MG/DX: convert each file individually
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
                        pixel_data = self._apply_rescale(pixel_data, dcm)
                        pixel_data = self._apply_photometric(pixel_data, dcm)

                        slice_thickness = float(getattr(dcm, 'SliceThickness', 1.0))
                        affine = self._build_affine_from_dicom(dcm, slice_spacing=slice_thickness)
                        
                        # 如果是2D图像，需要添加一个维度
                        if len(pixel_data.shape) == 2:
                            pixel_data = pixel_data[:, :, np.newaxis]
                        
                        # 创建NIfTI图像
                        nifti_img = nib.Nifti1Image(pixel_data, affine)
                        
                        # 生成输出文件名
                        output_filename = f"{self._sanitize_folder_name(series_name)}_{idx+1:04d}.nii.gz"
                        output_path = os.path.join(series_dir, output_filename)
                        nib.save(nifti_img, output_path)
                        
                        output_files.append(output_filename)
                        success_count += 1
                        
                        # 每处理10个文件输出一次进度
                        if (idx + 1) % 10 == 0:
                            print(f"      Converted {idx + 1}/{len(dicom_files)} files...")
                        
                    except Exception as e:
                        print(f"      ⚠️  Failed converting file {idx+1}: {e}")
                        continue
                
                if success_count > 0:
                    # 删除原始DICOM文件
                    self._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
                    for dcm_file in dicom_files:
                        try:
                            os.remove(dcm_file)
                        except:
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
                else:
                    return {'success': False, 'error': 'No files converted successfully'}
            
            else:
                # Non-DR/MG/DX: convert entire series to single file
                print(f"   ℹ️  {modality} modality: converting entire series to a single NIfTI file")
                
                # 单文件处理
                if len(dicom_files) == 1:
                    dcm = first_dcm
                    if not hasattr(dcm, 'pixel_array'):
                        return {'success': False, 'error': 'No pixel data'}
                    
                    pixel_data = dcm.pixel_array
                    pixel_data = self._apply_rescale(pixel_data, dcm)
                    pixel_data = self._apply_photometric(pixel_data, dcm)

                    slice_thickness = float(getattr(dcm, 'SliceThickness', 1.0))
                    affine = self._build_affine_from_dicom(dcm, slice_spacing=slice_thickness)
                    
                    nifti_img = nib.Nifti1Image(pixel_data, affine)
                    output_filename = f"{self._sanitize_folder_name(series_name)}.nii.gz"
                    output_path = os.path.join(series_dir, output_filename)
                    nib.save(nifti_img, output_path)
                    
                    # 删除原始DICOM文件
                    self._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
                    for file in dicom_files:
                        try:
                            os.remove(file)
                        except:
                            pass
                    
                    print(f"   ✅ Python libs conversion succeeded: {output_filename}")
                    return {
                        'success': True,
                        'method': 'python_libs',
                        'modality': modality,
                        'conversion_mode': 'series',
                        'output_file': output_filename
                    }
                
                # 多文件3D处理
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
                    except:
                        continue
                
                if not slice_info:
                    return {'success': False, 'error': 'Could not sort slices'}
                
                slice_info.sort(key=lambda x: x[0])
                
                slices = []
                positions = []
                for _, _, dcm, ipp in slice_info:
                    if hasattr(dcm, 'pixel_array'):
                        pixel_data = dcm.pixel_array
                        pixel_data = self._apply_rescale(pixel_data, dcm)
                        pixel_data = self._apply_photometric(pixel_data, dcm)
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

                affine = self._build_affine_from_dicom(first_dcm, slice_spacing=slice_spacing, slice_cosines=slice_cosines)
                
                nifti_img = nib.Nifti1Image(volume, affine)
                output_filename = f"{self._sanitize_folder_name(series_name)}.nii.gz"
                output_path = os.path.join(series_dir, output_filename)
                nib.save(nifti_img, output_path)
                
                # 删除原始DICOM文件
                self._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
                for file in dicom_files:
                    try:
                        os.remove(file)
                    except:
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
    
    def extract_dicom_metadata(self, organized_dir, output_excel=None):
        """提取DICOM元数据并保存为Excel文件。

        默认把 Excel 放在 organized_dir 的上级目录（每个检查子目录）。
        """
        if output_excel is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            output_excel = os.path.join(os.path.dirname(organized_dir), f"dicom_metadata_{timestamp}.xlsx")
        
        print(f"📊 Extracting DICOM metadata...")
        
        all_metadata = []
        
        # 遍历organized目录
        for series_folder in os.listdir(organized_dir):
            series_path = os.path.join(organized_dir, series_folder)
            
            if not os.path.isdir(series_path):
                continue
            
            print(f"📂 Processing series: {series_folder}")

            converted_files, converted_type = self._get_converted_files(series_path)
            
            # 获取DICOM文件（或查找剩余的.dcm文件）
            dicom_files = []
            for file in os.listdir(series_path):
                filepath = os.path.join(series_path, file)
                if file.endswith('.dcm') and os.path.isfile(filepath):
                    dicom_files.append(filepath)
            
            # 如果没有DICOM文件，尝试查找NIfTI文件以获取基本信息
            if not dicom_files:
                cache_path = os.path.join(series_path, "dicom_metadata_cache.json")
                if os.path.exists(cache_path):
                    try:
                        with open(cache_path, 'r', encoding='utf-8') as f:
                            cache = json.load(f)
                        cached_records = cache.get('records', [])
                        cached_modality = str(cache.get('modality', '')).upper()
                        read_all = cached_modality in ['DR', 'MG', 'DX']

                        if read_all:
                            converted_quality = [self._assess_converted_file_quality(p) for p in converted_files]
                            for idx, record in enumerate(cached_records):
                                record['Low_quality'] = converted_quality[idx] if idx < len(converted_quality) else 1
                                all_metadata.append(record)
                        else:
                            series_quality = self._assess_series_quality_converted(converted_files).get('low_quality', 1)
                            if cached_records:
                                cached_records[0]['Low_quality'] = series_quality
                                all_metadata.append(cached_records[0])
                        continue
                    except Exception:
                        pass

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
            
            # 先读取第一个文件判断Modality
            try:
                sample_file = dicom_files[0]
                dcm = pydicom.dcmread(sample_file, force=True)
                modality = getattr(dcm, 'Modality', '')
                
                # 判断是否需要遍历所有文件
                need_read_all = modality in ['DR', 'MG', 'DX']
                
                if need_read_all:
                    print(f"   ℹ️  Detected {modality} modality; will read all {len(dicom_files)} DICOM files")

                    records = self._collect_metadata_from_dicoms(
                        dicom_files=dicom_files,
                        series_folder=series_folder,
                        modality=modality,
                        read_all=True
                    )
                    converted_quality = [self._assess_converted_file_quality(p) for p in converted_files]

                    for idx, record in enumerate(records):
                        record['Low_quality'] = converted_quality[idx] if idx < len(converted_quality) else 1
                        all_metadata.append(record)

                        if (idx + 1) % 10 == 0:
                            print(f"      Processed {idx + 1}/{len(records)} files...")
                else:
                    # Original logic: read only representative file
                    print(f"   ℹ️  {modality} modality; reading representative file only")

                    records = self._collect_metadata_from_dicoms(
                        dicom_files=dicom_files,
                        series_folder=series_folder,
                        modality=modality,
                        read_all=False
                    )
                    if records:
                        records[0]['Low_quality'] = self._assess_series_quality_converted(converted_files).get('low_quality', 1)
                        all_metadata.append(records[0])
                    
            except Exception as e:
                print(f"     ❌ Failed processing series: {e}")
                
                continue
        
        if not all_metadata:
            print("❌ No metadata extracted")
            return None
        
        # 创建DataFrame并保存为Excel
        try:
            df = pd.DataFrame(all_metadata)
            
            # 重新排列列的顺序
            column_order = []
            
            # 优先显示的列
            priority_columns = ['SeriesFolder', 'FileName', 'SampleFileName', 'FileIndex', 
                            'TotalFilesInSeries', 'FilesReadForMetadata']
            for col in priority_columns:
                if col in df.columns:
                    column_order.append(col)
            
            # 重要的DICOM字段
            important_fields = ['PatientID', 'AccessionNumber', 'StudyDate', 'Modality',
                            'SeriesNumber', 'SeriesDescription', 'InstanceNumber']
            
            for field in important_fields:
                if field in df.columns and field not in column_order:
                    column_order.append(field)
            
            # 添加剩余的列
            for col in df.columns:
                if col not in column_order:
                    column_order.append(col)
            
            df = df[column_order]
            
            # 保存Excel，创建多个工作表
            with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
                # 主数据表
                df.to_excel(writer, sheet_name='DICOM_Metadata', index=False)
                
                # 创建汇总表（按Series汇总）
                summary_data = []
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
                
                # 调整列宽
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # 统计信息
            total_files_read = len(df)
            dr_mg_dx_series = df[df['Modality'].isin(['DR', 'MG', 'DX'])]['SeriesFolder'].nunique() if 'Modality' in df.columns else 0
            
            print(f"✅ Metadata extraction complete!")
            print(f"📄 Excel file: {output_excel}")
            print(f"📊 Total records: {total_files_read}")
            if dr_mg_dx_series > 0:
                print(f"📋 DR/MG/DX series count: {dr_mg_dx_series} (all files read)")

            self._append_mr_cleaned_sheet(df, output_excel)
            
            return output_excel
            
        except Exception as e:
            print(f"❌ Failed saving Excel file: {e}")
            return None

    def _append_mr_cleaned_sheet(self, df: pd.DataFrame, output_excel: str) -> None:
        """对 MR 记录做治理/规范化，并写回到同一个 Excel 的 MR_Cleaned sheet。"""
        try:
            if df is None or df.empty or 'Modality' not in df.columns:
                return

            mr_df = df[df['Modality'].astype(str).str.upper() == 'MR'].copy()
            if mr_df.empty:
                return

            print(f"\n🔬 MR_clean: processing {len(mr_df)} MR records...")

            from MR_clean import process_mri_dataframe

            # forward optional progress callback
            try:
                cleaned_df = process_mri_dataframe(mr_df, progress_callback=self.progress_callback)
            except TypeError:
                # fallback for older MR_clean signature
                cleaned_df = process_mri_dataframe(mr_df)

            with pd.ExcelWriter(
                output_excel,
                engine='openpyxl',
                mode='a',
                if_sheet_exists='replace',
            ) as writer:
                cleaned_df.to_excel(writer, sheet_name='MR_Cleaned', index=False)

            print("✅ MR_clean: MR_Cleaned sheet written.")
        except Exception as e:
            print(f"⚠️  MR_clean skipped/failed: {e}")
    
    def process_complete_workflow(self, accession_number, base_output_dir="./downloads",
                                auto_extract=True, auto_organize=True, auto_metadata=True,
                                keep_zip=True, keep_extracted=False, output_format='nifti',
                                parallel_pipeline=True):
        """完整的工作流程：下载 -> 整理 -> 转换 -> 提取元数据"""
        print(f"\n{'='*80}")
        print(f"🚀 Starting full DICOM processing workflow")
        print(f"📋 AccessionNumber: {accession_number}")
        print(f"{'='*80}")
        
        # 确保输出目录存在
        os.makedirs(base_output_dir, exist_ok=True)
        
        # 步骤1: 下载DICOM文件
        print(f"\n📥 Step 1: Download DICOM files")

        download_dir_holder = {'path': None}
        series_queue = Queue()
        series_info = {}
        series_lock = threading.Lock()
        download_done = threading.Event()

        def _on_series_downloaded(series_dir, series_meta):
            series_folder = os.path.basename(series_dir)
            series_queue.put((series_dir, series_folder))

        def _download_worker():
            try:
                download_path = self.download_study(
                    accession_number,
                    base_output_dir,
                    on_series_downloaded=_on_series_downloaded
                )
                download_dir_holder['path'] = download_path
            finally:
                download_done.set()

        def _organize_worker(organized_dir_local, fmt):
            while True:
                item = series_queue.get()
                if item is None:
                    series_queue.task_done()
                    break
                series_dir, series_folder = item
                try:
                    info = self._process_single_series(series_dir, series_folder, organized_dir_local, fmt)
                    if info:
                        with series_lock:
                            series_info[series_folder] = info
                except Exception as e:
                    print(f"⚠️  Series organize failed: {series_folder}: {e}")
                finally:
                    series_queue.task_done()

        if parallel_pipeline and auto_organize:
            organized_dir = os.path.join(base_output_dir, f"{accession_number}_organized")
            os.makedirs(organized_dir, exist_ok=True)

            download_thread = threading.Thread(target=_download_worker, daemon=True)
            organize_thread = threading.Thread(target=_organize_worker, args=(organized_dir, output_format), daemon=True)

            download_thread.start()
            organize_thread.start()

            # 等待下载完成
            download_thread.join()
            # 通知整理线程退出
            series_queue.put(None)
            series_queue.join()
            organize_thread.join()

            download_dir = download_dir_holder['path']
            if not download_dir:
                print("❌ Download failed, workflow terminated")
                return None
        else:
            download_dir = self.download_study(accession_number, base_output_dir)
            if not download_dir:
                print("❌ Download failed, workflow terminated")
                return None
        
        results = {
            'accession_number': accession_number,
            'zip_file': download_dir,  # 保持接口兼容性
            'extract_dir': download_dir,  # 保持接口兼容性
            'success': False
        }
        
        if auto_organize:
            # 步骤2: 整理DICOM文件
            print(f"\n📁 Step 2: Organize DICOM files by series (format: {output_format})")
            if parallel_pipeline:
                # 使用流水线整理结果
                organized_dir = os.path.join(base_output_dir, f"{accession_number}_organized")
                results['organized_dir'] = organized_dir
                results['series_info'] = series_info
            else:
                organized_dir, series_info = self.organize_dicom_files(download_dir, output_format=output_format)
                if not organized_dir:
                    print("❌ File organization failed, workflow terminated")
                    return results
                results['organized_dir'] = organized_dir
                results['series_info'] = series_info

            if auto_metadata:
                # 步骤3: 提取元数据 (独立线程)
                print(f"\n📊 Step 3: Extract DICOM metadata")
                excel_name = f"dicom_metadata_{accession_number}.xlsx"
                excel_path = os.path.join(os.path.dirname(organized_dir), excel_name)

                excel_holder = {'path': None}

                def _metadata_worker():
                    excel_holder['path'] = self.extract_dicom_metadata(organized_dir, output_excel=excel_path)

                metadata_thread = threading.Thread(target=_metadata_worker, daemon=True)
                metadata_thread.start()
                metadata_thread.join()

                excel_file = excel_holder['path']
                if excel_file:
                    results['excel_file'] = excel_file
                    results['success'] = True
                else:
                    print("⚠️  Metadata extraction failed, previous steps completed")
        
        # 打印最终结果
        print(f"\n{'='*80}")
        if results['success']:
            print(f"🎉 Workflow completed!")
            print(f"📁 Organized directory: {results.get('organized_dir', 'N/A')}")
            print(f"📄 Excel file: {results.get('excel_file', 'N/A')}")
            print(f"📊 Series count: {len(results.get('series_info', {}))}")
        else:
            print(f"⚠️  Workflow partially completed")
        print(f"{'='*80}")
        
        return results


def main():
    """主函数 - 演示完整工作流程"""
    print("🏥 Unified DICOM download and processing system")
    print("📡 Direct PACS server connection")
    
    # 创建客户端
    client = DICOMDownloadClient()
    
    # 检查PACS状态
    if not client.check_status():
        print("❌ PACS unavailable, exiting")
        return
    
    # 虚拟登录（保持接口兼容性）
    client.login("admin", "admin123")
    
    try:
        # 执行完整工作流程
        accession_number = "Z25043000836"  # 示例AccessionNumber
        
        results = client.process_complete_workflow(
            accession_number=accession_number,
            base_output_dir="./dicom_processed",
            auto_extract=True,  # 保持兼容性参数
            auto_organize=True,
            auto_metadata=True,
            keep_zip=False,     # 保持兼容性参数
            keep_extracted=False,
            output_format='nifti'  # 可选 'nifti' 或 'npz'
        )
        
        if results and results['success']:
            print(f"\n🎊 Processing complete! See the following files:")
            if 'excel_file' in results:
                print(f"   📄 Metadata Excel: {results['excel_file']}")
            if 'organized_dir' in results:
                print(f"   📁 Organized directory: {results['organized_dir']}")
        else:
            print(f"\n❌ Processing not fully successful")
    
    finally:
        # 虚拟登出
        client.logout()


if __name__ == "__main__":
    main()