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
from pathlib import Path
import pandas as pd
import pydicom
from collections import defaultdict
import re
import nibabel as nib
import numpy as np
from datetime import datetime
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

from pynetdicom import AE, evt
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
)
from pynetdicom import AllStoragePresentationContexts
from pydicom.dataset import Dataset

# 加载环境变量
load_dotenv()

os.environ['PATH'] = os.getcwd() + os.pathsep + os.environ['PATH']

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
                if os.path.exists("keywords.json"):
                    with open("keywords.json", 'r', encoding='utf-8') as f:
                        default_keywords = json.load(f)
                    print(f"⚠️  {tags_dir} not found, using keywords.json as default")
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
                print("✅ PACS connection status: OK")
                return True
            else:
                print("❌ Unable to connect to PACS")
                return False
        except Exception as e:
            print(f"❌ PACS connection error: {e}")
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
    
    def download_study(self, accession_number, output_dir=".", custom_folder_name=None):
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
        """保持接口兼容性，直接返回路径（因为不再有ZIP文件）"""
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
    
    def convert_dicom_to_nifti(self, series_dir, series_name):
        """将DICOM序列转换为NIfTI格式"""
        try:
            print(f"   🔄 Converting {series_name} to NIfTI...")
            
            # 尝试使用dcm2niix
            nifti_result = self._convert_with_dcm2niix(series_dir, series_name)
            if nifti_result and nifti_result.get('success'):
                return nifti_result
            
            # 使用Python库转换
            print(f"   ⚠️  dcm2niix not available, trying Python libraries...")
            nifti_result = self._convert_with_python_libs(series_dir, series_name)
            return nifti_result
            
        except Exception as e:
            print(f"   ❌ NIfTI conversion failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _convert_to_npz(self, series_dir, series_name):
        """将DICOM序列转换为NPZ格式，并按照要求规范化方向"""
        try:
            print(f"   🔄 Converting {series_name} to NPZ (Normalized)...")
            
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
                
            return {
                'success': True,
                'method': 'npz_normalized',
                'output_files': output_files
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
                        
                        # 处理数据类型
                        if pixel_data.dtype == np.uint16 and dcm.get('PixelRepresentation', 0) == 1:
                            pixel_data = pixel_data.astype(np.int16)
                        
                        # 获取像素间距
                        pixel_spacing = getattr(dcm, 'PixelSpacing', [1.0, 1.0])
                        slice_thickness = getattr(dcm, 'SliceThickness', 1.0)
                        
                        # 创建仿射矩阵
                        affine = np.eye(4)
                        affine[0, 0] = float(pixel_spacing[1])
                        affine[1, 1] = float(pixel_spacing[0])
                        affine[2, 2] = float(slice_thickness)
                        
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
                    pixel_spacing = getattr(dcm, 'PixelSpacing', [1.0, 1.0])
                    slice_thickness = getattr(dcm, 'SliceThickness', 1.0)
                    
                    affine = np.eye(4)
                    affine[0, 0] = float(pixel_spacing[1])
                    affine[1, 1] = float(pixel_spacing[0])
                    affine[2, 2] = float(slice_thickness)
                    
                    nifti_img = nib.Nifti1Image(pixel_data, affine)
                    output_filename = f"{self._sanitize_folder_name(series_name)}.nii.gz"
                    output_path = os.path.join(series_dir, output_filename)
                    nib.save(nifti_img, output_path)
                    
                    # 删除原始DICOM文件
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
                            z_pos = float(dcm.ImagePositionPatient[2])
                        elif hasattr(dcm, 'SliceLocation'):
                            z_pos = float(dcm.SliceLocation)
                        else:
                            z_pos = 0
                        slice_info.append((z_pos, filepath, dcm))
                    except:
                        continue
                
                if not slice_info:
                    return {'success': False, 'error': 'Could not sort slices'}
                
                slice_info.sort(key=lambda x: x[0])
                
                slices = []
                for _, _, dcm in slice_info:
                    if hasattr(dcm, 'pixel_array'):
                        slices.append(dcm.pixel_array)
                
                if not slices:
                    return {'success': False, 'error': 'No pixel data found'}
                
                volume = np.stack(slices, axis=2)
                
                pixel_spacing = getattr(first_dcm, 'PixelSpacing', [1.0, 1.0])
                
                if len(slice_info) > 1:
                    slice_thickness = abs(slice_info[1][0] - slice_info[0][0])
                else:
                    slice_thickness = getattr(first_dcm, 'SliceThickness', 1.0)
                
                affine = np.eye(4)
                affine[0, 0] = float(pixel_spacing[1])
                affine[1, 1] = float(pixel_spacing[0])
                affine[2, 2] = float(slice_thickness)
                
                nifti_img = nib.Nifti1Image(volume, affine)
                output_filename = f"{self._sanitize_folder_name(series_name)}.nii.gz"
                output_path = os.path.join(series_dir, output_filename)
                nib.save(nifti_img, output_path)
                
                # 删除原始DICOM文件
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
        """提取DICOM元数据并保存为Excel文件"""
        if output_excel is None:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            metadata_dir = os.path.join(os.path.dirname(organized_dir), "metadata")
            os.makedirs(metadata_dir, exist_ok=True)
            output_excel = os.path.join(metadata_dir, f"dicom_metadata_{timestamp}.xlsx")
        
        print(f"📊 Extracting DICOM metadata...")
        
        all_metadata = []
        
        # 遍历organized目录
        for series_folder in os.listdir(organized_dir):
            series_path = os.path.join(organized_dir, series_folder)
            
            if not os.path.isdir(series_path):
                continue
            
            print(f"📂 Processing series: {series_folder}")
            
            # 获取DICOM文件（或查找剩余的.dcm文件）
            dicom_files = []
            for file in os.listdir(series_path):
                filepath = os.path.join(series_path, file)
                if file.endswith('.dcm') and os.path.isfile(filepath):
                    dicom_files.append(filepath)
            
            # 如果没有DICOM文件，尝试查找NIfTI文件以获取基本信息
            if not dicom_files:
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
                    
                    # 遍历所有DICOM文件
                    for idx, dicom_file in enumerate(dicom_files):
                        try:
                            dcm = pydicom.dcmread(dicom_file, force=True)
                            
                            metadata = {
                                'SeriesFolder': series_folder,
                                'FileName': os.path.basename(dicom_file),
                                'FileIndex': idx + 1,
                                'TotalFilesInSeries': len(dicom_files)
                            }
                            
                            # 获取对应模态的字段列表
                            current_keywords = self.get_keywords(modality)
                            
                            # 提取关键字段
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
                                except:
                                    metadata[keyword] = ""
                            
                            all_metadata.append(metadata)
                            
                            # 每处理10个文件输出一次进度
                            if (idx + 1) % 10 == 0:
                                print(f"      Processed {idx + 1}/{len(dicom_files)} files...")
                            
                        except Exception as e:
                            print(f"     ⚠️  Failed reading file {os.path.basename(dicom_file)}: {e}")
                            continue
                else:
                    # Original logic: read only representative file
                    print(f"   ℹ️  {modality} modality; reading representative file only")
                    
                    metadata = {
                        'SeriesFolder': series_folder,
                        'SampleFileName': os.path.basename(sample_file),
                        'TotalFilesInSeries': len(dicom_files),
                        'FilesReadForMetadata': 1  # 标记只读取了一个文件
                    }
                    
                    # 获取对应模态的字段列表
                    current_keywords = self.get_keywords(modality)
                    
                    # 提取关键字段
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
                        except:
                            metadata[keyword] = ""
                    
                    all_metadata.append(metadata)
                    
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
                                keep_zip=True, keep_extracted=False, output_format='nifti'):
        """完整的工作流程：下载 -> 整理 -> 转换 -> 提取元数据"""
        print(f"\n{'='*80}")
        print(f"🚀 Starting full DICOM processing workflow")
        print(f"📋 AccessionNumber: {accession_number}")
        print(f"{'='*80}")
        
        # 确保输出目录存在
        os.makedirs(base_output_dir, exist_ok=True)
        
        # 步骤1: 下载DICOM文件
        print(f"\n📥 Step 1: Download DICOM files")
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
            organized_dir, series_info = self.organize_dicom_files(download_dir, output_format=output_format)
            if not organized_dir:
                print("❌ File organization failed, workflow terminated")
                return results
            
            results['organized_dir'] = organized_dir
            results['series_info'] = series_info
            
            if auto_metadata:
                # 步骤3: 提取元数据
                print(f"\n📊 Step 3: Extract DICOM metadata")
                excel_file = self.extract_dicom_metadata(organized_dir)
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