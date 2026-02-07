# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
DICOM 客户端统一模块

提供与 PACS 服务器通信、DICOM 下载、处理流程编排等功能。
支持 PACS 查询下载流程和本地上传处理流程。
"""

import os
import json
import time
import threading
import zipfile
from queue import Queue
import pandas as pd
import pydicom
import re
import sys
import logging
from types import SimpleNamespace
from src.core.metadata import extract_dicom_metadata as extract_dicom_metadata_impl
from src.core.organize import organize_dicom_files as organize_dicom_files_impl
from src.core.organize import process_single_series as process_single_series_impl
from src.core.convert import convert_dicom_to_nifti as convert_dicom_to_nifti_impl
from src.core.convert import convert_to_npz as convert_to_npz_impl
from src.core.convert import normalize_and_save_npz as normalize_and_save_npz_impl
from src.core.convert import convert_with_dcm2niix as convert_with_dcm2niix_impl
from src.core.convert import convert_with_python_libs as convert_with_python_libs_impl
from src.core.convert import apply_rescale as apply_rescale_impl
from src.core.convert import apply_photometric as apply_photometric_impl
from src.core.convert import build_affine_from_dicom as build_affine_from_dicom_impl
from src.core.preview import get_window_params as get_window_params_impl
from src.core.preview import apply_windowing as apply_windowing_impl
from src.core.preview import resize_with_aspect as resize_with_aspect_impl
from src.core.preview import normalize_2d_preview as normalize_2d_preview_impl
from src.core.preview import generate_series_preview as generate_series_preview_impl
from src.core.qc import assess_image_quality as assess_image_quality_impl
from src.core.qc import assess_image_quality_from_array as assess_image_quality_from_array_impl
from src.core.qc import assess_converted_file_quality as assess_converted_file_quality_impl
from src.core.qc import assess_series_quality_converted as assess_series_quality_converted_impl
from src.core.qc import assess_series_quality as assess_series_quality_impl
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
        # optional download progress callback: function(current_series, total_series, series_name)
        self.download_progress_callback = None
        # disk watermarks (GB) for download throttling
        try:
            self._download_high_watermark_gb = float(os.getenv('DOWNLOAD_HIGH_WATERMARK_GB', '45'))
            self._download_low_watermark_gb = float(os.getenv('DOWNLOAD_LOW_WATERMARK_GB', '40'))
        except Exception:
            self._download_high_watermark_gb = 45.0
            self._download_low_watermark_gb = 40.0
    
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
                        keywords_map['DEFAULT'] = json.load(f)
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
        elif "MR" in modality:
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

    def _get_dir_size_gb(self, directory):
        """计算目录大小（GB），用于磁盘水位判断。"""
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(directory):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        try:
                            total_size += os.path.getsize(filepath)
                        except Exception:
                            continue
        except Exception:
            return 0.0
        return total_size / (1024 ** 3)

    def _wait_for_disk_low(self, directory, sleep_sec=5):
        """当目录大小超过高水位时阻塞，直到降到低水位以下。

        该方法在下载循环中被调用以实现简单的回压，避免无限制拉取导致磁盘耗尽。
        """
        try:
            high = float(os.getenv('DOWNLOAD_HIGH_WATERMARK_GB', str(self._download_high_watermark_gb)))
            low = float(os.getenv('DOWNLOAD_LOW_WATERMARK_GB', str(self._download_low_watermark_gb)))
        except Exception:
            high = self._download_high_watermark_gb
            low = self._download_low_watermark_gb

        # 快速判断：如果目录不存在或大小小于高水位，立即返回
        try:
            current = self._get_dir_size_gb(directory)
        except Exception:
            return

        while current >= high:
            try:
                logger.warning(f"Disk high watermark reached ({current:.2f}GB >= {high}GB). Pausing downloads...")
            except Exception:
                pass
            time.sleep(sleep_sec)
            try:
                current = self._get_dir_size_gb(directory)
            except Exception:
                break
            if current <= low:
                try:
                    logger.info(f"Disk usage dropped to {current:.2f}GB <= low watermark {low}GB, resuming")
                except Exception:
                    pass
                break
    
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
                    
                    # 当磁盘空间达到高水位时，暂停下载以等待转换/清理
                    try:
                        # 使用 base output dir 作为磁盘检查目标（output_dir 参数）
                        self._wait_for_disk_low(output_path)
                    except Exception:
                        pass

                    # 发送C-MOVE请求
                    move_ds = Dataset()
                    move_ds.QueryRetrieveLevel = 'SERIES'
                    move_ds.StudyInstanceUID = series['StudyInstanceUID']
                    move_ds.SeriesInstanceUID = series['SeriesInstanceUID']
                    
                    print(f"   Sending C-MOVE request for Series {series_num}...")
                    
                    # 报告下载进度
                    if callable(self.download_progress_callback):
                        try:
                            progress_pct = 40 + int((i / len(series_metadata)) * 40)  # 40-80% 用于下载
                            self.download_progress_callback(i + 1, len(series_metadata), series_desc, progress_pct)
                        except Exception as cb_e:
                            print(f"   Progress callback error: {cb_e}")
                    
                    responses = assoc.send_c_move(
                        move_ds,
                        self.pacs_config['CALLING_AET'],
                        query_model=StudyRootQueryRetrieveInformationModelMove
                    )
                    
                    # 跟踪C-MOVE响应状态
                    move_status = None
                    for (status, identifier) in responses:
                        if status:
                            move_status = status.Status
                            if status.Status == 0x0000:
                                print(f"   Series {series_num} C-MOVE completed successfully")
                            elif status.Status != 0xFF00:  # 0xFF00 是Pending状态
                                print(f"   Series {series_num} C-MOVE status: 0x{status.Status:04X}")
                    
                    if move_status is None:
                        print(f"   ⚠️  Series {series_num}: No C-MOVE response received (timeout or network issue)")
                    
                    time.sleep(0.5)  # 短暂延迟，让文件写入完成
                    
                    # 通知外部：该Series下载完成
                    # 注意：回调必须在sleep之后调用，确保文件已完全写入磁盘
                    if callable(on_series_downloaded):
                        try:
                            on_series_downloaded(series_dir, series)
                        except Exception as e:
                            print(f"⚠️  Series callback failed: {e}")
                
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
        """解压zip_filepath到指定目录。

        若 zip_filepath 已是目录，则直接返回该目录。
        """
        if not zip_filepath:
            return None
        if os.path.isdir(zip_filepath):
            return zip_filepath
        if not os.path.isfile(zip_filepath):
            return None

        if extract_dir is None:
            base_name = os.path.splitext(os.path.basename(zip_filepath))[0]
            extract_dir = os.path.join(os.path.dirname(zip_filepath), base_name)

        os.makedirs(extract_dir, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
        except Exception:
            return None

        return extract_dir

    def _get_required_tag_names(self):
        """转换/预览所需的最小DICOM标签列表。"""
        return [
            'Modality',
            'WindowCenter',
            'WindowWidth',
            'Rows',
            'Columns',
            'PixelSpacing',
            'ImagerPixelSpacing',
            'PatientOrientation',
            'SpacingBetweenSlices',
            'SliceThickness',
            'PhotometricInterpretation',
            'RescaleSlope',
            'RescaleIntercept',
            'ImageOrientationPatient',
            'ImagePositionPatient'
        ]

    def _normalize_tag_value(self, value):
        if value is None:
            return None
        if hasattr(value, 'value'):
            value = value.value
        if hasattr(value, '__len__') and not isinstance(value, str):
            return [self._normalize_tag_value(v) for v in value]
        try:
            return float(value)
        except (TypeError, ValueError):
            return str(value)

    def _build_sample_tags(self, dcm):
        """从样本DICOM构建可序列化的tag信息。"""
        tags = {}
        for tag_name in self._get_required_tag_names():
            try:
                tags[tag_name] = self._normalize_tag_value(getattr(dcm, tag_name, None))
            except Exception:
                tags[tag_name] = None
        return tags

    def _load_sample_tags_from_cache(self, series_dir):
        cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
        if not os.path.exists(cache_path):
            return None
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
            return cache.get('sample_tags')
        except Exception:
            return None

    def _ensure_metadata_cache(self, series_dir, series_name, dicom_files, modality):
        cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
        if os.path.exists(cache_path):
            return
        self._cache_metadata_for_series(series_dir, series_name, dicom_files, modality)
        if os.path.exists(cache_path):
            return

        sample_tags = None
        try:
            if dicom_files:
                sample_dcm = pydicom.dcmread(dicom_files[0], force=True)
                sample_tags = self._build_sample_tags(sample_dcm)
                if not sample_tags.get('Modality'):
                    sample_tags['Modality'] = modality
        except Exception:
            sample_tags = None

        fallback_records = [
            {
                'SeriesFolder': series_name,
                'TotalFilesInSeries': len(dicom_files),
                'FilesReadForMetadata': 0,
                'Modality': modality
            }
        ]
        payload = {
            'modality': modality,
            'records': fallback_records,
            'sample_tags': sample_tags
        }
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            return

    def _write_minimal_cache(self, series_dir, series_name, modality, sample_dcm=None, file_count=0):
        cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
        if os.path.exists(cache_path):
            return

        sample_tags = None
        try:
            if sample_dcm is not None:
                sample_tags = self._build_sample_tags(sample_dcm)
                if not sample_tags.get('Modality'):
                    sample_tags['Modality'] = modality
        except Exception:
            sample_tags = None

        payload = {
            'modality': modality,
            'records': [
                {
                    'SeriesFolder': series_name,
                    'TotalFilesInSeries': file_count,
                    'FilesReadForMetadata': 0,
                    'Modality': modality
                }
            ],
            'sample_tags': sample_tags
        }
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            return
    
    def _is_dicom_file(self, filepath):
        """判断是否为DICOM文件"""
        if filepath.endswith("json") or filepath.endswith("csv") or filepath.endswith("txt"):
            return False
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
        
        name = re.sub(r'[<>"/\\|?*]', '_', str(name))
        name = name.strip()
        
        if len(name) > 50:
            name = name[:50]
        
        return name if name else "Unknown"
    
    def organize_dicom_files(self, extract_dir, organized_dir=None, output_format='nifti'):
        """按Series整理DICOM文件并转换为指定格式 (nifti 或 npz)"""
        return organize_dicom_files_impl(self, extract_dir, organized_dir, output_format)

    def _process_single_series(self, series_path, series_folder, organized_dir, output_format='nifti'):
        """处理单个Series目录：统计、转换并移动到 organized_dir。"""
        return process_single_series_impl(self, series_path, series_folder, organized_dir, output_format)
    
    def convert_dicom_to_nifti(self, series_dir, series_name):
        """将DICOM序列转换为NIfTI格式"""
        return convert_dicom_to_nifti_impl(self, series_dir, series_name)
    
    def _convert_to_npz(self, series_dir, series_name):
        """将DICOM序列转换为NPZ格式，并按照要求规范化方向"""
        return convert_to_npz_impl(self, series_dir, series_name)

    def _normalize_and_save_npz(self, nii_path, npz_path):
        """加载NIfTI，利用DICOM方向信息规范化并保存为NPZ"""
        return normalize_and_save_npz_impl(nii_path, npz_path)

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
                records = [
                    {
                        'SeriesFolder': series_name,
                        'TotalFilesInSeries': len(dicom_files),
                        'FilesReadForMetadata': 0,
                        'Modality': modality
                    }
                ]

            sample_tags = None
            try:
                sample_dcm = pydicom.dcmread(dicom_files[0], force=True)
                sample_tags = self._build_sample_tags(sample_dcm)
                if not sample_tags.get('Modality'):
                    sample_tags['Modality'] = modality
            except Exception:
                sample_tags = None

            cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
            payload = {
                "modality": modality,
                "records": records,
                "sample_tags": sample_tags
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
                        # 确保记录每张 2D 图像的 Rows/Columns 信息，供后续预览方向校正使用
                        try:
                            metadata['Rows'] = str(getattr(dcm, 'Rows', '') or '')
                            metadata['Columns'] = str(getattr(dcm, 'Columns', '') or '')
                        except Exception:
                            metadata['Rows'] = metadata.get('Rows', '')
                            metadata['Columns'] = metadata.get('Columns', '')
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
                try:
                    metadata['Rows'] = str(getattr(dcm, 'Rows', '') or '')
                    metadata['Columns'] = str(getattr(dcm, 'Columns', '') or '')
                except Exception:
                    metadata['Rows'] = metadata.get('Rows', '')
                    metadata['Columns'] = metadata.get('Columns', '')
                records.append(metadata)
        except Exception:
            return []
        return records

    def _build_metadata_record_from_sample(self, series_folder, sample_dcm, total_files, modality):
        """用样本DICOM构建单条元数据记录。"""
        metadata = {
            'SeriesFolder': series_folder,
            'SampleFileName': getattr(sample_dcm, 'filename', '') or '',
            'TotalFilesInSeries': total_files,
            'FilesReadForMetadata': 1,
            'Modality': modality
        }
        current_keywords = self.get_keywords(modality)
        for keyword in current_keywords:
            try:
                value = getattr(sample_dcm, keyword, None)
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
        return metadata

    def _get_series_sample_dicom(self, series_dir):
        """读取序列中的样本DICOM用于标签信息"""
        try:
            sample_tags = self._load_sample_tags_from_cache(series_dir)
            if isinstance(sample_tags, dict):
                modality = str(sample_tags.get('Modality') or '')
                sample_dcm = SimpleNamespace(**sample_tags)
                return sample_dcm, modality

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
        return get_window_params_impl(dcm)

    def _apply_windowing(self, image_2d, dcm):
        """应用窗宽窗位并归一化到0-255"""
        return apply_windowing_impl(image_2d, dcm)

    def _resize_with_aspect(self, img, aspect_ratio):
        """根据像素间距调整纵横比"""
        return resize_with_aspect_impl(img, aspect_ratio)

    def _normalize_2d_preview(self, img, target_size=896):
        """2D图像标准化到固定大小的方形画布"""
        return normalize_2d_preview_impl(img, target_size=target_size)

    def _generate_series_preview(self, series_dir, series_name, conversion_result, sample_dcm, modality):
        """为序列生成PNG预览图"""
        return generate_series_preview_impl(
            series_dir,
            series_name,
            conversion_result,
            sample_dcm,
            modality,
            self._sanitize_folder_name
        )
    
    def _convert_with_dcm2niix(self, series_dir, series_name):
        """使用dcm2niix工具转换"""
        return convert_with_dcm2niix_impl(self, series_dir, series_name)

    def _apply_rescale(self, pixel_data, dcm):
        """应用Rescale Slope/Intercept"""
        return apply_rescale_impl(pixel_data, dcm)

    def _apply_photometric(self, pixel_data, dcm):
        """处理Photometric Interpretation (MONOCHROME1/2)"""
        return apply_photometric_impl(pixel_data, dcm)

    def _build_affine_from_dicom(self, dcm, slice_spacing=1.0, slice_cosines=None):
        """基于DICOM方向信息构建NIfTI仿射矩阵 (RAS)"""
        return build_affine_from_dicom_impl(dcm, slice_spacing=slice_spacing, slice_cosines=slice_cosines)

    def _assess_image_quality(self, dcm):
        """基于直方图/对比度的简单质检，返回0/1"""
        return assess_image_quality_impl(dcm)

    def _assess_image_quality_from_array(self, pixel_data):
        """基于直方图/对比度的简单质检，返回0/1（输入为数组）"""
        return assess_image_quality_from_array_impl(pixel_data)

    def _assess_converted_file_quality(self, filepath):
        """基于转换后的NPZ/NIfTI文件做质检，返回0/1"""
        return assess_converted_file_quality_impl(filepath)

    def _assess_series_quality_converted(self, converted_files):
        """对转换后的序列做QC，<=200全量，>200中间±3抽样"""
        return assess_series_quality_converted_impl(converted_files)

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
        return assess_series_quality_impl(dicom_files, pydicom.dcmread)


    def _convert_with_python_libs(self, series_dir, series_name):
        """使用Python库转换DICOM到NIfTI"""
        return convert_with_python_libs_impl(self, series_dir, series_name)
    
    def extract_dicom_metadata(self, organized_dir, output_excel=None):
        return extract_dicom_metadata_impl(
            organized_dir=organized_dir,
            output_excel=output_excel,
            get_keywords=self.get_keywords,
            get_converted_files=self._get_converted_files,
            assess_converted_file_quality=self._assess_converted_file_quality,
            assess_series_quality_converted=self._assess_series_quality_converted,
            append_mr_cleaned_sheet=self._append_mr_cleaned_sheet
        )

    def _append_mr_cleaned_sheet(self, df: pd.DataFrame, output_excel: str) -> None:
        """对 MR 记录做治理/规范化，并写回到同一个 Excel 的 MR_Cleaned sheet。"""
        try:
            if df is None or df.empty or 'Modality' not in df.columns:
                return

            mr_df = df[df['Modality'].astype(str).str.upper() == 'MR'].copy()
            if mr_df.empty:
                return

            print(f"\n🔬 MR_clean: processing {len(mr_df)} MR records...")

            from src.core.mr_clean import process_mri_dataframe

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

    def process_upload_workflow(self, zip_path, base_output_dir, options=None):
        """上传ZIP流程：extract -> organize -> convert -> metadata。"""
        options = options or {}

        os.makedirs(base_output_dir, exist_ok=True)

        extract_dir = self.extract_zip(zip_path, os.path.join(base_output_dir, 'extracted'))
        if not extract_dir:
            return {
                'success': False,
                'error': 'Failed to extract zip',
                'extract_dir': None
            }

        organized_dir = extract_dir
        series_info = {}
        excel_file = None

        if options.get('auto_organize', True):
            organized_dir, series_info = self.organize_dicom_files(
                extract_dir,
                output_format=options.get('output_format', 'nifti')
            )

        if options.get('auto_metadata', True):
            excel_file = self.extract_dicom_metadata(organized_dir)

        return {
            'success': True,
            'extract_dir': extract_dir,
            'organized_dir': organized_dir,
            'excel_file': excel_file,
            'series_info': series_info,
            'series_count': len(series_info)
        }
    
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
        # allow configuring pending-series limit to apply backpressure when conversion is slow
        try:
            max_pending = int(os.getenv('MAX_PENDING_SERIES', '4'))
            if max_pending <= 0:
                max_pending = 4
        except Exception:
            max_pending = 4
        series_queue = Queue(maxsize=max_pending)
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

        # organize worker: multiple workers supported to convert concurrently
        try:
            num_converters = int(os.getenv('NUM_CONVERTERS', '2'))
            if num_converters <= 0:
                num_converters = 2
        except Exception:
            num_converters = 2

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
            # spawn multiple organizers
            organizer_threads = []
            for _ in range(num_converters):
                t = threading.Thread(target=_organize_worker, args=(organized_dir, output_format), daemon=True)
                t.start()
                organizer_threads.append(t)

            download_thread.start()

            # 等待下载完成
            download_thread.join()
            # 通知整理线程退出（放入与 worker 数相同的哨兵）
            for _ in range(len(organizer_threads)):
                series_queue.put(None)
            series_queue.join()
            for t in organizer_threads:
                t.join()

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
