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
import shutil
import threading
import zipfile
from queue import Queue, Empty
import pandas as pd
import pydicom
import re
import sys
import logging
import hashlib
import socket
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Callable, Any, Tuple
from types import SimpleNamespace
from src.core.constants import get_derived_keywords
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


# ==================== P0/P1: 健壮性辅助类和函数 ====================

@dataclass
class DownloadStats:
    """下载统计信息"""
    total_series: int = 0
    completed_series: int = 0
    failed_series: int = 0
    total_bytes: int = 0
    start_time: float = field(default_factory=time.time)
    errors: List[Dict] = field(default_factory=list)

    def get_summary(self) -> Dict:
        elapsed = time.time() - self.start_time
        return {
            'total_series': self.total_series,
            'completed_series': self.completed_series,
            'failed_series': self.failed_series,
            'total_mb': self.total_bytes / (1024 * 1024),
            'elapsed_sec': elapsed,
            'speed_mbps': (self.total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
        }


class AssociationManager:
    """P0: 关联管理器，支持重试和上下文管理"""

    def __init__(self, ae: AE, pacs_config: Dict):
        self.ae = ae
        self.pacs_config = pacs_config
        self.assoc = None
        self.retry_count = 0

    def connect(self, max_retries: int = 3, base_delay: float = 2.0) -> bool:
        """P0: 带指数退避的连接重试"""
        for attempt in range(max_retries):
            try:
                self.assoc = self.ae.associate(
                    self.pacs_config['PACS_IP'],
                    self.pacs_config['PACS_PORT'],
                    ae_title=self.pacs_config['CALLED_AET']
                )
                if self.assoc.is_established:
                    self.retry_count = attempt + 1
                    if attempt > 0:
                        logger.info(f"PACS connection succeeded after {attempt + 1} attempts")
                    return True
                else:
                    logger.warning(f"Association attempt {attempt + 1}/{max_retries} failed: not established")
            except Exception as e:
                logger.error(f"Association attempt {attempt + 1}/{max_retries} error: {e}")

            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"Waiting {delay:.1f}s before retry...")
                time.sleep(delay)

        return False

    def __enter__(self):
        if not self.connect():
            raise ConnectionError("Failed to establish PACS association after retries")
        return self.assoc

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.assoc and self.assoc.is_established:
            try:
                self.assoc.release()
            except Exception as e:
                logger.warning(f"Error releasing association: {e}")

    def reconnect(self, max_retries: int = 3, base_delay: float = 2.0):
        """P0: 在关联断开时重新建立连接（用于 C-MOVE 过程中关联意外断开）"""
        if self.assoc and self.assoc.is_established:
            try:
                self.assoc.release()
            except Exception:
                pass
        self.assoc = None
        logger.info("🔄 Rebuilding PACS association for C-MOVE retry...")
        return self.connect(max_retries=max_retries, base_delay=base_delay)


def _safe_thread_join(thread, timeout=None):
    """带超时的线程 join，兼容 eventlet。

    eventlet monkey_patch 后，threading.Thread.join(timeout) 在超时时会抛出
    eventlet.timeout.Timeout（BaseException 子类），与标准库"超时静默返回"
    的语义不同。该异常会穿透 except Exception，导致调用线程静默死亡
    （任务状态永远停留在 running）。这里统一捕获，保持标准库语义：
    超时仅表示线程仍在运行，调用方需自行用 is_alive() 判断。
    """
    try:
        thread.join(timeout=timeout)
    except BaseException:
        pass


class QueueWatchdog:
    """P2: 队列看门狗，防止死锁"""

    def __init__(self, queue: Queue, timeout: float = 300.0, max_alerts: int = 3):
        self.queue = queue
        self.timeout = timeout
        self.last_activity = time.time()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._alert_count = 0
        self._max_alerts = max_alerts

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        _safe_thread_join(self._thread, timeout=1.0)

    def update_activity(self):
        self.last_activity = time.time()

    def _monitor(self):
        while not self._stop_event.is_set():
            time.sleep(5.0)
            if time.time() - self.last_activity > self.timeout:
                # 队列为空是正常空闲（workflow 结束或尚未开始），不是死锁
                if self.queue.empty():
                    self.last_activity = time.time()
                    continue
                self._alert_count += 1
                if self._alert_count > self._max_alerts:
                    logger.warning(f"Queue watchdog: Max alerts ({self._max_alerts}) reached, stopping monitor")
                    break
                logger.error(
                    f"Queue watchdog: No activity for {self.timeout}s, "
                    f"potential deadlock detected (alert {self._alert_count}/{self._max_alerts})"
                )
                # 放入哨兵值来唤醒可能阻塞的 worker
                try:
                    self.queue.put(None, timeout=1.0)
                except:
                    pass
                # 重置活动时间，避免同一死锁状态被无限重复报告
                self.last_activity = time.time()


def compute_file_checksum(filepath: str, algorithm: str = 'md5') -> Optional[str]:
    """P3: 计算文件校验和"""
    try:
        hasher = hashlib.new(algorithm)
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        logger.warning(f"Failed to compute checksum for {filepath}: {e}")
        return None


class FailedSeriesTracker:
    """P0: 失败序列追踪器，支持重试"""

    def __init__(self):
        self.failed: Dict[str, Dict] = {}
        self._lock = threading.Lock()

    def add(self, series_uid: str, series_info: Dict, error: Exception):
        with self._lock:
            if series_uid not in self.failed:
                self.failed[series_uid] = {
                    'info': series_info,
                    'error': str(error),
                    'timestamp': time.time(),
                    'retry_count': 0
                }
            else:
                self.failed[series_uid]['retry_count'] += 1
                self.failed[series_uid]['error'] = str(error)
                self.failed[series_uid]['timestamp'] = time.time()

    def should_retry(self, series_uid: str, max_retries: int = 2) -> bool:
        with self._lock:
            if series_uid not in self.failed:
                return False
            return self.failed[series_uid]['retry_count'] < max_retries

    def get_retryable_series(self, max_retries: int = 2) -> List[Tuple[str, Dict]]:
        with self._lock:
            return [(uid, info['info']) for uid, info in self.failed.items()
                    if info['retry_count'] < max_retries]

    def get_summary(self) -> Dict:
        with self._lock:
            return {
                'total_failed': len(self.failed),
                'retryable': sum(1 for info in self.failed.values() if info['retry_count'] < 2),
                'permanent_failures': [uid for uid, info in self.failed.items() if info['retry_count'] >= 2]
            }


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
        # 磁盘剩余空间硬下限（GB）：C-STORE 写入时剩余低于该值直接拒收，
        # 避免磁盘写满后产生大量 ENOSPC 错误并拖垮 PACS 传输
        try:
            self._download_critical_free_gb = float(os.getenv('DOWNLOAD_CRITICAL_FREE_GB', '1'))
        except Exception:
            self._download_critical_free_gb = 1.0

        # P0: 下载统计信息
        self.download_stats = DownloadStats()
        # P0: 失败序列追踪器
        self.failed_series_tracker = FailedSeriesTracker()
        # P1: AE使用锁（防止多线程并发使用同一个AE）
        self._ae_lock = threading.Lock()
        # P3: 下载文件校验和缓存 {filepath: checksum}
        self._checksum_cache: Dict[str, str] = {}
        self._checksum_lock = threading.Lock()

    # 类级别的C-MOVE锁：防止多个实例同时启动C-STORE SCP导致端口冲突
    # C-MOVE协议要求客户端启动SCP服务器接收图像，固定端口无法支持并发
    _cmove_lock = threading.Lock()
    _cmove_waiter_count = 0
    _cmove_waiter_lock = threading.Lock()
    _cmove_base_timeouts = {
        'localizer': 60,      # 定位像/Scout 小文件快速超时
        'standard': 120,      # 常规序列（10-200 文件）
        'large': 240,         # 大序列（>200 文件或 3D）
        'maximum': 300,       # 网络层绝对上限
    }
    
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

    def _wait_for_disk_low(self, directory, sleep_sec=15):
        """当磁盘剩余空间低于低水位时阻塞，直到恢复到高水位以上。

        该方法在下载循环中被调用以实现简单的回压，避免无限制拉取导致磁盘耗尽。

        水位线含义为"要求保留的最小磁盘剩余空间（GB）"：
        - 剩余 < DOWNLOAD_LOW_WATERMARK_GB 时暂停下载；
        - 剩余 >= DOWNLOAD_HIGH_WATERMARK_GB 时恢复下载。

        使用 shutil.disk_usage 检测整个文件系统的真实剩余空间，
        而不是某个子目录大小（子目录大小永远达不到水位线，旧实现从未生效）。
        """
        try:
            high = float(os.getenv('DOWNLOAD_HIGH_WATERMARK_GB', str(self._download_high_watermark_gb)))
            low = float(os.getenv('DOWNLOAD_LOW_WATERMARK_GB', str(self._download_low_watermark_gb)))
        except Exception:
            high = self._download_high_watermark_gb
            low = self._download_low_watermark_gb

        def _free_gb():
            return shutil.disk_usage(directory).free / (1024 ** 3)

        # 快速判断：剩余空间充足则立即返回
        try:
            free = _free_gb()
        except Exception:
            return

        warned = False
        while free < low:
            if not warned:
                try:
                    logger.warning(
                        f"Disk free space low ({free:.2f}GB < {low}GB). "
                        f"Pausing downloads until free >= {high}GB..."
                    )
                except Exception:
                    pass
                warned = True
            time.sleep(sleep_sec)
            try:
                free = _free_gb()
            except Exception:
                break
            if free >= high:
                try:
                    logger.info(f"Disk free space recovered to {free:.2f}GB >= {high}GB, resuming downloads")
                except Exception:
                    pass
                break
    
    def logout(self):
        """保持接口兼容性的虚拟登出"""
        print(f"✅ Logout successful: {self.username}")
        return True
    
    def check_status(self, retries=1, base_delay=2.0):
        """检查PACS连接状态，支持重试

        Args:
            retries: 重试次数（默认1，即不额外重试）
            base_delay: 基础退避延迟（秒），每次重试翻倍
        """
        last_error = None
        for attempt in range(retries):
            try:
                assoc = self.ae.associate(
                    self.pacs_config['PACS_IP'],
                    self.pacs_config['PACS_PORT'],
                    ae_title=self.pacs_config['CALLED_AET']
                )

                if assoc.is_established:
                    assoc.release()
                    if attempt > 0:
                        logger.info(f"PACS connection OK after {attempt + 1} attempts")
                    else:
                        logger.debug("PACS connection status: OK")
                    return True
                else:
                    logger.warning(f"PACS connection attempt {attempt + 1}/{retries} failed: not established")
            except Exception as e:
                last_error = e
                logger.warning(f"PACS connection attempt {attempt + 1}/{retries} error: {e}")

            if attempt < retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"Waiting {delay:.1f}s before PACS retry...")
                time.sleep(delay)

        if last_error:
            logger.error(f"PACS connection failed after {retries} attempts: {last_error}")
        else:
            logger.error(f"PACS connection failed after {retries} attempts: association not established")
        return False
    
    def _query_series_metadata(self, accession_number, modality_filter=None, min_series_files=None, exclude_derived=True):
        """查询PACS获取Series元数据

        Args:
            accession_number: 检查号
            modality_filter: 可选，模态过滤（如 'MR', 'CT'）
            min_series_files: 可选，最小序列文件数，少于该值的序列将被过滤
            exclude_derived: 是否排除衍生序列（MPR, MIP, VR等），默认True
        """
        series_metadata = []


        # P1: 使用上下文管理器确保连接释放，P0: 带重试机制
        try:
            with AssociationManager(self.ae, self.pacs_config) as assoc:
                # 查询Study
                study_ds = Dataset()
                study_ds.QueryRetrieveLevel = "STUDY"
                study_ds.AccessionNumber = accession_number
                study_ds.StudyInstanceUID = ""
                study_ds.PatientID = ""
                study_ds.PatientName = ""
                study_ds.StudyDate = ""

                logger.info(f"🔍 Query AccessionNumber: {accession_number}")
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
                    logger.warning(f"⚠️  Can't Find AccessionNumber: {accession_number}")
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
                    series_ds.ImageType = ""  # 用于区分原始/派生图像
                    series_ds.SliceThickness = ""  # 用于过滤定位像(层厚为NA的)

                    responses = assoc.send_c_find(series_ds, StudyRootQueryRetrieveInformationModelFind)

                    for (status, identifier) in responses:
                        if status and status.Status in [0xFF00, 0xFF01]:
                            if identifier and hasattr(identifier, 'SeriesInstanceUID'):
                                series_modality = str(identifier.Modality) if hasattr(identifier, 'Modality') else ''

                                # Modality 过滤
                                if modality_filter:
                                    # 支持逗号分隔的多个模态，如 "MR,CT"
                                    # 自动展开同义词组，解决 PACS 命名不规范问题
                                    from src.core.constants import expand_modality_filter
                                    allowed_modalities = expand_modality_filter(modality_filter)
                                    if series_modality.upper() not in allowed_modalities:
                                        continue

                                series_desc = str(identifier.SeriesDescription) if hasattr(identifier, 'SeriesDescription') else ''

                                # 过滤衍生序列：检查ImageType是否为DERIVED
                                is_derived = False
                                if exclude_derived:
                                    image_type = getattr(identifier, 'ImageType', None)
                                    if image_type:
                                        # ImageType 第一个值才代表像素来源：DERIVED/ORIGINAL。
                                        # 第二个值 PRIMARY/SECONDARY 是采集上下文，不能用于过滤。
                                        if isinstance(image_type, (list, tuple)):
                                            first_val = str(image_type[0]).upper().strip() if image_type else ''
                                            if first_val == 'DERIVED':
                                                is_derived = True
                                        else:
                                            if 'DERIVED' in str(image_type).upper():
                                                is_derived = True
                                        if is_derived:
                                            logger.debug(f"   Filtered by ImageType (DERIVED): {series_desc}")

                                    # 过滤衍生序列：检查SeriesDescription关键词
                                    if not is_derived and series_desc:
                                        desc_upper = series_desc.upper()
                                        # 特殊处理：纯数字3D（如 "3D"）或作为单词的一部分
                                        for keyword in get_derived_keywords():
                                            # 使用单词边界匹配，避免误判（如 "MP" 匹配 "MPR"）
                                            if keyword in desc_upper:
                                                is_derived = True
                                                logger.debug(f"   Filtered by keyword '{keyword}': {series_desc}")
                                                break

                                    if is_derived:
                                        logger.info(f"   🚫 Filtered derived series: {series_desc}")
                                        continue

                                series_info = dict(study_info)
                                series_info.update({
                                    'StudyInstanceUID': study_uid,
                                    'SeriesInstanceUID': str(identifier.SeriesInstanceUID),
                                    'SeriesNumber': str(identifier.SeriesNumber) if hasattr(identifier, 'SeriesNumber') else '0',
                                    'SeriesDescription': series_desc if series_desc else 'Unknown',
                                    'Modality': series_modality
                                })
                                series_metadata.append(series_info)

                # 如果设置了最小文件数过滤，查询每个Series的Instance数量和层厚
                # 注意：只对3D模态（CT/MR等）应用此过滤，2D模态（DX/DR等）跳过
                # 同时过滤掉层厚为NA的序列（通常是定位像/Scout/Topogram）
                if min_series_files and min_series_files > 0:
                    # 定义3D模态列表（这些模态通常有多个切片文件）
                    volume_modalities = {'CT', 'MR', 'MRI', 'PT', 'NM', 'US'}

                    filtered_metadata = []
                    for series_info in series_metadata:
                        series_uid = series_info.get('SeriesInstanceUID')
                        study_uid = series_info.get('StudyInstanceUID')
                        series_modality = series_info.get('Modality', '').upper()

                        # 只对3D模态应用文件数过滤
                        if series_modality not in volume_modalities:
                            filtered_metadata.append(series_info)
                            continue

                        # 查询该Series的Instance数量和层厚
                        instance_ds = Dataset()
                        instance_ds.QueryRetrieveLevel = "SERIES"
                        instance_ds.StudyInstanceUID = study_uid
                        instance_ds.SeriesInstanceUID = series_uid
                        instance_ds.NumberOfSeriesRelatedInstances = ""
                        instance_ds.SliceThickness = ""

                        responses = assoc.send_c_find(instance_ds, StudyRootQueryRetrieveInformationModelFind)

                        instance_count = 0
                        slice_thickness = None
                        for (status, identifier) in responses:
                            if status and status.Status in [0xFF00, 0xFF01]:
                                if identifier and hasattr(identifier, 'NumberOfSeriesRelatedInstances'):
                                    try:
                                        instance_count = int(identifier.NumberOfSeriesRelatedInstances)
                                    except (ValueError, TypeError):
                                        instance_count = 0
                                # 获取层厚信息
                                if identifier and hasattr(identifier, 'SliceThickness'):
                                    try:
                                        st = identifier.SliceThickness
                                        if st is not None and str(st).strip():
                                            slice_thickness = float(st)
                                    except (ValueError, TypeError):
                                        slice_thickness = None
                                break

                        # 检查层厚是否有效（过滤定位像）
                        # 注意：只有当SliceThickness明确存在且无效(<=0)时才过滤
                        # 如果PACS没有返回该字段，则不过滤（保留到整理阶段通过实际文件数验证）
                        if slice_thickness is not None and slice_thickness <= 0:
                            logger.info(f"   🚫 Filtered out Series {series_info.get('SeriesNumber')} ({series_info.get('SeriesDescription')}): "
                                  f"SliceThickness={slice_thickness} (likely a scout/localizer)")
                            continue

                        # PACS 返回的实例数可能不可靠（某些 PACS 返回 0 或不准确）
                        # 策略：如果 PACS 返回的实例数 >= min_files，直接保留
                        # 如果 < min_files 或为 0，仍然保留，让整理阶段根据实际文件数判断
                        series_info['NumberOfSeriesRelatedInstances'] = instance_count
                        series_info['SliceThickness'] = slice_thickness

                        if instance_count >= min_series_files:
                            filtered_metadata.append(series_info)
                        elif instance_count == 0:
                            # PACS 返回 0，可能是数据未准备好，保留到整理阶段验证
                            logger.debug(f"   ⚠️  Series {series_info.get('SeriesNumber')} ({series_info.get('SeriesDescription')}): "
                                  f"PACS reports 0 files, will verify during organization")
                            filtered_metadata.append(series_info)
                        else:
                            # PACS 返回少量文件，但仍保留，整理阶段会再次验证实际文件数
                            logger.debug(f"   ⚠️  Series {series_info.get('SeriesNumber')} ({series_info.get('SeriesDescription')}): "
                                  f"PACS reports {instance_count} files < {min_series_files}, will verify during organization")
                            filtered_metadata.append(series_info)

                    series_metadata = filtered_metadata

                logger.info(f"📊 Find {len(series_metadata)} Series (after filtering)")
                if modality_filter:
                    from src.core.constants import expand_modality_filter
                    expanded = expand_modality_filter(modality_filter)
                    logger.debug(f"   (Modality filter: {modality_filter} → expanded: {expanded})")
                if min_series_files and min_series_files > 0:
                    logger.debug(f"   (Min files filter: {min_series_files})")
                if exclude_derived:
                    logger.debug(f"   (Derived series filtered by keywords: {get_derived_keywords()})")

        except ConnectionError as e:
            logger.error(f"❌ Failed to establish PACS connection: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Query metadata failed: {e}", exc_info=True)

        return series_metadata
    
    def download_study(self, accession_number, output_dir=".", custom_folder_name=None,
                       on_series_downloaded=None, modality_filter=None, min_series_files=None,
                       exclude_derived=True):
        """Download Study data (directly from PACS, no ZIP generation)

        Args:
            accession_number: 检查号
            output_dir: 输出目录
            custom_folder_name: 自定义文件夹名
            on_series_downloaded: 下载完成回调
            modality_filter: 可选，模态过滤（如 'MR', 'CT'，支持逗号分隔多个）
            min_series_files: 可选，最小序列文件数
            exclude_derived: 是否排除衍生序列，默认True
        """
        logger.info(f"🔍 Downloading AccessionNumber: {accession_number}")

        # 查询Series信息（应用过滤条件）
        series_metadata = self._query_series_metadata(
            accession_number,
            modality_filter=modality_filter,
            min_series_files=min_series_files,
            exclude_derived=exclude_derived
        )
        if not series_metadata:
            logger.error(f"❌ No data found for: {accession_number}")
            return None

        # 重置下载统计
        self.download_stats = DownloadStats(total_series=len(series_metadata))
        self.failed_series_tracker = FailedSeriesTracker()

        # 创建输出目录
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        if custom_folder_name:
            output_path = os.path.join(output_dir, custom_folder_name)
        else:
            output_path = os.path.join(output_dir, f"{accession_number}_{timestamp}")

        os.makedirs(output_path, exist_ok=True)

        # P3: 存储状态，包括校验和信息
        storage_state = {
            'current_path': '',
            'current_series_uid': '',  # 当前处理的SeriesInstanceUID
            'files_received': 0,
            'current_series_files': set(),  # 当前序列接收的文件
            'failed_files': [],  # 失败的文件记录
            'series_uid_to_dir': {},  # SeriesInstanceUID到目录的映射，避免竞态条件
            'unmapped_uids': set(),  # 已告警过的未注册SeriesInstanceUID，避免刷屏
            'disk_full': False  # 磁盘剩余空间不足标志，触发后快速失败
        }

        def handle_store(event):
            """P3: 处理C-STORE请求，包含数据完整性校验"""
            try:
                dataset = event.dataset
                dataset.file_meta = event.file_meta

                # 验证SOP Instance UID
                sop_instance_uid = dataset.SOPInstanceUID
                if not sop_instance_uid:
                    logger.error("❌ Received dataset without SOPInstanceUID")
                    return 0xA701

                # P0: 使用 SeriesInstanceUID 查找对应的目录，避免竞态条件
                # 不能使用 storage_state['current_path']，因为它可能被下一个Series的循环修改
                series_instance_uid = None
                try:
                    raw_series_uid = dataset.SeriesInstanceUID
                    # 归一化为纯字符串，避免 pydicom UID 类型/空白差异导致映射查找失败
                    series_instance_uid = str(raw_series_uid).strip() if raw_series_uid else None
                except AttributeError:
                    # SeriesInstanceUID 属性不存在，尝试从文件元数据获取
                    logger.warning(f"⚠️  Dataset has no SeriesInstanceUID attribute, SOPInstanceUID={sop_instance_uid[:20]}...")
                    # 使用当前路径作为fallback
                    series_dir = storage_state['current_path']
                except Exception as e:
                    logger.error(f"❌ Failed to get SeriesInstanceUID from dataset: {e}, SOPInstanceUID={sop_instance_uid[:20]}...")
                    return 0xA701

                if series_instance_uid:
                    series_dir = storage_state['series_uid_to_dir'].get(series_instance_uid)
                    if not series_dir:
                        # PACS 推送了未在 C-FIND 序列列表中注册的 Series：
                        # 单独存放到 unmapped_series_* 目录，避免混入其他序列造成数据污染。
                        # 不再使用 current_path 作为 fallback（会把不同序列混在一起）
                        series_dir = os.path.join(
                            output_path, f"unmapped_series_{series_instance_uid[-24:]}"
                        )
                        if series_instance_uid not in storage_state['unmapped_uids']:
                            storage_state['unmapped_uids'].add(series_instance_uid)
                            available_uids = list(storage_state['series_uid_to_dir'].keys())
                            logger.warning(
                                f"⚠️  No directory mapping for Series UID {series_instance_uid}, "
                                f"available mappings: {len(available_uids)} {available_uids}, "
                                f"storing separately in: {series_dir}"
                            )
                else:
                    # 没有 SeriesInstanceUID，使用当前路径
                    series_dir = storage_state['current_path']

                filename = f"{sop_instance_uid}.dcm"
                filepath = os.path.join(series_dir, filename)

                # 确保目录存在
                os.makedirs(series_dir, exist_ok=True)

                # 磁盘剩余空间检查：低于硬下限时拒收，避免写满磁盘产生大量 ENOSPC 错误
                if storage_state['files_received'] % 10 == 0 or storage_state['disk_full']:
                    try:
                        free_gb = shutil.disk_usage(series_dir).free / (1024 ** 3)
                        if free_gb < self._download_critical_free_gb:
                            storage_state['disk_full'] = True
                            logger.error(
                                f"❌ Disk nearly full (free {free_gb:.2f}GB < "
                                f"{self._download_critical_free_gb}GB), refusing to store {filename}"
                            )
                            return 0xA702
                    except Exception:
                        pass
                if storage_state['disk_full']:
                    return 0xA702

                # 保存文件
                try:
                    dataset.save_as(filepath, write_like_original=False)
                except Exception as e:
                    logger.error(f"❌ Failed to save dataset to {filepath}: {e}")
                    return 0xA700

                # P3: 验证文件完整性（文件可读且大小合理）
                if os.path.exists(filepath):
                    file_size = os.path.getsize(filepath)
                    if file_size < 128:  # DICOM文件最小头大小
                        logger.warning(f"⚠️  File {filename} is too small ({file_size} bytes), may be corrupted")
                        storage_state['failed_files'].append({'uid': sop_instance_uid, 'reason': 'too_small'})
                        return 0xA702

                    # P3: 计算并缓存校验和（可选，仅对关键文件）
                    # 通过环境变量 ENABLE_CHECKSUM=1 启用，默认关闭以减少 I/O 开销。
                    if os.environ.get('ENABLE_CHECKSUM', '0') == '1' and len(storage_state['current_series_files']) < 100:
                        checksum = compute_file_checksum(filepath)
                        if checksum:
                            with self._checksum_lock:
                                self._checksum_cache[filepath] = checksum

                    storage_state['current_series_files'].add(filepath)
                    storage_state['files_received'] += 1
                    self.download_stats.total_bytes += file_size

                    # 记录前5个文件和每10个文件
                    if storage_state['files_received'] <= 5 or storage_state['files_received'] % 10 == 0:
                        logger.info(f"   Received {storage_state['files_received']} files... (last: {filename[:40]}... in {os.path.basename(series_dir)})")
                else:
                    logger.error(f"❌ File {filepath} not found after save")
                    return 0xA700

                return 0x0000
            except Exception as e:
                logger.error(f"❌ Failed saving DICOM file: {e}", exc_info=True)
                return 0xA700

        # P0: 使用类级别的锁确保C-MOVE操作串行化
        # C-MOVE协议需要启动C-STORE SCP服务器接收图像，固定端口无法支持并发
        # 如果两个任务同时使用同一端口，会导致图像混杂到错误的目录
        # P2: 增加排队追踪，让日志更透明
        wait_start = time.time()
        with DICOMDownloadClient._cmove_waiter_lock:
            DICOMDownloadClient._cmove_waiter_count += 1
            waiters_ahead = DICOMDownloadClient._cmove_waiter_count - 1
        if waiters_ahead > 0:
            logger.info(f"⏳ {accession_number} waiting for C-MOVE lock ({waiters_ahead} task(s) ahead)...")
        else:
            logger.info(f"🔒 Acquiring C-MOVE lock for {accession_number}...")

        DICOMDownloadClient._cmove_lock.acquire()
        wait_sec = time.time() - wait_start
        with DICOMDownloadClient._cmove_waiter_lock:
            DICOMDownloadClient._cmove_waiter_count -= 1

        if wait_sec > 3:
            logger.info(f"🔓 C-MOVE lock acquired for {accession_number} after waiting {wait_sec:.1f}s, starting download...")
        else:
            logger.info(f"🔓 C-MOVE lock acquired for {accession_number}, starting download...")

        # 启动C-STORE SCP
        ae_scp = AE(ae_title=self.pacs_config['CALLING_AET'])
        ae_scp.supported_contexts = AllStoragePresentationContexts
        ae_scp.add_requested_context(StudyRootQueryRetrieveInformationModelMove)

        server = ae_scp.start_server(
            ('', self.pacs_config['CALLING_PORT']),
            block=False,
            evt_handlers=[(evt.EVT_C_STORE, handle_store)]
        )

        # P0: 跟踪失败的序列以便重试
        failed_series = []

        try:
            # P1: 使用上下文管理器管理关联
            with AssociationManager(self.ae, self.pacs_config) as assoc:
                # 下载每个Series（P0: 失败隔离）
                for i, series in enumerate(series_metadata):
                    series_num = series.get('SeriesNumber', f'Series{i+1}')
                    series_desc = series.get('SeriesDescription', 'Unknown')
                    series_uid = series.get('SeriesInstanceUID')
                    series_dir = os.path.join(output_path, f"{series_num:0>3}_{self._sanitize_folder_name(series_desc)}")

                    # P0: 注册SeriesInstanceUID到目录的映射，用于C-STORE回调查找
                    # 这避免了竞态条件：C-STORE可能在下一个Series的循环开始后才到达
                    if series_uid:
                        # 归一化为纯字符串，与 handle_store 中的查找保持一致
                        series_uid = str(series_uid).strip()
                        storage_state['series_uid_to_dir'][series_uid] = series_dir
                        logger.debug(f"   Registered series_uid mapping: {series_uid[:20]}... -> {series_dir}")
                    else:
                        logger.warning(f"   Series {series_num} has no SeriesInstanceUID, cannot register mapping")
                    storage_state['current_path'] = series_dir
                    storage_state['current_series_uid'] = series_uid
                    storage_state['current_series_files'] = set()  # 重置当前序列文件集合

                    try:
                        logger.info(f"📥 Downloading series {i+1}/{len(series_metadata)}: {series_num} - {series_desc}")

                        # 当磁盘剩余空间达到低水位时，暂停下载以等待转换/清理
                        try:
                            self._wait_for_disk_low(output_path)
                        except Exception:
                            pass

                        # 磁盘已满（C-STORE 拒收过文件），中止后续序列下载
                        if storage_state['disk_full']:
                            logger.error(
                                f"❌ Disk nearly full, aborting remaining "
                                f"{len(series_metadata) - i} series download"
                            )
                            break

                        # P2: 动态 C-MOVE 超时：根据 series 大小和类型调整
                        cmove_timeout = self._get_series_cmove_timeout(series)
                        # 同时调整网络层和应用层超时
                        assoc.dimse_timeout = cmove_timeout
                        assoc.network_timeout = min(cmove_timeout + 30, self._cmove_base_timeouts['maximum'])
                        logger.info(f"   ⏱️  C-MOVE timeout set to {cmove_timeout}s for Series {series_num}")

                        # P2: 关联健康检查，如果断开则直接失败，由外层重试阶段重建
                        if not assoc.is_established:
                            logger.warning(f"   ⚠️  Association lost before Series {series_num}, will retry with fresh connection")
                            raise ConnectionError(f"Association lost for series {series_num}, will retry")

                        # 发送C-MOVE请求
                        move_ds = Dataset()
                        move_ds.QueryRetrieveLevel = 'SERIES'
                        move_ds.StudyInstanceUID = series['StudyInstanceUID']
                        move_ds.SeriesInstanceUID = series_uid

                        logger.info(f"   Sending C-MOVE request for Series {series_num}...")

                        # 报告下载进度
                        if callable(self.download_progress_callback):
                            try:
                                progress_pct = 40 + int((i / len(series_metadata)) * 40)
                                self.download_progress_callback(i + 1, len(series_metadata), series_desc, progress_pct)
                            except Exception as cb_e:
                                logger.warning(f"   Progress callback error: {cb_e}")

                        # P2: 记录该 series 下载前的文件计数，用于后续判断是否有实际进展
                        files_before = storage_state['files_received']

                        responses = assoc.send_c_move(
                            move_ds,
                            self.pacs_config['CALLING_AET'],
                            query_model=StudyRootQueryRetrieveInformationModelMove
                        )

                        # 跟踪C-MOVE响应状态
                        move_status = None
                        error_messages = []
                        for (status, identifier) in responses:
                            if status:
                                move_status = status.Status
                                if status.Status == 0x0000:
                                    logger.info(f"   Series {series_num} C-MOVE completed successfully")
                                    self.download_stats.completed_series += 1
                                elif status.Status != 0xFF00:  # 0xFF00 是Pending状态
                                    error_msg = f"0x{status.Status:04X}"
                                    error_messages.append(error_msg)
                                    logger.warning(f"   Series {series_num} C-MOVE status: {error_msg}")

                        if move_status is None:
                            logger.warning(f"   ⚠️  Series {series_num}: No C-MOVE response received (timeout or network issue)")
                            raise TimeoutError(f"No C-MOVE response for series {series_num}")

                        # 检查是否有错误状态
                        if error_messages and move_status != 0x0000:
                            raise RuntimeError(f"C-MOVE failed with status: {error_messages[-1]}")

                        # P2: 检查是否有实际文件接收（防止 PACS 返回 0x0000 但无文件）
                        files_after = storage_state['files_received']
                        if files_after == files_before:
                            logger.warning(f"   ⚠️  Series {series_num}: C-MOVE completed but 0 files received")
                            # 不立即报错，某些 PACS 可能返回空序列，让后续流程处理

                        time.sleep(0.1)  # 短暂延迟，让文件写入完成（C-STORE 回调已同步写入）

                        # 通知外部：该Series下载完成
                        if callable(on_series_downloaded):
                            try:
                                on_series_downloaded(series_dir, series)
                            except Exception as e:
                                logger.warning(f"⚠️  Series callback failed: {e}")

                    except Exception as e:
                        # P0: 失败隔离 - 记录错误但继续处理下一个序列
                        logger.error(f"❌ Series {series_num} download failed: {e}")
                        self.failed_series_tracker.add(series_uid, series, e)
                        failed_series.append(series)
                        self.download_stats.failed_series += 1
                        self.download_stats.errors.append({
                            'series': series_num,
                            'error': str(e),
                            'timestamp': time.time()
                        })
                        # P2: 如果是连接错误，后续 series 也无法下载，跳出循环等待重试阶段统一处理
                        if isinstance(e, (ConnectionError, OSError)):
                            logger.warning(f"   ⚠️  Connection lost during Series {series_num}, breaking loop to retry with fresh association")
                            break
                        # 继续处理下一个序列，不中断整个下载流程
                        continue

            # P0: 尝试重试失败的序列
            # P2: 增强重试——重试前清理旧文件，并使用新关联
            retryable = self.failed_series_tracker.get_retryable_series()
            if retryable and storage_state['disk_full']:
                logger.error("⛔ Skipping retry phase: disk nearly full")
                retryable = []
            if retryable:
                logger.info(f"🔄 Attempting to retry {len(retryable)} failed series...")
                # 为重试创建新的关联（避免使用可能已断开的旧关联）
                retry_assoc_mgr = AssociationManager(self.ae, self.pacs_config)
                if retry_assoc_mgr.connect(max_retries=2, base_delay=2.0):
                    retry_assoc = retry_assoc_mgr.assoc
                    try:
                        for series_uid, series_info in retryable:
                            try:
                                series_num = series_info.get('SeriesNumber', 'Unknown')
                                series_desc = series_info.get('SeriesDescription', 'Unknown')
                                series_dir = os.path.join(output_path, f"{series_num:0>3}_{self._sanitize_folder_name(series_desc)}")

                                # P2: 重试前清理旧文件
                                removed = self._cleanup_series_dir(series_dir)
                                if removed > 0:
                                    logger.info(f"   🧹 Cleaned {removed} old file(s) before retrying Series {series_num}")

                                # P0: 注册SeriesInstanceUID到目录的映射，用于C-STORE回调查找
                                series_uid = str(series_uid).strip()
                                storage_state['series_uid_to_dir'][series_uid] = series_dir
                                storage_state['current_path'] = series_dir
                                storage_state['current_series_uid'] = series_uid
                                storage_state['current_series_files'] = set()

                                logger.info(f"🔄 Retrying Series {series_num}...")

                                # P2: 动态超时
                                cmove_timeout = self._get_series_cmove_timeout(series_info)
                                retry_assoc.dimse_timeout = cmove_timeout
                                retry_assoc.network_timeout = min(cmove_timeout + 30, self._cmove_base_timeouts['maximum'])

                                # P2: 重试前检查关联健康
                                if not retry_assoc.is_established:
                                    logger.warning(f"   ⚠️  Retry association lost, attempting reconnect...")
                                    if not retry_assoc_mgr.reconnect(max_retries=2, base_delay=2.0):
                                        logger.error(f"❌ Failed to reconnect for retry of Series {series_num}")
                                        continue
                                    retry_assoc = retry_assoc_mgr.assoc

                                move_ds = Dataset()
                                move_ds.QueryRetrieveLevel = 'SERIES'
                                move_ds.StudyInstanceUID = series_info['StudyInstanceUID']
                                move_ds.SeriesInstanceUID = series_uid

                                responses = retry_assoc.send_c_move(
                                    move_ds,
                                    self.pacs_config['CALLING_AET'],
                                    query_model=StudyRootQueryRetrieveInformationModelMove
                                )

                                move_status = None
                                for (status, identifier) in responses:
                                    if status:
                                        move_status = status.Status
                                        if status.Status == 0x0000:
                                            logger.info(f"   Series {series_num} retry successful")
                                            self.download_stats.completed_series += 1
                                            self.download_stats.failed_series -= 1
                                            if callable(on_series_downloaded):
                                                on_series_downloaded(series_dir, series_info)
                                            break
                                        elif status.Status != 0xFF00:
                                            logger.warning(f"   Series {series_num} retry status: 0x{status.Status:04X}")

                                if move_status is None:
                                    logger.warning(f"   Series {series_num} retry: No C-MOVE response")
                                elif move_status != 0x0000:
                                    logger.warning(f"   Series {series_num} retry failed with status 0x{move_status:04X}")

                                time.sleep(0.5)

                            except Exception as e:
                                logger.error(f"❌ Series {series_info.get('SeriesNumber')} retry failed: {e}")
                    finally:
                        if retry_assoc and retry_assoc.is_established:
                            try:
                                retry_assoc.release()
                            except Exception as e:
                                logger.warning(f"Error releasing retry association: {e}")
                else:
                    logger.error("❌ Failed to establish PACS connection for retry phase")

        except ConnectionError as e:
            logger.error(f"❌ Failed to establish PACS connection: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Download error: {e}", exc_info=True)
            return None
        finally:
            server.shutdown()
            # P2: 确保锁被释放（防御性编程，防止异常导致死锁）
            if DICOMDownloadClient._cmove_lock.locked():
                try:
                    DICOMDownloadClient._cmove_lock.release()
                    logger.debug(f"🔓 C-MOVE lock released for {accession_number}")
                except RuntimeError:
                    pass  # 锁已被释放

        # 磁盘满导致下载中止：返回 None 终止整个工作流
        if storage_state['disk_full']:
            logger.error(
                f"❌ Download aborted: disk nearly full "
                f"(free < {self._download_critical_free_gb}GB)"
            )
            return None

        # 打印下载统计
        stats_summary = self.download_stats.get_summary()
        logger.info(f"✅ Download complete! Stats: {stats_summary}")
        logger.info(f"📁 Files saved to: {output_path}")

        # 如果有永久失败的序列，记录到文件
        failure_summary = self.failed_series_tracker.get_summary()
        if failure_summary['permanent_failures']:
            failure_file = os.path.join(output_path, '.failed_series.json')
            try:
                with open(failure_file, 'w', encoding='utf-8') as f:
                    json.dump(failure_summary, f, indent=2)
                logger.warning(f"⚠️  {len(failure_summary['permanent_failures'])} series permanently failed, see {failure_file}")
            except Exception as e:
                logger.error(f"Failed to write failure log: {e}")

        # 保存校验和缓存供后续验证
        if self._checksum_cache:
            checksum_file = os.path.join(output_path, '.checksums.json')
            try:
                with open(checksum_file, 'w', encoding='utf-8') as f:
                    json.dump(self._checksum_cache, f, indent=2)
            except Exception as e:
                logger.error(f"Failed to write checksum cache: {e}")

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
        required_tags = self._get_required_tag_names()
        logger.info(f"[TAGS] Building sample tags, required_tags_count={len(required_tags)}")
        for tag_name in required_tags:
            try:
                tags[tag_name] = self._normalize_tag_value(getattr(dcm, tag_name, None))
            except Exception as e:
                logger.warning(f"[TAGS] Failed to get tag {tag_name}: {e}")
                tags[tag_name] = None
        logger.info(f"[TAGS] Built {len(tags)} tags")
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
                sample_dcm = pydicom.dcmread(dicom_files[0], force=True, stop_before_pixels=True)
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
            logger.info(f"[MINIMAL_CACHE] Skipping minimal cache for {series_name}: cache already exists")
            return

        sample_tags = None
        try:
            if sample_dcm is not None:
                sample_tags = self._build_sample_tags(sample_dcm)
                if not sample_tags.get('Modality'):
                    sample_tags['Modality'] = modality
        except Exception as e:
            logger.warning(f"[MINIMAL_CACHE] Failed to build sample_tags for {series_name}: {e}")
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
            logger.info(f"[MINIMAL_CACHE] Written minimal cache for {series_name}: file_count={file_count}, sample_tags={'present' if sample_tags else 'None'}")
        except Exception as e:
            logger.error(f"[MINIMAL_CACHE] Failed to write minimal cache for {series_name}: {e}")
            return
    
    def _is_dicom_file(self, filepath):
        """判断是否为DICOM文件"""
        lower_path = filepath.lower()
        if (
            lower_path.endswith(".json")
            or lower_path.endswith(".csv")
            or lower_path.endswith(".txt")
            or lower_path.endswith(".nii")
            or lower_path.endswith(".nii.gz")
            or lower_path.endswith(".npz")
            or lower_path.endswith(".png")
            or lower_path.endswith(".jpg")
            or lower_path.endswith(".jpeg")
            or lower_path.endswith(".bmp")
            or lower_path.endswith(".gif")
            or lower_path.endswith(".webp")
            or lower_path.endswith(".processing_lock")
            or lower_path.endswith(".processing_lock.dcm")
        ):
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
    
    def _wait_for_files_stable(self, directory, timeout=10, interval=2.0):
        """等待目录中的文件大小稳定（不再增长）。

        P1 优化：C-STORE 回调是同步写入的，文件在回调返回时已落盘。
        将检查间隔从 0.5s/3次 放宽到 2.0s/1次，减少 os.walk 开销。
        对于大目录（数千个 DICOM），60 次 walk → 最多 5 次 walk。

        Args:
            directory: 要检查的目录
            timeout: 最大等待时间（秒）
            interval: 检查间隔（秒）
        """
        start_time = time.time()
        prev_sizes = {}
        stable_count = 0
        required_stable_checks = 1  # 只需要1次稳定即可（C-STORE 已同步写入）

        while time.time() - start_time < timeout:
            current_sizes = {}
            total_files = 0

            try:
                for root, dirs, files in os.walk(directory):
                    for file in files:
                        if file.endswith('.dcm'):
                            filepath = os.path.join(root, file)
                            try:
                                size = os.path.getsize(filepath)
                                current_sizes[filepath] = size
                                total_files += 1
                            except Exception:
                                pass
            except Exception as e:
                logger.warning(f"   ⚠️ Error checking file sizes: {e}")
                break

            if current_sizes == prev_sizes and total_files > 0:
                stable_count += 1
                if stable_count >= required_stable_checks:
                    logger.info(f"   ✅ File system stable: {total_files} files ready")
                    return
            else:
                stable_count = 0
                if total_files > 0:
                    logger.debug(f"   ⏳ File system changing: {total_files} files, waiting...")

            prev_sizes = current_sizes.copy()
            time.sleep(interval)

        logger.warning(f"   ⚠️ File system stability timeout after {timeout}s")

    def _sanitize_folder_name(self, name):
        """清理文件夹名称，移除或替换Windows和dcm2niix不兼容的字符"""
        if not name:
            return "Unknown"

        name = str(name)

        # 1. 替换Windows非法字符 (包括冒号:和方括号[])
        name = re.sub(r'[<>"/\\|?*:]', '_', name)
        name = re.sub(r'[\[\]]', '_', name)

        # 2. 替换可能导致dcm2niix问题的字符组合
        # 点+空格（如 "303. X Elbow" -> "303_X Elbow"）
        name = re.sub(r'\.\s+', '_', name)
        # 多个连续空格转为单个下划线
        name = re.sub(r'\s+', '_', name)
        # 多个连续点转为单个
        name = re.sub(r'\.+', '.', name)
        # 多个连续下划线转为单个
        name = re.sub(r'_+', '_', name)

        # 3. 移除首尾的特殊字符
        name = name.strip('. _')

        # 4. 长度限制
        if len(name) > 50:
            name = name[:50]

        # 5. 确保不以点开头或结尾（Windows问题）
        name = name.strip('.')

        return name if name else "Unknown"

    def _get_series_cmove_timeout(self, series: Dict) -> int:
        """根据 series 特征计算动态 C-MOVE 超时（秒）

        策略：
        - Localizer/Scout/Survey（<10 文件或关键词匹配）：60 秒
        - 标准序列（10-200 文件）：120 秒
        - 大序列（>200 文件或 3D/MRA/DWI 等）：240 秒
        - 绝对上限：300 秒（网络层硬限制）
        """
        desc = series.get('SeriesDescription', '').upper()
        instance_count = series.get('NumberOfSeriesRelatedInstances', 0)
        modality = series.get('Modality', '').upper()

        # 尝试解析文件数
        try:
            num_files = int(instance_count) if instance_count else 0
        except (ValueError, TypeError):
            num_files = 0

        # Localizer / Scout / Survey / Locator 快速超时
        localizer_keywords = ['LOCALIZER', 'SCOUT', 'SURVEY', 'LOC ', 'LOCATOR',
                              'TOPOGRAM', 'PLANNING', 'PLAN']
        if num_files > 0 and num_files < 10:
            return self._cmove_base_timeouts['localizer']
        if any(kw in desc for kw in localizer_keywords):
            return self._cmove_base_timeouts['localizer']

        # 大序列特征
        large_keywords = ['3D', 'MRA', 'CEMRA', 'DWI', 'DTI', 'DIFFUSION',
                          'WHOLE BODY', 'WHOLE-BODY', 'VIBE', 'LAVA', 'BRAVO',
                          'SPACE', 'CUBE', 'VOLUMETRIC', 'CINE']
        if num_files > 200 or any(kw in desc for kw in large_keywords):
            return self._cmove_base_timeouts['large']

        # 标准序列
        return self._cmove_base_timeouts['standard']

    @staticmethod
    def _cleanup_series_dir(series_dir: str) -> int:
        """清理 series 目录中的旧 DICOM 文件，返回删除文件数"""
        if not os.path.isdir(series_dir):
            return 0
        removed = 0
        try:
            for fname in os.listdir(series_dir):
                if fname.startswith('.'):
                    continue
                fpath = os.path.join(series_dir, fname)
                if os.path.isfile(fpath):
                    try:
                        os.remove(fpath)
                        removed += 1
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"⚠️  Failed to cleanup series dir {series_dir}: {e}")
        return removed
    
    def organize_dicom_files(self, extract_dir, organized_dir=None, output_format='nifti', min_series_files=None):
        """按Series整理DICOM文件并转换为指定格式 (nifti 或 npz)"""
        return organize_dicom_files_impl(self, extract_dir, organized_dir, output_format, min_series_files=min_series_files)

    def _process_single_series(self, series_path, series_folder, output_format='nifti', min_series_files=None):
        """处理单个Series目录：统计、转换（原地处理，不再移动到 organized_dir）。"""
        return process_single_series_impl(self, series_path, series_folder, output_format, min_series_files=min_series_files)
    
    def convert_dicom_to_nifti(self, series_dir, series_name, dicom_files=None, sample_dcm=None, modality=None):
        """将DICOM序列转换为NIfTI格式"""
        return convert_dicom_to_nifti_impl(self, series_dir, series_name, dicom_files=dicom_files, sample_dcm=sample_dcm, modality=modality)
    
    def _convert_to_npz(self, series_dir, series_name, dicom_files=None, sample_dcm=None, modality=None):
        """将DICOM序列转换为NPZ格式，并按照要求规范化方向"""
        return convert_to_npz_impl(self, series_dir, series_name, dicom_files=dicom_files, sample_dcm=sample_dcm, modality=modality)

    def _normalize_and_save_npz(self, nii_path, npz_path):
        """加载NIfTI，利用DICOM方向信息规范化并保存为NPZ"""
        return normalize_and_save_npz_impl(nii_path, npz_path)

    def _cache_metadata_for_series(self, series_dir, series_name, dicom_files, modality):
        """缓存DICOM元数据，避免删除后无法提取标签"""
        logger.info(f"[CACHE] _cache_metadata_for_series called: series={series_name}, modality={modality}, dicom_files_count={len(dicom_files) if dicom_files else 0}")
        try:
            if not dicom_files:
                logger.warning(f"[CACHE] Skipping cache for {series_name}: dicom_files is empty")
                return

            read_all = modality in ['DR', 'MG', 'DX', 'CR']
            logger.info(f"[CACHE] Collecting metadata for {series_name}, read_all={read_all}")
            records = self._collect_metadata_from_dicoms(
                dicom_files=dicom_files,
                series_folder=series_name,
                modality=modality,
                read_all=read_all
            )
            logger.info(f"[CACHE] _collect_metadata_from_dicoms returned {len(records)} records for {series_name}")
            if not records:
                logger.warning(f"[CACHE] records empty for {series_name}, creating fallback record")
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
                logger.info(f"[CACHE] Reading sample DICOM for tags: {dicom_files[0]}")
                sample_dcm = pydicom.dcmread(dicom_files[0], force=True)
                sample_tags = self._build_sample_tags(sample_dcm)
                if not sample_tags.get('Modality'):
                    sample_tags['Modality'] = modality
                logger.info(f"[CACHE] _build_sample_tags returned {len(sample_tags)} tags for {series_name}")
            except Exception as e:
                logger.warning(f"[CACHE] Failed to build sample_tags for {series_name}: {e}")
                sample_tags = None

            cache_path = os.path.join(series_dir, "dicom_metadata_cache.json")
            payload = {
                "modality": modality,
                "records": records,
                "sample_tags": sample_tags
            }
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            logger.info(f"[CACHE] Written cache for {series_name}: {len(records)} records, sample_tags={'present' if sample_tags else 'None'}")
        except Exception as e:
            logger.error(f"[CACHE] _cache_metadata_for_series failed for {series_name}: {e}")
            return

    def _collect_metadata_from_dicoms(self, dicom_files, series_folder, modality, read_all):
        """从DICOM文件提取元数据（不含质控字段）"""
        records = []
        try:
            if not dicom_files:
                logger.warning(f"[COLLECT] dicom_files empty for {series_folder}")
                return records

            current_keywords = self.get_keywords(modality)
            logger.info(f"[COLLECT] series={series_folder}, modality={modality}, keywords_count={len(current_keywords)}, read_all={read_all}")

            if read_all:
                for idx, dicom_file in enumerate(dicom_files):
                    try:
                        dcm = pydicom.dcmread(dicom_file, force=True, stop_before_pixels=True)
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
                        # P0: 修复 Carestream BPE 乱码
                        from src.core.metadata import repair_bodypart_examined
                        repair_bodypart_examined(metadata)
                        records.append(metadata)
                    except Exception:
                        continue
            else:
                sample_file = dicom_files[0]
                logger.info(f"[COLLECT] Reading sample file for {series_folder}: {sample_file}")
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
                # P0: 修复 Carestream BPE 乱码
                from src.core.metadata import repair_bodypart_examined
                repair_bodypart_examined(metadata)
                records.append(metadata)
                logger.info(f"[COLLECT] Appended metadata record for {series_folder} with {len(metadata)} fields")
        except Exception as e:
            logger.error(f"[COLLECT] Exception in _collect_metadata_from_dicoms for {series_folder}: {e}")
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
        # P0: 修复 Carestream BPE 乱码
        from src.core.metadata import repair_bodypart_examined
        repair_bodypart_examined(metadata)
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
                if file.startswith('.'):
                    continue
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

    def _assess_converted_file_quality(self, filepath, modality=None, dicom_metadata=None):
        """基于转换后的NPZ/NIfTI文件做质检，返回0/1
        
        Args:
            filepath: 文件路径
            modality: 模态代码 (CT, MR, DX, etc.)，可选
            dicom_metadata: 可选的 DICOM 元数据字典，用于方向错误检测
        """
        return assess_converted_file_quality_impl(filepath, modality, dicom_metadata=dicom_metadata)

    def _assess_series_quality_converted(self, converted_files, modality=None, series_dir=None):
        """对转换后的序列做QC，<=200全量，>200中间±3抽样
        
        Args:
            converted_files: 转换后的文件路径列表
            modality: 模态代码 (CT, MR, DX, etc.)，可选
            series_dir: 序列目录路径，用于检测NIfTI方向错误
        """
        return assess_series_quality_converted_impl(converted_files, modality, series_dir)

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

    def rename_mr_series_folders(self, organized_dir, excel_path):
        """
        对 MR 序列目录进行重命名，使用 MR_clean 生成的标准化名称。
        新名称格式: {SeriesNumber:03d}_{StandardizedName}

        Args:
            organized_dir: organized 目录路径
            excel_path: metadata Excel 路径

        Returns:
            dict: 旧名称到新名称的映射字典，失败时返回空 dict
        """
        import pandas as pd
        import shutil
        from src.core.mr_clean import process_mri_dataframe, generate_standardized_name

        try:
            if not os.path.isfile(excel_path):
                logger.warning("[MR_RENAME] Excel not found, skipping rename")
                return {}

            # 读取 metadata
            df = pd.read_excel(excel_path, sheet_name='DICOM_Metadata')
            if df.empty or 'Modality' not in df.columns:
                return {}

            mr_df = df[df['Modality'].astype(str).str.upper() == 'MR'].copy()
            if mr_df.empty:
                logger.info("[MR_RENAME] No MR series found, skipping rename")
                return {}

            logger.info(f"[MR_RENAME] Processing {len(mr_df)} MR series for rename...")

            # 运行 MR_clean 获取分类结果
            try:
                cleaned_df = process_mri_dataframe(mr_df, progress_callback=self.progress_callback)
            except TypeError:
                cleaned_df = process_mri_dataframe(mr_df)

            # 生成标准化名称
            cleaned_df['standardizedName'] = cleaned_df.apply(generate_standardized_name, axis=1)

            # 建立旧名称到新名称的映射
            rename_map = {}
            for _, row in cleaned_df.iterrows():
                old_folder = str(row.get('SeriesFolder', '')).strip()
                series_num = row.get('SeriesNumber', '')
                std_name = str(row.get('standardizedName', 'UNKNOWN')).strip()

                if not old_folder:
                    continue

                # SeriesNumber 格式化
                try:
                    num_str = f"{int(float(series_num)):03d}"
                except (ValueError, TypeError):
                    num_str = str(series_num).zfill(3) if series_num else '000'

                new_folder = f"{num_str}_{std_name}"
                # 避免重复名称
                base_new = new_folder
                counter = 1
                while new_folder in rename_map.values() or new_folder == old_folder:
                    new_folder = f"{base_new}_{counter}"
                    counter += 1

                if old_folder != new_folder:
                    rename_map[old_folder] = new_folder

            if not rename_map:
                logger.info("[MR_RENAME] No folders need renaming")
                return {}

            # 执行重命名
            renamed_count = 0
            for old_name, new_name in rename_map.items():
                old_path = os.path.join(organized_dir, old_name)
                new_path = os.path.join(organized_dir, new_name)

                if not os.path.isdir(old_path):
                    logger.warning(f"[MR_RENAME] Source folder not found: {old_path}")
                    continue

                if os.path.exists(new_path):
                    logger.warning(f"[MR_RENAME] Target already exists, skipping: {new_path}")
                    continue

                try:
                    shutil.move(old_path, new_path)
                    logger.info(f"[MR_RENAME] Renamed: {old_name} -> {new_name}")
                    renamed_count += 1
                except Exception as e:
                    logger.error(f"[MR_RENAME] Failed to rename {old_name}: {e}")

            # 更新 Excel 中的 SeriesFolder
            if renamed_count > 0:
                df['SeriesFolder'] = df['SeriesFolder'].astype(str).replace(rename_map)
                with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                    df.to_excel(writer, sheet_name='DICOM_Metadata', index=False)
                logger.info(f"[MR_RENAME] Updated Excel with {renamed_count} renamed folders")

            return rename_map
        except Exception as e:
            logger.error(f"[MR_RENAME] MR series rename failed: {e}")
            return {}

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
                                parallel_pipeline=True, modality_filter=None, min_series_files=None,
                                exclude_derived=True):
        """完整的工作流程：下载 -> 整理 -> 转换 -> 提取元数据

        Args:
            accession_number: 检查号
            base_output_dir: 基础输出目录
            auto_extract: 自动解压（兼容性参数）
            auto_organize: 自动整理文件
            auto_metadata: 自动提取元数据
            keep_zip: 保留ZIP文件（兼容性参数）
            keep_extracted: 保留解压后的原始文件
            output_format: 输出格式（'nifti' 或 'npz'）
            parallel_pipeline: 是否使用并行流水线
            modality_filter: 可选，模态过滤（如 'MR', 'CT'，支持逗号分隔多个）
            min_series_files: 可选，最小序列文件数，少于该值的序列将被跳过
            exclude_derived: 是否排除衍生序列，默认True

        Args:
            accession_number: 检查号
            base_output_dir: 基础输出目录
            auto_extract: 自动解压（兼容性参数）
            auto_organize: 自动整理文件
            auto_metadata: 自动提取元数据
            keep_zip: 保留ZIP文件（兼容性参数）
            keep_extracted: 保留解压后的原始文件
            output_format: 输出格式（'nifti' 或 'npz'）
            parallel_pipeline: 是否使用并行流水线
            modality_filter: 可选，模态过滤（如 'MR', 'CT'，支持逗号分隔多个）
            min_series_files: 可选，最小序列文件数，少于该值的序列将被跳过
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 Starting full DICOM processing workflow")
        logger.info(f"📋 AccessionNumber: {accession_number}")
        if modality_filter:
            logger.info(f"🔍 Modality filter: {modality_filter}")
        if min_series_files:
            logger.info(f"📊 Min series files: {min_series_files}")
        if exclude_derived:
            logger.info(f"🚫 Exclude derived series: enabled")
        logger.info(f"{'='*80}")

        # 确保输出目录存在
        os.makedirs(base_output_dir, exist_ok=True)

        # 步骤1: 下载DICOM文件
        logger.info(f"\n📥 Step 1: Download DICOM files")

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

        # P2: 队列看门狗，防止死锁
        watchdog = QueueWatchdog(series_queue, timeout=300.0)

        def _on_series_downloaded(series_dir, series_meta):
            series_folder = os.path.basename(series_dir)
            series_queue.put((series_dir, series_folder))
            watchdog.update_activity()  # P2: 更新看门狗活动

        def _download_worker():
            try:
                download_path = self.download_study(
                    accession_number,
                    base_output_dir,
                    on_series_downloaded=_on_series_downloaded,
                    modality_filter=modality_filter,
                    min_series_files=min_series_files,
                    exclude_derived=exclude_derived
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

        def _organize_worker(fmt):
            watchdog.update_activity()  # P2: 更新看门狗活动
            while True:
                try:
                    # P2: 使用超时获取，避免永久阻塞
                    item = series_queue.get(timeout=60.0)
                except Empty:
                    logger.warning("⏱️  Organize worker: queue empty timeout, checking status...")
                    if download_done.is_set():
                        logger.info("   Download done, worker exiting")
                        break
                    continue

                if item is None:
                    series_queue.task_done()
                    break
                series_dir, series_folder = item

                # 直接处理单个 series。
                # 不再使用"超时线程"包装：Thread.join(timeout) 无法杀死超时线程，
                # 被抛弃的线程会永久滞留在后台，是线程泄漏的主要来源。
                # dcm2niix 子进程自身已有超时+强杀机制（_run_subprocess_with_timeout），
                # 无需再叠加一层线程级超时。
                info = None
                err = None
                try:
                    info = self._process_single_series(
                        series_dir, series_folder, fmt,
                        min_series_files=min_series_files
                    )
                except Exception as e:
                    err = e

                # 转换无结果时的兜底：若已存在 NIfTI 输出则直接采用
                if not info:
                    try:
                        existing_nifti = [
                            f for f in os.listdir(series_dir)
                            if f.endswith(('.nii.gz', '.nii')) and not f.startswith('.')
                        ]
                    except Exception:
                        existing_nifti = []
                    if existing_nifti:
                        logger.info(
                            f"   ✅ Series {series_folder} has existing NIfTI output, using it: {existing_nifti[0]}"
                        )
                        info = {
                            'path': series_dir,
                            'file_count': 0,  # 未知，但非关键
                            'files': []
                        }
                        err = None

                if info:
                    with series_lock:
                        series_info[series_folder] = info
                if err:
                    logger.warning(f"⚠️  Series organize failed: {series_folder}: {err}")

                watchdog.update_activity()  # P2: 更新看门狗活动
                series_queue.task_done()

        if parallel_pipeline and auto_organize:
            # P0: 原地处理 - 不再创建 organized 子目录
            # P2: 启动看门狗
            watchdog.start()
            try:
                download_thread = threading.Thread(target=_download_worker, daemon=True)
                # spawn multiple organizers
                organizer_threads = []
                for _ in range(num_converters):
                    t = threading.Thread(target=_organize_worker, args=(output_format,), daemon=True)
                    t.start()
                    organizer_threads.append(t)

                download_thread.start()

                # 等待下载完成
                download_thread.join()
                # 通知整理线程退出（放入与 worker 数相同的哨兵）
                for _ in range(len(organizer_threads)):
                    series_queue.put(None)
                # P0: 不再调用 series_queue.join()（Python Queue.join 无 timeout 参数，会永久等待）
                # 直接等待 worker 线程完成，带超时
                for t in organizer_threads:
                    _safe_thread_join(t, timeout=600)
                    if t.is_alive():
                        logger.warning(
                            "Organize worker did not exit within 600s timeout. "
                            "It may be stuck in a subprocess or I/O operation."
                        )
            finally:
                # P2: 停止看门狗（确保异常时也能停止，避免线程泄漏）
                watchdog.stop()

            download_dir = download_dir_holder['path']
            if not download_dir:
                logger.error("❌ Download failed, workflow terminated")
                return None
        else:
            download_dir = self.download_study(
                accession_number,
                base_output_dir,
                modality_filter=modality_filter,
                min_series_files=min_series_files,
                exclude_derived=exclude_derived
            )
            if not download_dir:
                logger.error("❌ Download failed, workflow terminated")
                return None

        results = {
            'accession_number': accession_number,
            'zip_file': download_dir,  # 保持接口兼容性
            'extract_dir': download_dir,  # 保持接口兼容性
            'success': False
        }

        if auto_organize:
            # 步骤2: 整理DICOM文件
            logger.info(f"\n📁 Step 2: Organize DICOM files by series (format: {output_format})")
            # P0: 原地处理 - organized_dir 就是 download_dir
            organized_dir = download_dir
            if parallel_pipeline:
                # 使用流水线整理结果 - 原地处理，文件已经在正确的位置
                results['organized_dir'] = organized_dir
                results['series_info'] = series_info
            else:
                # 等待文件系统稳定（确保所有下载的文件已完全写入磁盘）
                logger.info("   ⏳ Waiting for file system to stabilize...")
                time.sleep(0.5)
                # 检查下载目录中的文件状态
                self._wait_for_files_stable(download_dir)
                _, series_info = self.organize_dicom_files(download_dir, output_format=output_format, min_series_files=min_series_files)
                if not series_info:
                    # 区分真正的组织失败 vs 所有序列都被过滤掉
                    import glob
                    dicom_files = glob.glob(os.path.join(download_dir, '**', '*.dcm'), recursive=True)
                    if dicom_files:
                        logger.error(f"❌ All {len(dicom_files)} DICOM files were filtered out (min_files={min_series_files}). This accession has insufficient data.")
                        raise ValueError(f"All series filtered out: only {len(dicom_files)} files found, min required is {min_series_files}. This may be a scout/locator scan or incomplete study.")
                    else:
                        logger.error("❌ File organization failed: no DICOM files found in download directory")
                        raise ValueError("File organization failed: no DICOM files found")
                results['organized_dir'] = organized_dir
                results['series_info'] = series_info

            if auto_metadata:
                # 步骤3: 提取元数据 (独立线程)
                logger.info(f"\n📊 Step 3: Extract DICOM metadata")
                excel_name = f"dicom_metadata_{accession_number}.xlsx"
                excel_path = os.path.join(organized_dir, excel_name)

                excel_holder = {'path': None}

                def _metadata_worker():
                    try:
                        excel_holder['path'] = self.extract_dicom_metadata(organized_dir, output_excel=excel_path)
                    except Exception as e:
                        logger.error(f"❌ Metadata extraction error: {e}")
                        import traceback
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        excel_holder['path'] = None

                metadata_thread = threading.Thread(target=_metadata_worker, daemon=True)
                metadata_thread.start()
                # 带超时等待：元数据提取卡死时不能永久占用任务 worker
                _safe_thread_join(metadata_thread, timeout=600)
                if metadata_thread.is_alive():
                    logger.error(
                        "❌ Metadata extraction timed out after 600s, "
                        "continuing without metadata (background thread will be abandoned)"
                    )
                    excel_holder['path'] = None

                excel_file = excel_holder['path']
                if excel_file:
                    results['excel_file'] = excel_file
                    results['success'] = True
                else:
                    logger.warning("⚠️  Metadata extraction failed, previous steps completed")

        # 打印最终结果
        logger.info(f"\n{'='*80}")
        if results['success']:
            logger.info(f"🎉 Workflow completed!")
            logger.info(f"📁 Organized directory: {results.get('organized_dir', 'N/A')}")
            logger.info(f"📄 Excel file: {results.get('excel_file', 'N/A')}")
            logger.info(f"📊 Series count: {len(results.get('series_info', {}))}")
        else:
            logger.warning(f"⚠️  Workflow partially completed")
        logger.info(f"{'='*80}")

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
