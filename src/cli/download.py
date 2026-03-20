# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
命令行下载客户端模块

提供通过 HTTP API 与 DICOM 服务器通信的命令行工具，
支持断点续传、批量下载和生产者-消费者并行处理模式。
"""

import requests
import time
import os
import sys
import argparse
import zipfile
import shutil
from tqdm import tqdm
import pandas as pd
import json
import html
from datetime import datetime, timedelta
import tempfile
import threading
from queue import Queue, Empty as QueueEmpty
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import nibabel as nib
from PIL import Image
import pydicom
from pydicom.dataset import Dataset, FileDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

# 可以通过环境变量 SERVER_URL 覆盖默认地址，例如：
# export SERVER_URL="http://192.0.0.222:5005"
SERVER_URL = os.environ.get("SERVER_URL", "http://172.17.250.136:5005")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))  # 默认60秒超时
API_SINGLE = f"{SERVER_URL}/api/process/single"
API_STATUS = lambda task_id: f"{SERVER_URL}/api/task/{task_id}/status"
API_DOWNLOAD = lambda task_id: f"{SERVER_URL}/api/download/{task_id}/zip"

# 全局队列（生产者-消费者模式）
task_submit_queue = Queue()  # 待提交任务队列 (accession, options)
task_download_queue = Queue()  # 待下载任务队列 (task_id, accession, options)
task_process_queue = Queue()  # 待整理任务队列 (task_id, accession, zip_path, options)
progress_lock = threading.Lock()


def submit_task_worker(server_url, api_single, max_retries=3, retry_delay=2, queue_full_retry_delay=10):
    """生产者工作线程：提交任务到服务器"""
    while True:
        try:
            item = task_submit_queue.get(timeout=1)
            if item is None:  # 结束信号
                task_submit_queue.put(None)  # 通知其他线程
                break

            accession, options = item
            payload = {
                "accession_number": accession,
                "options": options
            }

            # 提交任务，带重试机制
            for attempt in range(max_retries):
                try:
                    response = requests.post(api_single, json=payload, timeout=REQUEST_TIMEOUT)

                    # 处理队列满的情况 (HTTP 503)
                    if response.status_code == 503:
                        error_msg = response.json().get('error', 'Task queue is full')
                        tqdm.write(f"[!] 服务器队列已满: {accession} - {error_msg}")
                        if attempt < max_retries - 1:
                            time.sleep(queue_full_retry_delay)
                            continue
                        else:
                            tqdm.write(f"[!] {accession} 达到最大重试次数，跳过")
                            break

                    if response.status_code != 200:
                        tqdm.write(f"[!] 提交任务失败 {accession} ({response.status_code}): {response.text}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                        break

                    data = response.json()
                    task_id = data.get('task_id')
                    status = data.get('status', 'started')

                    if task_id:
                        tqdm.write(f"[+] 任务已{'加入队列' if status == 'queued' else '启动'}: {accession} (ID: {task_id})")
                        task_download_queue.put((task_id, accession, options))
                    break

                except requests.exceptions.ConnectionError as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    tqdm.write(f"[!] 连接失败 {accession}: {e}")
                    break
                except Exception as e:
                    tqdm.write(f"[!] 提交异常 {accession}: {e}")
                    break

            task_submit_queue.task_done()

        except QueueEmpty:
            continue
        except Exception as e:
            tqdm.write(f"[!] 提交工作线程异常: {e}")


def poll_task_status(task_id, server_url, api_status, timeout=30):
    """轮询任务状态直到完成或失败"""
    while True:
        try:
            response = requests.get(api_status(task_id), timeout=timeout)
            if response.status_code != 200:
                return False, None

            data = response.json()
            status = data.get('status')

            if status == 'completed':
                return True, data.get('result')
            elif status in ('failed', 'cancelled'):
                return False, data.get('error')

            time.sleep(2)
        except Exception as e:
            return False, str(e)


def download_worker(output_dir, server_url, api_download, progress_tracker=None):
    """消费者工作线程：轮询状态并下载结果"""
    api_status = lambda task_id: f"{server_url}/api/task/{task_id}/status"

    while True:
        try:
            item = task_download_queue.get(timeout=1)
            if item is None:
                task_download_queue.put(None)
                break

            task_id, accession, options = item
            tqdm.write(f"[*] 开始监控任务: {accession}")

            # 轮询状态
            success, result = poll_task_status(task_id, server_url, api_status)

            if not success:
                tqdm.write(f"[!] 任务失败 {accession}: {result}")
                task_download_queue.task_done()
                continue

            # 下载结果
            download_url = api_download(task_id)
            target_zip = os.path.join(output_dir, f"result_{task_id}.zip")

            try:
                with requests.get(download_url, stream=True, timeout=(30, 300)) as r:
                    r.raise_for_status()
                    with open(target_zip, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

                tqdm.write(f"[+] 下载完成: {accession}")

                # 放入整理队列
                task_process_queue.put((task_id, accession, target_zip, options))

                # 更新进度
                if progress_tracker:
                    with progress_lock:
                        progress_tracker['downloaded'] += 1

            except Exception as e:
                tqdm.write(f"[!] 下载失败 {accession}: {e}")

            task_download_queue.task_done()

        except QueueEmpty:
            continue
        except Exception as e:
            tqdm.write(f"[!] 下载工作线程异常: {e}")


def process_download_worker(output_dir, progress_tracker=None):
    """整理工作线程：解压并整理下载的文件"""
    while True:
        try:
            item = task_process_queue.get(timeout=1)
            if item is None:
                task_process_queue.put(None)
                break

            task_id, accession, zip_path, options = item
            final_dir = os.path.join(output_dir, str(accession))
            tmp_dir = tempfile.mkdtemp(prefix=f"tmp_extract_{task_id}_", dir=output_dir)

            try:
                # 解压
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(tmp_dir)

                # 创建目标目录
                os.makedirs(final_dir, exist_ok=True)

                # 移动文件（平铺）
                moved_any = False
                for root, dirs, files in os.walk(tmp_dir):
                    for fname in files:
                        if fname.lower().endswith(('.nii', '.nii.gz', '.xlsx', '.png', '.npz')):
                            src = os.path.join(root, fname)
                            dest = os.path.join(final_dir, fname)

                            # 避免覆盖
                            if os.path.exists(dest):
                                base, ext = os.path.splitext(fname)
                                counter = 1
                                while os.path.exists(dest):
                                    new_name = f"{base}_{counter}{ext}"
                                    dest = os.path.join(final_dir, new_name)
                                    counter += 1

                            shutil.move(src, dest)
                            moved_any = True

                # 如果没有找到目标文件，移动整个目录结构
                if not moved_any:
                    for item_name in os.listdir(tmp_dir):
                        s = os.path.join(tmp_dir, item_name)
                        d = os.path.join(final_dir, item_name)
                        if os.path.exists(d):
                            base, ext = os.path.splitext(item_name)
                            counter = 1
                            while os.path.exists(d):
                                new_name = f"{base}_{counter}{ext}"
                                d = os.path.join(final_dir, new_name)
                                counter += 1
                        shutil.move(s, d)

                tqdm.write(f"[+] 整理完成: {final_dir}")

                # 清理临时文件
                try:
                    os.remove(zip_path)
                except:
                    pass
                try:
                    shutil.rmtree(tmp_dir)
                except:
                    pass

                # 更新进度
                if progress_tracker:
                    with progress_lock:
                        progress_tracker['processed'] += 1
                        progress_tracker['completed_accessions'].add(accession)

            except Exception as e:
                tqdm.write(f"[!] 整理失败 {accession}: {e}")

            task_process_queue.task_done()

        except QueueEmpty:
            continue
        except Exception as e:
            tqdm.write(f"[!] 整理工作线程异常: {e}")


def _col(row, *names, default=''):
    """从 pandas Series 取值，依次尝试多个列名，均不存在则返回 default。"""
    for name in names:
        if name in row.index and pd.notna(row[name]):
            return row[name]
    return default


def recover_dicom_from_nifti(nifti_path, df, output_dicom_dir):
    """根据 DICOM_Metadata DataFrame，将 nii/npz 文件恢复成 DICOM 文件。

    Excel 中每行对应一张切片（按 InstanceNumber 或行顺序对应 Z 轴）。

    Args:
        nifti_path: NIfTI (.nii/.nii.gz) 或 NPZ 文件路径
        df: DICOM_Metadata 工作表的 DataFrame
        output_dicom_dir: 输出 DICOM 目录
    """
    os.makedirs(output_dicom_dir, exist_ok=True)

    # 读取图像数据
    if nifti_path.lower().endswith('.npz'):
        arr = np.load(nifti_path)
        data = arr[list(arr.keys())[0]]  # shape [Z, Y, X]
        # 转为 [X, Y, Z]
        data = np.transpose(data, (2, 1, 0))
        pixel_spacing = ['1.0', '1.0']
        slice_thickness = '1.0'
    else:
        img = nib.load(nifti_path)
        data = np.asarray(img.get_fdata())
        affine = img.affine
        # 从 affine 提取像素间距
        voxel_sizes = np.sqrt((affine[:3, :3] ** 2).sum(axis=0))
        pixel_spacing = [f'{voxel_sizes[1]:.4f}', f'{voxel_sizes[0]:.4f}']
        slice_thickness = f'{abs(voxel_sizes[2]):.4f}'

    if data.ndim == 4:
        data = data[..., 0]  # 取第一个时间帧

    nx, ny, nz = data.shape

    # 按 InstanceNumber 排序 df（如有），长度不足则循环使用
    if 'InstanceNumber' in df.columns:
        df = df.sort_values('InstanceNumber').reset_index(drop=True)

    # 从第一行取序列级公共字段
    first = df.iloc[0] if len(df) > 0 else pd.Series(dtype=object)
    study_uid    = str(_col(first, 'StudyInstanceUID') or generate_uid())
    series_uid   = str(_col(first, 'SeriesInstanceUID') or generate_uid())
    patient_name = str(_col(first, 'PatientName', default='Unknown'))
    patient_id   = str(_col(first, 'PatientID', default='Unknown'))
    modality     = str(_col(first, 'Modality', default='OT'))
    series_desc  = str(_col(first, 'SeriesDescription', default=''))
    series_num   = str(_col(first, 'SeriesNumber', default='1'))

    # SOPClassUID：根据模态选择
    sop_class_map = {
        'CT': '1.2.840.10008.5.1.4.1.1.2',
        'MR': '1.2.840.10008.5.1.4.1.1.4',
        'PT': '1.2.840.10008.5.1.4.1.1.128',
        'NM': '1.2.840.10008.5.1.4.1.1.20',
    }
    sop_class_uid = sop_class_map.get(modality.upper(), '1.2.840.10008.5.1.4.1.1.4')

    # 像素数据全局归一化范围（保持层间一致）
    global_min = float(data.min())
    global_max = float(data.max())
    scale = (global_max - global_min) if global_max != global_min else 1.0

    for z in range(nz):
        row = df.iloc[z % len(df)] if len(df) > 0 else pd.Series(dtype=object)
        sop_uid = generate_uid()

        # 文件元信息
        file_meta = Dataset()
        file_meta.MediaStorageSOPClassUID = sop_class_uid
        file_meta.MediaStorageSOPInstanceUID = sop_uid
        file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

        ds = FileDataset(None, {}, file_meta=file_meta, preamble=b'\x00' * 128)
        ds.is_implicit_VR = False
        ds.is_little_endian = True

        # 患者 / 研究 / 序列信息
        ds.PatientName         = patient_name
        ds.PatientID           = patient_id
        ds.StudyInstanceUID    = study_uid
        ds.SeriesInstanceUID   = series_uid
        ds.SOPClassUID         = sop_class_uid
        ds.SOPInstanceUID      = sop_uid
        ds.Modality            = modality
        ds.SeriesDescription   = series_desc
        ds.SeriesNumber        = series_num
        ds.InstanceNumber      = str(_col(row, 'InstanceNumber', default=z + 1))

        # 图像几何信息
        ds.Rows                = ny
        ds.Columns             = nx
        ds.PixelSpacing        = pixel_spacing
        ds.SliceThickness      = slice_thickness
        ds.SliceLocation       = str(_col(row, 'SliceLocation', default=float(z)))

        # 像素格式
        ds.BitsAllocated       = 16
        ds.BitsStored          = 16
        ds.HighBit             = 15
        ds.SamplesPerPixel     = 1
        ds.PhotometricInterpretation = 'MONOCHROME2'
        ds.PixelRepresentation = 0  # unsigned

        # 像素数据：归一化到 0-4095（uint16）
        slice_data = data[:, :, z]
        pixel_array = ((slice_data - global_min) / scale * 4095).astype(np.uint16)
        ds.PixelData = pixel_array.T.tobytes()  # 转置为 row-major (Y, X)

        filepath = os.path.join(output_dicom_dir, f"slice_{z:04d}.dcm")
        pydicom.dcmwrite(filepath, ds)

    tqdm.write(f"  [+] 已恢复 {nz} 层 DICOM 到: {output_dicom_dir}")
    return True


def recover_dicom_for_accession(accession_dir):
    """为单个检查目录恢复所有序列的 DICOM 文件。

    规则：
      - 在 accession_dir 下查找 dicom_metadata_*.xlsx
      - 每个 xlsx 对应一个序列；根据文件名中的序列描述部分匹配同名 nii.gz / npz
      - 输出到 accession_dir/dicom/<series_name>/slice_NNNN.dcm
      - 若 dicom/ 目录已存在则跳过（避免重复恢复）
    """
    if not os.path.isdir(accession_dir):
        return False

    dicom_output_dir = os.path.join(accession_dir, "dicom")
    if os.path.exists(dicom_output_dir):
        tqdm.write(f"  [i] DICOM 目录已存在，跳过: {accession_dir}")
        return True

    # 收集 Excel 文件
    xlsx_files = [f for f in os.listdir(accession_dir)
                  if f.lower().endswith('.xlsx') and 'dicom_metadata' in f.lower()]
    if not xlsx_files:
        tqdm.write(f"  [!] 未找到 meta Excel 文件: {accession_dir}")
        return False

    # 收集图像文件（.nii / .nii.gz / .npz），建立 stem→path 映射
    image_map = {}
    for f in os.listdir(accession_dir):
        fl = f.lower()
        if fl.endswith('.nii.gz'):
            stem = f[:-7]
        elif fl.endswith('.nii') or fl.endswith('.npz'):
            stem = os.path.splitext(f)[0]
        else:
            continue
        image_map[stem] = os.path.join(accession_dir, f)

    if not image_map:
        tqdm.write(f"  [!] 未找到 NIfTI/NPZ 文件: {accession_dir}")
        return False

    tqdm.write(f"[*] 开始恢复 DICOM: {os.path.basename(accession_dir)}")
    success_count = 0

    for xlsx_file in xlsx_files:
        xlsx_path = os.path.join(accession_dir, xlsx_file)
        # dicom_metadata_<series_stem>.xlsx → series_stem
        series_stem = xlsx_file
        for prefix in ('dicom_metadata_', 'dicom_metadata'):
            if series_stem.lower().startswith(prefix):
                series_stem = series_stem[len(prefix):]
                break
        series_stem = os.path.splitext(series_stem)[0]

        # 找对应的图像文件
        image_path = image_map.get(series_stem)
        if not image_path:
            # 宽松匹配：stem 包含在 series_stem 中，或反之
            for stem, path in image_map.items():
                if stem in series_stem or series_stem in stem:
                    image_path = path
                    break

        if not image_path:
            tqdm.write(f"  [!] 找不到对应图像文件: {xlsx_file}（序列: {series_stem}）")
            continue

        try:
            df = pd.read_excel(xlsx_path, sheet_name='DICOM_Metadata')
        except Exception as e:
            tqdm.write(f"  [!] 读取 Excel 失败 {xlsx_file}: {e}")
            continue

        series_dicom_dir = os.path.join(dicom_output_dir, series_stem)
        try:
            if recover_dicom_from_nifti(image_path, df, series_dicom_dir):
                success_count += 1
        except Exception as e:
            tqdm.write(f"  [!] 恢复失败 {series_stem}: {e}")

    tqdm.write(f"[+] 恢复完成: {os.path.basename(accession_dir)} ({success_count}/{len(xlsx_files)} 个序列)")
    return success_count > 0


def merge_metadata_excel(output_dir, accession_list):
    """合并所有检查的 meta Excel 文件（原始文件保留不删除）。

    对每个 sheet（DICOM_Metadata、MR_Cleaned 等）分别合并，
    写入同一个输出 Excel 的不同 sheet。
    """
    # sheet_name → [DataFrame, ...]
    sheet_frames: dict = {}

    for accession in accession_list:
        accession_dir = os.path.join(output_dir, str(accession))
        if not os.path.isdir(accession_dir):
            continue

        xlsx_files = [f for f in os.listdir(accession_dir)
                      if f.lower().endswith('.xlsx') and 'dicom_metadata' in f.lower()]

        for xlsx_file in xlsx_files:
            xlsx_path = os.path.join(accession_dir, xlsx_file)
            try:
                all_sheets = pd.read_excel(xlsx_path, sheet_name=None)
            except Exception as e:
                tqdm.write(f"[!] 读取 Excel 失败 {xlsx_path}: {e}")
                continue

            for sheet_name, df in all_sheets.items():
                # 注入 AccessionNumber 列（插到最前面）
                df.insert(0, 'AccessionNumber', str(accession))
                sheet_frames.setdefault(sheet_name, []).append(df)

    if not sheet_frames:
        tqdm.write("[!] 没有可合并的元数据")
        return None

    output_path = os.path.join(output_dir, f"merged_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet_name, frames in sheet_frames.items():
            merged = pd.concat(frames, ignore_index=True)
            # sheet 名称限制 31 字符
            safe_name = sheet_name[:31]
            merged.to_excel(writer, index=False, sheet_name=safe_name)

    total_files = sum(len(v) for v in sheet_frames.values())
    tqdm.write(f"[+] 已合并元数据（{total_files} 个文件，{len(sheet_frames)} 个工作表）到: {output_path}")
    return output_path


def download_list_parallel(acc_list, output_dir="./downloads", fmt='nifti', modality=None,
                           min_files=10, exclude_derived=True, num_submitters=2, num_downloaders=3,
                           num_processors=2, recover_dicom=False):
    """批量下载多个 AccessionNumber 的结果，使用生产者-消费者并行模式。

    Args:
        acc_list: AccessionNumber 列表
        output_dir: 输出目录
        fmt: 输出格式 (nifti 或 npz)
        modality: 模态过滤
        min_files: 最小文件数
        exclude_derived: 是否排除衍生序列
        num_submitters: 任务提交线程数
        num_downloaders: 下载线程数
        num_processors: 整理线程数
        recover_dicom: 是否恢复 DICOM 文件
    """
    os.makedirs(output_dir, exist_ok=True)

    # 加载已有进度
    completed, timings, quality_records = load_progress(output_dir)

    # 过滤已完成的
    pending_list = [acc for acc in acc_list if str(acc) not in completed]
    total_pending = len(pending_list)

    if not pending_list:
        tqdm.write("[+] 所有任务已完成")
    else:
        tqdm.write(f"[*] 共 {len(acc_list)} 个任务，{len(pending_list)} 个待处理，{len(completed)} 个已完成")

    # 准备选项
    options = {
        "output_format": fmt,
        "auto_organize": True,
        "auto_metadata": True,
        "exclude_derived": exclude_derived
    }
    if modality:
        options["modality_filter"] = modality
    if min_files:
        options["min_series_files"] = min_files

    # 进度跟踪
    progress_tracker = {
        'submitted': 0,
        'downloaded': 0,
        'processed': 0,
        'completed_accessions': set()
    }

    # 启动工作线程
    threads = []

    # 提交线程（生产者）
    for i in range(num_submitters):
        t = threading.Thread(
            target=submit_task_worker,
            args=(SERVER_URL, API_SINGLE),
            name=f"Submitter-{i}"
        )
        t.daemon = True
        t.start()
        threads.append(t)

    # 下载线程（消费者1）
    for i in range(num_downloaders):
        t = threading.Thread(
            target=download_worker,
            args=(output_dir, SERVER_URL, API_DOWNLOAD, progress_tracker),
            name=f"Downloader-{i}"
        )
        t.daemon = True
        t.start()
        threads.append(t)

    # 整理线程（消费者2）
    for i in range(num_processors):
        t = threading.Thread(
            target=process_download_worker,
            args=(output_dir, progress_tracker),
            name=f"Processor-{i}"
        )
        t.daemon = True
        t.start()
        threads.append(t)

    # 将任务放入提交队列
    for accession in pending_list:
        task_submit_queue.put((accession, options))

    # 发送结束信号
    for _ in range(num_submitters):
        task_submit_queue.put(None)

    # 使用 tqdm 显示进度
    with tqdm(total=total_pending, desc="下载进度") as pbar:
        last_completed = 0
        while True:
            with progress_lock:
                current_completed = len(progress_tracker['completed_accessions'])

            if current_completed > last_completed:
                pbar.update(current_completed - last_completed)
                last_completed = current_completed

            if current_completed >= total_pending and task_submit_queue.empty() and task_download_queue.empty() and task_process_queue.empty():
                break

            time.sleep(0.5)

    # 等待队列处理完成
    task_submit_queue.join()
    task_download_queue.join()
    task_process_queue.join()

    # 保存进度
    with progress_lock:
        for acc in progress_tracker['completed_accessions']:
            completed.add(str(acc))
    save_progress(output_dir, completed, timings, quality_records)

    # 获取所有已完成的 accession 列表（包括之前完成的）
    all_completed = [acc for acc in acc_list if str(acc) in completed]

    # 恢复 DICOM（如果需要）
    if recover_dicom:
        tqdm.write("\n[*] 开始恢复 DICOM 文件...")
        for accession in tqdm(all_completed, desc="恢复 DICOM"):
            accession_dir = os.path.join(output_dir, str(accession))
            recover_dicom_for_accession(accession_dir)

    # 合并 meta Excel
    tqdm.write("\n[*] 开始合并元数据...")
    merge_metadata_excel(output_dir, all_completed)

    # 生成质量报告
    tqdm.write("\n[*] 生成质量报告...")
    for accession in all_completed:
        if str(accession) not in quality_records:
            quality_entry = collect_accession_quality(accession, output_dir)
            if quality_entry is not None:
                quality_records[str(accession)] = quality_entry

    report_path = generate_quality_report_html(output_dir, quality_records)
    if report_path:
        tqdm.write(f"[+] 质量报表已生成: {report_path}")

    return len(progress_tracker['completed_accessions'])


# 保持向后兼容的函数
def main(cli_args=None):
    """提交单个 accession 的任务并下载结果（兼容旧接口）。"""
    if cli_args is None:
        parser = argparse.ArgumentParser(description="DICOM下载客户端测试工具")
        parser.add_argument("accession", help="AccessionNumber (例如: Z25043000836)")
        parser.add_argument("--output_dir", default="./downloads", help="下载结果存放目录")
        parser.add_argument("--format", choices=['nifti', 'npz'], default='nifti', help="输出格式 (nifti 或 npz)")
        parser.add_argument("--modality", default=None, help="模态过滤，如 MR, CT (可逗号分隔多个)")
        parser.add_argument("--min_files", type=int, default=10, help="最小序列文件数，少于该值的序列将被跳过 (默认: 10)")
        parser.add_argument("--include_derived", action="store_true", help="包含衍生序列 (MPR, MIP, VR等)，默认会过滤掉")
        parser.add_argument("--recover-dicom", action="store_true", help="根据 meta Excel 恢复 DICOM 文件到 dicom/ 子目录")
        parser.add_argument("--parallel",default=True ,action="store_true", help="使用生产者-消费者并行模式（批量下载时推荐）")
        parser.add_argument("--num-submitters", type=int, default=2, help="并行模式：任务提交线程数 (默认: 2)")
        parser.add_argument("--num-downloaders", type=int, default=3, help="并行模式：下载线程数 (默认: 3)")
        parser.add_argument("--num-processors", type=int, default=2, help="并行模式：整理线程数 (默认: 2)")
        args = parser.parse_args()

        # 如果是单个下载（非批量），使用旧流程
        if not args.parallel:
            return _main_single(args)
        else:
            # 并行模式（单任务）
            download_list_parallel(
                [args.accession],
                output_dir=args.output_dir,
                fmt=args.format,
                modality=args.modality,
                min_files=args.min_files,
                exclude_derived=not args.include_derived,
                num_submitters=args.num_submitters,
                num_downloaders=args.num_downloaders,
                num_processors=args.num_processors,
                recover_dicom=args.recover_dicom
            )
            return True
    else:
        args = cli_args
        return _main_single(args)


def _main_single(args):
    """单任务下载流程（原始逻辑）。"""
    filter_info = []
    if args.modality:
        filter_info.append(f"Modality={args.modality}")
    if getattr(args, 'min_files', None):
        filter_info.append(f"MinFiles={args.min_files}")
    if not getattr(args, 'include_derived', False):
        filter_info.append("ExcludeDerived")
    filter_str = f" ({', '.join(filter_info)})" if filter_info else ""
    print(f"🚀 启动任务: AccessionNumber={args.accession}, 格式={args.format}{filter_str}")

    # 提交任务
    options = {
        "output_format": args.format,
        "auto_organize": True,
        "auto_metadata": True,
        "exclude_derived": not getattr(args, 'include_derived', False)
    }
    if args.modality:
        options["modality_filter"] = args.modality
    if getattr(args, 'min_files', None):
        options["min_series_files"] = args.min_files

    payload = {
        "accession_number": args.accession,
        "options": options
    }

    # 提交任务，带重试机制
    max_retries = 3
    retry_delay = 2
    queue_full_retry_delay = 10

    for attempt in range(max_retries):
        try:
            print(f"[*] 正在提交任务 (尝试 {attempt + 1}/{max_retries})...")
            response = requests.post(API_SINGLE, json=payload, timeout=REQUEST_TIMEOUT)

            if response.status_code == 503:
                error_msg = response.json().get('error', 'Task queue is full')
                print(f"[!] 服务器队列已满: {error_msg}")
                if attempt < max_retries - 1:
                    print(f"[*] 等待 {queue_full_retry_delay} 秒后重试...")
                    time.sleep(queue_full_retry_delay)
                    continue
                else:
                    print(f"[!] 已达到最大重试次数")
                    return False

            if response.status_code != 200:
                print(f"[!] 提交任务失败 ({response.status_code}): {response.text}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                return False

            data = response.json()
            task_id = data.get('task_id')
            status = data.get('status', 'started')
            if status == 'queued':
                print(f"[+] 任务已加入队列，ID: {task_id} (等待中...)")
            else:
                print(f"[+] 任务已启动，ID: {task_id}")
            break
        except Exception as e:
            print(f"[!] 提交失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return False
    else:
        return False

    # 轮询状态
    success, result = poll_task_status_single(task_id)

    if success:
        ok = download_and_extract(task_id, args.output_dir, accession=args.accession)

        # 恢复 DICOM（如果需要）
        if getattr(args, 'recover_dicom', False):
            accession_dir = os.path.join(args.output_dir, str(args.accession))
            recover_dicom_for_accession(accession_dir)

        return bool(ok)
    else:
        return False


def poll_task_status_single(task_id):
    """单任务状态轮询（原始逻辑）。"""
    print(f"[*] 正在监控任务: {task_id}")
    last_log_idx = 0

    while True:
        try:
            response = requests.get(API_STATUS(task_id), timeout=30)
            if response.status_code != 200:
                print(f"[!] 获取状态失败: {response.text}")
                return False, None

            data = response.json()
            status = data.get('status')
            progress = data.get('progress', 0)
            step = data.get('current_step', '')
            logs = data.get('logs', [])

            # 打印新日志
            if len(logs) > last_log_idx:
                for log in logs[last_log_idx:]:
                    print(f"  [{log['timestamp']}] {log['message']}")
                last_log_idx = len(logs)

            # 打印进度
            sys.stdout.write(f"\r    进度: [{progress}%] 步骤: {step} ".ljust(60))
            sys.stdout.flush()

            if status == 'completed':
                print("\n[+] 任务处理成功完成！")
                return True, data.get('result')
            elif status == 'failed':
                print(f"\n[!] 任务失败: {data.get('error')}")
                return False, None
            elif status == 'cancelled':
                print("\n[!] 任务被取消")
                return False, None

            time.sleep(2)
        except Exception as e:
            print(f"\n[!] 轮询出错: {e}")
            return False, None


def download_and_extract(task_id, output_dir, accession=None):
    """下载结果并解压（原始逻辑）。"""
    download_url = API_DOWNLOAD(task_id)
    target_zip = os.path.join(output_dir, f"result_{task_id}.zip")

    os.makedirs(output_dir, exist_ok=True)

    tmp_dir = tempfile.mkdtemp(prefix=f"tmp_extract_{task_id}_", dir=output_dir)
    final_dir_name = str(accession) if accession else str(task_id)
    final_dir = os.path.join(output_dir, final_dir_name)

    print(f"[*] 正在下载结果到: {target_zip}")

    try:
        with requests.get(download_url, stream=True, timeout=(30, 300)) as r:
            r.raise_for_status()
            with open(target_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        print(f"[+] 下载完成，正在解压...")

        with zipfile.ZipFile(target_zip, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)

        os.makedirs(final_dir, exist_ok=True)

        # 移动文件
        moved_any = False
        for root, dirs, files in os.walk(tmp_dir):
            for fname in files:
                if fname.lower().endswith(('.nii', '.nii.gz', '.xlsx', '.png', '.npz')):
                    src = os.path.join(root, fname)
                    dest = os.path.join(final_dir, fname)

                    if os.path.exists(dest):
                        base, ext = os.path.splitext(fname)
                        counter = 1
                        while os.path.exists(dest):
                            new_name = f"{base}_{counter}{ext}"
                            dest = os.path.join(final_dir, new_name)
                            counter += 1

                    shutil.move(src, dest)
                    moved_any = True

        if not moved_any:
            for item in os.listdir(tmp_dir):
                s = os.path.join(tmp_dir, item)
                d = os.path.join(final_dir, item)
                if os.path.exists(d):
                    base, ext = os.path.splitext(item)
                    counter = 1
                    while os.path.exists(d):
                        new_name = f"{base}_{counter}{ext}"
                        d = os.path.join(final_dir, new_name)
                        counter += 1
                shutil.move(s, d)

        print(f"[+] 处理完成。文件位于: {final_dir}")

        # 清理
        try:
            os.remove(target_zip)
        except:
            pass
        try:
            shutil.rmtree(tmp_dir)
        except:
            pass

        return True
    except Exception as e:
        print(f"[!] 下载或解压失败: {e}")
        try:
            if os.path.exists(target_zip):
                os.remove(target_zip)
        except:
            pass
        try:
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
        except:
            pass
        return False


def _is_low_quality(value):
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) == 1
    value_str = str(value).strip().lower()
    return value_str in {"1", "true", "yes", "y", "低质量", "poor", "bad"}


def collect_accession_quality(accession, output_dir):
    """采集单个 accession 的质量信息。"""
    accession_str = str(accession)
    accession_dir = os.path.join(output_dir, accession_str)
    if not os.path.isdir(accession_dir):
        return None

    xlsx_files = [
        os.path.join(accession_dir, name)
        for name in os.listdir(accession_dir)
        if name.lower().endswith('.xlsx') and 'dicom_metadata' in name.lower()
    ]

    if not xlsx_files:
        return {
            'accession': accession_str,
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'total_images': 0,
            'low_quality_images': 0,
            'reasons': {},
            'files': []
        }

    total_images = 0
    low_quality_images = 0
    reason_counter = {}
    file_summaries = []

    for xlsx_path in sorted(xlsx_files):
        try:
            df = pd.read_excel(xlsx_path, sheet_name='DICOM_Metadata')
        except Exception:
            continue

        if 'Low_quality' not in df.columns or 'Low_quality_reason' not in df.columns:
            continue

        file_total = len(df)
        low_mask = df['Low_quality'].apply(_is_low_quality)
        low_df = df[low_mask]
        file_low = int(low_mask.sum())

        file_reason_counter = {}
        if not low_df.empty:
            for reason in low_df['Low_quality_reason'].fillna('').astype(str):
                clean_reason = reason.strip() or '未提供原因'
                reason_counter[clean_reason] = reason_counter.get(clean_reason, 0) + 1
                file_reason_counter[clean_reason] = file_reason_counter.get(clean_reason, 0) + 1

        total_images += file_total
        low_quality_images += file_low
        file_summaries.append({
            'file': os.path.basename(xlsx_path),
            'total_images': file_total,
            'low_quality_images': file_low,
            'reasons': file_reason_counter
        })

    return {
        'accession': accession_str,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'total_images': int(total_images),
        'low_quality_images': int(low_quality_images),
        'reasons': reason_counter,
        'files': file_summaries
    }


def generate_quality_report_html(output_dir, quality_records):
    """根据质量记录生成 HTML 质量报表。"""
    if not quality_records:
        return None

    report_path = os.path.join(output_dir, 'quality_report.html')

    all_records = list(quality_records.values())
    total_images = sum(int(item.get('total_images', 0)) for item in all_records)
    low_quality_images = sum(int(item.get('low_quality_images', 0)) for item in all_records)
    normal_images = max(total_images - low_quality_images, 0)

    reason_counter = {}
    for item in all_records:
        for reason, count in (item.get('reasons') or {}).items():
            reason_counter[reason] = reason_counter.get(reason, 0) + int(count)

    sorted_reasons = sorted(reason_counter.items(), key=lambda x: x[1], reverse=True)
    max_reason_count = sorted_reasons[0][1] if sorted_reasons else 0

    low_ratio = (low_quality_images / total_images * 100.0) if total_images else 0.0
    normal_ratio = (normal_images / total_images * 100.0) if total_images else 0.0

    reason_rows = []
    for reason, count in sorted_reasons:
        width = (count / max_reason_count * 100.0) if max_reason_count else 0.0
        reason_rows.append(
            f"""
            <tr>
                <td>{html.escape(str(reason))}</td>
                <td>{count}</td>
                <td><div class=\"bar-wrap\"><div class=\"bar\" style=\"width:{width:.2f}%\"></div></div></td>
            </tr>
            """
        )

    accession_rows = []
    for accession, item in sorted(quality_records.items(), key=lambda x: x[0]):
        acc_total = int(item.get('total_images', 0))
        acc_low = int(item.get('low_quality_images', 0))
        acc_ratio = (acc_low / acc_total * 100.0) if acc_total else 0.0
        top_reason = ''
        if item.get('reasons'):
            top_reason = max(item['reasons'].items(), key=lambda x: x[1])[0]
        accession_rows.append(
            f"""
            <tr>
                <td>{html.escape(str(accession))}</td>
                <td>{acc_total}</td>
                <td>{acc_low}</td>
                <td>{acc_ratio:.2f}%</td>
                <td>{html.escape(top_reason or '-')}</td>
            </tr>
            """
        )

    html_content = f"""<!DOCTYPE html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>DICOM 质量报表</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; color: #222; }}
    h1, h2 {{ margin: 0 0 10px; }}
    .summary {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }}
    .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px 16px; min-width: 180px; }}
    .card .num {{ font-size: 24px; font-weight: bold; margin-top: 6px; }}
    .chart {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; }}
    .bar-line {{ margin: 10px 0; }}
    .label {{ display: inline-block; width: 120px; }}
    .bar-wrap {{ display: inline-block; vertical-align: middle; width: calc(100% - 220px); height: 14px; background: #f1f3f5; border-radius: 8px; overflow: hidden; }}
    .bar {{ height: 100%; background: #3b82f6; }}
    .value {{ display: inline-block; width: 90px; text-align: right; margin-left: 8px; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; font-size: 14px; }}
    th {{ background: #f7f7f7; }}
    .muted {{ color: #666; font-size: 13px; }}
  </style>
</head>
<body>
  <h1>DICOM 下载质量报表</h1>
  <p class=\"muted\">生成时间：{html.escape(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</p>

  <div class=\"summary\">
    <div class=\"card\"><div>总图像数量</div><div class=\"num\">{total_images}</div></div>
    <div class=\"card\"><div>质量差图像数量</div><div class=\"num\">{low_quality_images}</div></div>
    <div class=\"card\"><div>质量差占比</div><div class=\"num\">{low_ratio:.2f}%</div></div>
    <div class=\"card\"><div>检查数量</div><div class=\"num\">{len(all_records)}</div></div>
  </div>

  <div class=\"chart\">
    <h2>总体质量分布</h2>
    <div class=\"bar-line\">
      <span class=\"label\">正常图像</span>
      <div class=\"bar-wrap\"><div class=\"bar\" style=\"width:{normal_ratio:.2f}%\"></div></div>
      <span class=\"value\">{normal_images} ({normal_ratio:.2f}%)</span>
    </div>
    <div class=\"bar-line\">
      <span class=\"label\">质量差图像</span>
      <div class=\"bar-wrap\"><div class=\"bar\" style=\"width:{low_ratio:.2f}%\"></div></div>
      <span class=\"value\">{low_quality_images} ({low_ratio:.2f}%)</span>
    </div>
  </div>

  <h2>质量差原因分布</h2>
  <table>
    <thead><tr><th>原因</th><th>数量</th><th>占比条形图</th></tr></thead>
    <tbody>
      {''.join(reason_rows) if reason_rows else '<tr><td colspan="3">暂无低质量原因数据</td></tr>'}
    </tbody>
  </table>

  <h2>按检查汇总</h2>
  <table>
    <thead><tr><th>Accession</th><th>总图像数</th><th>质量差图像数</th><th>质量差占比</th><th>主要原因</th></tr></thead>
    <tbody>
      {''.join(accession_rows) if accession_rows else '<tr><td colspan="5">暂无数据</td></tr>'}
    </tbody>
  </table>
</body>
</html>
"""

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    return report_path


PROGRESS_FILENAME = ".download_progress.json"


def load_progress(output_dir):
    path = os.path.join(output_dir, PROGRESS_FILENAME)
    if not os.path.exists(path):
        return set(), {}, {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            completed = data.get('completed', [])
            timings = data.get('timings', {})
            quality_records = data.get('quality_records', {})
            timings = {str(k): float(v) for k, v in timings.items()}
            if not isinstance(quality_records, dict):
                quality_records = {}
            return set(completed), timings, quality_records
    except Exception:
        return set(), {}, {}


def save_progress(output_dir, completed_set, timings_dict=None, quality_records=None):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, PROGRESS_FILENAME)
    try:
        payload = {'completed': sorted(list(completed_set))}
        if timings_dict:
            payload['timings'] = {str(k): float(v) for k, v in timings_dict.items()}
        if quality_records:
            payload['quality_records'] = quality_records
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[!] 无法保存进度: {e}")


def download_list(acc_list, output_dir="./downloads", fmt='nifti', modality=None, min_files=10,
                  exclude_derived=True, recover_dicom=False, parallel=False,
                  num_submitters=2, num_downloaders=3, num_processors=2):
    """批量下载多个 AccessionNumber 的结果。

    支持串行或生产者-消费者并行模式。
    """
    if parallel:
        return download_list_parallel(
            acc_list, output_dir, fmt, modality, min_files, exclude_derived,
            num_submitters, num_downloaders, num_processors, recover_dicom
        )

    # 串行模式（原始逻辑，增加了 recover_dicom 支持）
    completed, timings, quality_records = load_progress(output_dir)
    total = len(acc_list)

    # 恢复 DICOM（对于已完成的）
    if recover_dicom:
        for accession in acc_list:
            if str(accession) in completed:
                accession_dir = os.path.join(output_dir, str(accession))
                recover_dicom_for_accession(accession_dir)

    # 合并 meta Excel
    all_completed = [acc for acc in acc_list if str(acc) in completed]
    if all_completed:
        merge_metadata_excel(output_dir, all_completed)

    filter_info = []
    if modality:
        filter_info.append(f"Modality={modality}")
    if min_files:
        filter_info.append(f"MinFiles={min_files}")
    if exclude_derived:
        filter_info.append("ExcludeDerived")
    if filter_info:
        tqdm.write(f"[i] 过滤条件: {', '.join(filter_info)}")

    for accession in tqdm(acc_list):
        if str(accession) in completed:
            tqdm.write(f"\n--- 跳过（已完成） AccessionNumber: {accession} ---")
            continue
        tqdm.write(f"\n=== 处理 AccessionNumber: {accession} ===")

        main_args = argparse.Namespace(
            accession=str(accession),
            output_dir=output_dir,
            format=fmt,
            modality=modality,
            min_files=min_files,
            include_derived=not exclude_derived,
            recover_dicom=False  # 在串行模式下不在这里处理，由外部统一处理
        )

        start_t = time.time()
        ok = _main_single(main_args)
        elapsed = time.time() - start_t

        if ok:
            completed.add(str(accession))
            timings[str(accession)] = elapsed

            if str(accession) not in quality_records:
                quality_entry = collect_accession_quality(accession, output_dir)
                if quality_entry is not None:
                    quality_records[str(accession)] = quality_entry

            if recover_dicom:
                accession_dir = os.path.join(output_dir, str(accession))
                recover_dicom_for_accession(accession_dir)

            all_times = list(timings.values())
            avg_sec = sum(all_times) / len(all_times) if all_times else elapsed
            remaining = total - len(completed)
            remaining_sec = avg_sec * remaining
            eta = datetime.now() + timedelta(seconds=remaining_sec)

            save_progress(output_dir, completed, timings, quality_records)
            tqdm.write(f"[+] 标记为已完成: {accession} (耗时 {elapsed:.2f} s)")
            tqdm.write(f"    平均: {avg_sec:.2f} s/accession；剩余: {remaining}，预计完成: {eta.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            tqdm.write(f"[!] 处理失败，将在下次继续尝试: {accession}")

    # 合并 meta Excel
    all_completed = [acc for acc in acc_list if str(acc) in completed]
    if all_completed:
        merge_metadata_excel(output_dir, all_completed)

    report_path = generate_quality_report_html(output_dir, quality_records)
    if report_path:
        tqdm.write(f"[+] 质量报表已生成: {report_path}")


if __name__ == "__main__":
    main()
