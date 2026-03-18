# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
命令行下载客户端模块

提供通过 HTTP API 与 DICOM 服务器通信的命令行工具，
支持断点续传和批量下载功能。
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

# 可以通过环境变量 SERVER_URL 覆盖默认地址，例如：
# export SERVER_URL="http://192.0.0.222:5005"
SERVER_URL = os.environ.get("SERVER_URL", "http://172.17.250.136:5005")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))  # 默认60秒超时
API_SINGLE = f"{SERVER_URL}/api/process/single"
API_STATUS = lambda task_id: f"{SERVER_URL}/api/task/{task_id}/status"
API_DOWNLOAD = lambda task_id: f"{SERVER_URL}/api/download/{task_id}/zip"

def poll_task_status(task_id):
    """轮询任务状态直到完成或失败"""
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
            
            # 打印进度信息
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
            
            time.sleep(2)  # 每2秒轮询一次
            
        except Exception as e:
            print(f"\n[!] 轮询出错: {e}")
            return False, None

def download_and_extract(task_id, output_dir, accession=None):
    """下载结果并解压。

    把最终的 nii / nii.gz 文件放到 output_dir/<AccessionNumber>/ 下（平铺，不在创建额外顶层目录）。
    如果未提供 accession，则使用 task_id 作为目录名（向后兼容）。
    """
    download_url = API_DOWNLOAD(task_id)
    target_zip = os.path.join(output_dir, f"result_{task_id}.zip")

    # 确保输出目录存在（Windows 下需要先创建，否则临时目录创建会失败）
    os.makedirs(output_dir, exist_ok=True)

    # 使用临时目录解压，然后将 nii 文件移动到最终目录（避免嵌套一层）
    tmp_dir = tempfile.mkdtemp(prefix=f"tmp_extract_{task_id}_", dir=output_dir)
    final_dir_name = str(accession) if accession else str(task_id)
    final_dir = os.path.join(output_dir, final_dir_name)

    print(f"[*] 正在下载结果到: {target_zip}")

    try:
        with requests.get(download_url, stream=True, timeout=(30, 300)) as r:  # 连接超时30秒，读取超时5分钟
            r.raise_for_status()
            with open(target_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        print(f"[+] 下载完成，正在解压到临时目录: {tmp_dir}")

        with zipfile.ZipFile(target_zip, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)

        # 创建目标目录
        os.makedirs(final_dir, exist_ok=True)

        # 将所有 nii / nii.gz / xlsx / png 文件从临时目录移动到 final_dir（平铺）
        moved_any = False
        for root, dirs, files in os.walk(tmp_dir):
            for fname in files:
                if (
                    fname.lower().endswith('.nii')
                    or fname.lower().endswith('.nii.gz')
                    or fname.lower().endswith('.xlsx')
                    or fname.lower().endswith('.png')
                ):
                    src = os.path.join(root, fname)
                    dest = os.path.join(final_dir, fname)

                    # 如果目标已存在，添加后缀避免覆盖
                    if os.path.exists(dest):
                        base, ext = os.path.splitext(fname)
                        counter = 1
                        while True:
                            new_name = f"{base}_{counter}{ext}"
                            dest = os.path.join(final_dir, new_name)
                            if not os.path.exists(dest):
                                break
                            counter += 1

                    shutil.move(src, dest)
                    moved_any = True

        # 如果没有找到 nii 文件，则将整个解压内容移动到 final_dir（保持原结构）
        if not moved_any:
            # 移动所有顶层内容下移到 final_dir
            for item in os.listdir(tmp_dir):
                s = os.path.join(tmp_dir, item)
                d = os.path.join(final_dir, item)
                if os.path.exists(d):
                    # 冲突时，尝试重命名
                    base, ext = os.path.splitext(item)
                    counter = 1
                    while True:
                        new_name = f"{base}_{counter}{ext}"
                        d = os.path.join(final_dir, new_name)
                        if not os.path.exists(d):
                            break
                        counter += 1
                shutil.move(s, d)

        print(f"[+] 处理完成。文件位于: {final_dir}")
        # 清理临时文件和 zip
        try:
            os.remove(target_zip)
        except Exception:
            pass
        try:
            shutil.rmtree(tmp_dir)
        except Exception:
            pass

        return True
    except Exception as e:
        print(f"[!] 下载或解压失败: {e}")
        # 尝试清理临时目录和 zip
        try:
            if os.path.exists(target_zip):
                os.remove(target_zip)
        except Exception:
            pass
        try:
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
        except Exception:
            pass
        return False

def main(cli_args=None):
    """提交单个 accession 的任务并下载结果。

    如果 cli_args 为 None，则从命令行解析参数并在结束后返回；
    如果传入 argparse.Namespace，则使用该参数并返回布尔表示成功/失败。
    """
    if cli_args is None:
        parser = argparse.ArgumentParser(description="DICOM下载客户端测试工具")
        parser.add_argument("accession", help="AccessionNumber (例如: Z25043000836)")
        parser.add_argument("--output_dir", default="./downloads", help="下载结果存放目录")
        parser.add_argument("--format", choices=['nifti', 'npz'], default='nifti', help="输出格式 (nifti 或 npz)")
        parser.add_argument("--modality", default=None, help="模态过滤，如 MR, CT (可逗号分隔多个)")
        parser.add_argument("--min_files", type=int, default=10, help="最小序列文件数，少于该值的序列将被跳过 (默认: 10)")
        parser.add_argument("--include_derived", action="store_true", help="包含衍生序列 (MPR, MIP, VR等)，默认会过滤掉")
        args = parser.parse_args()
    else:
        args = cli_args

    filter_info = []
    if args.modality:
        filter_info.append(f"Modality={args.modality}")
    if args.min_files:
        filter_info.append(f"MinFiles={args.min_files}")
    if not args.include_derived:
        filter_info.append("ExcludeDerived")
    filter_str = f" ({', '.join(filter_info)})" if filter_info else ""
    print(f"🚀 启动任务: AccessionNumber={args.accession}, 格式={args.format}{filter_str}")

    # 步骤1: 提交任务
    options = {
        "output_format": args.format,
        "auto_organize": True,
        "auto_metadata": True,
        "exclude_derived": not args.include_derived  # 默认True（过滤衍生序列）
    }
    if args.modality:
        options["modality_filter"] = args.modality
    if args.min_files:
        options["min_series_files"] = args.min_files

    payload = {
        "accession_number": args.accession,
        "options": options
    }

    # 提交任务，带重试机制
    max_retries = 3
    retry_delay = 2
    queue_full_retry_delay = 10  # 队列满时的重试间隔

    for attempt in range(max_retries):
        try:
            print(f"[*] 正在提交任务 (尝试 {attempt + 1}/{max_retries})...")
            response = requests.post(API_SINGLE, json=payload, timeout=REQUEST_TIMEOUT)

            # 处理队列满的情况 (HTTP 503)
            if response.status_code == 503:
                error_msg = response.json().get('error', 'Task queue is full')
                print(f"[!] 服务器队列已满: {error_msg}")
                if attempt < max_retries - 1:
                    print(f"[*] 等待 {queue_full_retry_delay} 秒后重试...")
                    time.sleep(queue_full_retry_delay)
                    continue
                else:
                    print(f"[!] 已达到最大重试次数，服务器队列已满，请稍后重试")
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
        except requests.exceptions.ConnectionError as e:
            print(f"[!] 连接失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"[*] 等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                print(f"[!] 已达到最大重试次数，放弃提交")
                return False
        except requests.exceptions.Timeout as e:
            print(f"[!] 请求超时 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return False
        except Exception as e:
            print(f"[!] 通信失败: {e}")
            return False
    else:
        # 循环正常结束但没有 break（理论上不会执行到这里）
        return False

    # 步骤2: 轮询状态
    success, result = poll_task_status(task_id)

    if success:
        # 步骤3: 下载（将 accession 传入以便按 AccessionNumber 命名目录）
        ok = download_and_extract(task_id, args.output_dir, accession=args.accession)
        return bool(ok)
    else:
        return False

PROGRESS_FILENAME = ".download_progress.json"


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
    """采集单个 accession 的质量信息。

    从 output_dir/<accession>/ 下的 dicom_metadata*.xlsx 中读取 DICOM_Metadata 工作表，
    汇总 Low_quality 与 Low_quality_reason。
    """
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
            # ensure timings are floats
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
            # convert timings to simple serializable map
            payload['timings'] = {str(k): float(v) for k, v in timings_dict.items()}
        if quality_records:
            payload['quality_records'] = quality_records
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[!] 无法保存进度: {e}")


def download_list(acc_list, output_dir="./downloads", fmt='nifti', modality=None, min_files=10, exclude_derived=True):
    """批量下载多个 AccessionNumber 的结果，并支持断点续传。

    进度记录保存在 output_dir/.download_progress.json，程序在每次成功下载后更新该文件。
    """
    completed, timings, quality_records = load_progress(output_dir)
    total = len(acc_list)
    # timings: dict accession->seconds (float)

    # 对历史已完成但未记录质量数据的 accession 做一次补采
    missing_quality = [acc for acc in completed if acc not in quality_records]
    for accession in missing_quality:
        quality_entry = collect_accession_quality(accession, output_dir)
        if quality_entry is not None:
            quality_records[str(accession)] = quality_entry
    if missing_quality:
        save_progress(output_dir, completed, timings, quality_records)

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
            include_derived=not exclude_derived
        )
        # 计时并执行
        start_t = time.time()
        ok = main(main_args)
        elapsed = time.time() - start_t

        if ok:
            # 仅在成功下载并解压后标记为完成
            completed.add(str(accession))
            timings[str(accession)] = elapsed

            quality_entry = collect_accession_quality(accession, output_dir)
            if quality_entry is not None:
                quality_records[str(accession)] = quality_entry

            # 计算平均速度（秒/acc）基于所有已知 timings
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

    report_path = generate_quality_report_html(output_dir, quality_records)
    if report_path:
        tqdm.write(f"[+] 质量报表已生成: {report_path}")


if __name__ == "__main__":
    df=pd.read_excel('input/selected_samples_details_filtered.xlsx')
    acc_list=df['影像号'].tolist()
    download_list(acc_list)
