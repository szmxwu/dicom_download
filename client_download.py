# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
DICOM Processing System - Client Side Test Script
Usage: python test.py <AccessionNumber> [--output_dir ./downloads] [--format nifti|npz]
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
import tempfile

SERVER_URL = "http://172.17.250.136:5005"
API_SINGLE = f"{SERVER_URL}/api/process/single"
API_STATUS = lambda task_id: f"{SERVER_URL}/api/task/{task_id}/status"
API_DOWNLOAD = lambda task_id: f"{SERVER_URL}/api/download/{task_id}/zip"

def poll_task_status(task_id):
    """轮询任务状态直到完成或失败"""
    print(f"[*] 正在监控任务: {task_id}")
    last_log_idx = 0
    
    while True:
        try:
            response = requests.get(API_STATUS(task_id))
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
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(target_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        print(f"[+] 下载完成，正在解压到临时目录: {tmp_dir}")

        with zipfile.ZipFile(target_zip, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)

        # 创建目标目录
        os.makedirs(final_dir, exist_ok=True)

        # 将所有 nii / nii.gz 文件从临时目录移动到 final_dir（平铺）
        moved_any = False
        for root, dirs, files in os.walk(tmp_dir):
            for fname in files:
                if fname.lower().endswith('.nii') or fname.lower().endswith('.nii.gz'):
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
        args = parser.parse_args()
    else:
        args = cli_args

    print(f"🚀 启动任务: AccessionNumber={args.accession}, 格式={args.format}")

    # 步骤1: 提交任务
    payload = {
        "accession_number": args.accession,
        "options": {
            "output_format": args.format,
            "auto_organize": True,
            "auto_metadata": True
        }
    }

    try:
        response = requests.post(API_SINGLE, json=payload)
        if response.status_code != 200:
            print(f"[!] 提交任务失败 ({response.status_code}): {response.text}")
            return False

        task_id = response.json().get('task_id')
        print(f"[+] 任务已启动，ID: {task_id}")

        # 步骤2: 轮询状态
        success, result = poll_task_status(task_id)

        if success:
            # 步骤3: 下载（将 accession 传入以便按 AccessionNumber 命名目录）
            ok = download_and_extract(task_id, args.output_dir, accession=args.accession)
            return bool(ok)
        else:
            return False

    except Exception as e:
        print(f"[!] 通信失败: {e}")
        return False

PROGRESS_FILENAME = ".download_progress.json"


def load_progress(output_dir):
    path = os.path.join(output_dir, PROGRESS_FILENAME)
    if not os.path.exists(path):
        return set()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            completed = data.get('completed', [])
            return set(completed)
    except Exception:
        return set()


def save_progress(output_dir, completed_set):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, PROGRESS_FILENAME)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump({'completed': sorted(list(completed_set))}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[!] 无法保存进度: {e}")


def download_list(acc_list, output_dir="./downloads", fmt='nifti'):
    """批量下载多个 AccessionNumber 的结果，并支持断点续传。

    进度记录保存在 output_dir/.download_progress.json，程序在每次成功下载后更新该文件。
    """
    completed = load_progress(output_dir)

    for accession in tqdm(acc_list):
        if str(accession) in completed:
            tqdm.write(f"\n--- 跳过（已完成） AccessionNumber: {accession} ---")
            continue

        tqdm.write(f"\n=== 处理 AccessionNumber: {accession} ===")
        main_args = argparse.Namespace(
            accession=str(accession),
            output_dir=output_dir,
            format=fmt
        )
        ok = main(main_args)

        if ok:
            # 仅在成功下载并解压后标记为完成
            completed.add(str(accession))
            save_progress(output_dir, completed)
            tqdm.write(f"[+] 标记为已完成: {accession}")
        else:
            tqdm.write(f"[!] 处理失败，将在下次继续尝试: {accession}")


if __name__ == "__main__":
    df=pd.read_excel('input/selected_samples_details_filtered.xlsx')
    acc_list=df['影像号'].tolist()
    download_list(acc_list)