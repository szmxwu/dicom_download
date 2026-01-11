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

SERVER_URL = "http://127.0.0.1:5005"
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

def download_and_extract(task_id, output_dir):
    """下载结果并解压"""
    download_url = API_DOWNLOAD(task_id)
    target_zip = os.path.join(output_dir, f"result_{task_id}.zip")
    extract_to = os.path.join(output_dir, task_id)
    
    print(f"[*] 正在下载结果到: {target_zip}")
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(target_zip, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        print(f"[+] 下载完成，正在解压到: {extract_to}")
        
        with zipfile.ZipFile(target_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            
        print(f"[+] 处理完成。文件位于: {extract_to}")
        os.remove(target_zip) # 清理zip
        return True
    except Exception as e:
        print(f"[!] 下载或解压失败: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="DICOM下载客户端测试工具")
    parser.add_argument("accession", help="AccessionNumber (例如: Z25043000836)")
    parser.add_argument("--output_dir", default="./downloads", help="下载结果存放目录")
    parser.add_argument("--format", choices=['nifti', 'npz'], default='nifti', help="输出格式 (nifti 或 npz)")
    
    args = parser.parse_args()
    
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
            return
        
        task_id = response.json().get('task_id')
        print(f"[+] 任务已启动，ID: {task_id}")
        
        # 步骤2: 轮询状态
        success, result = poll_task_status(task_id)
        
        if success:
            # 步骤3: 下载
            download_and_extract(task_id, args.output_dir)
            
    except Exception as e:
        print(f"[!] 通信失败: {e}")

if __name__ == "__main__":
    main()
