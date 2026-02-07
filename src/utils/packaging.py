# -*- coding: utf-8 -*-
"""
结果打包工具模块

提供将处理结果打包为 ZIP 文件的功能。
"""

import os
import zipfile
from typing import Iterable, Optional


def create_result_zip(
    source_dir: str,
    task_id: str,
    result_dir: str,
    extra_files: Optional[Iterable[str]] = None
) -> str:
    """
    创建结果 ZIP 压缩包

    将源目录中的所有文件打包为 ZIP 文件，并可选择性地包含额外文件。

    Args:
        source_dir: 源目录路径，包含需要打包的文件
        task_id: 任务标识符，用于生成 ZIP 文件名
        result_dir: 结果目录路径，ZIP 文件将保存至此
        extra_files: 可选的额外文件列表，这些文件将被添加到 ZIP 根目录

    Returns:
        str: 生成的 ZIP 文件完整路径

    Example:
        >>> zip_path = create_result_zip(
        ...     "/data/organized",
        ...     "task_001",
        ...     "/data/results",
        ...     ["/data/metadata.xlsx"]
        ... )
        >>> print(zip_path)
        /data/results/result_task_001.zip
    """
    # 确保使用绝对路径，避免相对路径问题
    result_dir = os.path.abspath(result_dir)
    source_dir = os.path.abspath(source_dir)
    
    # 检查源目录是否存在
    if not os.path.exists(source_dir):
        raise FileNotFoundError(f"Source directory not found: {source_dir}")
    if not os.path.isdir(source_dir):
        raise NotADirectoryError(f"Source path is not a directory: {source_dir}")
    
    os.makedirs(result_dir, exist_ok=True)
    zip_path = os.path.join(result_dir, f"result_{task_id}.zip")

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # 打包源目录中的所有文件
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arc_name)

        # 添加额外文件到 ZIP 根目录
        if extra_files:
            for extra_path in extra_files:
                if not extra_path or not os.path.exists(extra_path):
                    continue
                # 安全检查：跳过已在源目录中的文件，避免重复
                try:
                    base_dir = os.path.abspath(source_dir)
                    extra_abs = os.path.abspath(extra_path)
                    if os.path.commonpath([base_dir, extra_abs]) == base_dir:
                        continue
                except Exception:
                    pass
                arc_name = os.path.basename(extra_path)
                zipf.write(extra_path, arc_name)

    return zip_path
