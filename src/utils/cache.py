# -*- coding: utf-8 -*-
"""
磁盘缓存管理模块

为 PACS 下载任务提供基于参数的磁盘缓存，避免重复下载相同条件的检查。
缓存目录位于 results/ 下，复用现有的自动清理机制。
"""

import hashlib
import json
import os
import shutil
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)

CACHE_DIR_NAME = "cache"


def _sanitize_accession(accession_number: str) -> str:
    """清理 AccessionNumber，使其适合作为目录名。"""
    return "".join(c for c in accession_number if c.isalnum() or c in "_-").strip()


def _compute_param_hash(accession_number: str, options: Dict) -> str:
    """根据参数计算缓存哈希。"""
    params = {
        "accession_number": accession_number,
        "modality_filter": options.get("modality_filter"),
        "min_series_files": options.get("min_series_files"),
        "exclude_derived": options.get("exclude_derived", True),
        "output_format": options.get("output_format", "nifti"),
    }
    param_str = json.dumps(params, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(param_str.encode("utf-8")).hexdigest()[:8]


def get_cache_dir(results_base_dir: str, accession_number: str, options: Dict) -> str:
    """获取缓存目录路径。"""
    sanitized = _sanitize_accession(accession_number)
    param_hash = _compute_param_hash(accession_number, options)
    return os.path.join(results_base_dir, CACHE_DIR_NAME, f"{sanitized}_{param_hash}")


def cache_exists(cache_dir: str) -> bool:
    """检查缓存是否存在且有效（包含 organized 子目录和 ZIP）。"""
    if not os.path.isdir(cache_dir):
        return False
    organized_dir = os.path.join(cache_dir, "organized")
    zip_file = os.path.join(cache_dir, "result.zip")
    excel_file = os.path.join(cache_dir, "metadata.xlsx")
    has_organized = os.path.isdir(organized_dir) and any(os.listdir(organized_dir))
    has_zip = os.path.isfile(zip_file)
    has_excel = os.path.isfile(excel_file)
    valid = has_organized and has_zip and has_excel
    if valid:
        logger.info(f"[CACHE] Cache hit: {cache_dir}")
    return valid


def copy_from_cache(cache_dir: str, target_dir: str, task_id: str) -> Dict:
    """从缓存复制结果到任务目录。"""
    organized_dir = os.path.join(cache_dir, "organized")
    zip_file = os.path.join(cache_dir, "result.zip")
    excel_file = os.path.join(cache_dir, "metadata.xlsx")

    # 目标路径
    target_organized = os.path.join(target_dir, "organized")
    target_zip = os.path.join(os.path.dirname(target_dir), f"result_{task_id}.zip")

    logger.info(f"[CACHE] Copying from cache: {cache_dir} -> {target_dir}")

    if os.path.exists(target_organized):
        shutil.rmtree(target_organized)
    shutil.copytree(organized_dir, target_organized)

    if os.path.exists(target_zip):
        os.remove(target_zip)
    shutil.copy2(zip_file, target_zip)

    # 复制 Excel 到目标 organized 目录
    target_excel = None
    if os.path.isfile(excel_file):
        target_excel = os.path.join(target_organized, os.path.basename(excel_file))
        shutil.copy2(excel_file, target_excel)

    # 扫描 series_info
    series_info = {}
    for item in os.listdir(target_organized):
        item_path = os.path.join(target_organized, item)
        if os.path.isdir(item_path):
            series_info[item] = {"path": item_path}

    return {
        "success": True,
        "organized_dir": target_organized,
        "excel_file": target_excel,
        "result_zip": target_zip,
        "series_info": series_info,
        "from_cache": True,
    }


def save_to_cache(source_dir: str, cache_dir: str, zip_path: Optional[str] = None,
                  excel_path: Optional[str] = None) -> bool:
    """将任务结果保存到缓存目录。"""
    try:
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
        os.makedirs(cache_dir, exist_ok=True)

        cache_organized = os.path.join(cache_dir, "organized")
        cache_zip = os.path.join(cache_dir, "result.zip")
        cache_excel = os.path.join(cache_dir, "metadata.xlsx")

        # 复制 organized 目录
        if os.path.exists(cache_organized):
            shutil.rmtree(cache_organized)
        if os.path.isdir(source_dir):
            shutil.copytree(source_dir, cache_organized)

        # 复制 ZIP
        if zip_path and os.path.isfile(zip_path):
            shutil.copy2(zip_path, cache_zip)

        # 复制 Excel
        if excel_path and os.path.isfile(excel_path):
            shutil.copy2(excel_path, cache_excel)

        logger.info(f"[CACHE] Saved to cache: {cache_dir}")

        # 保存成功后强制执行容量检查（LRU 淘汰），防止缓存无限增长
        try:
            enforce_cache_limit(os.path.dirname(cache_dir))
        except Exception as e:
            logger.warning(f"[CACHE] enforce_cache_limit failed: {e}")

        return True
    except Exception as e:
        logger.error(f"[CACHE] Failed to save cache: {e}")
        return False


# 缓存容量上限默认值（GB），可通过环境变量 CACHE_MAX_GB 覆盖
DEFAULT_CACHE_MAX_GB = 20.0


def get_cache_max_gb() -> float:
    """读取缓存容量上限（GB）。"""
    try:
        val = float(os.getenv('CACHE_MAX_GB', str(DEFAULT_CACHE_MAX_GB)))
        return val if val > 0 else DEFAULT_CACHE_MAX_GB
    except Exception:
        return DEFAULT_CACHE_MAX_GB


def enforce_cache_limit(cache_base: str) -> int:
    """LRU 淘汰：当缓存总大小超过上限时，从最旧访问的条目开始删除。

    Args:
        cache_base: 缓存根目录（results/cache）

    Returns:
        被淘汰的条目数量
    """
    if not os.path.isdir(cache_base):
        return 0

    max_gb = get_cache_max_gb()
    limit_bytes = max_gb * (1024 ** 3)

    entries = []
    total_bytes = 0
    try:
        for item in os.listdir(cache_base):
            item_path = os.path.join(cache_base, item)
            if not os.path.isdir(item_path):
                continue
            size = _get_dir_size(item_path)
            try:
                atime = os.path.getatime(item_path)
            except OSError:
                atime = 0.0
            entries.append((atime, size, item_path))
            total_bytes += size
    except Exception as e:
        logger.warning(f"[CACHE] Failed to scan cache for LRU: {e}")
        return 0

    if total_bytes <= limit_bytes:
        return 0

    logger.info(
        f"[CACHE] Cache size {total_bytes / (1024**3):.2f}GB exceeds limit {max_gb}GB, starting LRU eviction"
    )
    entries.sort(key=lambda x: x[0])  # 最旧访问的在前

    evicted = 0
    for _atime, size, path in entries:
        if total_bytes <= limit_bytes:
            break
        try:
            shutil.rmtree(path)
            total_bytes -= size
            evicted += 1
            logger.info(f"[CACHE] Evicted: {os.path.basename(path)} ({size / (1024**3):.2f}GB)")
        except Exception as e:
            logger.warning(f"[CACHE] Failed to evict {path}: {e}")

    if evicted:
        logger.info(
            f"[CACHE] LRU eviction done: removed {evicted} entries, "
            f"cache now {total_bytes / (1024**3):.2f}GB (limit {max_gb}GB)"
        )
    return evicted


def clear_all_cache(results_base_dir: str) -> Dict:
    """清除所有缓存，返回统计信息。"""
    cache_base = os.path.join(results_base_dir, CACHE_DIR_NAME)
    if not os.path.exists(cache_base):
        return {"cleared": 0, "freed_gb": 0.0, "message": "No cache directory found"}

    cleared = 0
    freed_bytes = 0
    errors = []

    try:
        for item in os.listdir(cache_base):
            item_path = os.path.join(cache_base, item)
            try:
                if os.path.isdir(item_path):
                    size = _get_dir_size(item_path)
                    shutil.rmtree(item_path)
                    freed_bytes += size
                    cleared += 1
                elif os.path.isfile(item_path):
                    size = os.path.getsize(item_path)
                    os.remove(item_path)
                    freed_bytes += size
                    cleared += 1
            except Exception as e:
                errors.append(f"{item}: {e}")
                logger.warning(f"[CACHE] Failed to clear {item_path}: {e}")
    except Exception as e:
        logger.error(f"[CACHE] Failed to clear cache: {e}")
        errors.append(str(e))

    freed_gb = freed_bytes / (1024 ** 3)
    return {
        "cleared": cleared,
        "freed_gb": freed_gb,
        "errors": errors,
        "message": f"Cleared {cleared} items, freed {freed_gb:.2f} GB",
    }


def _get_dir_size(directory: str) -> int:
    """计算目录总大小（字节）。"""
    total = 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if os.path.exists(fp):
                    total += os.path.getsize(fp)
    except Exception:
        pass
    return total


def get_cache_stats(results_base_dir: str) -> Dict:
    """获取缓存统计信息。"""
    cache_base = os.path.join(results_base_dir, CACHE_DIR_NAME)
    if not os.path.exists(cache_base):
        return {"entries": 0, "size_gb": 0.0}

    entries = 0
    total_bytes = 0
    try:
        for item in os.listdir(cache_base):
            item_path = os.path.join(cache_base, item)
            if os.path.isdir(item_path):
                entries += 1
                total_bytes += _get_dir_size(item_path)
    except Exception:
        pass

    return {
        "entries": entries,
        "size_gb": total_bytes / (1024 ** 3),
    }
