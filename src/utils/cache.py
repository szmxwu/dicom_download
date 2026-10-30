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
import stat
import threading
import time
import logging
from functools import wraps
from typing import Optional, Dict

from src.utils.offload import run_cpu_bound
from src.core.constants import get_filter_rules_fingerprint, CONVERSION_GEOMETRY_VERSION

# 使用共享 logger（项目约定），保证缓存步骤的耗时在 app.log 中可见
logger = logging.getLogger('DICOMApp')

CACHE_DIR_NAME = "cache"


def _link_or_copy(src: str, dst: str) -> None:
    """同卷优先硬链接（瞬时、零额外磁盘占用），失败时回退到完整复制。

    缓存目录与任务结果目录同处于 results/ 下（同卷），缓存文件均为
    只写一次的产物（.dcm/.nii.gz/.xlsx/.zip），硬链接是安全的：
    任务目录被清理守护进程删除后，缓存副本（链接）仍然有效，反之亦然。
    Windows NTFS 支持文件硬链接；FAT32/跨卷等场景自动回退复制。
    """
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def _copytree_link(src_dir: str, dst_dir: str) -> int:
    """递归复制目录树，文件层面优先硬链接。返回文件数。"""
    count = 0
    for root, _dirs, files in os.walk(src_dir):
        rel = os.path.relpath(root, src_dir)
        out_root = dst_dir if rel == '.' else os.path.join(dst_dir, rel)
        os.makedirs(out_root, exist_ok=True)
        for name in files:
            _link_or_copy(os.path.join(root, name), os.path.join(out_root, name))
            count += 1
    return count


def _rmtree_onerror(func, path, exc_info):
    """shutil.rmtree 的 onerror 回调：处理 Windows 只读文件/目录（WinError 5）。

    去除只读属性后重试同一操作；仍失败则重新抛出原异常，交给上层重试。
    """
    try:
        os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
        func(path)
        return
    except Exception:
        pass
    raise exc_info[1]


def _rmtree_robust(path: str, retries: int = 3, delay: float = 1.0) -> None:
    """Windows 兼容的目录删除。

    - 只读文件/目录：自动去除只读属性（WinError 5）
    - 文件被临时占用（WinError 32，如杀毒软件/Windows 索引器正在扫描）
      或目录暂时非空（WinError 145）：退避重试
    - 最后一次仍失败则抛出异常，由调用方记录
    """
    last_err = None
    for attempt in range(retries):
        try:
            shutil.rmtree(path, onerror=_rmtree_onerror)
            return
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                logger.debug(
                    f"[CACHE] rmtree {path} attempt {attempt + 1}/{retries} failed: {e}, retrying..."
                )
                time.sleep(delay)
    raise last_err


def _remove_file_robust(path: str, retries: int = 3, delay: float = 1.0) -> None:
    """与 _rmtree_robust 同理的单文件删除（去只读 + 占用重试）。"""
    for attempt in range(retries):
        try:
            os.remove(path)
            return
        except PermissionError:
            # WinError 5（只读）/ WinError 32（被占用）：去只读后重试
            try:
                os.chmod(path, stat.S_IWRITE)
            except OSError:
                pass
        if attempt < retries - 1:
            time.sleep(delay)
    # 最终一次，让原始异常抛出给调用方记录
    os.remove(path)


def _sanitize_accession(accession_number: str) -> str:
    """清理 AccessionNumber，使其适合作为目录名。"""
    return "".join(c for c in accession_number if c.isalnum() or c in "_-").strip()


def _compute_param_hash(accession_number: str, options: Dict) -> str:
    """根据参数计算缓存哈希。

    除下载参数外，还混入两项与过滤结果直接相关的因子：
    - derived_keywords：任务级关键词覆盖（options.derived_keywords），
      不同关键词集合必须落到不同的缓存条目；
    - filter_rules：constants.py 过滤规则指纹（关键词默认值/例外表/白名单），
      规则代码修改后旧缓存条目自动失配，避免旧过滤逻辑的结果被命中。
    """
    from src.core.mr_semantics import rules_fingerprint
    from src.core.mr_index import indexing_enabled
    task_keywords = options.get("derived_keywords")
    params = {
        "accession_number": accession_number,
        "modality_filter": options.get("modality_filter"),
        "min_series_files": options.get("min_series_files"),
        "exclude_derived": options.get("exclude_derived", True),
        "output_format": options.get("output_format", "nifti"),
        "conversion_geometry": CONVERSION_GEOMETRY_VERSION if options.get("output_format", "nifti") == "nifti" else None,
        "auto_metadata": options.get("auto_metadata", True),
        "auto_organize": options.get("auto_organize", True),
        "rename_mr_series": options.get("rename_mr_series", False),
        "derived_keywords": sorted(str(k).upper() for k in task_keywords) if task_keywords else None,
        "extra_derived_keywords": sorted(
            str(k).upper() for k in options.get("extra_derived_keywords") or []
        ) or None,
        "filter_rules": get_filter_rules_fingerprint(),
        "mr_semantic_rules": rules_fingerprint() if options.get('auto_metadata', True) or options.get('rename_mr_series') else None,
        "mr_frame_index": indexing_enabled(),
    }
    param_str = json.dumps(params, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(param_str.encode("utf-8")).hexdigest()[:8]


def get_cache_dir(results_base_dir: str, accession_number: str, options: Dict) -> str:
    """获取缓存目录路径。"""
    sanitized = _sanitize_accession(accession_number)
    param_hash = _compute_param_hash(accession_number, options)
    return os.path.join(results_base_dir, CACHE_DIR_NAME, f"{sanitized}_{param_hash}")


# 缓存读写与 LRU 淘汰的互斥状态。锁只保护微秒级集合操作，
# 但会同时被 eventlet greenlet 和 tpool 真实线程使用，必须是真实 OS 锁，
# 不能是 monkey-patched green Lock（D19 的 lost-wakeup 死锁风险）。
def _create_cache_state_lock():
    try:
        from eventlet.patcher import is_monkey_patched, original
        if is_monkey_patched('thread'):
            return original('threading').Lock()
    except ImportError:
        pass
    return threading.Lock()


_cache_state_lock = _create_cache_state_lock()
_cache_active_paths = set()
_cache_evicting_paths = set()


def _cache_path_key(path):
    return os.path.abspath(path)


def _try_mark_cache_active(path):
    key = _cache_path_key(path)
    with _cache_state_lock:
        if key in _cache_evicting_paths or key in _cache_active_paths:
            return False
        _cache_active_paths.add(key)
        return True


def _unmark_cache_active(path):
    with _cache_state_lock:
        _cache_active_paths.discard(_cache_path_key(path))


def _try_mark_evicting(path):
    key = _cache_path_key(path)
    with _cache_state_lock:
        if key in _cache_active_paths or key in _cache_evicting_paths:
            return False
        _cache_evicting_paths.add(key)
        return True


def _unmark_evicting(path):
    with _cache_state_lock:
        _cache_evicting_paths.discard(_cache_path_key(path))


def _cache_operation_guard(func):
    """保存/读取与 LRU 删除互斥；冲突时安全回退到 PACS 或下次重试。"""
    @wraps(func)
    def wrapper(cache_dir, *args, **kwargs):
        if not _try_mark_cache_active(cache_dir):
            if func.__name__ == 'save_to_cache':
                return False
            return {'success': False, 'error': 'cache operation in progress'}
        try:
            return func(cache_dir, *args, **kwargs)
        finally:
            _unmark_cache_active(cache_dir)
    return wrapper


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


@_cache_operation_guard
def copy_from_cache(cache_dir: str, target_dir: str, task_id: str) -> Dict:
    """从缓存复制结果到任务目录。"""
    organized_dir = os.path.join(cache_dir, "organized")
    zip_file = os.path.join(cache_dir, "result.zip")
    excel_file = os.path.join(cache_dir, "metadata.xlsx")

    # 目标路径
    target_organized = os.path.join(target_dir, "organized")
    target_zip = os.path.join(os.path.dirname(target_dir), f"result_{task_id}.zip")

    logger.info(f"[CACHE] Copying from cache: {cache_dir} -> {target_dir}")
    t0 = time.time()

    if os.path.exists(target_organized):
        _rmtree_robust(target_organized)
    n_files = _copytree_link(organized_dir, target_organized)

    if os.path.exists(target_zip):
        _remove_file_robust(target_zip)
    _link_or_copy(zip_file, target_zip)
    os.chmod(target_zip, 0o640)

    # 刷新目标 ZIP 的时间戳（关键，勿删）：
    # Windows 下硬链接沿用缓存原件的旧 mtime/ctime（NTFS 建链不刷新），
    # copy2 回退路径同样保留旧 mtime —— 结果清理守护进程会把这条"旧" ZIP
    # 当作超龄文件立即删掉，造成"任务 Completed 但 /api/download/zip 404"（K2）。
    # Linux 下 link() 会刷 ctime，这里一并刷新 mtime 统一行为。
    try:
        os.utime(target_zip, None)
    except OSError:
        pass

    # 刷新缓存条目的访问时间，保证 LRU 淘汰（按条目 atime 排序）把
    # 刚被命中的条目视为最近使用，避免热数据被误淘汰。
    try:
        os.utime(cache_dir, None)
    except OSError:
        pass

    # 复制 Excel 到目标 organized 目录
    target_excel = None
    if os.path.isfile(excel_file):
        target_excel = os.path.join(target_organized, os.path.basename(excel_file))
        _link_or_copy(excel_file, target_excel)

    logger.info(f"[CACHE] Copy from cache done: {n_files} files in {time.time() - t0:.1f}s")

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


@_cache_operation_guard
def save_to_cache(source_dir: str, cache_dir: str, zip_path: Optional[str] = None,
                  excel_path: Optional[str] = None) -> bool:
    """将任务结果保存到缓存目录。"""
    t0 = time.time()
    try:
        if os.path.exists(cache_dir):
            _rmtree_robust(cache_dir)
        os.makedirs(cache_dir, exist_ok=True)

        cache_organized = os.path.join(cache_dir, "organized")
        cache_zip = os.path.join(cache_dir, "result.zip")
        cache_excel = os.path.join(cache_dir, "metadata.xlsx")

        # 复制 organized 目录（同卷硬链接，避免 Windows 上小文件全量复制的巨大开销）
        n_files = 0
        if os.path.exists(cache_organized):
            _rmtree_robust(cache_organized)
        if os.path.isdir(source_dir):
            n_files = _copytree_link(source_dir, cache_organized)

        # 复制 ZIP，并显式保证缓存及硬链接目标可回传读取
        if zip_path and os.path.isfile(zip_path):
            _link_or_copy(zip_path, cache_zip)
            os.chmod(cache_zip, 0o640)

        # 复制 Excel
        if excel_path and os.path.isfile(excel_path):
            _link_or_copy(excel_path, cache_excel)

        logger.info(
            f"[CACHE] Saved to cache: {cache_dir} "
            f"({n_files} files in {time.time() - t0:.1f}s)"
        )

        # 保存成功后触发容量检查（LRU 淘汰）。异步执行，不阻塞任务收尾
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

# LRU 淘汰异步执行状态：同时只跑一轮；删除失败的条目记入冷却列表
_evict_lock = threading.Lock()
_evict_running = False
_evict_failures = {}  # path -> 上次失败时间戳
_EVICT_FAIL_COOLDOWN_SEC = 1800.0  # 30 分钟内不再重试删不掉的条目


def get_cache_max_gb() -> float:
    """读取缓存容量上限（GB）。"""
    try:
        val = float(os.getenv('CACHE_MAX_GB', str(DEFAULT_CACHE_MAX_GB)))
        return val if val > 0 else DEFAULT_CACHE_MAX_GB
    except Exception:
        return DEFAULT_CACHE_MAX_GB


def get_cache_min_free_gb() -> float:
    """缓存需要协助恢复的磁盘空闲水位（GB），默认跟随下载恢复线。"""
    try:
        default = os.getenv('DOWNLOAD_HIGH_WATERMARK_GB', '45')
        value = float(os.getenv('CACHE_MIN_FREE_GB', default))
        return value if value >= 0 else 0.0
    except Exception:
        return 0.0


def enforce_cache_limit(cache_base: str, async_run: bool = True) -> int:
    """LRU 淘汰入口：缓存超限时从最旧访问的条目开始删除。

    默认异步执行（后台线程，同时只跑一轮）：淘汰涉及全量目录扫描和大量删除，
    同步执行会拖慢 save_to_cache（表现为 "Generating results" 阶段卡住数秒~数十秒，
    尤其存在删不掉的条目时每轮重试都要等待退避）。

    Args:
        cache_base: 缓存根目录（results/cache）
        async_run: True 时立即返回（后台淘汰），False 时同步执行并返回淘汰数量
    """
    global _evict_running
    if not async_run:
        return _evict_lru_sync(cache_base)
    with _evict_lock:
        if _evict_running:
            return 0
        _evict_running = True

    def _bg():
        global _evict_running
        try:
            # eventlet 下后台线程是 greenlet，磁盘 I/O 不切换会让出不了事件循环；
            # run_cpu_bound 在 eventlet 下转真实 OS 线程，普通环境下直接调用
            run_cpu_bound(_evict_lru_sync, cache_base)
        except Exception as e:
            logger.warning(f"[CACHE] Background eviction failed: {e}")
        finally:
            with _evict_lock:
                _evict_running = False

    threading.Thread(target=_bg, daemon=True).start()
    return 0


def _find_readonly_file(path: str) -> Optional[str]:
    """在目录树中找出一个只读文件（用于淘汰失败时的成因诊断），没有则返回 None。"""
    try:
        for root, _dirs, files in os.walk(path):
            for name in files:
                fp = os.path.join(root, name)
                try:
                    if not os.stat(fp).st_mode & stat.S_IWRITE:
                        return fp
                except OSError:
                    continue
    except Exception:
        pass
    return None


def _evict_lru_sync(cache_base: str) -> int:
    """LRU 淘汰同步实现（扫描 + 删除）。

    删除失败的条目记入冷却列表（_evict_failures），冷却期内跳过，
    避免文件长期被占用（Excel 打开/杀毒扫描/权限问题）时每轮保存都反复重试。
    """
    if not os.path.isdir(cache_base):
        return 0

    max_gb = get_cache_max_gb()
    limit_bytes = max_gb * (1024 ** 3)
    min_free_gb = get_cache_min_free_gb()
    target_free_bytes = min_free_gb * (1024 ** 3)
    try:
        free_bytes = shutil.disk_usage(cache_base).free
    except Exception:
        free_bytes = None

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

    if total_bytes <= limit_bytes and (free_bytes is None or free_bytes >= target_free_bytes):
        return 0

    free_desc = 'unknown' if free_bytes is None else f'{free_bytes / (1024**3):.2f}'
    logger.info(
        f"[CACHE] Starting LRU eviction: cache={total_bytes / (1024**3):.2f}GB "
        f"(limit={max_gb}GB), free={free_desc}GB (target={min_free_gb}GB)"
    )
    entries.sort(key=lambda x: x[0])  # 最旧访问的在前

    evicted = 0
    skipped_locked = 0
    now = time.time()
    for _atime, size, path in entries:
        if total_bytes <= limit_bytes and (free_bytes is None or free_bytes >= target_free_bytes):
            break
        # 冷却期内跳过此前删不掉的条目（文件被 Excel/杀毒/索引器占用等）
        last_fail = _evict_failures.get(path)
        if last_fail is not None and now - last_fail < _EVICT_FAIL_COOLDOWN_SEC:
            skipped_locked += 1
            continue
        if not _try_mark_evicting(path):
            # 正在保存/读取或另一轮淘汰中的条目留到下一轮，避免半成品缓存。
            skipped_locked += 1
            continue
        try:
            _rmtree_robust(path)
            _evict_failures.pop(path, None)
            total_bytes -= size
            evicted += 1
            logger.info(f"[CACHE] Evicted: {os.path.basename(path)} ({size / (1024**3):.2f}GB)")
        except Exception as e:
            if not os.path.exists(path):
                # 路径已被其他清理者并发删除（如历史上的 cleanup 守护进程误删 cache）：
                # 视为淘汰成功，不计入冷却列表
                _evict_failures.pop(path, None)
                total_bytes -= size
                evicted += 1
                logger.info(f"[CACHE] Evicted (already removed concurrently): {os.path.basename(path)}")
            else:
                _evict_failures[path] = now
                # 附带诊断：区分"只读属性"与"文件被占用/权限"两类成因
                hint = _find_readonly_file(path)
                extra = f" [诊断: 存在只读文件 {hint}]" if hint else " [诊断: 非只读问题，文件可能被外部进程(Excel/杀毒/索引器)占用或 ACL 拒绝]"
                logger.warning(f"[CACHE] Failed to evict {path}: {e}{extra}")
        finally:
            _unmark_evicting(path)
            try:
                # 硬链接可能让删除逻辑目录并不释放空间，必须重新采样真实空闲量。
                free_bytes = shutil.disk_usage(cache_base).free
            except Exception:
                free_bytes = None

    if evicted or skipped_locked:
        final_free_desc = 'unknown' if free_bytes is None else f'{free_bytes / (1024**3):.2f}'
        logger.info(
            f"[CACHE] LRU eviction done: removed {evicted} entries, "
            f"skipped {skipped_locked} locked entries (cooldown), "
            f"cache now {total_bytes / (1024**3):.2f}GB (limit {max_gb}GB), "
            f"free={final_free_desc}GB (target={min_free_gb}GB)"
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
                    _rmtree_robust(item_path)
                    freed_bytes += size
                    cleared += 1
                elif os.path.isfile(item_path):
                    size = os.path.getsize(item_path)
                    _remove_file_robust(item_path)
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

