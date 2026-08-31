# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Flask Web 应用主模块

提供 DICOM 处理 Web 服务和 REST API 接口，支持：
- PACS 配置管理
- 单条/批量任务处理
- 文件上传处理
- WebSocket 实时通信
"""

# 优先使用 eventlet 异步模式，避免大量 Socket.IO 长轮询连接导致线程爆炸。
# eventlet 必须在其他库（特别是 threading、socket）导入前完成 monkey_patch。
try:
    import eventlet
    eventlet.monkey_patch()
    SOCKETIO_ASYNC_MODE = 'eventlet'
    logger_startup_mode = 'eventlet'
except ImportError:
    SOCKETIO_ASYNC_MODE = 'threading'
    logger_startup_mode = 'threading'

# logging 的锁必须是真实 OS 锁：monkey_patch 会把 threading.RLock 换成 green 锁，
# 而 tpool 真实线程（run_cpu_bound 卸载的 CPU 密集代码）里的 logger 调用会在真实
# 线程上注册 green waiter，事件循环唤醒时报 greenlet.error 且唤醒丢失 → greenlet
# 永久卡死（曾导致 C-MOVE 假死、_cmove_lock 永久占用、全服务无响应）。
# 注意：不能 `from src.utils.offload import ...`——app.py 必须先完成 monkey_patch
# 才能 import src 包，这里用函数内延迟导入。
def _fix_logging_locks_for_eventlet():
    try:
        from src.utils.offload import fix_logging_locks_for_eventlet
        fix_logging_locks_for_eventlet()
    except Exception:
        pass

_fix_logging_locks_for_eventlet()

import os
import sys
import stat

# 启动时加载项目根目录的 .env（PACS 连接参数、清理阈值等运行时配置）。
# 关键：必须在创建任何 DICOMDownloadClient、读取任何 os.getenv 之前执行，
# 否则服务会静默使用代码内的硬编码默认值（如错误的 CALLING_PORT/CALLING_AET），
# 导致 PACS 无法回传文件（C-MOVE 成功但收不到 C-STORE）。
# override=False：真实环境变量优先于 .env 文件。
try:
    from dotenv import load_dotenv
    _STARTUP_ENV_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        '.env'
    )
    load_dotenv(_STARTUP_ENV_PATH)
except Exception:
    pass  # python-dotenv 不可用时回退到代码内默认值
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Queue, Empty

# 将项目根目录添加到 Python 路径（确保能找到 src 模块）
# 从 src/web/app.py 向上两级到达项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Flask/Jinja2 3.0+ 兼容性修复
# 在导入 Flask 之前先修复所有兼容性问题
try:
    from markupsafe import Markup, escape
    import sys
    # 创建 jinja2 模块的兼容性层
    import jinja2
    jinja2.Markup = Markup
    jinja2.escape = escape
    sys.modules['jinja2'].Markup = Markup
    sys.modules['jinja2'].escape = escape
except ImportError:
    pass

from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import time
import uuid
import threading
import shutil
import logging
from logging.handlers import RotatingFileHandler
from werkzeug.utils import secure_filename

from dotenv import set_key
import secrets

# 导入我们的DICOM处理客户端
from src.client.unified import DICOMDownloadClient
from src.utils.packaging import create_result_zip
# monkey_patch 已完成（见文件头部），此处导入 offload 是安全的。
# create_result_zip/save_to_cache 涉及 GB 级磁盘读写，在 contended HDD 上的
# 阻塞式 read/write 会把整个 eventlet hub 冻住（页面"卡死"），必须经 tpool 卸载。
from src.utils.offload import run_cpu_bound
from src.utils.cache import (
    get_cache_dir, cache_exists, copy_from_cache,
    save_to_cache, clear_all_cache, get_cache_stats,
    CACHE_DIR_NAME
)
from src.core.constants import (
    DEFAULT_DERIVED_SERIES_KEYWORDS,
    get_derived_keywords
)

def get_base_path():
    """获取程序运行时的根目录路径，兼容 PyInstaller 打包"""
    if hasattr(sys, '_MEIPASS'):
        return sys._MEIPASS
    return os.path.abspath(".")

def get_project_root():
    """获取项目根目录路径（从 src/web/ 向上两级）"""
    current_file = os.path.abspath(__file__)
    # src/web/app.py -> 向上两级到达项目根目录
    return os.path.dirname(os.path.dirname(os.path.dirname(current_file)))

# Flask应用配置 - 指定静态文件和模板路径（从项目根目录查找）
project_root = get_project_root()
app = Flask(__name__,
            static_folder=os.path.join(project_root, 'static'),
            template_folder=os.path.join(project_root, 'templates'))

_secret_key = os.environ.get('FLASK_SECRET_KEY')
if not _secret_key:
    # 仅用于本地/临时运行；生产环境请通过环境变量提供固定值
    _secret_key = secrets.token_hex(32)
app.config['SECRET_KEY'] = _secret_key
app.config['UPLOAD_FOLDER'] = os.path.abspath('./uploads')
app.config['RESULT_FOLDER'] = os.path.abspath('./results')
app.config['MAX_CONTENT_LENGTH'] = 1500 * 1024 * 1024  # 1500MB最大文件大小

# 创建必要的目录
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULT_FOLDER'], exist_ok=True)
os.makedirs('./temp', exist_ok=True)
os.makedirs('./logs', exist_ok=True)

# 配置日志系统
def setup_logging():
    logger = logging.getLogger('DICOMApp')
    logger.setLevel(logging.INFO)
    
    # 防止重复添加 handler
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    )

    # 文件日志 (按大小回滚)
    file_handler = RotatingFileHandler(
        'logs/app.log', maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 把 pynetdicom 的日志（DIMSE 超时、关联中止、C-STORE 错误等）并入 app.log。
    # 历史上 pynetdicom 只写自己的 logger，DIMSE 超时等关键事件在 app.log 中完全
    # 不可见，极大增加假死问题的定位难度。
    # 级别设为 WARNING：DIMSE 超时/关联异常都是 ERROR/WARNING，不会漏；
    # INFO 级含每个 C-STORE 一行（"Received Store Request"），会刷屏。
    for _pynetdicom_logger_name in ('pynetdicom', 'pynetdicom.dimse', 'pynetdicom.association', 'pynetdicom.events'):
        _pl = logging.getLogger(_pynetdicom_logger_name)
        _pl.setLevel(logging.WARNING)
        if file_handler not in _pl.handlers:
            _pl.addHandler(file_handler)
        _pl.propagate = False

    return logger

logger = setup_logging()

# WebSocket支持
socketio = SocketIO(app, cors_allowed_origins="*", async_mode=SOCKETIO_ASYNC_MODE)

# ============ 任务队列系统配置 ============
MAX_CONCURRENT_TASKS = 3  # 最大并发任务数
TASK_QUEUE_MAX_SIZE = 100  # 任务队列最大长度

# 全局线程池执行器 - 限制并发数
task_executor = ThreadPoolExecutor(
    max_workers=MAX_CONCURRENT_TASKS,
    thread_name_prefix='TaskWorker'
)

# 任务队列 - 存储待处理的任务ID
task_queue = Queue(maxsize=TASK_QUEUE_MAX_SIZE)

# 队列锁 - 保护队列操作
_queue_lock = threading.Lock()

# 当前正在运行的任务数
_running_task_count = 0

# 全局变量存储处理任务
processing_tasks = {}
_task_lock = threading.Lock()  # 任务字典锁，防止竞态条件

# 已完成任务缓存，避免每次请求全量序列化
_completed_tasks_lock = threading.Lock()
_completed_tasks_cache = []
_completed_task_ids = set()
_serialized_history_cache = []
MAX_COMPLETED_TASK_CACHE = 500  # 已完成任务缓存上限，防止内存无限增长

# 任务取消标志字典 - 用于真正中断任务执行
_task_cancel_flags = {}

# 任务清理配置
TASK_MAX_AGE_HOURS = 24  # 任务最大保留时间（小时）
TASK_CLEANUP_INTERVAL = 3600  # 清理检查间隔（秒）


def _cleanup_old_tasks():
    """后台线程：定期清理过期任务"""
    while True:
        try:
            time.sleep(TASK_CLEANUP_INTERVAL)
            _do_cleanup_old_tasks()
        except Exception as e:
            logger.error(f"任务清理失败: {e}")


# ============ 任务队列管理函数 ============

def _get_queue_status():
    """获取队列状态"""
    global _running_task_count
    with _queue_lock:
        pending_count = task_queue.qsize()
        running_count = _running_task_count
        return {
            'pending': pending_count,
            'running': running_count,
            'max_concurrent': MAX_CONCURRENT_TASKS,
            'queue_capacity': TASK_QUEUE_MAX_SIZE,
            'utilization': round(running_count / MAX_CONCURRENT_TASKS * 100, 1) if MAX_CONCURRENT_TASKS > 0 else 0
        }


def _submit_task_to_queue(task):
    """
    提交任务到队列系统

    Args:
        task: ProcessingTask 实例

    Returns:
        bool: 是否成功提交
    """
    global _running_task_count

    try:
        with _queue_lock:
            # 检查队列是否已满
            if task_queue.full():
                logger.warning(f"Task queue is full, rejecting task {task.task_id}")
                task.update_status('failed', error='Task queue is full, please try again later')
                return False

            # 将任务加入队列
            task_queue.put(task.task_id)
            task.update_status('pending', 0, 'Waiting in queue')
            task.add_log(f'Task queued, position: {task_queue.qsize()}')
            logger.info(f"Task {task.task_id} added to queue (queue size: {task_queue.qsize()})")

        # 尝试启动队列处理器
        _process_queue()
        return True

    except Exception as e:
        logger.error(f"Failed to submit task {task.task_id} to queue: {e}")
        task.update_status('failed', error=f'Failed to queue task: {str(e)}')
        return False


def _process_queue():
    """处理队列中的任务"""
    global _running_task_count

    with _queue_lock:
        # 检查当前运行任务数是否已达上限
        if _running_task_count >= MAX_CONCURRENT_TASKS:
            logger.debug(f"Max concurrent tasks ({MAX_CONCURRENT_TASKS}) reached, waiting...")
            return

        # 尝试获取队列中的下一个任务
        try:
            task_id = task_queue.get_nowait()
        except Empty:
            return  # 队列为空

        # 增加运行计数
        _running_task_count += 1

    # 在锁外执行任务
    try:
        task = processing_tasks.get(task_id)
        if not task:
            logger.warning(f"Task {task_id} not found in processing_tasks")
            with _queue_lock:
                _running_task_count -= 1
            return

        if task.is_cancelled():
            logger.info(f"Task {task_id} was cancelled, skipping")
            with _queue_lock:
                _running_task_count -= 1
            return

        # 确定任务类型并调用相应函数
        if task.task_type == 'single':
            task_func = process_single_task
        elif task.task_type == 'batch':
            task_func = process_batch_task
        elif task.task_type == 'upload':
            task_func = process_upload_task
        else:
            logger.error(f"Unknown task type: {task.task_type}")
            with _queue_lock:
                _running_task_count -= 1
            return

        # 提交到线程池执行
        def _task_wrapper():
            global _running_task_count
            # 记录实际开始执行时间：耗时统计从出队这一刻起算，不含排队等待
            task.start_time = time.time()
            queue_wait = task.start_time - task.created_at
            if queue_wait > 1:
                task.add_log(f'Queue wait: {queue_wait:.0f}s (not counted in processing duration)')
            try:
                task_func(task)
            except Exception as e:
                logger.error(f"Task {task_id} execution error: {e}")
                try:
                    task.update_status('failed')
                    task.error = str(e)
                except Exception:
                    pass
            finally:
                with _queue_lock:
                    _running_task_count -= 1
                # 尝试处理队列中的下一个任务
                _process_queue()

        # 在线程池中执行
        task_executor.submit(_task_wrapper)
        logger.info(f"Task {task_id} started execution (running: {_running_task_count})")

    except Exception as e:
        logger.error(f"Error processing queue: {e}")
        with _queue_lock:
            _running_task_count = max(0, _running_task_count - 1)
        _process_queue()  # 尝试下一个


def _do_cleanup_old_tasks():
    """执行清理过期任务"""
    cutoff_time = time.time() - (TASK_MAX_AGE_HOURS * 3600)
    cleaned_count = 0

    with _task_lock:
        # 找出过期任务
        expired_tasks = []
        for task_id, task in processing_tasks.items():
            if task.end_time and task.end_time < cutoff_time:
                expired_tasks.append(task_id)

        # 删除过期任务
        for task_id in expired_tasks:
            del processing_tasks[task_id]
            _task_cancel_flags.pop(task_id, None)
            cleaned_count += 1

    if cleaned_count > 0:
        logger.info(f"清理了 {cleaned_count} 个过期任务（超过 {TASK_MAX_AGE_HOURS} 小时）")


# 启动后台清理线程
cleanup_thread = threading.Thread(target=_cleanup_old_tasks, daemon=True)
cleanup_thread.start()

# 创建DICOM客户端实例用于系统状态检查（不登录）
dicom_client_checker = DICOMDownloadClient()

ENV_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')


def _parse_port(value, field_name: str) -> int:
    if value is None:
        raise ValueError(f"{field_name} is required")
    try:
        port = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be an integer")
    if port < 1 or port > 65535:
        raise ValueError(f"{field_name} must be between 1 and 65535")
    return port


def _normalize_aet(value, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required")
    aet = str(value).strip()
    if not aet:
        raise ValueError(f"{field_name} is required")
    if len(aet) > 16:
        raise ValueError(f"{field_name} must be at most 16 characters")
    # DICOM AE Titles are typically uppercase ASCII
    aet = aet.upper()
    for ch in aet:
        if ord(ch) < 32 or ord(ch) > 126:
            raise ValueError(f"{field_name} contains invalid characters")
    return aet


def _normalize_host(value) -> str:
    if value is None:
        raise ValueError("PACS_IP is required")
    host = str(value).strip()
    if not host:
        raise ValueError("PACS_IP is required")
    if len(host) > 255:
        raise ValueError("PACS_IP is too long")
    return host

# 自动清理配置（从 .env 读取，若不存在则写回 .env）
try:
    CLEANUP_THRESHOLD_GB = float(os.getenv('CLEANUP_THRESHOLD_GB', '50'))
except Exception:
    CLEANUP_THRESHOLD_GB = 50.0

try:
    CLEANUP_TARGET_GB = float(os.getenv('CLEANUP_TARGET_GB', '40'))
except Exception:
    CLEANUP_TARGET_GB = 40.0

# 磁盘剩余空间触发阈值（GB）：当 results 所在磁盘剩余空间低于该值时，
# 无论 results 目录大小如何都强制触发清理，避免磁盘被写满
try:
    CLEANUP_MIN_FREE_GB = float(os.getenv('CLEANUP_MIN_FREE_GB', '50'))
except Exception:
    CLEANUP_MIN_FREE_GB = 50.0

# 周期性清理检查间隔（秒）
try:
    DISK_CLEANUP_INTERVAL_SEC = int(os.getenv('DISK_CLEANUP_INTERVAL_SEC', '300'))
except Exception:
    DISK_CLEANUP_INTERVAL_SEC = 300

# 清理最小年龄（分钟）：早于此年龄的结果目录/ZIP 一律不删。
# 关键保护：任务完成后客户端需要窗口期下载 ZIP；且 Windows 下旧文件常被
# 杀毒/索引器/Excel 锁定删不掉，若无年龄保护，清理循环会一直推进到
# 最新生成的 ZIP（实测任务完成后 1.5 秒 ZIP 即被删，客户端稳定 404）
try:
    CLEANUP_MIN_AGE_MINUTES = float(os.getenv('CLEANUP_MIN_AGE_MINUTES', '60'))
except Exception:
    CLEANUP_MIN_AGE_MINUTES = 60.0

# 清理并发保护：同一时刻只允许一个清理任务运行
_cleanup_lock = threading.Lock()

# 将读取到的默认值持久化到 .env，便于用户修改与持久化配置
try:
    # 写入整数值时保留整型格式，浮点数保留原样
    thr_val = str(int(CLEANUP_THRESHOLD_GB) if float(CLEANUP_THRESHOLD_GB).is_integer() else CLEANUP_THRESHOLD_GB)
    tgt_val = str(int(CLEANUP_TARGET_GB) if float(CLEANUP_TARGET_GB).is_integer() else CLEANUP_TARGET_GB)
    free_val = str(int(CLEANUP_MIN_FREE_GB) if float(CLEANUP_MIN_FREE_GB).is_integer() else CLEANUP_MIN_FREE_GB)
    set_key(ENV_FILE_PATH, 'CLEANUP_THRESHOLD_GB', thr_val)
    set_key(ENV_FILE_PATH, 'CLEANUP_TARGET_GB', tgt_val)
    set_key(ENV_FILE_PATH, 'CLEANUP_MIN_FREE_GB', free_val)
    age_val = str(int(CLEANUP_MIN_AGE_MINUTES) if float(CLEANUP_MIN_AGE_MINUTES).is_integer() else CLEANUP_MIN_AGE_MINUTES)
    set_key(ENV_FILE_PATH, 'CLEANUP_MIN_AGE_MINUTES', age_val)
except Exception as e:
    logger.warning(f"无法将清理阈值写入 {ENV_FILE_PATH}: {e}")

def get_directory_size(directory):
    """计算目录总大小（GB）"""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.exists(filepath):
                    total_size += os.path.getsize(filepath)
    except (OSError, IOError) as e:
        logger.warning(f"计算目录大小时出错: {str(e)}")
    return total_size / (1024 ** 3)  # 转换为GB

def _remove_readonly(func, path, exc_info):
    """shutil.rmtree 的 onerror 回调：尝试移除只读属性后重试删除"""
    try:
        os.chmod(path, stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
        func(path)
    except Exception:
        pass


def _force_remove_file(filepath):
    """强制删除单个文件，处理 Windows 句柄未释放问题"""
    try:
        os.chmod(filepath, stat.S_IWRITE)
        os.remove(filepath)
        return True
    except PermissionError:
        # Windows: 文件可能被占用，尝试重命名后删除（释放句柄的 trick）
        try:
            temp_name = filepath + '.deleteme'
            os.rename(filepath, temp_name)
            os.chmod(temp_name, stat.S_IWRITE)
            os.remove(temp_name)
            return True
        except Exception:
            pass
    except Exception:
        pass
    return False


def _safe_rmtree(dirpath):
    """安全删除目录树，处理 Windows 上的权限和句柄问题"""
    if not os.path.exists(dirpath):
        return True

    # 策略1: 使用 onerror 回调的 shutil.rmtree
    try:
        shutil.rmtree(dirpath, onerror=_remove_readonly)
        if not os.path.exists(dirpath):
            return True
    except Exception:
        pass

    # 策略2: 逐个文件删除，对顽固文件使用重命名 trick
    failed_paths = []
    for root, dirs, files in os.walk(dirpath, topdown=False):
        for name in files:
            fpath = os.path.join(root, name)
            if not _force_remove_file(fpath):
                failed_paths.append(fpath)
        for name in dirs:
            dpath = os.path.join(root, name)
            try:
                os.rmdir(dpath)
            except OSError:
                failed_paths.append(dpath)

    try:
        os.rmdir(dirpath)
        return True
    except OSError:
        if os.path.exists(dirpath):
            failed_paths.append(dirpath)

    if failed_paths:
        logger.warning(f"清理无法删除 {len(failed_paths)} 个路径（可能仍被占用）: {failed_paths[:3]}...")
    return len(failed_paths) == 0


def _get_free_space_gb(path):
    """获取 path 所在磁盘的真实剩余空间（GB）"""
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except Exception:
        return None


def cleanup_old_results():
    """清理旧的结果文件，保持磁盘空间在合理范围内

    触发条件（满足其一即清理）：
    1. results 目录大小 >= CLEANUP_THRESHOLD_GB（原有逻辑）
    2. results 所在磁盘剩余空间 < CLEANUP_MIN_FREE_GB（防止磁盘被写满，
       无论 results 目录本身多大都会触发）
    """
    results_dir = app.config['RESULT_FOLDER']

    # 并发保护：已有清理任务运行时跳过，避免多个清理线程互相竞争
    if not _cleanup_lock.acquire(blocking=False):
        logger.debug("Another cleanup task is running, skipping")
        return

    try:
        current_size = get_directory_size(results_dir)
        free_gb = _get_free_space_gb(results_dir)

        size_triggered = current_size >= CLEANUP_THRESHOLD_GB
        free_triggered = free_gb is not None and free_gb < CLEANUP_MIN_FREE_GB
        if not size_triggered and not free_triggered:
            return

        if free_triggered:
            logger.warning(
                f"磁盘剩余空间不足 (free {free_gb:.2f}GB < {CLEANUP_MIN_FREE_GB}GB), 强制启动清理"
            )
        free_desc = f"{free_gb:.2f}GB" if free_gb is not None else "未知"
        logger.info(f"结果目录大小: {current_size:.2f}GB, 磁盘剩余: {free_desc}, 启动自动清理")

        # 获取所有子目录（任务目录和ZIP文件）
        items_to_check = []

        try:
            # 扫描所有文件和目录
            for item in os.listdir(results_dir):
                if item == CACHE_DIR_NAME:
                    continue  # 缓存目录自管理（LRU），不扫描、不清理
                item_path = os.path.join(results_dir, item)
                if os.path.exists(item_path):
                    # 有效时间戳取 mtime/ctime 的较新值，不用 atime：
                    # - 本函数自身的 os.walk 大小扫描会读目录，relatime 下会把
                    #   目录 atime 刷成"现在"，atime 在本场景毫无参考价值
                    # - Windows 默认不更新 atime；其 ctime 即创建时间，可保护新建文件
                    # - mtime 反映内容最后写入（任务完成时刻）
                    try:
                        ts = max(
                            os.path.getmtime(item_path),
                            os.path.getctime(item_path),
                        )
                    except OSError:
                        continue  # 扫描瞬间被并发删除/锁定，跳过
                    size = 0

                    if os.path.isfile(item_path):
                        size = os.path.getsize(item_path) / (1024 ** 3)
                    elif os.path.isdir(item_path):
                        size = get_directory_size(item_path)

                    items_to_check.append({
                        'path': item_path,
                        'name': item,
                        'ts': ts,
                        'size': size,
                        'is_dir': os.path.isdir(item_path)
                    })

        except Exception as e:
            logger.error(f"扫描结果目录失败: {str(e)}")
            return

        # 排除正在进行的任务
        active_task_ids = [task.task_id for task in processing_tasks.values()
                          if task.status in ['running', 'pending']]

        # 过滤掉正在进行的任务
        min_age_sec = CLEANUP_MIN_AGE_MINUTES * 60
        now = time.time()
        items_to_clean = []
        skipped_young = 0
        for item in items_to_check:
            # 缓存目录由 cache.py 自己的 LRU 淘汰管理，严禁在这里删除：
            # 历史上曾整树删除 38GB 缓存（"删除: cache (38.17GB)"），
            # 既清空缓存导致重复下载，又与 LRU 淘汰/缓存写入竞争产生
            # WinError 3（路径被并发删除）/WinError 5（部分删除残留）
            if item['name'] == CACHE_DIR_NAME:
                continue

            # 年龄保护：过新的结果（刚完成的任务 ZIP/目录）绝不清理。
            # 否则当旧文件被锁定删不掉时，清理循环会一直推进到最新文件
            if now - item['ts'] < min_age_sec:
                skipped_young += 1
                continue

            # 检查是否为活跃任务目录
            is_active = False
            for task_id in active_task_ids:
                if task_id in item['name']:
                    is_active = True
                    break

            if not is_active:
                items_to_clean.append(item)

        if skipped_young:
            logger.info(f"清理跳过 {skipped_young} 个不足 {CLEANUP_MIN_AGE_MINUTES:.0f} 分钟的新文件/目录")

        if not items_to_clean:
            logger.info("所有文件都属于活跃任务，跳过清理")
            return

        # 按有效时间戳排序，先删除最旧的
        items_to_clean.sort(key=lambda x: x['ts'])

        # 计算需要清理的总量：取"目录大小达标"与"剩余空间达标"所需删除量的较大值
        target_to_clean = 0.0
        if size_triggered:
            target_to_clean = max(target_to_clean, current_size - CLEANUP_TARGET_GB)
        if free_triggered:
            # 清理到剩余空间恢复至 CLEANUP_MIN_FREE_GB + 10GB 缓冲
            target_to_clean = max(target_to_clean, (CLEANUP_MIN_FREE_GB + 10.0) - free_gb)

        cleaned_size = 0
        for item in items_to_clean:
            if cleaned_size >= target_to_clean:
                break

            try:
                logger.info(f"删除: {item['name']} ({item['size']:.2f}GB)")

                if item['is_dir']:
                    _safe_rmtree(item['path'])
                else:
                    if not _force_remove_file(item['path']):
                        logger.warning(f"无法删除文件（可能仍被占用）: {item['name']}")

                # 如果删除成功，目录/文件应该不存在了
                if not os.path.exists(item['path']):
                    cleaned_size += item['size']

            except Exception as e:
                logger.error(f"删除 {item['name']} 失败: {str(e)}")

        final_size = get_directory_size(results_dir)
        final_free = _get_free_space_gb(results_dir)
        logger.info(f"清理完成: {current_size:.2f}GB → {final_size:.2f}GB (清理了 {cleaned_size:.2f}GB)")
        if free_triggered and final_free is not None and final_free < CLEANUP_MIN_FREE_GB:
            logger.error(
                f"⚠️ 已删除所有可清理项，但磁盘剩余空间仍不足 "
                f"({final_free:.2f}GB < {CLEANUP_MIN_FREE_GB}GB)，"
                f"磁盘可能被 results 目录以外的数据占满"
            )
    finally:
        _cleanup_lock.release()

def check_and_cleanup_results():
    """检查并清理结果目录的后台任务"""
    def cleanup_thread():
        try:
            cleanup_old_results()
        except Exception as e:
            logger.error(f"自动清理失败: {str(e)}")
    
    # 异步执行清理，避免阻塞主线程（并发由 _cleanup_lock 保护）
    threading.Thread(target=cleanup_thread, daemon=True).start()


def _periodic_disk_cleanup_loop():
    """后台线程：周期性检查磁盘并清理。

    原清理只在任务完成后触发，批量下载过程中磁盘写满时清理不会运行；
    现在无论是否有任务完成，都按 DISK_CLEANUP_INTERVAL_SEC 周期检查。
    """
    while True:
        time.sleep(DISK_CLEANUP_INTERVAL_SEC)
        try:
            cleanup_old_results()
        except Exception as e:
            logger.error(f"周期性磁盘清理失败: {e}")


_periodic_cleanup_thread = threading.Thread(
    target=_periodic_disk_cleanup_loop, daemon=True, name='PeriodicDiskCleanup'
)
_periodic_cleanup_thread.start()

class ProcessingTask:
    """处理任务类 - 修复版"""
    def __init__(self, task_id, task_type, parameters):
        self.task_id = task_id
        self.task_type = task_type
        self.parameters = parameters
        self.status = 'pending'
        self.progress = 0
        self.current_step = ''
        self.steps = []
        self.result = None
        self.error = None
        self.created_at = time.time()  # 入队时间
        self.start_time = None         # 实际开始执行时间（出队后由 _task_wrapper 设置）
        self.end_time = None
        self.logs = []
        self._cancelled = False  # 取消标志
        self._log_buffer = []     # 日志缓冲区，用于批量发送
        self._last_emit_time = 0  # 上次发送时间
        self._emit_interval = 0.5  # 最小发送间隔（秒）

    def get_duration(self):
        """实际处理时长（秒，不含排队等待）。尚未开始执行时返回 None。

        批量提交时任务可能在队列中排队很久，排队时间不属于下载/处理耗时，
        否则 web 页面的单检查耗时会严重失真。
        """
        if self.start_time is None:
            return None
        return (self.end_time or time.time()) - self.start_time

    def get_queue_wait(self):
        """排队等待时长（秒）。已开始的返回实际等待值，排队中返回到当前为止的等待值。"""
        if self.start_time is not None:
            return self.start_time - self.created_at
        if self.status == 'pending':
            return time.time() - self.created_at
        return None

    def is_cancelled(self):
        """检查任务是否被取消"""
        # 检查本地标志
        if self._cancelled:
            return True
        # 检查全局取消标志
        return _task_cancel_flags.get(self.task_id, False)

    def cancel(self):
        """标记任务为已取消"""
        self._cancelled = True
        _task_cancel_flags[self.task_id] = True
        self.status = 'cancelled'
        self.end_time = time.time()

    def check_cancellation(self, step_name=""):
        """检查取消标志，如已取消则抛出异常中断执行"""
        if self.is_cancelled():
            raise InterruptedError(f"Task {self.task_id} cancelled at step: {step_name}")

    def _should_emit(self):
        """判断是否应该发送WebSocket更新（节流控制）"""
        current_time = time.time()
        if current_time - self._last_emit_time >= self._emit_interval:
            self._last_emit_time = current_time
            return True
        return False

    def _emit_update(self, force=False):
        """发送WebSocket更新（内部方法）"""
        if not force and not self._should_emit():
            return
        
        try:
            socketio.emit('task_update', {
                'task_id': self.task_id,
                'status': self.status,
                'progress': self.progress,
                'current_step': self.current_step,
                'logs': self.logs[-100:],  # 发送最新100条日志，确保前端能看到完整历史
                'logs_total': len(self.logs)  # 全量条数，供前端做窗口对齐的增量渲染
            })
        except Exception as e:
            logger.error(f"WebSocket发送失败: {str(e)}")

    def add_log(self, message, level='info'):
        """添加日志"""
        log_entry = {
            'timestamp': time.strftime('%H:%M:%S'),
            'level': level,
            'message': message
        }
        self.logs.append(log_entry)
        
        # 使用统一日志系统记录
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(f"[Task {self.task_id}] {message}")
        
        # 通过WebSocket发送更新（节流控制）
        self._emit_update()

    def update_status(self, status, progress=None, step=None):
        """更新任务状态"""
        # 如果任务已取消，不再更新状态
        if self.is_cancelled() and status not in ['cancelled', 'failed']:
            logger.warning(f"Task {self.task_id} is cancelled, ignoring status update to {status}")
            return
        
        self.status = status
        # 终态时锁定计时终点：任何失败/完成分支忘记设置 end_time 都会导致
        # get_duration() 退化为 (now - start_time)，监控页面的耗时显示无限增长
        # （2026-08-31 实证："No result returned" 失败分支未设 end_time，
        # 实际处理 2.4 分钟的任务在监控页显示 11+ 分钟且持续跳动）
        if status in ('completed', 'failed', 'cancelled') and self.end_time is None:
            self.end_time = time.time()
        if progress is not None:
            self.progress = progress
        if step is not None:
            self.current_step = step
        
        # 使用 logger 记录状态转换
        logger.info(f"Task {self.task_id} status update: {status} ({progress or 0}% - {step or 'N/A'})")
        
        # 通过WebSocket发送更新（强制发送状态变更）
        self._emit_update(force=True)


class TaskLogHandler(logging.Handler):
    """将标准日志输出捕获到 ProcessingTask 的日志队列中。

    在任务处理期间临时安装到 root logger，使所有通过 logging
    输出的日志（包括 Flask/Werkzeug 错误、第三方库日志等）
    都能同步显示在 Web UI 的 process_log 容器中。
    """
    def __init__(self, task: ProcessingTask):
        super().__init__()
        self.task = task
        # 捕获 INFO 及以上级别
        self.setLevel(logging.INFO)

    def emit(self, record: logging.LogRecord) -> None:
        # 避免递归：若当前正在由 Task.add_log 触发，则跳过
        if record.name == 'DICOMApp' and f"[Task {self.task.task_id}]" in record.getMessage():
            return
        # 过滤掉 Werkzeug 高频访问日志，避免刷屏
        if record.name == 'werkzeug':
            return
        # 将日志级别映射为前端使用的级别名称
        level_map = {
            logging.DEBUG: 'debug',
            logging.INFO: 'info',
            logging.WARNING: 'warning',
            logging.ERROR: 'error',
            logging.CRITICAL: 'error',
        }
        level = level_map.get(record.levelno, 'info')
        # 组装时间戳与消息
        timestamp = time.strftime('%H:%M:%S', time.localtime(record.created))
        message = self.format(record)
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'message': message,
        }
        self.task.logs.append(log_entry)
        self.task._emit_update()


def _attach_task_log_handler(task: ProcessingTask):
    """为当前任务附加日志捕获 Handler，返回该 Handler 以便后续移除。

    附加到 root logger 并临时将其级别降为 INFO，从而捕获所有模块的
    INFO 及以上日志（包括 DICOMApp、Flask、第三方库等）。
    """
    handler = TaskLogHandler(task)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    # 保存原始级别并在任务期间临时设为 INFO
    original_level = root.level
    if original_level > logging.INFO:
        root.setLevel(logging.INFO)

    root.addHandler(handler)
    return handler, original_level


def _detach_task_log_handler(handler_and_level):
    """移除任务日志捕获 Handler 并恢复 root logger 原始级别。"""
    if not handler_and_level:
        return
    handler, original_level = handler_and_level
    if handler:
        try:
            logging.getLogger().removeHandler(handler)
        except Exception:
            pass
    try:
        logging.getLogger().setLevel(original_level)
    except Exception:
        pass


@app.route('/api/debug/test-connection')
def test_connection():
    """测试PACS连接的调试接口"""
    try:
        client = DICOMDownloadClient()
        status = client.check_status()
        
        return jsonify({
            'pacs_connected': status,
            'pacs_config': {
                'ip': client.pacs_config['PACS_IP'],
                'port': client.pacs_config['PACS_PORT'],
                'calling_aet': client.pacs_config['CALLING_AET'],
                'called_aet': client.pacs_config['CALLED_AET']
            },
            'message': 'PACS connection OK' if status else 'PACS connection failed'
        })
    except Exception as e:
        return jsonify({
            'error': str(e),
            'message': 'Error occurred during test connection'
        }), 500
# 路由定义
@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')

@app.route('/api/client/download')
def download_client_script():
    """下载 CLI 客户端脚本"""
    file_name = request.args.get('file', 'download.py')
    # 只允许下载这两个脚本，防止路径遍历
    allowed = {'download.py', 'download_batch.py'}
    if file_name not in allowed:
        file_name = 'download.py'
    cli_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cli', file_name)
    return send_file(cli_path, as_attachment=True, download_name=file_name, mimetype='text/x-python')

@app.route('/api/process/single', methods=['POST'])
def process_single():
    """处理单个AccessionNumber"""
    logger.debug(f"process_single被调用，IP: {request.remote_addr}")
    try:
        data = request.json
        accession_number = data.get('accession_number')
        options = data.get('options', {})

        if not accession_number:
            return jsonify({'error': 'Please provide AccessionNumber'}), 400

        # INFO 级审计：记录提交来源与完整 options（历史上出现过客户端自定义
        # derived_keywords 覆盖默认列表导致过滤漏网，DEBUG 级日志无法事后溯源）
        logger.info(f"process_single submit: IP={request.remote_addr}, accession={accession_number}, options={options}")
        
        # 创建任务
        task_id = str(uuid.uuid4())
        task = ProcessingTask(task_id, 'single', {
            'accession_number': accession_number,
            'options': options
        })
        
        processing_tasks[task_id] = task

        # 提交到任务队列
        if not _submit_task_to_queue(task):
            return jsonify({'error': 'Task queue is full, please try again later'}), 503

        return jsonify({
            'task_id': task_id,
            'status': 'queued',
            'message': f'任务已加入队列，等待处理AccessionNumber: {accession_number}'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/process/batch', methods=['POST'])
def process_batch():
    """批量处理多个AccessionNumber"""
    try:
        data = request.json
        accession_numbers = data.get('accession_numbers', [])
        options = data.get('options', {})

        if not accession_numbers:
            return jsonify({'error': 'Please provide AccessionNumber list'}), 400

        # INFO 级审计：记录提交来源与完整 options（accession 列表可能很长，只记数量）
        logger.info(f"process_batch submit: IP={request.remote_addr}, accessions={len(accession_numbers)}, options={options}")
        
        # 创建任务
        task_id = str(uuid.uuid4())
        task = ProcessingTask(task_id, 'batch', {
            'accession_numbers': accession_numbers,
            'options': options
        })
        
        processing_tasks[task_id] = task

        # 提交到任务队列
        if not _submit_task_to_queue(task):
            return jsonify({'error': 'Task queue is full, please try again later'}), 503

        return jsonify({
            'task_id': task_id,
            'status': 'queued',
            'message': f'批量任务已加入队列，等待处理 {len(accession_numbers)} 个研究'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/process/upload', methods=['POST'])
def process_upload():
    """处理上传的ZIP文件"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.lower().endswith('.zip'):
            return jsonify({'error': 'Only ZIP files are supported'}), 400
        
        # 保存上传的文件
        filename = secure_filename(file.filename)
        timestamp = int(time.time())
        filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # 获取处理选项
        options = {}
        for key in request.form:
            options[key] = request.form[key] == 'true'
        
        # 创建任务
        task_id = str(uuid.uuid4())
        task = ProcessingTask(task_id, 'upload', {
            'filepath': filepath,
            'filename': filename,
            'options': options
        })
        
        processing_tasks[task_id] = task

        # 提交到任务队列
        if not _submit_task_to_queue(task):
            return jsonify({'error': 'Task queue is full, please try again later'}), 503

        return jsonify({
            'task_id': task_id,
            'status': 'queued',
            'message': f'上传文件已加入队列，等待处理: {file.filename}'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/task/<task_id>/status')
def get_task_status(task_id):
    """获取任务状态"""
    task = processing_tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    
    return jsonify({
        'task_id': task.task_id,
        'status': task.status,
        'progress': task.progress,
        'current_step': task.current_step,
        'steps': task.steps,
        # 只返回最近 500 条日志：批量下载时 logs 可积累数万条，
        # 全量序列化会在 eventlet 单线程上长时间占用 CPU，拖垮整个 Web 层
        'logs': task.logs[-500:],
        'logs_total': len(task.logs),
        'result': task.result,
        'error': task.error,
        # 实际处理时长（不含排队等待）；排队中为 null
        'duration': task.get_duration(),
        'queue_wait': task.get_queue_wait()
    })

@app.route('/api/queue/status')
def get_queue_status():
    """获取任务队列状态"""
    try:
        status = _get_queue_status()
        # 添加活跃任务列表
        active_tasks = []
        pending_tasks = []

        with _task_lock:
            for task_id, task in processing_tasks.items():
                task_info = {
                    'task_id': task.task_id,
                    'task_type': task.task_type,
                    'status': task.status,
                    'progress': task.progress,
                    'current_step': task.current_step
                }
                if task.status == 'running':
                    active_tasks.append(task_info)
                elif task.status == 'pending':
                    pending_tasks.append(task_info)

        status['active_tasks'] = active_tasks
        status['pending_tasks'] = pending_tasks

        return jsonify(status)
    except Exception as e:
        logger.error(f"Failed to get queue status: {e}")
        return jsonify({'error': str(e)}), 500


def _serialize_task_history(task: 'ProcessingTask') -> dict:
    result = task.result or {}
    duration = task.get_duration()
    summary = ''

    if task.task_type == 'single':
        summary = task.parameters.get('accession_number', '')
    elif task.task_type == 'batch':
        count = len(task.parameters.get('accession_numbers', []) or [])
        summary = f"{count} studies"
    elif task.task_type == 'upload':
        summary = task.parameters.get('filename', '')

    return {
        'task_id': task.task_id,
        'task_type': task.task_type,
        'status': task.status,
        'summary': summary,
        'start_time': task.start_time if task.start_time is not None else task.created_at,
        'end_time': task.end_time,
        'duration': duration,  # 实际处理时长，不含排队等待；未开始执行为 None
        'queue_wait': task.get_queue_wait(),
        'has_excel': bool(result.get('excel_file')),
        'has_zip': bool(result.get('result_zip') or result.get('zip_file')),
        'series_count': len(result.get('series_info', {}) or {}) if isinstance(result.get('series_info'), dict) else result.get('series_count', 0)
    }


def _parse_pagination_param(value, default, min_value=1, max_value=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default

    if parsed < min_value:
        parsed = min_value
    if max_value is not None and parsed > max_value:
        parsed = max_value
    return parsed


def _refresh_completed_cache_from_tasks():
    completed_tasks = [t for t in processing_tasks.values() if t.status == 'completed']
    completed_tasks.sort(key=lambda x: x.end_time or x.start_time or x.created_at, reverse=True)
    with _completed_tasks_lock:
        _completed_tasks_cache.clear()
        _completed_tasks_cache.extend(completed_tasks)
        _completed_task_ids.clear()
        _completed_task_ids.update(t.task_id for t in completed_tasks)
        _serialized_history_cache.clear()
        _serialized_history_cache.extend([_serialize_task_history(task) for task in completed_tasks])


def _record_task_completion(task: 'ProcessingTask'):
    with _completed_tasks_lock:
        if task.task_id in _completed_task_ids:
            return
        _completed_task_ids.add(task.task_id)
        _completed_tasks_cache.insert(0, task)
        _serialized_history_cache.insert(0, _serialize_task_history(task))
        # 截断过期缓存，防止长期运行时内存无限增长
        while len(_completed_tasks_cache) > MAX_COMPLETED_TASK_CACHE:
            evicted = _completed_tasks_cache.pop()
            _serialized_history_cache.pop()
            _completed_task_ids.discard(evicted.task_id)


@app.route('/api/tasks/history')
def get_task_history():
    """返回已完成任务的历史列表"""
    page = _parse_pagination_param(request.args.get('page'), 1, min_value=1)
    page_size = _parse_pagination_param(request.args.get('page_size'), 20, min_value=1, max_value=200)

    if not _serialized_history_cache:
        _refresh_completed_cache_from_tasks()

    total = len(_serialized_history_cache)
    total_pages = (total + page_size - 1) // page_size if total > 0 else 0
    if total_pages > 0 and page > total_pages:
        page = total_pages

    start = (page - 1) * page_size
    end = start + page_size
    paged_tasks = _serialized_history_cache[start:end]

    return jsonify({
        'tasks': paged_tasks,
        'page': page,
        'page_size': page_size,
        'total': total,
        'total_pages': total_pages
    })

@app.route('/api/task/<task_id>/cancel', methods=['POST'])
def cancel_task(task_id):
    """取消任务 - 修复版，真正中断任务执行"""
    task = processing_tasks.get(task_id)
    if not task:
        return jsonify({'error': 'Task not found'}), 404
    
    if task.status in ['completed', 'failed', 'cancelled']:
        return jsonify({'error': 'Task completed, cannot cancel'}), 400
    
    # 设置取消标志，任务线程会在检查点抛出InterruptedError
    task.cancel()
    task.add_log('任务已被用户取消', 'warning')
    
    # 强制发送更新
    task._emit_update(force=True)
    
    return jsonify({'message': 'Task cancelled'})

@app.route('/api/download/<task_id>/<file_type>')
def download_result(task_id, file_type):
    """下载处理结果"""
    task = processing_tasks.get(task_id)
    if not task or not task.result:
        logger.warning(f"下载请求失败: 任务不存在或无结果 - task_id={task_id}")
        return jsonify({'error': 'Result file not found'}), 404
    
    try:
        if file_type == 'excel' and 'excel_file' in task.result:
            file_path = task.result['excel_file']
            logger.info(f"下载Excel文件: {file_path}")
            if not file_path or not os.path.exists(file_path):
                logger.error(f"Excel文件不存在: {file_path}")
                return jsonify({'error': f'File not found: {file_path}'}), 404
            
            # Windows下使用pathlib处理路径
            from pathlib import Path
            file_path = str(Path(file_path).resolve())
            
            return send_file(
                file_path,
                as_attachment=True,
                download_name=f"metadata_{task_id}.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        elif (file_type == 'zip' or file_type == 'result_zip') and (task.result.get('zip_file') or task.result.get('result_zip')):
            path = task.result.get('result_zip') or task.result.get('zip_file')
            logger.info(f"下载ZIP文件: path={path}, exists={os.path.exists(path) if path else False}")
            
            if not path:
                logger.error("ZIP路径为空")
                return jsonify({'error': 'ZIP path is empty'}), 404
            
            # Windows下使用pathlib处理路径
            from pathlib import Path
            abs_path = str(Path(path).resolve())
            logger.info(f"绝对路径: {abs_path}, exists={os.path.exists(abs_path)}")
            
            if not os.path.exists(abs_path):
                logger.error(f"ZIP文件不存在: {abs_path}")
                # 尝试列出results目录内容帮助调试
                try:
                    result_dir = app.config['RESULT_FOLDER']
                    files = os.listdir(result_dir)
                    logger.info(f"Results目录内容: {files}")
                except Exception as list_e:
                    logger.error(f"无法列出results目录: {list_e}")
                return jsonify({'error': f'ZIP file not found: {abs_path}'}), 404
            
            # 检查文件大小
            try:
                file_size = os.path.getsize(abs_path)
                logger.info(f"ZIP文件大小: {file_size} bytes")
                if file_size == 0:
                    logger.error("ZIP文件大小为0")
                    return jsonify({'error': 'ZIP file is empty'}), 500
            except Exception as size_e:
                logger.error(f"无法获取文件大小: {size_e}")
            
            logger.info(f"开始发送文件: {abs_path}")
            response = send_file(
                abs_path,
                as_attachment=True,
                download_name=f"organized_{task_id}.zip",
                mimetype='application/zip'
            )
            logger.info(f"文件发送成功: {abs_path}")
            return response
        else:
            logger.warning(f"不支持的文件类型: {file_type}, result keys={list(task.result.keys())}")
            return jsonify({'error': 'File type not supported'}), 400
            
    except Exception as e:
        logger.error(f"下载文件时出错: {type(e).__name__}: {e}", exc_info=True)
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'error': f'{type(e).__name__}: {str(e)}'}), 500

@app.route('/api/system/status')
def system_status():
    """获取系统状态"""
    # 检查DICOM服务状态
    dicom_status = 'unknown'
    error_msg = None
    try:
        # 每次调用创建独立的客户端，避免持久连接状态异常
        client = DICOMDownloadClient()
        if client.check_status():
            dicom_status = 'connected'
        else:
            dicom_status = 'disconnected'
    except Exception as e:
        dicom_status = 'error'
        error_msg = str(e)
        logger.error(f"DICOM状态检查失败: {error_msg}")
    
    # 获取存储空间信息
    results_size_gb = get_directory_size(app.config['RESULT_FOLDER'])
    
    response = {
        'status': 'running',
        'active_tasks': len([t for t in processing_tasks.values() if t.status == 'running']),
        'total_tasks': len(processing_tasks),
        'dicom_service_status': dicom_status,
        'storage': {
            'results_size_gb': round(results_size_gb, 2),
            'cleanup_threshold_gb': CLEANUP_THRESHOLD_GB,
            'cleanup_needed': results_size_gb >= CLEANUP_THRESHOLD_GB
        }
    }
    
    if error_msg:
        response['dicom_error'] = error_msg

    return jsonify(response)


@app.route('/api/cache/stats')
def cache_stats():
    """获取磁盘缓存统计信息"""
    stats = get_cache_stats(app.config['RESULT_FOLDER'])
    return jsonify(stats)


@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    """后台异步清除磁盘缓存"""
    def do_clear():
        try:
            result = clear_all_cache(app.config['RESULT_FOLDER'])
            logger.info(f"Cache cleared: {result['message']}")
        except Exception as e:
            logger.error(f"Cache clear failed: {e}")

    threading.Thread(target=do_clear, daemon=True).start()
    return jsonify({'status': 'started', 'message': 'Cache clearing started in background'})


@app.route('/api/system/monitoring')
def get_system_monitoring():
    """Get detailed system monitoring information for the monitoring dashboard."""
    import platform
    import shutil

    # 尝试导入psutil，如果失败则使用替代方案
    try:
        import psutil
        has_psutil = True
    except ImportError:
        has_psutil = False
        logger.warning("psutil not available, using fallback methods for system monitoring")

    # 获取磁盘使用情况
    disk_info = {}
    try:
        # 获取应用所在磁盘的使用情况
        app_path = os.path.abspath('.')
        if has_psutil:
            disk_usage = psutil.disk_usage(app_path)
            disk_info = {
                'total_gb': round(disk_usage.total / (1024**3), 2),
                'used_gb': round(disk_usage.used / (1024**3), 2),
                'free_gb': round(disk_usage.free / (1024**3), 2),
                'percent_used': round(disk_usage.percent, 1),
                'path': app_path
            }
        else:
            # 使用shutil作为替代方案
            usage = shutil.disk_usage(app_path)
            total_gb = usage.total / (1024**3)
            used_gb = usage.used / (1024**3)
            free_gb = usage.free / (1024**3)
            percent_used = round((usage.used / usage.total) * 100, 1) if usage.total > 0 else 0
            disk_info = {
                'total_gb': round(total_gb, 2),
                'used_gb': round(used_gb, 2),
                'free_gb': round(free_gb, 2),
                'percent_used': percent_used,
                'path': app_path
            }
    except Exception as e:
        logger.warning(f"获取磁盘信息失败: {e}")
        disk_info = {'error': str(e)}

    # 获取内存使用情况
    memory_info = {}
    try:
        if has_psutil:
            memory = psutil.virtual_memory()
            memory_info = {
                'total_gb': round(memory.total / (1024**3), 2),
                'available_gb': round(memory.available / (1024**3), 2),
                'percent_used': memory.percent
            }
        else:
            # 没有psutil时，尝试读取/proc/meminfo（Linux）
            try:
                with open('/proc/meminfo', 'r') as f:
                    meminfo = f.read()
                mem_total = 0
                mem_available = 0
                for line in meminfo.split('\n'):
                    if line.startswith('MemTotal:'):
                        mem_total = int(line.split()[1]) * 1024  # kB to bytes
                    elif line.startswith('MemAvailable:'):
                        mem_available = int(line.split()[1]) * 1024
                if mem_total > 0:
                    used = mem_total - mem_available
                    memory_info = {
                        'total_gb': round(mem_total / (1024**3), 2),
                        'available_gb': round(mem_available / (1024**3), 2),
                        'percent_used': round((used / mem_total) * 100, 1)
                    }
                else:
                    memory_info = {'error': 'Unable to read memory info'}
            except Exception:
                memory_info = {'error': 'Memory info requires psutil on this platform'}
    except Exception as e:
        logger.warning(f"获取内存信息失败: {e}")
        memory_info = {'error': str(e)}

    # 获取CPU使用情况
    cpu_info = {}
    try:
        if has_psutil:
            cpu_info = {
                'percent': psutil.cpu_percent(interval=0.1),
                'count': psutil.cpu_count(),
                'load_avg': os.getloadavg() if hasattr(os, 'getloadavg') else None
            }
        else:
            # 没有psutil时的基本CPU信息
            cpu_count = os.cpu_count() or 1
            cpu_info = {
                'percent': None,  # 无法获取 without psutil
                'count': cpu_count,
                'load_avg': os.getloadavg() if hasattr(os, 'getloadavg') else None
            }
    except Exception as e:
        logger.warning(f"获取CPU信息失败: {e}")
        cpu_info = {'error': str(e)}

    # 获取各目录大小
    directory_sizes = {}
    try:
        for dir_name, dir_path in [
            ('results', app.config['RESULT_FOLDER']),
            ('uploads', app.config['UPLOAD_FOLDER']),
            ('temp', './temp'),
            ('logs', os.path.join(get_project_root(), 'logs'))
        ]:
            if os.path.exists(dir_path):
                dir_size = get_directory_size(dir_path)
                directory_sizes[dir_name] = {
                    'path': dir_path,
                    'size_gb': round(dir_size, 2)
                }
    except Exception as e:
        logger.warning(f"获取目录大小失败: {e}")

    # 获取日志文件列表
    log_files = []
    try:
        # 使用绝对路径，避免工作目录问题
        logs_dir = os.path.join(get_project_root(), 'logs')
        if os.path.exists(logs_dir):
            for file in os.listdir(logs_dir):
                if file.endswith('.log'):
                    file_path = os.path.join(logs_dir, file)
                    try:
                        stat = os.stat(file_path)
                        log_files.append({
                            'name': file,
                            'size_mb': round(stat.st_size / (1024 * 1024), 2),
                            'modified': stat.st_mtime
                        })
                    except Exception:
                        pass
            # 按修改时间排序，最新的在前
            log_files.sort(key=lambda x: x['modified'], reverse=True)
    except Exception as e:
        logger.warning(f"获取日志文件列表失败: {e}")

    # 获取活跃任务详情
    active_tasks_info = []
    try:
        for task_id, task in processing_tasks.items():
            if task.status in ['running', 'pending']:
                task_info = {
                    'task_id': task_id,
                    'type': task.task_type,
                    'status': task.status,
                    'progress': task.progress,
                    'current_step': task.current_step,
                    'start_time': task.start_time if task.start_time is not None else task.created_at,
                    # 运行中显示实际处理时长；排队中显示已等待时长
                    'elapsed_seconds': round(time.time() - (task.start_time if task.start_time is not None else task.created_at), 1),
                    'queue_wait_seconds': round(task.get_queue_wait(), 1) if task.get_queue_wait() is not None else None,
                    'parameters': {
                        'accession_number': task.parameters.get('accession_number', 'N/A') if task.task_type == 'single' else None,
                        'batch_count': len(task.parameters.get('accession_numbers', [])) if task.task_type == 'batch' else None
                    }
                }
                active_tasks_info.append(task_info)
    except Exception as e:
        logger.warning(f"获取活跃任务信息失败: {e}")

    # 获取近期完成的任务（最近10个）
    recent_completed = []
    try:
        all_tasks = []
        for task_id, task in processing_tasks.items():
            if task.status in ['completed', 'failed', 'cancelled']:
                # 提取 AccessionNumber 或标识
                accession_number = ''
                if task.task_type == 'single':
                    accession_number = task.parameters.get('accession_number', '')
                elif task.task_type == 'batch':
                    acc_list = task.parameters.get('accession_numbers', []) or []
                    accession_number = f"{len(acc_list)} studies" if len(acc_list) != 1 else acc_list[0]
                elif task.task_type == 'upload':
                    accession_number = task.parameters.get('filename', '')

                # 提取简短错误原因（仅 failed 状态）
                error_summary = ''
                if task.status == 'failed' and task.error:
                    err = str(task.error)
                    if 'No series found in PACS' in err:
                        error_summary = 'No series found'
                    elif 'timeout' in err.lower() or 'timed out' in err.lower():
                        error_summary = 'Timeout'
                    elif 'connection' in err.lower() or 'network' in err.lower():
                        error_summary = 'Connection error'
                    elif 'cancelled' in err.lower() or '取消' in err:
                        error_summary = 'Cancelled'
                    elif 'disk full' in err.lower() or 'no space' in err.lower():
                        error_summary = 'Disk full'
                    elif 'permission' in err.lower():
                        error_summary = 'Permission denied'
                    else:
                        # 取第一个句子或前 30 字符
                        err_short = err.split('.')[0].split('\n')[0].strip()
                        error_summary = err_short[:30] + ('...' if len(err_short) > 30 else '')

                all_tasks.append({
                    'task_id': task_id,
                    'type': task.task_type,
                    'status': task.status,
                    'accession_number': accession_number,
                    'error_summary': error_summary,
                    'end_time': task.end_time,
                    # 实际处理时长，不含排队等待；排队中就被取消/失败的为 None
                    'elapsed_seconds': round(task.get_duration(), 1) if task.get_duration() is not None else None
                })
        # 按结束时间排序，取最近10个
        all_tasks.sort(key=lambda x: x.get('end_time') or 0, reverse=True)
        recent_completed = all_tasks[:10]
    except Exception as e:
        logger.warning(f"获取近期任务信息失败: {e}")

    # 获取PACS连接状态
    pacs_status = {}
    try:
        if dicom_client_checker.check_status():
            pacs_status = {
                'connected': True,
                'config': {
                    'pacs_ip': os.getenv('PACS_IP', 'N/A'),
                    'pacs_port': os.getenv('PACS_PORT', 'N/A'),
                    'calling_aet': os.getenv('CALLING_AET', 'N/A'),
                    'called_aet': os.getenv('CALLED_AET', 'N/A'),
                    'calling_port': os.getenv('CALLING_PORT', 'N/A')
                }
            }
        else:
            pacs_status = {'connected': False, 'error': 'Connection failed'}
    except Exception as e:
        pacs_status = {'connected': False, 'error': str(e)}

    # 系统信息
    system_info = {
        'platform': platform.platform(),
        'python_version': platform.python_version(),
        'uptime_seconds': None  # 需要psutil才能获取
    }

    return jsonify({
        'timestamp': time.time(),
        'system': system_info,
        'disk': disk_info,
        'memory': memory_info,
        'cpu': cpu_info,
        'directories': directory_sizes,
        'log_files': log_files,
        'active_tasks': active_tasks_info,
        'recent_completed': recent_completed,
        'pacs_connection': pacs_status,
        'task_summary': {
            'running': len([t for t in processing_tasks.values() if t.status == 'running']),
            'pending': len([t for t in processing_tasks.values() if t.status == 'pending']),
            'completed': len([t for t in processing_tasks.values() if t.status == 'completed']),
            'failed': len([t for t in processing_tasks.values() if t.status == 'failed']),
            'cancelled': len([t for t in processing_tasks.values() if t.status == 'cancelled']),
            'total': len(processing_tasks)
        }
    })


@app.route('/api/pacs-config', methods=['GET'])
def get_pacs_config():
    """Get current PACS configuration (from environment/defaults)."""
    defaults = {
        'PACS_IP': '172.17.250.192',
        'PACS_PORT': 2104,
        'CALLING_AET': 'WMX01',
        'CALLED_AET': 'pacsFIR',
        'CALLING_PORT': 1103,
    }

    def _get_env_int(key, default):
        try:
            return int(os.getenv(key, default))
        except (TypeError, ValueError):
            return default

    return jsonify({
        'PACS_IP': os.getenv('PACS_IP', defaults['PACS_IP']),
        'PACS_PORT': _get_env_int('PACS_PORT', defaults['PACS_PORT']),
        'CALLING_AET': os.getenv('CALLING_AET', defaults['CALLING_AET']),
        'CALLED_AET': os.getenv('CALLED_AET', defaults['CALLED_AET']),
        'CALLING_PORT': _get_env_int('CALLING_PORT', defaults['CALLING_PORT']),
    })


@app.route('/api/pacs-config', methods=['POST'])
def set_pacs_config():
    """Persist PACS configuration to .env and update process env for new connections."""
    global dicom_client_checker

    data = request.json or {}
    try:
        pacs_ip = _normalize_host(data.get('PACS_IP'))
        pacs_port = _parse_port(data.get('PACS_PORT'), 'PACS_PORT')
        calling_aet = _normalize_aet(data.get('CALLING_AET'), 'CALLING_AET')
        called_aet = _normalize_aet(data.get('CALLED_AET'), 'CALLED_AET')
        calling_port = _parse_port(data.get('CALLING_PORT'), 'CALLING_PORT')
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    try:
        # Write to .env
        os.makedirs(os.path.dirname(ENV_FILE_PATH), exist_ok=True)
        set_key(ENV_FILE_PATH, 'PACS_IP', pacs_ip)
        set_key(ENV_FILE_PATH, 'PACS_PORT', str(pacs_port))
        set_key(ENV_FILE_PATH, 'CALLING_AET', calling_aet)
        set_key(ENV_FILE_PATH, 'CALLED_AET', called_aet)
        set_key(ENV_FILE_PATH, 'CALLING_PORT', str(calling_port))

        # Update in-memory env so new client instances pick it up without restart
        os.environ['PACS_IP'] = pacs_ip
        os.environ['PACS_PORT'] = str(pacs_port)
        os.environ['CALLING_AET'] = calling_aet
        os.environ['CALLED_AET'] = called_aet
        os.environ['CALLING_PORT'] = str(calling_port)

        # Refresh global checker client
        dicom_client_checker = DICOMDownloadClient()

        return jsonify({'message': 'Configuration saved'}), 200
    except Exception as e:
        return jsonify({'error': f'Failed to save configuration: {str(e)}'}), 500


@app.route('/api/logs/download/<filename>')
def download_log_file(filename):
    """下载日志文件"""
    # 安全检查：只允许下载logs目录下的.log文件
    if not filename.endswith('.log') or '..' in filename or '/' in filename:
        return jsonify({'error': 'Invalid filename'}), 400

    # 使用绝对路径，避免工作目录问题
    log_path = os.path.join(get_project_root(), 'logs', filename)

    # 检查文件是否存在
    if not os.path.exists(log_path) or not os.path.isfile(log_path):
        return jsonify({'error': 'Log file not found'}), 404

    try:
        return send_file(
            log_path,
            as_attachment=True,
            download_name=filename,
            mimetype='text/plain'
        )
    except Exception as e:
        logger.error(f"下载日志文件失败: {e}")
        return jsonify({'error': f'Failed to download log file: {str(e)}'}), 500

# 处理任务函数
def _parse_derived_keywords(options):
    """从任务 options 解析本次生效的衍生序列过滤关键词（仅当前任务，不修改全局配置）。

    两种用法：
    - ``derived_keywords``: 整体替换默认列表（web 页面关键词编辑器用此语义，
      页面总是提交完整列表）。⚠️ API 客户端只传少数几个关键词会拆掉全部默认
      防线（2026-08-31 实证：1 个关键词的自定义列表漏掉 'KEY'，KEY_IMAGES PR
      序列被整体下载）。
    - ``extra_derived_keywords``: 在默认列表基础上追加（API 客户端推荐），
      返回 默认列表 + 追加项。

    Returns:
        list[str] | None: 合法的关键词列表；未提供或为空时返回 None（用全局默认）。
    """
    def _validate(items):
        validated = []
        for k in items[:100]:  # 最多 100 个关键词
            if isinstance(k, str):
                k = k.strip().upper()
                if k and len(k) <= 50 and k not in validated:
                    validated.append(k)
        return validated

    extra = options.get('extra_derived_keywords')
    if isinstance(extra, list) and extra:
        merged = list(get_derived_keywords())
        for k in _validate(extra):
            if k not in merged:
                merged.append(k)
        return merged

    raw = options.get('derived_keywords')
    if not isinstance(raw, list):
        return None
    return _validate(raw) or None


def process_single_task(task):
    """处理单个AccessionNumber任务 - 修复版，支持取消检查"""
    client_logged_in = False
    task_client = None
    task_log_handler = _attach_task_log_handler(task)
    
    try:
        # 立即更新状态，确保WebSocket发送
        task.update_status('running', 5, 'Connecting to DICOM service')
        task.add_log("Connecting to DICOM service...")
        
        # 检查取消标志
        task.check_cancellation("initial_connect")
        
        # 添加调试日志
        logger.debug(f"开始处理任务: {task.task_id}")
        logger.debug(f"AccessionNumber: {task.parameters['accession_number']}")
        
        # 创建新的DICOM客户端实例
        try:
            task_client = DICOMDownloadClient()
            task.add_log("DICOM client created successfully")
        except Exception as e:
            task.add_log(f"Failed to create DICOM client: {str(e)}", 'error')
            raise Exception(f"Failed to create DICOM client: {str(e)}")
        
        # 检查取消标志
        task.check_cancellation("after_client_create")
        
        # 检查PACS连接状态（带重试，应对偶发的网络抖动或PACS负载高峰）
        task.update_status('running', 8, 'Checking PACS connection')
        task.add_log("Checking PACS connection status...")

        pacs_ok = False
        last_error = None
        for attempt in range(3):
            try:
                if task_client.check_status(retries=3, base_delay=2.0):
                    pacs_ok = True
                    if attempt > 0:
                        task.add_log(f"PACS connection normal (recovered after {attempt + 1} attempts)")
                    else:
                        task.add_log("PACS connection normal")
                    break
                else:
                    msg = f"PACS connection attempt {attempt + 1}/3 failed"
                    task.add_log(msg, 'warning')
                    last_error = "PACS association not established"
            except Exception as e:
                last_error = str(e)
                task.add_log(f"PACS connection attempt {attempt + 1}/3 error: {last_error}", 'warning')

            if attempt < 2:
                delay = 2.0 * (2 ** attempt)
                task.add_log(f"Retrying PACS connection in {delay:.0f}s...")
                time.sleep(delay)

        if not pacs_ok:
            error_msg = f"PACS service unavailable after retries ({last_error}). Please check network and PACS configuration."
            task.add_log(error_msg, 'error')
            raise Exception(error_msg)
        
        # 检查取消标志
        task.check_cancellation("after_pacs_check")
        
        # 自动登录（兼容性接口：当前DICOM客户端不做真实认证）
        task.update_status('running', 10, 'Logging in to DICOM service')
        task.add_log("Logging in to DICOM service...")
        
        try:
            username = os.getenv('DICOM_USERNAME', '')
            password = os.getenv('DICOM_PASSWORD', '')
            if not task_client.login(username, password):
                raise Exception("DICOM service login failed")
            client_logged_in = True
            task.add_log("DICOM service login successful")
        except Exception as e:
            task.add_log(f"Login failed: {str(e)}", 'error')
            raise

        # Attach progress callback so MR_clean can report progress back to task logs
        def _mr_progress(msg, stage=None):
            try:
                task.add_log(msg)
            except Exception:
                pass
            logger.debug(f"MR_PROGRESS[{stage}]: {msg}")

        task_client.progress_callback = _mr_progress
        
        # Attach download progress callback to update task status during C-MOVE
        def _download_progress(current_series, total_series, series_name, progress_pct):
            try:
                # 检查取消标志
                if task.is_cancelled():
                    raise InterruptedError("Task cancelled during download")
                
                step_name = f"Downloading series {current_series}/{total_series}: {series_name}"
                task.update_status('running', progress_pct, step_name)
                if current_series % 3 == 0 or current_series == total_series:  # 每3个series记录一次日志，避免日志过多
                    task.add_log(f"Downloaded {current_series}/{total_series} series: {series_name}")
            except InterruptedError:
                raise
            except Exception as e:
                logger.warning(f"Download progress callback error: {e}")
        
        task_client.download_progress_callback = _download_progress
        
        # 获取参数
        accession_number = task.parameters['accession_number']
        options = task.parameters.get('options', {})

        # 获取过滤参数
        modality_filter = options.get('modality_filter')
        min_series_files = options.get('min_series_files')
        exclude_derived = options.get('exclude_derived', True)  # 默认启用衍生序列过滤
        derived_keywords = _parse_derived_keywords(options)  # 仅本任务生效的关键词覆盖
        if derived_keywords is not None:
            # 覆盖语义（整体替换默认列表），记录完整内容便于事后排查过滤漏网
            task.add_log(f"Using task-specific derived keywords ({len(derived_keywords)} items, REPLACES defaults): {derived_keywords}")
            logger.warning(
                f"Task {task.task_id}: task-specific derived_keywords REPLACE the default list "
                f"({len(derived_keywords)} items): {derived_keywords}"
            )
        if min_series_files is not None:
            try:
                min_series_files = int(min_series_files)
            except (ValueError, TypeError):
                min_series_files = None

        task.update_status('running', 15, 'Preparing process')
        task.add_log(f"Start processing AccessionNumber: {accession_number}")
        
        # 检查取消标志
        task.check_cancellation("before_create_dir")
        
        # 创建结果目录
        result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        os.makedirs(result_dir, exist_ok=True)
        task.add_log(f"Created result directory: {result_dir}")

        # --- 磁盘缓存检查 ---
        cache_dir = get_cache_dir(app.config['RESULT_FOLDER'], accession_number, options)
        if cache_exists(cache_dir):
            task.update_status('running', 30, 'Loading from disk cache')
            task.add_log(f"Cache hit for {accession_number}, loading from disk...")
            try:
                cached_results = copy_from_cache(cache_dir, result_dir, task.task_id)
                if cached_results.get('success'):
                    task.result = cached_results
                    task.update_status('completed', 100, 'Completed (from cache)')
                    task.add_log('✅ Loaded from disk cache successfully!', 'success')
                    task.end_time = time.time()
                    _record_task_completion(task)
                    logger.info(f"Task {task.task_id} loaded from cache: {cache_dir}")
                    return
            except Exception as e:
                task.add_log(f"Cache load failed, will download from PACS: {e}", 'warning')
                logger.warning(f"Cache load failed for {task.task_id}: {e}")

        # 设置处理步骤
        task.steps = ['Connect Service', 'Query Data', 'Download Files', 'Organize Files', 'NIfTI Conversion', 'Extract Metadata']
        
        # 执行处理流程
        task.update_status('running', 30, 'Querying PACS data')
        task.add_log('Querying data from PACS...')
        
        # 检查取消标志
        task.check_cancellation("before_query")
        
        # 先查询是否存在数据（应用过滤条件）
        try:
            series_metadata = task_client._query_series_metadata(
                accession_number,
                modality_filter=modality_filter,
                min_series_files=min_series_files,
                exclude_derived=exclude_derived,
                derived_keywords=derived_keywords
            )
            if not series_metadata:
                filter_info = []
                if modality_filter:
                    filter_info.append(f"Modality filter: {modality_filter}")
                if min_series_files:
                    filter_info.append(f"Min files: {min_series_files}")
                if exclude_derived:
                    filter_info.append(f"Exclude derived")
                filter_str = f" ({', '.join(filter_info)})" if filter_info else ""
                raise Exception(f"No series found in PACS for AccessionNumber: {accession_number}{filter_str}")
            task.add_log(f"Found {len(series_metadata)} Series")
        except Exception as e:
            task.add_log(f"Query failed: {str(e)}", 'error')
            raise
        
        task.update_status('running', 40, 'Downloading DICOM files')
        task.add_log('Downloading DICOM files...')
        
        try:
            # 使用已登录的客户端处理
            results = task_client.process_complete_workflow(
                accession_number=accession_number,
                base_output_dir=result_dir,
                auto_extract=options.get('auto_extract', True),
                auto_organize=options.get('auto_organize', True),
                auto_metadata=options.get('auto_metadata', True),
                keep_zip=options.get('keep_zip', True),
                keep_extracted=options.get('keep_extracted', False),
                output_format=options.get('output_format', 'nifti'),
                parallel_pipeline=True,  # 启用下载-转换并行流水线，下载与整理/转换重叠执行
                modality_filter=modality_filter,
                min_series_files=min_series_files,
                exclude_derived=exclude_derived,
                derived_keywords=derived_keywords
            )
            
            if results and results.get('success'):
                task.update_status('running', 90, 'Generating results')
                task.add_log('Process successful, generating results...')

                # --- MR 序列重命名 ---
                if options.get('rename_mr_series') and results.get('organized_dir') and results.get('excel_file'):
                    task.add_log('Renaming MR series folders...')
                    try:
                        rename_map = task_client.rename_mr_series_folders(
                            results['organized_dir'],
                            results['excel_file']
                        )
                        if rename_map:
                            task.add_log('MR series folders renamed successfully')
                            # 仅更新原有 series_info 中的序列，避免引入被 organize 过滤掉的目录
                            if results.get('series_info'):
                                updated_series_info = {}
                                for old_name, info in results['series_info'].items():
                                    new_name = rename_map.get(old_name, old_name)
                                    new_path = os.path.join(results['organized_dir'], new_name)
                                    if os.path.isdir(new_path):
                                        updated_series_info[new_name] = {"path": new_path}
                                results['series_info'] = updated_series_info
                        else:
                            task.add_log('MR series rename skipped or failed', 'warning')
                    except Exception as rename_e:
                        logger.warning(f"MR rename error: {rename_e}")
                        task.add_log(f'MR series rename failed: {rename_e}', 'warning')
                
                # 创建结果ZIP文件
                if results.get('organized_dir'):
                    task.add_log('Creating result ZIP...')
                    logger.info(f"Creating ZIP: source={results['organized_dir']}, task_id={task.task_id}, result_dir={app.config['RESULT_FOLDER']}")
                    try:
                        zip_path = run_cpu_bound(
                            create_result_zip,
                            results['organized_dir'],
                            task.task_id,
                            app.config['RESULT_FOLDER'],
                            extra_files=[results.get('excel_file')],
                            include_subdirs=set(results.get('series_info', {}).keys())
                        )
                        results['result_zip'] = zip_path
                        task.add_log(f'Result ZIP created: {zip_path}')
                        logger.info(f"ZIP created successfully: {zip_path}, exists={os.path.exists(zip_path)}")
                    except Exception as zip_e:
                        logger.error(f"Failed to create ZIP: {zip_e}", exc_info=True)
                        task.add_log(f'Failed to create ZIP: {str(zip_e)}', 'error')
                        raise
                
                # --- 保存到磁盘缓存 ---
                try:
                    run_cpu_bound(
                        save_to_cache,
                        results['organized_dir'],
                        cache_dir,
                        zip_path=results.get('result_zip'),
                        excel_path=results.get('excel_file')
                    )
                    task.add_log(f"Saved result to disk cache")
                    logger.info(f"Task {task.task_id} saved to cache: {cache_dir}")
                except Exception as cache_e:
                    logger.warning(f"Failed to save cache for {task.task_id}: {cache_e}")

                task.result = results
                task.update_status('completed', 100, 'Completed')
                task.add_log('✅ Process completed successfully!', 'success')
                task.end_time = time.time()
                _record_task_completion(task)
                
                # 输出成功日志
                logger.debug(f"任务完成: {task.task_id}")
                
                # 任务完成后检查并清理结果目录
                check_and_cleanup_results()
                
            else:
                error_msg = results.get('error', 'Unknown error during process') if results else \
                    'No files received from PACS (accession may not exist in PACS, or images unavailable)'
                task.add_log(f'Process failed: {error_msg}', 'error')
                task.update_status('failed')
                task.error = error_msg
                
        except Exception as e:
            task.add_log(f'Error during process: {str(e)}', 'error')
            raise
            
    except InterruptedError:
        # 任务被取消，不要标记为失败
        task.add_log('Process cancelled by user', 'warning')
        # 状态已在cancel()方法中设置
        logger.info(f"任务被取消: {task.task_id}")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"任务处理失败: {task.task_id}, 错误: {error_msg}")
        task.add_log(f'Process error: {error_msg}', 'error')
        task.update_status('failed')
        task.error = error_msg
        task.end_time = time.time()

    except BaseException as e:
        # eventlet 环境下 GreenletExit / eventlet.Timeout 等 BaseException 会穿透
        # 上面的 except Exception，导致 worker 静默死亡、任务状态永远停留在 running。
        # 必须兜底：记录完整堆栈并把任务标记为失败，避免僵尸任务占用运行队列。
        logger.error(
            f"任务异常终止(BaseException): {task.task_id}, "
            f"类型: {type(e).__name__}, 错误: {e}",
            exc_info=True
        )
        task.add_log(f'Process aborted: {type(e).__name__}: {e}', 'error')
        task.update_status('failed')
        task.error = f"{type(e).__name__}: {e}"
        task.end_time = time.time()
    
    finally:
        # 确保登出
        if client_logged_in and task_client:
            try:
                task_client.logout()
                task.add_log("Logged out from DICOM service")
                logger.debug(f"已登出DICOM服务")
            except Exception as e:
                task.add_log(f"Error during logout: {str(e)}", 'warning')
                logger.warning(f"登出失败: {str(e)}")
        _detach_task_log_handler(task_log_handler)

def process_batch_task(task):
    """处理批量AccessionNumber任务 - 修复版，支持去重和取消检查"""
    client_logged_in = False
    task_client = None  # 初始化变量
    task_log_handler = _attach_task_log_handler(task)
    try:
        accession_numbers = task.parameters['accession_numbers']
        options = task.parameters.get('options', {})

        # 获取过滤参数
        modality_filter = options.get('modality_filter')
        min_series_files = options.get('min_series_files')
        exclude_derived = options.get('exclude_derived', True)  # 默认启用衍生序列过滤
        derived_keywords = _parse_derived_keywords(options)  # 仅本任务生效的关键词覆盖
        if min_series_files is not None:
            try:
                min_series_files = int(min_series_files)
            except (ValueError, TypeError):
                min_series_files = None
        if modality_filter:
            task.add_log(f"Modality filter: {modality_filter}")
        if min_series_files:
            task.add_log(f"Min series files: {min_series_files}")
        if exclude_derived:
            task.add_log(f"Exclude derived series: enabled")
        if derived_keywords is not None:
            # 覆盖语义（整体替换默认列表），记录完整内容便于事后排查过滤漏网
            task.add_log(f"Using task-specific derived keywords ({len(derived_keywords)} items, REPLACES defaults): {derived_keywords}")
            logger.warning(
                f"Task {task.task_id}: task-specific derived_keywords REPLACE the default list "
                f"({len(derived_keywords)} items): {derived_keywords}"
            )

        # 去重处理，保持顺序
        seen = set()
        unique_accession_numbers = []
        for acc in accession_numbers:
            if acc and acc not in seen:
                seen.add(acc)
                unique_accession_numbers.append(acc)

        if len(unique_accession_numbers) < len(accession_numbers):
            task.add_log(f"Removed {len(accession_numbers) - len(unique_accession_numbers)} duplicates, {len(unique_accession_numbers)} unique studies to process")
        
        accession_numbers = unique_accession_numbers
        
        task.update_status('running', 5, 'Connecting to DICOM service')
        task.add_log("Connecting to DICOM service...")
        
        # 检查取消标志
        task.check_cancellation("before_connect")
        
        # 创建新的DICOM客户端实例并登录
        task_client = DICOMDownloadClient()
        
        # 自动登录（兼容性接口：当前DICOM客户端不做真实认证）
        task.add_log("Logging in to DICOM service...")
        username = os.getenv('DICOM_USERNAME', '')
        password = os.getenv('DICOM_PASSWORD', '')
        if not task_client.login(username, password):
            raise Exception("DICOM service login failed, please check service status")
        
        client_logged_in = True
        task.add_log("DICOM service login successful")
        
        # Attach progress callback so MR_clean can report progress back to task logs
        def _mr_progress(msg, stage=None):
            try:
                task.add_log(msg)
            except Exception:
                pass
            logger.debug(f"MR_PROGRESS[{stage}]: {msg}")

        task_client.progress_callback = _mr_progress
        
        task.update_status('running', 10, 'Initializing batch process')
        task.add_log(f"Start batch processing {len(accession_numbers)} studies")
        
        results = []
        total = len(accession_numbers)
        
        for i, accno in enumerate(accession_numbers):
            # 检查取消标志
            task.check_cancellation(f"before_processing_{accno}")
            
            # 计算进度 (10-90%用于处理，剩余用于整理)
            progress = 10 + int((i / total) * 80)
            task.update_status('running', progress, f'Processing {accno} ({i+1}/{total})')
            task.add_log(f'Processing study {i+1}/{total}: {accno}')
            
            # 创建单独的结果目录
            result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id, accno)
            os.makedirs(result_dir, exist_ok=True)

            # --- 磁盘缓存检查 ---
            cache_dir = get_cache_dir(app.config['RESULT_FOLDER'], accno, options)
            if cache_exists(cache_dir):
                task.add_log(f"{accno}: Cache hit, loading from disk...")
                try:
                    cached_result = copy_from_cache(cache_dir, result_dir, task.task_id)
                    if cached_result.get('success'):
                        results.append(cached_result)
                        task.add_log(f'{accno}: Loaded from cache')
                        continue
                except Exception as e:
                    task.add_log(f"{accno}: Cache load failed, will download: {e}", 'warning')
            
            try:
                result = task_client.process_complete_workflow(
                    accession_number=accno,
                    base_output_dir=result_dir,
                    auto_extract=options.get('auto_extract', True),
                    auto_organize=options.get('auto_organize', True),
                    auto_metadata=options.get('auto_metadata', True),
                    output_format=options.get('output_format', 'nifti'),
                    parallel_pipeline=True,  # 启用下载-转换并行流水线，下载与整理/转换重叠执行
                    modality_filter=modality_filter,
                    min_series_files=min_series_files,
                    exclude_derived=exclude_derived,
                    derived_keywords=derived_keywords
                )

                # --- MR 序列重命名 ---
                if options.get('rename_mr_series') and result and result.get('organized_dir') and result.get('excel_file'):
                    try:
                        rename_map = task_client.rename_mr_series_folders(
                            result['organized_dir'],
                            result['excel_file']
                        )
                        # 仅更新原有 series_info 中的序列
                        if rename_map and result.get('series_info'):
                            updated_series_info = {}
                            for old_name, info in result['series_info'].items():
                                new_name = rename_map.get(old_name, old_name)
                                new_path = os.path.join(result['organized_dir'], new_name)
                                if os.path.isdir(new_path):
                                    updated_series_info[new_name] = {"path": new_path}
                            result['series_info'] = updated_series_info
                    except Exception as rename_e:
                        task.add_log(f'{accno}: MR rename failed: {rename_e}', 'warning')

                # --- 保存到磁盘缓存 ---
                if result and result.get('success') and result.get('organized_dir'):
                    try:
                        zip_path = None
                        if result.get('organized_dir'):
                            zip_path = run_cpu_bound(
                                create_result_zip,
                                result['organized_dir'],
                                task.task_id,
                                app.config['RESULT_FOLDER'],
                                extra_files=[result.get('excel_file')],
                                include_subdirs=set(result.get('series_info', {}).keys())
                            )
                            result['result_zip'] = zip_path
                        run_cpu_bound(
                            save_to_cache,
                            result['organized_dir'],
                            cache_dir,
                            zip_path=zip_path,
                            excel_path=result.get('excel_file')
                        )
                    except Exception as cache_e:
                        logger.warning(f"Batch cache save failed for {accno}: {cache_e}")

                results.append(result)
                task.add_log(f'{accno} Process completed')
                
            except Exception as e:
                task.add_log(f'{accno} Process failed: {str(e)}', 'error')
                results.append({'accession_number': accno, 'error': str(e)})

            # 每完成一个 accession 检查一次磁盘（清理内部有并发锁，重复调用无副作用）
            check_and_cleanup_results()

        task.update_status('running', 95, 'Creating batch results')
        task.add_log("Creating batch result files...")
        
        # 合并结果
        batch_result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        zip_path = run_cpu_bound(
            create_result_zip,
            batch_result_dir,
            f"batch_{task.task_id}",
            app.config['RESULT_FOLDER']
        )
        
        # 计算详细的批处理统计信息
        total_processed = len([r for r in results if r.get('success')])
        total_failed = len([r for r in results if r.get('error')])
        
        # 收集质量统计数据
        quality_stats = {
            'normal': 0,
            'low_quality': 0,
            'fixed': 0,
            'unknown': 0
        }
        total_images = 0
        total_series = 0
        
        for r in results:
            if r.get('success'):
                series_info = r.get('series_info', {})
                total_series += len(series_info)
                
                # 尝试从Excel元数据文件中读取质量统计
                excel_file = r.get('excel_file')
                if excel_file and os.path.exists(excel_file):
                    try:
                        import pandas as pd
                        df = pd.read_excel(excel_file)
                        if 'Low_quality' in df.columns:
                            for _, row in df.iterrows():
                                low_quality = row.get('Low_quality', 0)
                                fixed = row.get('Fixed', '')
                                if fixed == 'Yes' or 'Fixed' in str(row.get('Low_quality_reason', '')):
                                    quality_stats['fixed'] += 1
                                elif low_quality == 0 or low_quality == False:
                                    quality_stats['normal'] += 1
                                elif low_quality == 1 or low_quality == True:
                                    quality_stats['low_quality'] += 1
                                else:
                                    quality_stats['unknown'] += 1
                                total_images += 1
                        else:
                            # 如果没有质量列，只统计文件数量
                            total_images += len(df)
                            quality_stats['unknown'] += len(df)
                    except Exception as e:
                        # 如果读取Excel失败，使用series_info统计
                        for series_name, series_data in series_info.items():
                            file_count = series_data.get('file_count', 0)
                            total_images += file_count
                            quality_stats['unknown'] += file_count
                else:
                    # 如果没有Excel文件，使用series_info统计
                    for series_name, series_data in series_info.items():
                        file_count = series_data.get('file_count', 0)
                        total_images += file_count
                        quality_stats['unknown'] += file_count
        
        # 计算处理时间（不含排队等待）
        duration = task.get_duration() or 0.0
        avg_speed = total_images / duration if duration > 0 else 0
        
        task.result = {
            'batch_results': results,
            'result_zip': zip_path,
            'total_processed': total_processed,
            'total_failed': total_failed,
            'total_studies': len(accession_numbers),
            'total_series': total_series,
            'total_images': total_images,
            'duration': round(duration, 2),
            'avg_speed': round(avg_speed, 2),
            'quality_distribution': quality_stats
        }
        
        task.update_status('completed', 100, 'Batch process completed')
        task.add_log('Batch process completed')
        task.end_time = time.time()
        _record_task_completion(task)
        
        # 批量任务完成后检查并清理结果目录
        check_and_cleanup_results()
        
    except InterruptedError:
        # 任务被取消，已在上面的循环中处理
        task.add_log('Batch process cancelled by user', 'warning')
        task.update_status('cancelled')
        task.end_time = time.time()
    except Exception as e:
        task.add_log(f'Batch process error: {str(e)}', 'error')
        task.update_status('failed')
        task.error = str(e)
        task.end_time = time.time()

    except BaseException as e:
        # 同 process_single_task：兜底捕获 BaseException，避免任务状态永远停留在 running
        logger.error(
            f"批量任务异常终止(BaseException): {task.task_id}, "
            f"类型: {type(e).__name__}, 错误: {e}",
            exc_info=True
        )
        task.add_log(f'Batch process aborted: {type(e).__name__}: {e}', 'error')
        task.update_status('failed')
        task.error = f"{type(e).__name__}: {e}"
        task.end_time = time.time()
    
    finally:
        # 确保登出
        if client_logged_in and task_client:
            try:
                task_client.logout()
                task.add_log("Logged out from DICOM service")
            except Exception as e:
                task.add_log(f"Error during logout: {str(e)}", 'warning')
        _detach_task_log_handler(task_log_handler)

def process_upload_task(task):
    """处理上传文件任务 - 修复版，支持取消检查"""
    task_log_handler = _attach_task_log_handler(task)
    try:
        filepath = task.parameters['filepath']
        options = task.parameters['options']
        
        task.update_status('running', 5, 'Initializing upload process')
        task.add_log(f"Start processing uploaded file: {task.parameters['filename']}")
        
        # 检查取消标志
        task.check_cancellation("initial")
        
        # 创建本地DICOM客户端实例（无需登录，仅用于本地文件处理）
        local_client = DICOMDownloadClient()
        
        # 检查取消标志
        task.check_cancellation("after_client_create")
        
        # 创建结果目录
        result_dir = os.path.join(app.config['RESULT_FOLDER'], task.task_id)
        os.makedirs(result_dir, exist_ok=True)
        
        task.steps = ['Extract Files', 'Organize Files', 'NIfTI Conversion', 'Extract Metadata']

        # 处理上传流程
        task.update_status('running', 20, 'Processing upload workflow')
        task.add_log("Processing uploaded ZIP file...")
        
        # 检查取消标志
        task.check_cancellation("before_processing")
        
        result = local_client.process_upload_workflow(filepath, result_dir, options)

        if result.get('success'):
            organized_dir = result.get('organized_dir')
            series_info = result.get('series_info', {})
            excel_file = result.get('excel_file')

            # 检查取消标志
            task.check_cancellation("before_create_zip")
            
            task.update_status('running', 95, 'Creating result files')
            task.add_log("Creating result files...")
            zip_source = organized_dir or result_dir
            zip_path = run_cpu_bound(
                create_result_zip,
                zip_source,
                task.task_id,
                app.config['RESULT_FOLDER'],
                extra_files=[excel_file],
                include_subdirs=set(series_info.keys())
            )

            task.result = {
                'extract_dir': result.get('extract_dir'),
                'organized_dir': organized_dir,
                'excel_file': excel_file,
                'result_zip': zip_path,
                'series_count': len(series_info)
            }

            task.update_status('completed', 100, 'Upload process completed')
            task.add_log('Upload process completed')
            task.end_time = time.time()
            _record_task_completion(task)

            # 上传文件处理完成后检查并清理结果目录
            check_and_cleanup_results()
        else:
            task.add_log('Upload processing failed', 'error')
            task.update_status('failed')
            task.error = result.get('error') or 'Failed to process uploaded file'
            
    except InterruptedError:
        # 任务被取消
        task.add_log('Upload process cancelled by user', 'warning')
        task.update_status('cancelled')
        task.end_time = time.time()
    except Exception as e:
        task.add_log(f'Upload process error: {str(e)}', 'error')
        task.update_status('failed')
        task.error = str(e)
        task.end_time = time.time()
    except BaseException as e:
        # 同 process_single_task：兜底捕获 BaseException，避免任务状态永远停留在 running
        logger.error(
            f"上传任务异常终止(BaseException): {task.task_id}, "
            f"类型: {type(e).__name__}, 错误: {e}",
            exc_info=True
        )
        task.add_log(f'Upload process aborted: {type(e).__name__}: {e}', 'error')
        task.update_status('failed')
        task.error = f"{type(e).__name__}: {e}"
        task.end_time = time.time()
    finally:
        _detach_task_log_handler(task_log_handler)


# WebSocket事件处理
@socketio.on('connect')
def handle_connect():
    logger.debug('客户端已连接')

@socketio.on('disconnect')
def handle_disconnect():
    logger.debug('客户端已断开')

@socketio.on('subscribe_task')
def handle_subscribe_task(data):
    """订阅任务更新"""
    task_id = data.get('task_id')
    if task_id in processing_tasks:
        emit('task_subscribed', {'task_id': task_id})

def _ensure_ssl_cert(ssl_dir):
    """生成或复用自签名 SSL 证书，返回 (cert_path, key_path)。"""
    import datetime
    import ipaddress as _ipaddress
    os.makedirs(ssl_dir, exist_ok=True)
    cert_path = os.path.join(ssl_dir, 'cert.pem')
    key_path  = os.path.join(ssl_dir, 'key.pem')

    # 证书存在且未过期（剩余 > 7 天）则直接复用
    if os.path.exists(cert_path) and os.path.exists(key_path):
        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            with open(cert_path, 'rb') as f:
                cert = x509.load_pem_x509_certificate(f.read(), default_backend())
            remaining = cert.not_valid_after_utc - datetime.datetime.now(datetime.timezone.utc)
            if remaining.days > 7:
                return cert_path, key_path
        except Exception:
            pass  # 证书损坏，重新生成

    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.backends import default_backend

        key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )
        with open(key_path, 'wb') as f:
            f.write(key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            ))

        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, 'dicom-server'),
        ])
        san_list = [x509.DNSName('localhost'), x509.DNSName('*')]
        # 把本机所有网卡 IP 都加入 SAN，避免浏览器 IP 访问时证书不匹配
        import socket as _socket
        try:
            hostname = _socket.gethostname()
            for addr in _socket.getaddrinfo(hostname, None):
                ip_str = addr[4][0]
                try:
                    san_list.append(x509.IPAddress(_ipaddress.ip_address(ip_str)))
                except Exception:
                    pass
        except Exception:
            pass
        for fixed_ip in ('127.0.0.1', '0.0.0.0'):
            try:
                san_list.append(x509.IPAddress(_ipaddress.IPv4Address(fixed_ip)))
            except Exception:
                pass

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3650))
            .add_extension(x509.SubjectAlternativeName(san_list), critical=False)
            .sign(key, hashes.SHA256(), default_backend())
        )
        with open(cert_path, 'wb') as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))

        logger.info(f"🔒 已生成自签名 SSL 证书: {cert_path}")
    except Exception as e:
        logger.error(f"❌ SSL 证书生成失败: {e}")
        raise

    return cert_path, key_path


# ========== 衍生序列过滤关键词配置 API ==========

@app.route('/api/filter-keywords', methods=['GET'])
def get_filter_keywords():
    """获取当前生效的衍生序列过滤关键词列表（只读）。

    页面可编辑关键词，但修改仅随任务 options.derived_keywords 对当次下载生效；
    不提供全局写入口——永久修改只能改 src/core/constants.py 的默认值。
    """
    return jsonify({
        'keywords': get_derived_keywords(),
        'default_keywords': DEFAULT_DERIVED_SERIES_KEYWORDS,
        'count': len(get_derived_keywords())
    })


def _acquire_single_instance_lock():
    """单实例锁：防止同一机器上启动多个服务实例。

    eventlet 下两个进程可以同时 LISTEN 同一端口（SO_REUSEPORT 语义），
    内核会把 HTTP 连接负载均衡到两个进程——任务状态、C-STORE SCP 被撕裂，
    产生收不到文件的僵尸任务（2026-08-27 线上实测）。
    返回锁文件句柄（必须保持引用防止 GC 释放锁）；已被占用则直接退出。
    """
    lock_path = os.path.join(project_root, 'logs', 'app.lock')
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    fd = open(lock_path, 'a+')
    try:
        if os.name == 'nt':
            import msvcrt
            fd.seek(0)
            if not fd.read(1):
                fd.write('\0')  # 文本模式打开，写 str；msvcrt 只需文件非空以锁定首字节
                fd.flush()
            fd.seek(0)
            msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logger.error(f"❌ 已有另一个实例在运行（锁文件 {lock_path} 被占用），本实例退出")
        sys.exit(1)
    return fd


if __name__ == '__main__':
    _single_instance_lock = _acquire_single_instance_lock()
    logger.info("="*60)
    logger.info("🏥 DICOM处理Web应用启动")
    logger.info("="*60)

    enable_https = os.getenv('ENABLE_HTTPS', 'false').lower() in ('true', '1', 'yes')
    ssl_context = None
    protocol = 'http'
    if enable_https:
        ssl_dir = os.path.join(project_root, 'ssl')
        try:
            cert_path, key_path = _ensure_ssl_cert(ssl_dir)
            ssl_context = (cert_path, key_path)
            protocol = 'https'
            logger.info(f"🔒 HTTPS 已启用 (证书: {cert_path})")
            logger.info("   ⚠️  首次访问时浏览器会提示证书不受信任，点击'高级→继续访问'即可")
        except Exception as ssl_err:
            logger.error(f"❌ HTTPS 启动失败，回退为 HTTP: {ssl_err}")

    logger.info(f"📡 访问地址: {protocol}://172.17.250.136:5005")
    
    # 检查DICOM服务连接状态
    try:
        checker = DICOMDownloadClient()
        if checker.check_status():
            logger.info("✅ PACS服务连接正常")
            logger.info(f"   - PACS IP: {checker.pacs_config['PACS_IP']}")
            logger.info(f"   - PACS Port: {checker.pacs_config['PACS_PORT']}")
            logger.info(f"   - Calling AET: {checker.pacs_config['CALLING_AET']}")
            logger.info(f"   - Called AET: {checker.pacs_config['CALLED_AET']}")
        else:
            logger.warning("⚠️  PACS服务连接异常，下载功能可能不可用")
    except Exception as e:
        logger.error(f"⚠️  无法连接PACS服务: {str(e)}")
        logger.info("   仅支持本地文件上传处理")
    
    logger.info("="*60)
    if os.getenv('DICOM_USERNAME') or os.getenv('DICOM_PASSWORD'):
        logger.info("🔐 已从环境变量读取DICOM登录信息")
    else:
        logger.info("🔐 未配置DICOM登录信息（当前实现无需真实认证）")
    logger.info("🚀 系统已就绪，等待用户请求...")
    logger.info("📡 访问地址:")
    logger.info(f"   - {protocol}://localhost:5005")
    logger.info(f"   - {protocol}://127.0.0.1:5005")
    logger.info(f"   - {protocol}://172.17.250.136:5005")
    logger.info("="*60)

    # 启动应用
    logger.info(f"🧵 SocketIO async mode: {SOCKETIO_ASYNC_MODE}")
    if SOCKETIO_ASYNC_MODE == 'eventlet':
        # eventlet 模式：单进程即可处理大量长轮询/WebSocket 连接
        run_kwargs = dict(host='0.0.0.0', port=5005, debug=False)
        if ssl_context:
            run_kwargs['ssl_context'] = ssl_context
        socketio.run(app, **run_kwargs)
    else:
        # threading 模式回退（本地开发、未安装 eventlet 的环境）
        socketio.run(app, host='0.0.0.0', port=5005, debug=False,
                     allow_unsafe_werkzeug=True, ssl_context=ssl_context)
