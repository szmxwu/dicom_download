# -*- coding: utf-8 -*-
"""跨任务重内存处理背压。

只用于 eventlet green 工作流层或普通 OS 线程的调用边界；不得在
run_cpu_bound() 的真实线程内再次获取这里的 threading 原语。
"""

import logging
import os
import threading
import time
from contextlib import contextmanager

logger = logging.getLogger('DICOMApp')


def _positive_int_env(name, default):
    try:
        value = int(os.getenv(name, str(default)))
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _nonnegative_float_env(name, default):
    try:
        value = float(os.getenv(name, str(default)))
        return value if value >= 0 else default
    except (TypeError, ValueError):
        return default


_MAX_HEAVY_WORKERS = _positive_int_env('HEAVY_PROCESSING_MAX_WORKERS', 2)
_MIN_AVAILABLE_GB = _nonnegative_float_env('MEMORY_MIN_AVAILABLE_GB', 2.0)
_PRESSURE_RETRY_SEC = 1.0
_PRESSURE_LOG_INTERVAL_SEC = 60.0

_heavy_slots = threading.BoundedSemaphore(_MAX_HEAVY_WORKERS)
_heavy_state_lock = threading.Lock()
_heavy_active = 0
_last_pressure_log = 0.0


def get_available_memory_gb():
    """返回系统可用内存（GiB）；无法判断时返回 None，不误报低内存。"""
    try:
        import psutil
        return psutil.virtual_memory().available / (1024 ** 3)
    except ImportError:
        pass
    except Exception:
        return None

    if os.name == 'posix':
        try:
            with open('/proc/meminfo', 'r', encoding='ascii') as meminfo:
                for line in meminfo:
                    if line.startswith('MemAvailable:'):
                        return int(line.split()[1]) / (1024 ** 2)
        except (OSError, ValueError, IndexError):
            pass
    return None


def _pressure_log_due():
    global _last_pressure_log
    now = time.monotonic()
    if now - _last_pressure_log < _PRESSURE_LOG_INTERVAL_SEC:
        return False
    _last_pressure_log = now
    return True


@contextmanager
def heavy_processing_slot():
    """获取一个全局转换/预览/QC槽位。

    可用内存低于阈值时，已有重操作期间不让新操作继续叠加；但始终允许
    唯一一个重操作运行，避免共享服务器长期低内存导致流水线永久饿死。
    """
    global _heavy_active
    while True:
        _heavy_slots.acquire()
        available_gb = get_available_memory_gb()
        with _heavy_state_lock:
            active_before = _heavy_active
            low_memory = available_gb is not None and available_gb < _MIN_AVAILABLE_GB
            should_wait = low_memory and active_before > 0
            if not should_wait:
                _heavy_active += 1
            log_pressure = should_wait and _pressure_log_due()

        if not should_wait:
            if low_memory and active_before == 0 and _pressure_log_due():
                logger.warning(
                    '[MEMORY] Available memory %.2f GiB is below %.2f GiB; '
                    'allowing one heavy operation to preserve progress',
                    available_gb, _MIN_AVAILABLE_GB
                )
            break

        _heavy_slots.release()
        if log_pressure:
            logger.warning(
                '[MEMORY] Available memory %.2f GiB is below %.2f GiB; '
                '%d heavy operations active, deferring additional work',
                available_gb, _MIN_AVAILABLE_GB, active_before
            )
        time.sleep(_PRESSURE_RETRY_SEC)

    try:
        yield available_gb
    finally:
        with _heavy_state_lock:
            _heavy_active = max(0, _heavy_active - 1)
        _heavy_slots.release()
