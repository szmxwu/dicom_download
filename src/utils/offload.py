# -*- coding: utf-8 -*-
"""
CPU 密集操作卸载工具。

背景：Web 模式使用 eventlet（monkey_patch 后 threading.Thread 变为 greenlet，
所有"线程"实际在单个 OS 线程上协作调度）。纯 CPU 密集的 Python 代码
（nibabel 体数据缩放、numpy 运算、pydicom 批量解析）不会发生协作式切换，
会把 eventlet 事件循环饿死，表现为下载流水线正常但 HTTP/SocketIO 完全无响应。

run_cpu_bound() 在 eventlet 环境下通过 tpool 把这类调用丢到真实 OS 线程执行，
调用方 greenlet 挂起等待，事件循环保持响应；非 eventlet 环境直接同步调用。
"""

import logging

logger = logging.getLogger('DICOMApp')

_tpool = None
_tpool_checked = False


def fix_logging_locks_for_eventlet():
    """eventlet 环境下把 logging 的锁替换为真实 OS 锁。

    背景：monkey_patch(thread=True) 会把 threading.RLock 换成 green 锁，
    而 run_cpu_bound() 卸载到真实线程执行的代码里有大量 logger 调用。
    真实线程获取 green 锁时会在该线程上注册 waiter greenlet；当事件循环
    在主线程上唤醒这个 waiter 时抛出 greenlet.error: Cannot switch to a
    different thread——唤醒丢失，等待方 greenlet 永久卡死（曾导致 C-MOVE
    等待永不返回、_cmove_lock 永久占用、整个服务假死）。

    真实锁在 uncontended 时就是一次原子操作，greenlet 侧使用完全安全；
    contended 时仅短暂阻塞，可接受。需在 monkey_patch 之后尽早调用。
    """
    try:
        from eventlet.patcher import is_monkey_patched, original
    except ImportError:
        return
    if not is_monkey_patched('thread'):
        return
    real_threading = original('threading')

    # 模块级锁（handler 注册表操作用）
    logging._lock = real_threading.RLock()

    # 之后创建的 Handler 使用真锁
    def _create_real_lock(self):
        self.lock = real_threading.RLock()
    logging.Handler.createLock = _create_real_lock

    # 已存在的 Handler 直接换掉 green 锁（防御性，正常路径下此时还没有 handler）
    try:
        loggers = [logging.getLogger()] + [
            lg for lg in logging.Logger.manager.loggerDict.values()
            if isinstance(lg, logging.Logger)
        ]
        for lg in loggers:
            for h in lg.handlers:
                h.lock = real_threading.RLock()
    except Exception:
        pass


def _get_tpool():
    """eventlet 已激活（thread 被打补丁）时返回 tpool 模块，否则返回 None。"""
    global _tpool, _tpool_checked
    if _tpool_checked:
        return _tpool
    _tpool_checked = True
    try:
        from eventlet.patcher import is_monkey_patched
        if is_monkey_patched('thread'):
            from eventlet import tpool
            _tpool = tpool
    except ImportError:
        _tpool = None
    return _tpool


def run_cpu_bound(func, *args, **kwargs):
    """
    在 eventlet 环境下将纯 CPU 密集函数放到真实 OS 线程执行，避免饿死事件循环。

    要求 func 内部不使用 eventlet 的 green 原语（green Lock/Queue/socket），
    只允许文件 I/O、subprocess 和纯计算。非 eventlet 环境等价于直接调用。
    """
    tpool = _get_tpool()
    if tpool is None:
        return func(*args, **kwargs)
    return tpool.execute(func, *args, **kwargs)
