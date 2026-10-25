"""Client reconnect and cache/disk-watermark recovery regressions."""
import ast
import json
import logging
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.utils import cache


APP_SOURCE = Path(__file__).resolve().parents[1] / 'src/web/app.py'


def load_app_function(name, namespace):
    node = next(
        node for node in ast.parse(APP_SOURCE.read_text(encoding='utf-8')).body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(APP_SOURCE), 'exec'), namespace)
    return namespace[name]


def cache_key(_results, accession, options):
    return json.dumps([accession, options], sort_keys=True)


def make_task(accession='A', options=None, status='running', slot_released=False):
    return SimpleNamespace(
        task_id='old-task', task_type='single', status=status,
        parameters={'accession_number': accession, 'options': options or {}},
        slot_released=slot_released,
    )


def task_namespace(task):
    return {
        'processing_tasks': {task.task_id: task},
        '_task_lock': threading.RLock(),
        'get_cache_dir': cache_key,
        'app': SimpleNamespace(config={'RESULT_FOLDER': 'results'}),
    }


@pytest.mark.parametrize('status', ['pending', 'running'])
def test_reusable_task_matches_same_workload(status):
    options = {'output_format': 'nifti'}
    task = make_task(options=options, status=status)
    task.task_id = 'original-id'
    task.parameters['options'] = options
    namespace = task_namespace(task)
    namespace['processing_tasks'] = {task.task_id: task}
    finder = load_app_function('_find_reusable_single_task', namespace)

    assert finder('A', options) is task
    assert finder('A', {'output_format': 'npz'}) is None
    assert finder('B', options) is None


@pytest.mark.parametrize('status', ['completed', 'failed', 'cancelled'])
def test_terminal_or_abandoned_tasks_are_not_reused(status):
    options = {'output_format': 'nifti'}
    task = make_task(options=options, status=status)
    namespace = task_namespace(task)
    finder = load_app_function('_find_reusable_single_task', namespace)
    assert finder('A', options) is None

    abandoned = make_task(options=options, status='running', slot_released=True)
    namespace = task_namespace(abandoned)
    finder = load_app_function('_find_reusable_single_task', namespace)
    assert finder('A', options) is None


def test_process_single_returns_original_task_for_reconnect():
    options = {'output_format': 'nifti'}
    existing = make_task(options=options)
    existing.task_id = 'original-id'
    existing.parameters['options'] = options
    namespace = task_namespace(existing)
    namespace['processing_tasks'] = {existing.task_id: existing}
    load_app_function('_find_reusable_single_task', namespace)
    namespace.update({
        'request': SimpleNamespace(
            json={'accession_number': 'A', 'options': options}, remote_addr='client'),
        'logger': logging.getLogger('offline'),
        'ProcessingTask': Mock(),
        '_submit_task_to_queue': Mock(return_value=True),
        'jsonify': lambda value: value,
        'app': SimpleNamespace(
            config={'RESULT_FOLDER': 'results'},
            route=lambda *args, **kwargs: (lambda func: func),
        ),
    })
    handler = load_app_function('process_single', namespace)

    response = handler()

    assert response['task_id'] == 'original-id'
    assert response['status'] == 'existing'
    assert response['reused'] is True
    namespace['ProcessingTask'].assert_not_called()
    namespace['_submit_task_to_queue'].assert_not_called()


def cache_entries(tmp_path):
    entries = []
    for index, size in enumerate([1, 2, 3, 4]):
        path = tmp_path / f'entry-{index}'
        path.mkdir()
        (path / 'payload.bin').write_bytes(b'x' * size)
        atime = 1000 + index
        os.utime(path, (atime, atime))
        entries.append(path)
    return entries


@pytest.fixture(autouse=True)
def clear_eviction_state(monkeypatch):
    monkeypatch.setattr(
        cache.os.path, 'getatime',
        lambda path: int(Path(path).name.rsplit('-', 1)[-1]) + 1000
    )
    cache._evict_failures.clear()
    cache._cache_active_paths.clear()
    cache._cache_evicting_paths.clear()
    yield
    cache._evict_failures.clear()
    cache._cache_active_paths.clear()
    cache._cache_evicting_paths.clear()


def test_lru_evicts_by_capacity_only(tmp_path, monkeypatch):
    entries = cache_entries(tmp_path)
    monkeypatch.setenv('CACHE_MAX_GB', str(7 / 1024 ** 3))
    monkeypatch.setenv('CACHE_MIN_FREE_GB', '0')
    monkeypatch.setattr(cache.shutil, 'disk_usage', lambda _: SimpleNamespace(free=0))

    removed = cache._evict_lru_sync(str(tmp_path))

    assert removed == 2
    assert not entries[0].exists() and not entries[1].exists()
    assert entries[2].exists() and entries[3].exists()


def test_lru_evicts_until_free_watermark_recovered(tmp_path, monkeypatch):
    entries = cache_entries(tmp_path)
    monkeypatch.setenv('CACHE_MAX_GB', '100')
    monkeypatch.setenv('CACHE_MIN_FREE_GB', str(6 / 1024 ** 3))
    free_samples = iter([0, 1, 3, 6, 9])
    monkeypatch.setattr(
        cache.shutil, 'disk_usage', lambda _: SimpleNamespace(free=next(free_samples)))

    removed = cache._evict_lru_sync(str(tmp_path))

    assert removed == 3
    assert not entries[0].exists() and not entries[1].exists() and not entries[2].exists()
    assert entries[3].exists()


def test_lru_evicts_when_both_limits_are_satisfied(tmp_path, monkeypatch):
    entries = cache_entries(tmp_path)
    monkeypatch.setenv('CACHE_MAX_GB', str(7 / 1024 ** 3))
    monkeypatch.setenv('CACHE_MIN_FREE_GB', str(6 / 1024 ** 3))
    free_samples = iter([0, 1, 3, 6, 9])
    monkeypatch.setattr(
        cache.shutil, 'disk_usage', lambda _: SimpleNamespace(free=next(free_samples)))

    removed = cache._evict_lru_sync(str(tmp_path))

    assert removed == 3
    assert entries[3].exists()


def test_lru_is_noop_within_both_limits(tmp_path, monkeypatch):
    entries = cache_entries(tmp_path)
    monkeypatch.setenv('CACHE_MAX_GB', '100')
    monkeypatch.setenv('CACHE_MIN_FREE_GB', '1')
    monkeypatch.setattr(cache.shutil, 'disk_usage', lambda _: SimpleNamespace(free=1024 ** 3))

    assert cache._evict_lru_sync(str(tmp_path)) == 0
    assert all(path.exists() for path in entries)


def test_lru_rechecks_real_free_space_instead_of_estimating(tmp_path, monkeypatch):
    entries = cache_entries(tmp_path)
    monkeypatch.setenv('CACHE_MAX_GB', '100')
    monkeypatch.setenv('CACHE_MIN_FREE_GB', str(6 / 1024 ** 3))
    # 模拟硬链接：删除逻辑目录后真实空闲仍不增长，不能提前停止。
    monkeypatch.setattr(cache.shutil, 'disk_usage', lambda _: SimpleNamespace(free=0))

    removed = cache._evict_lru_sync(str(tmp_path))

    assert removed == 4
    assert not any(path.exists() for path in entries)


def test_lru_skips_entries_with_active_cache_io(tmp_path, monkeypatch):
    entries = cache_entries(tmp_path)
    monkeypatch.setenv('CACHE_MAX_GB', str(7 / 1024 ** 3))
    monkeypatch.setenv('CACHE_MIN_FREE_GB', '0')
    monkeypatch.setattr(cache.shutil, 'disk_usage', lambda _: SimpleNamespace(free=0))
    assert cache._try_mark_cache_active(str(entries[0]))

    removed = cache._evict_lru_sync(str(tmp_path))
    cache._unmark_cache_active(str(entries[0]))

    assert removed == 2
    assert entries[0].exists()
    assert not entries[1].exists() and not entries[2].exists()
    assert entries[3].exists()


@pytest.mark.parametrize('option_name,left,right', [
    ('auto_metadata', True, False),
    ('auto_organize', True, False),
    ('rename_mr_series', False, True),
    ('extra_derived_keywords', ['AI_'], ['OTHER']),
])
def test_cache_fingerprint_covers_execution_options(option_name, left, right):
    base = {'output_format': 'nifti'}
    left_options = {**base, option_name: left}
    right_options = {**base, option_name: right}

    assert cache.get_cache_dir('results', 'A', left_options) != cache.get_cache_dir(
        'results', 'A', right_options)


def test_concurrent_reconnects_register_only_one_task():
    from concurrent.futures import ThreadPoolExecutor

    options = {'output_format': 'nifti'}
    namespace = task_namespace(make_task(options=options))
    namespace['processing_tasks'] = {}
    load_app_function('_find_reusable_single_task', namespace)

    class FakeProcessingTask:
        def __init__(self, task_id, task_type, parameters):
            self.task_id = task_id
            self.task_type = task_type
            self.parameters = parameters
            self.status = 'pending'
            self.client_ip = None
            self.slot_released = False

    namespace.update({
        'request': SimpleNamespace(
            json={'accession_number': 'A', 'options': options}, remote_addr='client'),
        'logger': logging.getLogger('offline'),
        'ProcessingTask': FakeProcessingTask,
        'uuid': __import__('uuid'),
        '_submit_task_to_queue': Mock(return_value=True),
        'jsonify': lambda value: value,
        'app': SimpleNamespace(
            config={'RESULT_FOLDER': 'results'},
            route=lambda *args, **kwargs: (lambda func: func),
        ),
    })
    handler = load_app_function('process_single', namespace)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda _: handler(), range(16)))

    assert len({response['task_id'] for response in responses}) == 1
    assert sum(response.get('reused') is True for response in responses) == 15
    assert len(namespace['processing_tasks']) == 1
    namespace['_submit_task_to_queue'].assert_called_once()


def test_result_cleanup_always_schedules_cache_maintenance(tmp_path):
    enforce = Mock()
    namespace = {
        '_cleanup_lock': threading.Lock(),
        'app': SimpleNamespace(config={'RESULT_FOLDER': str(tmp_path)}),
        'get_directory_size': lambda _: 0.0,
        '_get_free_space_gb': lambda _: 100.0,
        'CLEANUP_THRESHOLD_GB': 50.0,
        'CLEANUP_MIN_FREE_GB': 10.0,
        'CACHE_DIR_NAME': 'cache',
        'logger': logging.getLogger('offline'),
        'os': os,
        'enforce_cache_limit': enforce,
    }
    cleanup = load_app_function('cleanup_old_results', namespace)

    cleanup()

    enforce.assert_called_once_with(str(tmp_path / 'cache'))


def test_cache_state_lock_is_real_os_lock_under_eventlet():
    eventlet = pytest.importorskip('eventlet')
    from eventlet.patcher import original
    from src.utils.offload import run_cpu_bound

    real_threading = original('threading')
    assert type(cache._cache_state_lock) is type(real_threading.Lock())

    def contend():
        for _ in range(100):
            if cache._try_mark_cache_active('synthetic'):
                cache._unmark_cache_active('synthetic')

    jobs = [eventlet.spawn(run_cpu_bound, contend) for _ in range(2)]
    with eventlet.Timeout(10):
        for job in jobs:
            job.wait()
