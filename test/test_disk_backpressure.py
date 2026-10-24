"""Disk watermark backpressure regressions; no PACS or real disk changes."""
from types import SimpleNamespace
from unittest.mock import Mock

from src.client import unified


def make_client():
    client = unified.DICOMDownloadClient.__new__(unified.DICOMDownloadClient)
    client._download_high_watermark_gb = 45.0
    client._download_low_watermark_gb = 40.0
    return client


def test_disk_wait_resumes_at_high_watermark(monkeypatch):
    monkeypatch.setenv('DOWNLOAD_LOW_WATERMARK_GB', '100')
    monkeypatch.setenv('DOWNLOAD_HIGH_WATERMARK_GB', '120')
    free_values = iter([90.0, 120.0])
    monkeypatch.setattr(
        unified.shutil, 'disk_usage',
        lambda _: SimpleNamespace(free=next(free_values) * 1024 ** 3)
    )
    monkeypatch.setattr(unified.time, 'sleep', lambda _: None)
    logger = Mock()
    monkeypatch.setattr(unified, 'logger', logger)

    make_client()._wait_for_disk_low('/synthetic')

    assert 'Pausing downloads until free >= 120.0GB' in logger.warning.call_args[0][0]
    assert 'resuming downloads' in logger.info.call_args[0][0]


def test_disk_wait_relogs_every_five_minutes(monkeypatch):
    monkeypatch.setenv('DOWNLOAD_LOW_WATERMARK_GB', '100')
    monkeypatch.setenv('DOWNLOAD_HIGH_WATERMARK_GB', '120')
    free_values = iter([90.0, 90.0, 90.0, 120.0])
    monkeypatch.setattr(
        unified.shutil, 'disk_usage',
        lambda _: SimpleNamespace(free=next(free_values) * 1024 ** 3)
    )
    monkeypatch.setattr(unified.time, 'sleep', lambda _: None)
    monotonic_values = iter([1000.0, 1001.0, 1301.0])
    monkeypatch.setattr(unified.time, 'monotonic', lambda: next(monotonic_values))
    logger = Mock()
    monkeypatch.setattr(unified, 'logger', logger)

    make_client()._wait_for_disk_low('/synthetic')

    assert logger.warning.call_count == 2
    assert logger.info.call_count == 1
