"""D24 offline workflow + real ZIP regressions. No client/PACS initialization.

The Web handler is compiled from its actual source function to avoid importing
app.py (background services and environment setup).
Only its external service dependencies are substituted; delivery code is real.
"""
import ast
import logging
import shutil
import socket
import sys
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

import nibabel as nib
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_conversion_failures import study, partial_dcm2niix
from src.client import unified
from src.client.unified import DICOMDownloadClient
from src.core import convert
from src.core.organize import organize_dicom_files, process_single_series
from src.utils.packaging import create_result_zip


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError('Network/PACS access forbidden in offline regressions')
    monkeypatch.setattr(socket.socket, 'connect', blocked)
    monkeypatch.setattr(socket.socket, 'connect_ex', blocked)
    monkeypatch.setattr(unified.AE, 'associate', blocked)
    monkeypatch.setattr(DICOMDownloadClient, '__init__', blocked)
    monkeypatch.setattr(unified, 'QueueWatchdog', lambda *a, **k: SimpleNamespace(
        start=lambda: None, stop=lambda: None, update_activity=lambda: None))
    monkeypatch.setenv('NUM_CONVERTERS', '2')


@pytest.fixture
def workflow(study, tmp_path, monkeypatch):
    client, bad, files, ds = study
    good = bad.parent / '002_DX'
    shutil.copytree(bad, good)
    originals = {p.relative_to(bad.parent).as_posix(): p.read_bytes()
                 for folder in (bad, good) for p in folder.glob('*.dcm')}
    calls = []
    # Filesystem iteration order is not guaranteed; force failed series first.
    import os
    listdir = os.listdir
    monkeypatch.setattr(os, 'listdir', lambda path: sorted(listdir(path)))

    def download(*args, on_series_downloaded=None, **kwargs):
        if on_series_downloaded:
            # Failure first: serial processing must still reach the next series.
            for folder in (bad, good):
                on_series_downloaded(str(folder), {})
        return str(bad.parent)

    client.download_study = download
    client.extract_zip = lambda *a, **k: str(bad.parent)
    client._wait_for_files_stable = lambda *a: None
    client._get_series_sample_dicom = lambda path: (ds, 'DX')
    client.organize_dicom_files = lambda *a, **k: organize_dicom_files(client, *a, **k)
    client._process_single_series = lambda path, name, fmt, **k: process_single_series(
        client, path, name, path, output_format=fmt, **k)
    client.extract_dicom_metadata = lambda *a, **k: None

    def configure(fmt, failure, all_failed=False):
        def converter(path, name, **kwargs):
            calls.append(name)
            folder = Path(path)
            output = folder / ('converted.nii.gz' if fmt == 'nifti' else 'converted.npz')
            # Residual output must NOT override a failed result or exception.
            if fmt == 'nifti':
                nib.save(nib.Nifti1Image(np.ones((2, 2, 2)), np.eye(4)), output)
            else:
                np.savez_compressed(output, data=np.ones((2, 2, 2)))
            if name == bad.name or all_failed:
                if failure == 'exception':
                    raise ValueError('geometry verification failed')
                if failure == 'empty_exception':
                    raise RuntimeError()
                if failure == 'none':
                    return None
                return {'success': False, 'error': '' if failure == 'empty_error' else 'incomplete'}
            # Model the converter's D23 success-only cleanup, never the organizer's.
            for source in folder.glob('*.dcm'):
                source.unlink()
            return {'success': True, 'output_files': [output.name]}
        client.convert_dicom_to_nifti = converter
        client._convert_to_npz = converter

    def run(mode, fmt='nifti', metadata=False):
        if mode == 'upload':
            return DICOMDownloadClient.process_upload_workflow(
                client, 'synthetic.zip', str(tmp_path),
                {'output_format': fmt, 'auto_metadata': metadata})
        return DICOMDownloadClient.process_complete_workflow(
            client, 'SYNTHETIC', str(tmp_path), output_format=fmt,
            auto_metadata=metadata, parallel_pipeline=(mode == 'parallel'))

    return SimpleNamespace(client=client, bad=bad, good=good, originals=originals,
                           configure=configure, run=run, calls=calls)


def assert_zip(result, workflow, tmp_path):
    # Same production include_subdirs contract: failed entries MUST remain here.
    path = create_result_zip(result['organized_dir'], 'offline', str(tmp_path / 'delivery'),
                             extra_files=[result.get('excel_file')],
                             include_subdirs=set(result['series_info']))
    with zipfile.ZipFile(path) as archive:
        assert archive.testzip() is None
        names = archive.namelist()
        for name, data in workflow.originals.items():
            if name.startswith(workflow.bad.name + '/'):
                assert archive.read(name) == data
        assert any(n.startswith(workflow.good.name + '/') and n.endswith(('.nii.gz', '.npz'))
                   for n in names)
        assert not any(n.endswith('.processing_lock') for n in names)
        if result.get('excel_file'):
            assert Path(result['excel_file']).name in names
    return path


@pytest.mark.parametrize('mode', ['serial', 'parallel', 'upload'])
@pytest.mark.parametrize('fmt', ['nifti', 'npz'])
@pytest.mark.parametrize('failure', ['result', 'exception', 'none', 'empty_error', 'empty_exception'])
def test_mixed_success_delivers_raw_failed_series(workflow, mode, fmt, failure, tmp_path):
    workflow.configure(fmt, failure)
    result = workflow.run(mode, fmt)
    assert result['success'] is True
    assert set(workflow.calls) == {workflow.bad.name, workflow.good.name}
    assert set(result['conversion_errors']) == {workflow.bad.name}
    assert result['conversion_errors'][workflow.bad.name]
    assert result['series_info'][workflow.good.name]['conversion_error'] is None
    assert result['series_info'][workflow.bad.name]['conversion_error'] == result['conversion_errors'][workflow.bad.name]
    assert_zip(result, workflow, tmp_path)


@pytest.mark.parametrize('mode', ['serial', 'parallel', 'upload'])
@pytest.mark.parametrize('fmt', ['nifti', 'npz'])
@pytest.mark.parametrize('failure', ['result', 'exception', 'none', 'empty_error', 'empty_exception'])
def test_all_failed_is_failed_despite_residual_outputs(workflow, mode, fmt, failure):
    workflow.configure(fmt, failure, all_failed=True)
    workflow.client.extract_dicom_metadata = lambda *a, **k: pytest.fail('must stop before metadata')
    result = workflow.run(mode, fmt, metadata=True)
    assert result['success'] is False
    assert 'all series' in result['error']
    assert set(result['conversion_errors']) == {workflow.bad.name, workflow.good.name}
    assert len(workflow.calls) == 2
    assert 'result_zip' not in result
    for name, data in workflow.originals.items():
        assert (workflow.bad.parent / name).read_bytes() == data


@pytest.mark.parametrize('mode', ['serial', 'parallel', 'upload'])
@pytest.mark.parametrize('outcome', ['success', 'none', 'exception'])
def test_optional_metadata_does_not_cancel_delivery(workflow, mode, outcome, tmp_path):
    workflow.configure('nifti', 'result')
    def metadata(directory, output_excel=None):
        if outcome == 'exception':
            raise OSError('synthetic metadata failure')
        if outcome == 'none':
            return None
        path = Path(output_excel or Path(directory) / 'metadata.xlsx')
        path.write_bytes(b'synthetic optional metadata')
        return str(path)
    workflow.client.extract_dicom_metadata = metadata
    result = workflow.run(mode, metadata=True)
    assert result['success'] is True
    assert bool(result.get('metadata_warning')) == (outcome != 'success')
    assert set(result['conversion_errors']) == {workflow.bad.name}
    assert_zip(result, workflow, tmp_path)


@pytest.mark.parametrize('mode', ['serial', 'parallel', 'upload'])
def test_real_d23_incomplete_series_stays_failed_in_mixed_delivery(workflow, mode, monkeypatch, tmp_path):
    import pydicom
    partial_dcm2niix(monkeypatch)
    broken_path = workflow.bad / '1.dcm'
    broken = pydicom.dcmread(broken_path)
    del broken.PixelData
    broken.save_as(broken_path)
    workflow.originals[f'{workflow.bad.name}/1.dcm'] = broken_path.read_bytes()
    workflow.client.convert_dicom_to_nifti = lambda *a, **k: convert.convert_dicom_to_nifti(
        workflow.client, *a, **k)
    result = workflow.run(mode)
    assert result['success'] is True
    assert set(result['conversion_errors']) == {workflow.bad.name}
    assert not list(workflow.good.glob('*.dcm'))
    assert_zip(result, workflow, tmp_path)


@pytest.mark.parametrize('mode', ['serial', 'parallel', 'upload'])
def test_no_eligible_series_cannot_succeed(workflow, mode):
    shutil.rmtree(workflow.bad)
    shutil.rmtree(workflow.good)
    result = workflow.run(mode)
    assert result['success'] is False
    assert 'no eligible series' in result['error']


@pytest.mark.parametrize('mode', ['serial', 'parallel'])
@pytest.mark.parametrize('fmt', [None, False])
def test_raw_dicom_mode_is_not_conversion_failure(workflow, mode, fmt):
    workflow.client.convert_dicom_to_nifti = lambda *a, **k: pytest.fail('raw mode')
    result = workflow.run(mode, fmt)
    assert result['success'] is True
    assert not result.get('conversion_errors')
    for name, data in workflow.originals.items():
        assert (workflow.bad.parent / name).read_bytes() == data


@pytest.mark.parametrize('mode', ['serial', 'parallel'])
def test_metadata_timeout_is_optional(workflow, mode, monkeypatch, tmp_path):
    # Release the worker explicitly: no abandoned thread survives this test.
    release = unified.threading.Event()
    finished = unified.threading.Event()
    def metadata(*args, **kwargs):
        try:
            release.wait()
        finally:
            finished.set()
    workflow.configure('nifti', 'result')
    workflow.client.extract_dicom_metadata = metadata
    monkeypatch.setenv('METADATA_TIMEOUT_SEC', '0')
    try:
        result = workflow.run(mode, metadata=True)
        assert result['success'] is True
        assert result['metadata_warning']
        assert not result.get('excel_file')
        assert_zip(result, workflow, tmp_path)
    finally:
        release.set()
        assert finished.wait(timeout=5)


def load_web_handler(name, namespace):
    source = Path(__file__).resolve().parents[1] / 'src/web/app.py'
    node = next(n for n in ast.parse(source.read_text(encoding='utf-8')).body
                if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), 'exec'), namespace)
    return namespace[name]


@pytest.mark.parametrize('mode', ['serial', 'parallel', 'upload'])
@pytest.mark.parametrize('outcome', ['mixed', 'all_failed', 'zip_failed'])
def test_real_web_task_delivers_or_fails(workflow, mode, outcome, tmp_path):
    import os
    from types import MethodType
    workflow.configure('nifti', 'exception', all_failed=(outcome == 'all_failed'))
    client = workflow.client
    client.check_status = lambda **k: True
    client.login = lambda *a: True
    client.logout = lambda: None
    client._query_series_metadata = lambda *a, **k: [{}, {}]
    def complete(**kwargs):
        # Web currently selects parallel; force the serial branch for parity coverage.
        kwargs['parallel_pipeline'] = mode == 'parallel'
        return DICOMDownloadClient.process_complete_workflow(client, **kwargs)
    client.process_complete_workflow = complete
    client.process_upload_workflow = MethodType(DICOMDownloadClient.process_upload_workflow, client)
    task = SimpleNamespace(
        task_id='synthetic', parameters={
            'accession_number': 'SYNTHETIC', 'filepath': 'synthetic.zip',
            'filename': 'synthetic.zip', 'options': {}},
        result=None, error=None, status=None, logs=[],
        check_cancellation=lambda *a: None, is_cancelled=lambda: False)
    task.add_log = lambda msg, *a: task.logs.append(msg)
    task.update_status = lambda status, *a: setattr(task, 'status', status)
    zip_calls = []
    def package(*args, **kwargs):
        zip_calls.append(1)
        if outcome == 'zip_failed':
            raise OSError('synthetic ZIP failure')
        return create_result_zip(*args, **kwargs)
    noop = lambda *a, **k: None
    namespace = dict(
        os=os, time=time, logger=logging.getLogger('offline'),
        app=SimpleNamespace(config={'RESULT_FOLDER': str(tmp_path / 'web-results')}),
        DICOMDownloadClient=lambda: client, _attach_task_log_handler=noop,
        _detach_task_log_handler=noop, _parse_derived_keywords=lambda options: None,
        get_cache_dir=lambda *a: str(tmp_path / 'unused-cache'), cache_exists=lambda *a: False,
        save_to_cache=noop, run_cpu_bound=lambda fn, *a, **k: fn(*a, **k),
        create_result_zip=package, _record_task_completion=noop, check_and_cleanup_results=noop)
    handler = load_web_handler('process_upload_task' if mode == 'upload' else 'process_single_task', namespace)
    handler(task)
    if outcome != 'mixed':
        assert task.status == 'failed'
        assert task.error
        assert len(zip_calls) == (outcome == 'zip_failed')
        assert not (task.result or {}).get('result_zip')
        if outcome == 'all_failed':
            assert set(task.result['conversion_errors']) == {workflow.bad.name, workflow.good.name}
        return
    assert task.status == 'completed', task.logs
    assert task.result['success'] is True
    assert set(task.result['conversion_errors']) == {workflow.bad.name}
    assert task.result['metadata_warning']
    assert any('Partial success' in msg for msg in task.logs)
    with zipfile.ZipFile(task.result['result_zip']) as archive:
        for name, data in workflow.originals.items():
            if name.startswith(workflow.bad.name + '/'):
                assert archive.read(name) == data
        assert f'{workflow.good.name}/converted.nii.gz' in archive.namelist()
