"""Offline regressions: partial conversion must retain source images."""
import sys
from pathlib import Path
from types import SimpleNamespace

import nibabel as nib
import numpy as np
import pydicom
import pytest
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, SecondaryCaptureImageStorage, generate_uid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.core import convert
from src.core.organize import process_single_series, organize_dicom_files


@pytest.fixture
def study(tmp_path):
    folder = tmp_path / '001_DX'
    folder.mkdir()
    files = []
    for i in range(2):
        meta = FileMetaDataset()
        meta.TransferSyntaxUID = ExplicitVRLittleEndian
        meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
        meta.MediaStorageSOPInstanceUID = generate_uid()
        ds = FileDataset(None, {}, file_meta=meta, preamble=b'\0' * 128)
        ds.SOPClassUID = meta.MediaStorageSOPClassUID
        ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
        ds.Modality = 'DX'
        ds.ImageType = ['ORIGINAL', 'PRIMARY']
        ds.Rows = ds.Columns = 16
        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = 'MONOCHROME2'
        ds.BitsAllocated = ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 0
        ds.PixelSpacing = [1, 1]
        ds.PixelData = (np.arange(256, dtype=np.uint16) + i).tobytes()
        path = folder / f'{i}.dcm'
        ds.save_as(path)
        files.append(str(path))
    client = SimpleNamespace(
        _sanitize_folder_name=lambda name: name,
        _ensure_metadata_cache=lambda *a, **k: None,
        _cache_metadata_for_series=lambda *a, **k: None,
        _write_minimal_cache=lambda *a, **k: None,
        _generate_series_preview=lambda *a, **k: None,
        _is_dicom_file=lambda path: str(path).endswith('.dcm'),
        _load_sample_tags_from_cache=lambda *a: {},
    )
    # Existing metadata prevents unrelated preview-cache fallback work.
    (folder / 'dicom_metadata_cache.json').write_text('{}')
    return client, folder, files, ds


def partial_dcm2niix(monkeypatch):
    def run(cmd, **kwargs):
        if '-h' in cmd:
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        name = cmd[cmd.index('-f') + 1]
        if name.endswith('0002'):
            return SimpleNamespace(returncode=1, stdout='', stderr='simulated WinError 32')
        target = Path(cmd[cmd.index('-o') + 1]) / (name + '.nii.gz')
        nib.save(nib.Nifti1Image(np.arange(256, dtype=np.float32).reshape(16, 16, 1), np.eye(4)), target)
        return SimpleNamespace(returncode=0, stdout='', stderr='')
    monkeypatch.setattr(convert, '_run_subprocess_with_timeout', run)
    monkeypatch.setattr(convert.time, 'sleep', lambda _: None)


def test_dcm2niix_partial_failure_retains_all_sources(study, monkeypatch):
    client, folder, files, ds = study
    partial_dcm2niix(monkeypatch)
    result = convert.convert_with_dcm2niix(client, str(folder), 'DX', files, ds, 'DX')
    assert result['success'] is False
    assert result['file_count'] == 1
    assert all(Path(p).exists() for p in files)


@pytest.mark.parametrize('save_to_disk', [True, False])
def test_python_partial_failure_retains_all_sources(study, save_to_disk):
    client, folder, files, ds = study
    broken = pydicom.dcmread(files[1])
    del broken.PixelData
    broken.save_as(files[1])
    result = convert.convert_with_python_libs(
        client, str(folder), 'DX', files, ds, 'DX', save_to_disk=save_to_disk
    )
    assert result['success'] is False
    assert result['file_count'] == 1
    assert all(Path(p).exists() for p in files)


def test_partial_dcm2niix_falls_back_with_complete_sources(study, monkeypatch):
    client, folder, files, ds = study
    partial_dcm2niix(monkeypatch)
    result = convert.convert_dicom_to_nifti(client, str(folder), 'DX', files, ds, 'DX')
    assert result['success'] is True
    assert result['method'] == 'python_libs'
    assert result['file_count'] == 2
    assert all((folder / name).is_file() for name in result['output_files'])
    assert not any(Path(p).exists() for p in files)


def test_organizer_propagates_conversion_failure(study):
    client, folder, files, ds = study
    client.convert_dicom_to_nifti = lambda *a, **k: {'success': False, 'error': 'incomplete'}
    info = process_single_series(client, str(folder), '001_DX', str(folder.parent))
    assert info['conversion_error'] == 'incomplete'
    assert not (folder / '.processing_lock').exists()
    _, series_info = organize_dicom_files(client, str(folder.parent), output_format='nifti')
    assert series_info['001_DX']['conversion_error'] == 'incomplete'
    assert all(Path(p).exists() for p in files)


def test_subprocess_output_and_timeout():
    result = convert._run_subprocess_with_timeout(
        [sys.executable, '-c', "import os; os.write(1,b'hello'); os.write(2,b'\\xff')"], 5
    )
    assert result.returncode == 0
    assert result.stdout == 'hello'
    assert result.stderr
    result = convert._run_subprocess_with_timeout(
        [sys.executable, '-c', 'import time; time.sleep(30)'], .1
    )
    assert result.returncode == -1
    assert 'Timeout' in result.stderr


def test_subprocess_spawn_failure(tmp_path):
    result = convert._run_subprocess_with_timeout([str(tmp_path / 'missing.exe')], 1)
    assert result.returncode == -1
    assert result.stderr


def test_pipeline_does_not_complete_failed_conversion(study, monkeypatch):
    from src.client.unified import DICOMDownloadClient
    from src.client import unified
    client, folder, files, ds = study
    # Pure workflow stub: no client initialization, PACS association or .env access.
    def download(*args, on_series_downloaded=None, **kwargs):
        on_series_downloaded(str(folder), {})
        return str(folder.parent)
    client.download_study = download
    client._process_single_series = lambda *a, **k: {
        'path': str(folder), 'conversion_error': 'incomplete'
    }
    client.extract_dicom_metadata = lambda *a, **k: pytest.fail('must not generate success metadata')
    monkeypatch.setattr(unified, 'QueueWatchdog', lambda *a, **k: SimpleNamespace(
        start=lambda: None, stop=lambda: None, update_activity=lambda: None
    ))
    result = DICOMDownloadClient.process_complete_workflow(
        client, 'SYNTHETIC', str(folder.parent), parallel_pipeline=True
    )
    assert result['success'] is False
    assert 'incomplete' in result['error']


def test_partial_nifti_does_not_erase_pipeline_exception(study,monkeypatch):
    from src.client.unified import DICOMDownloadClient
    from src.client import unified
    client,folder,files,ds=study
    nib.save(nib.Nifti1Image(np.zeros((3,4,2),np.int16),np.eye(4)),folder/'partial.nii.gz')
    def download(*args,on_series_downloaded=None,**kwargs):
        on_series_downloaded(str(folder),{});return str(folder.parent)
    def failed(*args,**kwargs):raise ValueError('geometry verification failed')
    client.download_study=download;client._process_single_series=failed
    client.extract_dicom_metadata=lambda *a,**k:pytest.fail('must not approve partial output')
    monkeypatch.setattr(unified,'QueueWatchdog',lambda *a,**k:SimpleNamespace(start=lambda:None,stop=lambda:None,update_activity=lambda:None))
    result=DICOMDownloadClient.process_complete_workflow(client,'SYNTHETIC',str(folder.parent),parallel_pipeline=True)
    assert result['success'] is False and 'geometry verification failed' in result['error']


def test_eventlet_subprocess_keeps_hub_responsive():
    eventlet = pytest.importorskip('eventlet')
    beats = []
    def heartbeat():
        while True:
            beats.append(1)
            eventlet.sleep(.01)
    worker = eventlet.spawn(heartbeat)
    try:
        result = convert._run_subprocess_with_timeout(
            [sys.executable, '-c', 'import time; time.sleep(30)'], .3
        )
        assert result.returncode == -1
        assert len(beats) >= 5
    finally:
        worker.kill()


def test_eventlet_concurrent_subprocess_workers():
    eventlet = pytest.importorskip('eventlet')
    from eventlet.patcher import original
    from src.utils.offload import run_cpu_bound
    assert convert._native_subprocess.threading is original('threading')
    assert convert._native_subprocess.time is original('time')
    jobs = [eventlet.spawn(
        run_cpu_bound, convert._run_subprocess_with_timeout,
        [sys.executable, '-c', 'import time; time.sleep(.05)'], 5
    ) for _ in range(3)]
    with eventlet.Timeout(10):
        assert all(job.wait().returncode == 0 for job in jobs)


@pytest.mark.parametrize('taskkill_fails', [False, True])
def test_windows_timeout_kills_tree_before_parent(monkeypatch, taskkill_fails):
    native = convert._native_subprocess
    calls = []
    class Process:
        pid = 12345
        returncode = None
        def wait(self, timeout):
            if self.returncode is None:
                raise native.TimeoutExpired('synthetic', timeout)
            return self.returncode
        def poll(self):
            return self.returncode
        def kill(self):
            calls.append('kill')
            self.returncode = -1
    proc = Process()
    def taskkill(cmd, **kwargs):
        assert cmd == ['taskkill', '/F', '/T', '/PID', '12345']
        assert proc.poll() is None
        calls.append('taskkill')
        if taskkill_fails:
            raise OSError('taskkill unavailable')
        proc.returncode = -1
    monkeypatch.setattr(convert, 'sys', SimpleNamespace(platform='win32'))
    monkeypatch.setattr(convert, '_native_subprocess', SimpleNamespace(
        Popen=lambda *a, **k: proc, run=taskkill,
        TimeoutExpired=native.TimeoutExpired, CompletedProcess=native.CompletedProcess,
        DEVNULL=native.DEVNULL,
    ))
    result = convert._run_native_subprocess(['synthetic'], .1)
    assert result.returncode == -1
    assert calls == (['taskkill', 'kill'] if taskkill_fails else ['taskkill'])


def test_python_libs_rejects_truly_blank_xray(study):
    client, folder, files, ds = study
    blank = pydicom.dcmread(files[1])
    blank.PixelData = np.full(256, 4095, dtype=np.uint16).tobytes()
    blank.save_as(files[1])

    result = convert.convert_with_python_libs(client, str(folder), 'DX', files, ds, 'DX')

    assert result['success'] is False
    assert result['file_count'] == 1
    assert all(Path(path).exists() for path in files)


def test_python_libs_accepts_nonblank_pixels_with_tiny_rescale_slope(study):
    client, folder, files, ds = study
    tiny = pydicom.dcmread(files[0])
    tiny.RescaleSlope = 1e-9
    tiny.save_as(files[0])

    result = convert.convert_with_python_libs(client, str(folder), 'DX', files, ds, 'DX')

    assert result['success'] is True
    assert result['file_count'] == 2


def test_dcm2niix_rejects_unverified_normalization(study, monkeypatch):
    client, folder, files, ds = study
    def run(cmd, **kwargs):
        if '-h' in cmd:
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        name = cmd[cmd.index('-f') + 1]
        target = Path(cmd[cmd.index('-o') + 1]) / (name + '.nii.gz')
        data = np.arange(256, dtype=np.float32).reshape(16, 16, 1)
        nib.save(nib.Nifti1Image(data, np.eye(4)), target)
        return SimpleNamespace(returncode=0, stdout='', stderr='')
    monkeypatch.setattr(convert, '_run_subprocess_with_timeout', run)
    monkeypatch.setattr(
        convert, 'normalize_2d_nifti_display',
        lambda *args, **kwargs: {'action': 'not_verifiable', 'message': 'synthetic mismatch'}
    )

    result = convert.convert_with_dcm2niix(client, str(folder), 'DX', files, ds, 'DX')

    assert result['success'] is False
    assert result['file_count'] == 0
    assert all(Path(path).exists() for path in files)


def test_npz_fallback_removes_sources_and_intermediate_nifti(study, monkeypatch):
    client, folder, files, ds = study
    partial_dcm2niix(monkeypatch)
    client._assess_series_quality_converted = lambda *args, **kwargs: {
        'low_quality': 0, 'low_quality_ratio': 0.0,
        'qc_mode': 'sample', 'qc_sample_indices': [0],
    }

    result = convert.convert_to_npz(client, str(folder), 'DX', files, ds, 'DX')

    assert result['success'] is True
    assert len(result['output_files']) == 2
    assert not list(folder.rglob('*.dcm'))
    assert not list(folder.rglob('*.nii.gz'))
    assert all((folder / name).is_file() for name in result['output_files'])


def test_npz_fails_closed_when_rejected_nifti_cannot_be_removed(study, monkeypatch):
    client, folder, files, ds = study
    client._assess_series_quality_converted = lambda *args, **kwargs: {
        'low_quality': 0, 'low_quality_ratio': 0.0,
        'qc_mode': 'sample', 'qc_sample_indices': [0],
    }
    def run(cmd, **kwargs):
        if '-h' in cmd:
            return SimpleNamespace(returncode=0, stdout='', stderr='')
        name = cmd[cmd.index('-f') + 1]
        target = Path(cmd[cmd.index('-o') + 1]) / (name + '.nii.gz')
        data = np.arange(256, dtype=np.float32).reshape(16, 16, 1)
        nib.save(nib.Nifti1Image(data, np.eye(4)), target)
        return SimpleNamespace(returncode=0, stdout='', stderr='')
    monkeypatch.setattr(convert, '_run_subprocess_with_timeout', run)
    monkeypatch.setattr(
        convert, 'normalize_2d_nifti_display',
        lambda *args, **kwargs: {'action': 'not_verifiable', 'message': 'synthetic mismatch'}
    )
    original_remove = convert.os.remove
    def locked_nifti(path):
        if str(path).endswith('.nii.gz'):
            raise PermissionError('synthetic file lock')
        return original_remove(path)
    monkeypatch.setattr(convert.os, 'remove', locked_nifti)

    result = convert.convert_to_npz(client, str(folder), 'DX', files, ds, 'DX')

    assert result['success'] is False
    assert list(folder.rglob('*.nii.gz'))


def test_python_libs_removes_extensionless_sources(study):
    client, folder, files, ds = study
    extensionless = []
    for index, path in enumerate(files):
        target = folder / str(index)
        Path(path).rename(target)
        extensionless.append(str(target))

    result = convert.convert_with_python_libs(
        client, str(folder), 'DX', extensionless, ds, 'DX'
    )

    assert result['success'] is True
    assert not any(Path(path).exists() for path in extensionless)
