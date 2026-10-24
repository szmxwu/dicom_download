"""Memory regressions for QC, triplane previews, and heavy-work backpressure.

All fixtures are synthetic; no PACS or patient data is involved.
"""
import threading
import time
from types import SimpleNamespace

import nibabel as nib
import numpy as np
import pytest

from src.core import preview, qc
from src.utils import memory


def test_qc_samples_before_full_float32_conversion(monkeypatch):
    data = np.arange(600_000, dtype=np.int16).reshape(100, 100, 60)
    original_asarray = np.asarray

    def no_full_float32(array, *args, **kwargs):
        if kwargs.get('dtype') == np.float32 and getattr(array, 'size', 0) > 200_000:
            raise AssertionError('QC converted the full volume to float32 before sampling')
        return original_asarray(array, *args, **kwargs)

    monkeypatch.setattr(qc.np, 'asarray', no_full_float32)
    result = qc.assess_image_quality_from_array(data, 'DX')
    expected = data.ravel()[::3].astype(np.float32)
    p2, p98 = np.percentile(expected, [2.0, 98.0])

    assert not result.metrics.get('error')
    assert result.metrics['original_pixel_count'] == 200_000
    assert result.metrics['p2'] == pytest.approx(round(float(p2), 2))
    assert result.metrics['p98'] == pytest.approx(round(float(p98), 2))


def test_qc_nonfinite_filter_precedes_sampling_exactly_like_legacy_qc():
    data = np.full(400_001, np.nan, dtype=np.float32)
    data[1::2] = np.arange(200_000, dtype=np.float32)
    data[3] = np.inf

    result = qc.assess_image_quality_from_array(data, 'DX')
    legacy = data[np.isfinite(data)].ravel()
    legacy = legacy[::max(1, legacy.size // 200_000)]
    p2, p98 = np.percentile(legacy, [2.0, 98.0])

    assert not result.metrics.get('error')
    assert result.metrics['original_pixel_count'] == legacy.size
    assert result.metrics['p2'] == pytest.approx(round(float(p2), 2))
    assert result.metrics['p98'] == pytest.approx(round(float(p98), 2))


@pytest.mark.parametrize('modality', ['CT', 'DX'])
def test_nifti_qc_does_not_call_get_fdata(tmp_path, monkeypatch, modality):
    path = tmp_path / 'synthetic.nii.gz'
    data = np.arange(8 * 8 * 3, dtype=np.int16).reshape(8, 8, 3)
    nib.save(nib.Nifti1Image(data, np.eye(4)), path)
    image = nib.load(path)

    def forbidden():
        raise AssertionError('get_fdata allocated a float64 volume')

    image.get_fdata = forbidden
    monkeypatch.setattr(qc.nib, 'load', lambda _: image)

    result = qc.assess_converted_file_quality(str(path), modality)

    assert not result.metrics.get('error')
    assert result.metrics['modality'] == modality


def test_nifti_qc_defers_nonunit_scaling_to_the_sample(tmp_path, monkeypatch):
    path = tmp_path / 'scaled.nii.gz'
    data = np.arange(600_000, dtype=np.int16).reshape(600, 1000)
    image = nib.Nifti1Image(data, np.eye(4))
    image.header.set_slope_inter(2.0, -1024.0)
    nib.save(image, path)

    def forbidden_scaled_volume(*args, **kwargs):
        raise AssertionError('QC materialized the scaled float64 volume')

    monkeypatch.setattr(nib.arrayproxy.ArrayProxy, '__array__', forbidden_scaled_volume)
    result = qc.assess_converted_file_quality(str(path), 'DX')

    legacy = (data.astype(np.float64) * 2.0 - 1024.0).astype(np.float32).ravel()[::3]
    p2, p98 = np.percentile(legacy, [2.0, 98.0])
    assert not result.metrics.get('error')
    assert result.metrics['p2'] == pytest.approx(round(float(p2), 2))
    assert result.metrics['p98'] == pytest.approx(round(float(p98), 2))


class SliceOnlyData:
    def __init__(self, data):
        self.data = data
        self.shape = data.shape
        self.ndim = data.ndim
        self.dtype = data.dtype
        self.keys = []

    def __getitem__(self, key):
        self.keys.append(key)
        if all(index == slice(None) for index in key[:3]):
            raise AssertionError('preview requested the complete spatial volume')
        return self.data[key]


@pytest.mark.parametrize('shape', [(5, 6, 7), (5, 6, 7, 2)])
@pytest.mark.parametrize('affine', [
    np.diag([-1.0, 1.0, 1.0, 1.0]),
    np.array([[0., 1., 0., 0.], [1., 0., 0., 0.],
              [0., 0., 2., 0.], [0., 0., 0., 1.]]),
])
def test_canonical_triplane_reads_only_three_slices_with_real_reorientation(shape, affine):
    data = np.arange(np.prod(shape), dtype=np.int16).reshape(shape)
    dataobj = SliceOnlyData(data)
    image = nib.Nifti1Image(data, affine)
    image._dataobj = dataobj
    image.get_fdata = lambda: (_ for _ in ()).throw(
        AssertionError('preview called get_fdata'))

    canonical, axial, sagittal, coronal = preview._canonical_triplane_slices(image)

    reference = nib.as_closest_canonical(nib.Nifti1Image(data, affine))
    first = np.asanyarray(reference.dataobj)
    first = first[..., 0] if len(shape) == 4 else first
    mid_x, mid_y, mid_z = (size // 2 for size in reference.shape[:3])
    assert len(dataobj.keys) == 3
    np.testing.assert_allclose(canonical.affine, reference.affine)
    assert canonical.shape == reference.shape
    assert canonical.header.get_zooms() == reference.header.get_zooms()
    np.testing.assert_array_equal(axial, np.transpose(first[:, :, mid_z], (1, 0))[::-1, ::-1])
    np.testing.assert_array_equal(sagittal, np.transpose(first[mid_x, :, :], (1, 0))[::-1, ::-1])
    np.testing.assert_array_equal(coronal, np.transpose(first[:, mid_y, :], (1, 0))[::-1, ::-1])
    assert all(item.dtype == np.float32 for item in (axial, sagittal, coronal))


def test_lazy_reorientation_matches_nibabel_for_all_48_orientations():
    import itertools

    shape = (5, 6, 7)
    data = np.arange(np.prod(shape), dtype=np.int16).reshape(shape)
    for permutation in itertools.permutations(range(3)):
        for flips in itertools.product([1, -1], repeat=3):
            orientation = np.array([
                [permutation[axis], flips[axis]] for axis in range(3)
            ])
            affine = np.eye(4)
            for axis in range(3):
                affine[:, axis] = 0
                affine[int(orientation[axis, 0]), axis] = (
                    orientation[axis, 1] * (axis + 1)
                )
            dataobj = SliceOnlyData(data)
            image = nib.Nifti1Image(data, affine)
            image.header.set_zooms((1.1, 1.2, 1.3))
            image._dataobj = dataobj
            reference_image = nib.Nifti1Image(data, affine, image.header)
            reference = nib.as_closest_canonical(reference_image)

            canonical, axial, sagittal, coronal = preview._canonical_triplane_slices(image)
            volume = np.asanyarray(reference.dataobj)
            mid_x, mid_y, mid_z = (size // 2 for size in reference.shape[:3])
            message = f'orientation={orientation.tolist()}'

            assert len(dataobj.keys) == 3, message
            np.testing.assert_allclose(canonical.affine, reference.affine, err_msg=message)
            assert canonical.shape == reference.shape, message
            assert canonical.header.get_zooms() == reference.header.get_zooms(), message
            np.testing.assert_array_equal(
                axial, np.transpose(volume[:, :, mid_z], (1, 0))[::-1, ::-1], err_msg=message)
            np.testing.assert_array_equal(
                sagittal, np.transpose(volume[mid_x, :, :], (1, 0))[::-1, ::-1], err_msg=message)
            np.testing.assert_array_equal(
                coronal, np.transpose(volume[:, mid_y, :], (1, 0))[::-1, ::-1], err_msg=message)


def test_triplane_preview_still_writes_png(tmp_path):
    path = tmp_path / 'synthetic.nii.gz'
    data = np.arange(5 * 6 * 7, dtype=np.int16).reshape(5, 6, 7)
    nib.save(nib.Nifti1Image(data, np.eye(4)), path)
    sample = SimpleNamespace(
        WindowWidth=400, WindowCenter=40, RescaleSlope=1, RescaleIntercept=0,
        PhotometricInterpretation='MONOCHROME2', SliceThickness=2.5,
        ImageOrientationPatient=[1, 0, 0, 0, 1, 0],
    )

    output = preview._generate_3d_triplane_preview(
        str(path), str(tmp_path), 'synthetic', sample, 'CT', lambda value: value)

    assert output and (tmp_path / 'synthetic_preview.png').is_file()


@pytest.fixture
def reset_memory_gate(monkeypatch):
    monkeypatch.setattr(memory, '_heavy_slots', threading.BoundedSemaphore(2))
    monkeypatch.setattr(memory, '_heavy_active', 0)
    monkeypatch.setattr(memory, '_last_pressure_log', 0.0)
    monkeypatch.setattr(memory, '_PRESSURE_RETRY_SEC', 0.01)


def test_memory_gate_releases_slot_after_exception(reset_memory_gate, monkeypatch):
    monkeypatch.setattr(memory, 'get_available_memory_gb', lambda: 100.0)
    with pytest.raises(RuntimeError):
        with memory.heavy_processing_slot():
            assert memory._heavy_active == 1
            raise RuntimeError('synthetic')
    assert memory._heavy_active == 0
    with memory.heavy_processing_slot():
        assert memory._heavy_active == 1


def test_memory_gate_enforces_configured_worker_limit(reset_memory_gate, monkeypatch):
    monkeypatch.setattr(memory, 'get_available_memory_gb', lambda: 100.0)
    entered = threading.Event()

    def third_worker():
        with memory.heavy_processing_slot():
            entered.set()

    with memory.heavy_processing_slot():
        with memory.heavy_processing_slot():
            worker = threading.Thread(target=third_worker)
            worker.start()
            time.sleep(0.05)
            assert memory._heavy_active == 2
            assert not entered.is_set()
        worker.join(2)

    assert entered.is_set()
    assert memory._heavy_active == 0


def test_low_memory_allows_one_worker_but_defers_the_second(reset_memory_gate, monkeypatch):
    monkeypatch.setattr(memory, 'get_available_memory_gb', lambda: 0.25)
    entered = threading.Event()
    finished = threading.Event()

    def second_worker():
        with memory.heavy_processing_slot():
            entered.set()
        finished.set()

    with memory.heavy_processing_slot():
        worker = threading.Thread(target=second_worker)
        worker.start()
        time.sleep(0.05)
        assert not entered.is_set()
    worker.join(2)

    assert entered.is_set() and finished.is_set()
    assert memory._heavy_active == 0
