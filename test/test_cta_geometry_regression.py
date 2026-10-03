"""CTA long-volume regressions; no patient information or PACS connection."""
import json
import tracemalloc
from pathlib import Path
import numpy as np
import nibabel as nib
import pydicom
import pytest
from pydicom.dataset import Dataset
from src.core import convert
from src.utils import cache


def datasets(count=7):
    records=[]
    rng=np.random.default_rng(17)
    for i in range(count):
        d=Dataset();d.ImageOrientationPatient=[1,0,0,0,1,0]
        d.ImagePositionPatient=[-249.522,-39.3,1063.53-i*.45]
        d.PixelSpacing=[.8,1.2];d.Rows=16;d.Columns=24;d.SliceThickness=.9
        d.PatientPosition='FFS';d.Modality='CT';d.SamplesPerPixel=1
        d.PhotometricInterpretation='MONOCHROME2';d.BitsAllocated=16;d.BitsStored=16;d.HighBit=15;d.PixelRepresentation=0
        d.file_meta=Dataset();d.file_meta.TransferSyntaxUID=pydicom.uid.ExplicitVRLittleEndian
        d.PixelData=rng.integers(0,4096,(16,24),dtype=np.uint16).tobytes()
        records.append(d)
    return records


def test_long_slice_order_has_linear_memory_and_is_independent_of_input_order():
    positions=np.c_[np.zeros(6000),np.zeros(6000),np.arange(6000)[::-1]*.45]
    tracemalloc.start();order,step=convert._canonical_slice_order(positions);_,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
    assert peak<10*1024*1024
    assert np.all(np.diff(positions[order,2])>0) and step[2]>0
    reversed_order,reversed_step=convert._canonical_slice_order(positions[::-1])
    np.testing.assert_allclose(positions[order],positions[::-1][reversed_order])
    np.testing.assert_allclose(step,reversed_step)


def test_source_pixels_recover_wrong_si_sign_and_fractional_origin():
    records=datasets();truth,ordered=convert.build_dicom_truth_affine(records)
    volume=np.stack([d.pixel_array for d in records],axis=2).astype(np.float32)
    wrong=convert.build_affine_from_dicom(records[0],slice_spacing=.45)
    wrong[2,3]+=.137  # old permutation precheck rejected this before probing pixels
    image=nib.Nifti1Image(volume,wrong)
    fixed,result=convert.verify_and_fix_orientation(image,records)
    assert 'affine_mismatch' in result['fixes'] and result['probe_corr']>.999
    assert result['action']==convert.ORIENTATION_CHECK_MIRROR_CORRECTED
    first_world=fixed.affine@np.array([0,0,0,1.])
    assert first_world[2]==pytest.approx(1063.53)
    last_world=fixed.affine@np.array([0,0,len(records)-1,1.])
    assert last_world[2]==pytest.approx(float(records[-1].ImagePositionPatient[2]))


def test_unmatched_source_pixels_are_not_silently_passed():
    records=datasets();truth,_=convert.build_dicom_truth_affine(records)
    image=nib.Nifti1Image(np.zeros((16,24,7),np.float32),truth)
    _,result=convert.verify_and_fix_orientation(image,records)
    assert result['action']==convert.ORIENTATION_CHECK_NOT_VERIFIABLE
    assert not result['fixes']


def test_duplicate_or_nonuniform_positions_are_unverified():
    records=datasets();records[-1].ImagePositionPatient=records[-2].ImagePositionPatient
    assert convert.build_dicom_truth_affine(records) is None


def test_conflicting_qform_is_repaired_even_when_preferred_sform_matches_source():
    records=datasets();truth,ordered=convert.build_dicom_truth_affine(records)
    volume=np.stack([d.pixel_array for d in ordered],axis=2)
    image=nib.Nifti1Image(volume,truth)
    wrong=truth.copy();wrong[:3,2]*=2
    image.set_qform(wrong,code=1);image.set_sform(truth,code=1)
    fixed,result=convert.verify_and_fix_orientation(image,records)
    assert 'geometry_forms_mismatch' in result['fixes']
    np.testing.assert_allclose(fixed.get_qform(),truth,atol=1e-3)
    np.testing.assert_allclose(fixed.get_sform(),truth,atol=1e-3)
    np.testing.assert_array_equal(np.asanyarray(fixed.dataobj),volume)


def test_small_spacing_error_accumulated_over_long_ct_is_source_corrected():
    records=datasets(1025);truth,ordered=convert.build_dicom_truth_affine(records)
    volume=np.stack([d.pixel_array for d in ordered],axis=2)
    wrong=truth.copy();wrong[2,2]=.4500732421875
    image=nib.Nifti1Image(volume,wrong)
    fixed,result=convert.verify_and_fix_orientation(image,records)
    assert result['max_source_geometry_error_mm']>.05
    assert 'affine_mismatch' in result['fixes']
    np.testing.assert_allclose(fixed.affine,truth,atol=1e-6)
    np.testing.assert_array_equal(np.asanyarray(fixed.dataobj),volume)


def test_sheared_source_affine_does_not_publish_approximated_qform():
    image=nib.Nifti1Image(np.zeros((3,4,5),np.int16),np.eye(4))
    affine=np.eye(4);affine[0,2]=.2
    fixed=convert._rebuild_nifti(image,np.asanyarray(image.dataobj),affine)
    assert int(fixed.header['qform_code'])==0
    np.testing.assert_allclose(fixed.get_sform(),affine,atol=1e-6)


@pytest.mark.parametrize('tag',['StudyInstanceUID','SeriesInstanceUID','FrameOfReferenceUID'])
def test_identical_geometry_from_different_source_identities_is_not_merged(tag):
    records=datasets()
    for d in records:setattr(d,tag,'1.2.3')
    setattr(records[-1],tag,'1.2.4')
    assert convert.build_dicom_truth_affine(records) is None


def test_missing_header_rejects_complete_output_instead_of_checking_a_subset(tmp_path,monkeypatch):
    monkeypatch.setattr(convert.pydicom,'dcmread',lambda *a,**k:(_ for _ in ()).throw(OSError('unreadable')))
    result=convert.verify_and_fix_orientation_file(str(tmp_path/'unused.nii.gz'),['one.dcm'])
    assert result['action']==convert.ORIENTATION_CHECK_SKIPPED
    assert 'incomplete' in result['message']


def test_raw_row_column_mapping_uses_correct_spacing_and_origin():
    d=datasets()[0];a=convert.build_affine_from_dicom(d,slice_spacing=.45)
    np.testing.assert_allclose(a[:3,0],[0,-.8,0])
    np.testing.assert_allclose(a[:3,1],[-1.2,0,0])
    np.testing.assert_allclose(a[:3,3],[249.522,39.3,1063.53])


def test_conversion_geometry_version_invalidates_cached_nifti(monkeypatch):
    options={'output_format':'nifti'};before=cache._compute_param_hash('SYNTHETIC',options)
    raw_before=cache._compute_param_hash('SYNTHETIC',{'output_format':'dicom'})
    monkeypatch.setattr(cache,'CONVERSION_GEOMETRY_VERSION','different-version')
    assert cache._compute_param_hash('SYNTHETIC',options)!=before
    assert cache._compute_param_hash('SYNTHETIC',{'output_format':'dicom'})==raw_before


@pytest.mark.parametrize('action',[convert.ORIENTATION_CHECK_SKIPPED,convert.ORIENTATION_CHECK_NOT_VERIFIABLE])
def test_failed_ct_verification_keeps_raw_dicom_and_returns_failure(tmp_path,monkeypatch,action):
    from types import SimpleNamespace
    class Client:
        def _sanitize_folder_name(self,name):return name
        def _ensure_metadata_cache(self,*args):pass
        def _record_orientation_check(self,*args):pass
    paths=[]
    for i in range(2):
        file=tmp_path/f'{i}.dcm';file.write_bytes(b'synthetic');paths.append(str(file))
    def subprocess_result(cmd,**kwargs):
        if '-o' in cmd:
            nib.save(nib.Nifti1Image(np.zeros((3,4,2),np.int16),np.eye(4)),tmp_path/'fixture.nii.gz')
        return SimpleNamespace(returncode=0,stdout='',stderr='')
    monkeypatch.setattr(convert,'_run_subprocess_with_timeout',subprocess_result)
    monkeypatch.setattr(convert,'verify_and_fix_orientation_file',lambda *args:{'action':action,'fixes':[]})
    result=convert.convert_with_dcm2niix(Client(),str(tmp_path),'fixture',dicom_files=paths,modality='CT')
    assert result['success'] is False
    assert all(Path(file).exists() for file in paths)


def test_series_wrapper_forwards_output_format_by_name(monkeypatch):
    from src.client import unified
    calls=[]
    monkeypatch.setattr(unified,'process_single_series_impl',lambda *args,**kwargs:calls.append((args,kwargs)))
    unified.DICOMDownloadClient._process_single_series(object(),'SYNTHETIC_PATH','SYNTHETIC_SERIES',output_format='dicom')
    assert calls[0][0][-1]=='SYNTHETIC_PATH'
    assert calls[0][1]['output_format']=='dicom'


def test_unverified_receipt_is_recorded_and_exported_not_hidden_as_normal(tmp_path):
    from src.client.unified import DICOMDownloadClient
    from src.core.metadata import _merge_orientation_fixes
    DICOMDownloadClient._record_orientation_check(object(),str(tmp_path),{
        'action':'orientation_not_verifiable','fixes':[],'probe_corr':None,
        'message':'source pixels unavailable'})
    receipt=json.loads((tmp_path/'dicom_metadata_cache.json').read_text())
    assert receipt['orientation_check']['action']=='orientation_not_verifiable'
    row={};_merge_orientation_fixes(row,receipt)
    assert row['OrientationStatus']=='orientation_not_verifiable'
    assert row['OrientationPixelGeometryVerified'] is False
    assert 'policy_version' in receipt['orientation_check']


def test_affine_only_rebuild_preserves_scaled_integer_hu_exactly(tmp_path):
    raw=np.arange(16*24*7,dtype=np.int16).reshape(16,24,7)
    image=nib.Nifti1Image(raw,np.eye(4));image.header.set_slope_inter(1,-1024)
    source=tmp_path/'source.nii.gz';nib.save(image,source);loaded=nib.load(source)
    hu=np.asanyarray(loaded.dataobj);affine=loaded.affine.copy();affine[2,2]=-1
    rebuilt=convert._rebuild_nifti(loaded,hu,affine)
    dest=tmp_path/'corrected.nii.gz';nib.save(rebuilt,dest)
    np.testing.assert_array_equal(np.asanyarray(nib.load(dest).dataobj),hu)


def test_affine_only_rebuild_does_not_quantize_fractional_hu(tmp_path):
    values=np.linspace(-1023.713,3091.287,16*24*7).reshape(16,24,7)
    image=nib.Nifti1Image(values,np.eye(4));image.set_data_dtype(np.int16)
    rebuilt=convert._rebuild_nifti(image,values,np.eye(4))
    dest=tmp_path/'float_corrected.nii.gz';nib.save(rebuilt,dest)
    np.testing.assert_array_equal(np.asanyarray(nib.load(dest).dataobj),values)


@pytest.mark.parametrize('organize',[False,True])
def test_success_does_not_require_optional_metadata(tmp_path,organize):
    from types import SimpleNamespace
    from src.client.unified import DICOMDownloadClient
    client=SimpleNamespace(download_study=lambda *a,**k:str(tmp_path),
        _wait_for_files_stable=lambda *a:None,
        organize_dicom_files=lambda *a,**k:(str(tmp_path),{'fixture':{'conversion_error':None}}))
    result=DICOMDownloadClient.process_complete_workflow(client,'SYNTHETIC',str(tmp_path),
        parallel_pipeline=False,auto_organize=organize,auto_metadata=False,output_format='dicom')
    assert result['success'] is True
