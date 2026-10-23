"""CT success must describe the entire source set, including Python fallback."""
from pathlib import Path
from types import SimpleNamespace
import nibabel as nib
import numpy as np
import pydicom
import pytest
from pydicom.dataset import Dataset,FileDataset,FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian,CTImageStorage,generate_uid
from src.core import convert


@pytest.fixture
def ct(tmp_path):
    files=[];study=generate_uid();series=generate_uid();frame=generate_uid();rng=np.random.default_rng(71)
    for k in range(5):
        meta=FileMetaDataset();meta.TransferSyntaxUID=ExplicitVRLittleEndian;meta.MediaStorageSOPClassUID=CTImageStorage;meta.MediaStorageSOPInstanceUID=generate_uid()
        d=FileDataset(None,{},file_meta=meta,preamble=b'\0'*128)
        d.SOPClassUID=CTImageStorage;d.SOPInstanceUID=meta.MediaStorageSOPInstanceUID
        d.StudyInstanceUID=study;d.SeriesInstanceUID=series;d.FrameOfReferenceUID=frame
        d.Modality='CT';d.Rows=16;d.Columns=24;d.SamplesPerPixel=1;d.PhotometricInterpretation='MONOCHROME2'
        d.BitsAllocated=16;d.BitsStored=16;d.HighBit=15;d.PixelRepresentation=0
        d.ImageOrientationPatient=[1,0,0,0,1,0];d.ImagePositionPatient=[0,0,-k*.45];d.PixelSpacing=[.8,1.2];d.SliceThickness=.9
        d.PixelData=rng.integers(0,3000,(16,24),dtype=np.uint16).tobytes()
        path=tmp_path/f'{k}.dcm';d.save_as(path);files.append(str(path))
    client=SimpleNamespace(_sanitize_folder_name=lambda x:x,_ensure_metadata_cache=lambda *a:None,_record_orientation_check=lambda *a:None)
    return client,files,pydicom.dcmread(files[0])


@pytest.mark.parametrize('failure',['header','pixels','mixed_shape','identity'])
@pytest.mark.parametrize('save_to_disk',[False,True])
def test_python_ct_rejects_source_subset_or_missing_identity(ct,tmp_path,monkeypatch,failure,save_to_disk):
    client,files,sample=ct;read=pydicom.dcmread
    if failure=='header':
        def broken(path,*a,**kw):
            if str(path)==files[-1]:raise OSError('unreadable source')
            return read(path,*a,**kw)
        monkeypatch.setattr(convert.pydicom,'dcmread',broken)
    else:
        for f in (files if failure=='identity' else files[-1:]):
            d=read(f)
            if failure=='pixels':del d.PixelData
            if failure=='mixed_shape':d.Columns=18;d.PixelData=np.ones((16,18),np.uint16).tobytes()
            if failure=='identity':del d.StudyInstanceUID
            d.save_as(f)
    result=convert.convert_with_python_libs(client,str(tmp_path),'CT',files,sample,'CT',save_to_disk=save_to_disk)
    assert not result['success']
    assert all(Path(f).exists() for f in files)
    assert not (tmp_path/'CT.nii.gz').exists()


def test_python_ct_complete_source_still_passes(ct,tmp_path):
    client,files,sample=ct
    result=convert.convert_with_python_libs(client,str(tmp_path),'CT',files,sample,'CT')
    assert result['success'] and result['slice_count']==len(files)


@pytest.mark.parametrize('engine',['python','dcm2niix'])
def test_single_file_multiframe_ct_is_not_blindly_certified(ct,tmp_path,monkeypatch,engine):
    client,files,sample=ct;f=files[0];d=pydicom.dcmread(f);d.NumberOfFrames=2;d.PixelData=np.arange(16*24*2,dtype=np.uint16).tobytes();d.save_as(f)
    if engine=='dcm2niix':
        def run(cmd,**kw):
            if '-h' not in cmd:nib.save(nib.Nifti1Image(np.ones((16,24,2)),np.eye(4)),tmp_path/'CT.nii.gz')
            return SimpleNamespace(returncode=0,stdout='',stderr='')
        monkeypatch.setattr(convert,'_run_subprocess_with_timeout',run)
        result=convert.convert_with_dcm2niix(client,str(tmp_path),'CT',[f],d,'CT')
    else:result=convert.convert_with_python_libs(client,str(tmp_path),'CT',[f],d,'CT')
    assert not result['success']
    assert Path(f).exists()


@pytest.mark.parametrize('engine',['python','dcm2niix'])
def test_single_frame_ct_still_passes_source_pixel_verification(ct,tmp_path,monkeypatch,engine):
    client,files,sample=ct;f=files[0]
    if engine=='dcm2niix':
        def run(cmd,**kw):
            if '-h' not in cmd:
                affine,_=convert.build_dicom_truth_affine([sample])
                nib.save(nib.Nifti1Image(sample.pixel_array[:,:,None],affine),tmp_path/'CT.nii.gz')
            return SimpleNamespace(returncode=0,stdout='',stderr='')
        monkeypatch.setattr(convert,'_run_subprocess_with_timeout',run)
        result=convert.convert_with_dcm2niix(client,str(tmp_path),'CT',[f],sample,'CT')
    else:result=convert.convert_with_python_libs(client,str(tmp_path),'CT',[f],sample,'CT')
    assert result['success']
    assert result['orientation_check']['action'] in ('ok','orientation_mirror_corrected')
