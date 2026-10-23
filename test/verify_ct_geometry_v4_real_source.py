"""Read-only small real-DICOM fallback replay; no live PACS/client startup."""
import hashlib,json,sys,tempfile
from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pydicom
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from src.core import convert

root=Path('/home/ubuntu/work/python/CT_YOLO');manifest=root/'derived/anatomy_sensor_v3/20261001_phase0/direction_incident_v2/private_geometry_manifest.json'
source=Path('/mnt/h/dicom/cta_low_limb/0YLINXBD/YIBU2DZ1')
rows=sorted(json.loads(manifest.read_text())['records'],key=lambda r:r['instance'])[:24]
files=[source/r['file'] for r in rows];hashes=[hashlib.sha256(p.read_bytes()).hexdigest() for p in files]
client=SimpleNamespace(_sanitize_folder_name=lambda x:x)
with tempfile.TemporaryDirectory(prefix='ct_geometry_v4_readonly_') as tmp:
    result=convert.convert_with_python_libs(client,tmp,'real_CT_readonly',[str(p) for p in files],pydicom.dcmread(files[0]),'CT',save_to_disk=False)
    assert result['success'] and result['slice_count']==len(files)
    img=result['nifti_img'];truth,ordered=convert.build_dicom_truth_affine([pydicom.dcmread(p) for p in files])
    volume=np.stack([convert.apply_photometric(convert.apply_rescale(d.pixel_array,d),d) for d in ordered],axis=2)
    layout,corr=convert._probe_content_layout(np.asanyarray(img.dataobj),ordered)
    # Verify every selected HU, independently mapping the discrete layout.
    inverse=np.linalg.inv(layout);errors=[]
    for k in range(volume.shape[2]):
        ii,jj=np.meshgrid(np.arange(volume.shape[0]),np.arange(volume.shape[1]),indexing='ij')
        points=np.stack([ii.ravel(),jj.ravel(),np.full(ii.size,k),np.ones(ii.size)])
        q=np.rint((layout@points)[:3]).astype(int)
        errors.append(float(np.max(np.abs(np.asanyarray(img.dataobj)[tuple(q)]-volume[:,:,k].ravel()))))
    assert max(errors)==0
    assert hashes==[hashlib.sha256(p.read_bytes()).hexdigest() for p in files]
    out=Path('test/output/ct_geometry_v4_20261023');out.mkdir(parents=True,exist_ok=True)
    receipt=dict(real_DICOM_selected_contiguous_planes=len(files),HU_voxels_verified=int(volume.size),max_HU_error=max(errors),source_SHA256_before_after_equal=True,
        source_manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),source_file_SHA256=hashes,
        orientation_check=result['orientation_check'],source_directory_written=False,live_PACS_used=False,
        scope='24-plane real source fallback regression; not a new full 2432-plane or production acceptance')
    (out/'real_source_replay.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n')
    print('REAL_SOURCE_READ_ONLY_REPLAY_PASS',len(files),volume.size,max(errors))
