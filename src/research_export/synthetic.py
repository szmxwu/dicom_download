"""Generate a clearly synthetic commissioning fixture, never a clinical export."""
import argparse,json
from pathlib import Path
import numpy as np
import nibabel as nib
from PIL import Image,PngImagePlugin
from openpyxl import Workbook
from .publish import digest

def generate(path):
 p=Path(path);p.mkdir(mode=0o700,parents=True,exist_ok=True)
 a=np.arange(120,dtype=np.int16).reshape(4,5,6);im=nib.Nifti1Image(a,np.diag([2.,3.,4.,1.]));im.header['descrip']=b'SYNTHETIC_MARKER_NOT_A_PATIENT';im.header.extensions.append(nib.nifti1.Nifti1Extension(6,b'SYNTHETIC_MARKER_NOT_A_PATIENT'));nib.save(im,p/'synthetic_volume.nii.gz');np.savez_compressed(p/'synthetic_volume.npz',data=a.astype(np.float32))
 info=PngImagePlugin.PngInfo();info.add_text('PatientID','SYNTHETIC_MARKER_NOT_A_PATIENT');Image.fromarray(np.arange(256,dtype=np.uint8).reshape(16,16)).save(p/'synthetic_preview.png',pnginfo=info)
 b=Workbook();b.active.append(['PatientID','AccessionNumber','StudyDate','Rows','Modality','SeriesNumber']);b.active.append(['SYNTHETIC_MARKER_NOT_A_PATIENT','SYNTHETIC_CASE','20000101',4,'CT',1]);s=b.create_sheet('Series_Summary');s.append(['PatientID','AccessionNumber','Rows']);s.append(['SYNTHETIC_MARKER_NOT_A_PATIENT','SYNTHETIC_CASE',4]);b.save(p/'synthetic_metadata.xlsx')
 r={'policy_version':'research-v1','reviewer':'synthetic-commissioning-only','files':[{'path':x.name,'case_ref':'SYNTHETIC_CASE','series_number':1,'sha256':digest(x),'pixel_review':{'status':'approved','evidence':'deterministic synthetic numeric arrays; no clinical pixels'},'quality_review':{'status':'approved','evidence':'deterministic varying arrays and known affine'}} for x in sorted(p.iterdir()) if x.suffix in ('.gz','.npz','.png','.xlsx')]};(p/'review.json').write_text(json.dumps(r,indent=2)+'\n')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('directory');generate(p.parse_args().directory)
