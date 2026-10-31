import importlib.util,tarfile
from pathlib import Path
out=Path('/home/wmx/research_export_stage_20261030/vendor.tar')
with tarfile.open(out,'w') as tar:
 for name in ['numpy','nibabel','PIL','openpyxl','et_xmlfile','packaging','typing_extensions']:
  spec=importlib.util.find_spec(name)
  if spec is None:continue
  path=Path(spec.origin);path=path.parent if spec.submodule_search_locations is not None else path
  tar.add(path,arcname=path.name,filter=lambda x:None if '__pycache__' in x.name else x)
  for sibling in path.parent.glob(('pillow' if name=='PIL' else path.name)+'.libs'):
   tar.add(sibling,arcname=sibling.name)
print('SELECTED_RUNTIME_DEPENDENCIES_STAGED')
