#!/usr/bin/python3
"""Root upgrade of the independent export services; never touches port 5005."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tarfile
import time

p = argparse.ArgumentParser()
p.add_argument('--host', choices=('193', '217'), required=True)
p.add_argument('--archive', required=True)
p.add_argument('--sha256', required=True)
a = p.parse_args()
if os.geteuid() != 0:
    raise SystemExit('ROOT_REQUIRED')
expected = 'P3650' if a.host == '193' else 'wmx-Z11PA-D8'
if socket.gethostname() != expected:
    raise SystemExit('WRONG_HOST')
if a.host == '217' and subprocess.check_output(
        ['findmnt', '-n', '-o', 'UUID', '/srv/research']).decode().strip() != '524c4596-9d32-4f77-90c2-97274cff3c8d':
    raise SystemExit('WRONG_RESEARCH_DISK')
raw = Path(a.archive).read_bytes()
if hashlib.sha256(raw).hexdigest() != a.sha256:
    raise SystemExit('ARCHIVE_DIGEST_MISMATCH')
files = {}
with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
    for item in archive:
        name = Path(item.name)
        allowed = (str(name).startswith('src/research_export/') or
                   str(name).startswith('deploy/research_export/') or
                   str(name) == 'test/test_research_export.py')
        if not item.isfile() or name.is_absolute() or '..' in name.parts or not allowed:
            raise SystemExit('UNEXPECTED_ARCHIVE_MEMBER')
        files[str(name)] = archive.extractfile(item).read()
service = 'research-export' if a.host == '193' else 'research-fetch'
runtime = Path('/opt')/service
stamp = time.strftime('%Y%m%d_%H%M%S', time.gmtime())
backup = Path('/root')/(service+'_pre_multiday_'+stamp)
backup.mkdir(mode=0o700)
shutil.copytree(runtime/'research_export', backup/'research_export')
config = Path('/etc/research-fetch/config.json') if a.host == '217' else None
if config:
    config_stat=config.stat()
    shutil.copy2(config, backup/'config.json')
subprocess.run(['systemctl', 'stop', service+'.service'], check=True)
try:
    for name, data in files.items():
        if name.startswith('src/research_export/'):
            dest = runtime/'research_export'/Path(name).name
            dest.write_bytes(data)
            os.chown(dest, 0, 0)
            os.chmod(dest, 0o644)
    if config:
        settings = json.loads(config.read_text())
        settings['min_free_bytes'] = 20*1024**3
        st = config.stat()
        temp = config.with_suffix('.new')
        temp.write_text(json.dumps(settings)+'\n')
        os.chown(temp, st.st_uid, st.st_gid)
        os.chmod(temp, 0o640)
        temp.replace(config)
    subprocess.run(['systemctl', 'start', service+'.service'], check=True)
    for attempt in range(10):
        time.sleep(1)
        if subprocess.run(['systemctl', 'is-active', '--quiet', service+'.service']).returncode == 0:
            if a.host == '193':
                try:
                    with socket.create_connection(('192.0.0.193', 8443), timeout=1):
                        break
                except OSError:
                    continue
            elif Path('/run/research-fetch/request.sock').is_socket():
                break
    else:
        raise RuntimeError('SERVICE_START_FAILED')
except Exception:
    subprocess.run(['systemctl', 'stop', service+'.service'], check=False)
    shutil.rmtree(runtime/'research_export')
    shutil.copytree(backup/'research_export', runtime/'research_export')
    if config:
        shutil.copy2(backup/'config.json', config)
        os.chown(config, config_stat.st_uid, config_stat.st_gid)
    subprocess.run(['systemctl', 'start', service+'.service'], check=True)
    raise SystemExit('UPGRADE_FAILED_RUNTIME_ROLLED_BACK')
if a.host == '193':
    project = Path('/home/wmx/work/dicom_download_prod')
    for name, data in files.items():
        dest = project/name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        shutil.chown(dest, user='wmx', group='wmx')
        os.chmod(dest, 0o644)
print(json.dumps({'result': 'UPGRADED', 'host': a.host, 'backup': str(backup),
                  'archive_sha256': a.sha256, 'legacy_5005_restarted': False}))
