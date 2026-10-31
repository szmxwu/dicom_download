#!/usr/bin/python3
"""H193 root deployment, separate from the existing 5005 process."""
import tarfile
import grp,json,os,pwd,secrets,shutil,socket,subprocess
from pathlib import Path
if os.geteuid()!=0 or socket.gethostname()!='P3650':raise SystemExit('Wrong host or privilege')
project=Path('/home/wmx/work/dicom_download_prod');stage=Path('/home/wmx/research_export_stage_20261030');runtime=Path('/opt/research-export');etc=Path('/etc/research-export');data=Path('/srv/research-export');state=Path('/var/lib/research-export')
try:user=pwd.getpwnam('research-export')
except KeyError:
 subprocess.run(['useradd','--system','--user-group','--no-create-home','--shell','/usr/sbin/nologin','research-export'],check=True);user=pwd.getpwnam('research-export')
if user.pw_shell!='/usr/sbin/nologin':raise SystemExit('Existing account mismatch')
def folder(path,mode=0o750,uid=0,gid=None):
 path.mkdir(parents=True,exist_ok=True);os.chown(path,uid,user.pw_gid if gid is None else gid);os.chmod(path,mode)
folder(runtime,0o755,gid=0);folder(etc);folder(data);folder(data/'published');folder(data/'staging',0o700,gid=0);folder(data/'quarantine',0o700,gid=0);folder(state,0o700,user.pw_uid)
shutil.copytree(project/'src/research_export',runtime/'research_export',dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__'))
for f in (runtime/'research_export').glob('*.py'):os.chown(f,0,0);os.chmod(f,0o644)
if (stage/'vendor.tar').exists():
 vendor=runtime/'vendor';vendor.mkdir(exist_ok=True,mode=0o755)
 with tarfile.open(stage/'vendor.tar') as archive:
  for item in archive:
   if item.name.startswith('/') or '..' in Path(item.name).parts or not (item.isfile() or item.isdir()):raise SystemExit('Unsafe vendor archive')
 subprocess.run(['tar','-xf',str(stage/'vendor.tar'),'-C',str(vendor)],check=True)
 subprocess.run(['chown','-R','root:root',str(vendor)],check=True);subprocess.run(['chmod','-R','go-w',str(vendor)],check=True)
for name in ('launch_gateway.py','launch_admin.py'):shutil.copy2(project/'deploy/research_export'/name,runtime/name);os.chown(runtime/name,0,0)
for name in ('ca.crt','server.crt','server.key'):
 shutil.copyfile(stage/name,etc/name);os.chown(etc/name,0,user.pw_gid);os.chmod(etc/name,0o640)
if not (etc/'policy.json').exists():(etc/'policy.json').write_text('{"grants":{}}\n')
os.chown(etc/'policy.json',0,user.pw_gid);os.chmod(etc/'policy.json',0o640)
if not (etc/'mapping.key').exists():
 fd=os.open(etc/'mapping.key',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.write(fd,secrets.token_bytes(32));os.close(fd)
config={'host':'192.0.0.193','port':8443,'certificate':str(etc/'server.crt'),'key':str(etc/'server.key'),'ca':str(etc/'ca.crt'),'policy':str(etc/'policy.json'),'published':str(data/'published'),'state':str(state)}
(etc/'gateway.json').write_text(json.dumps(config)+'\n');os.chown(etc/'gateway.json',0,user.pw_gid);os.chmod(etc/'gateway.json',0o640)
for name in ('research-export.service','research-export-egress.service'):shutil.copyfile(project/'deploy/research_export'/name,Path('/etc/systemd/system')/name)
helper=Path('/usr/local/libexec/research-export-egress');shutil.copyfile(project/'deploy/research_export/egress.py',helper);os.chmod(helper,0o755);os.chown(helper,0,0)
admin=Path('/usr/local/sbin/research-export-admin');admin.write_text('#!/bin/sh\nexec /opt/ia-runtime/bin/python3.12 -I /opt/research-export/launch_admin.py --root /srv/research-export --policy /etc/research-export/policy.json --mapping-key /etc/research-export/mapping.key "$@"\n');os.chmod(admin,0o750)
subprocess.run(['systemctl','daemon-reload'],check=True);subprocess.run(['systemctl','enable','--now','research-export-egress.service','research-export.service'],check=True)
(stage/'server.key').unlink()
print('GATEWAY_INSTALLED')
