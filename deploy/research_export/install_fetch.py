#!/usr/bin/python3
import grp,json,os,pwd,shutil,subprocess
from pathlib import Path
if os.geteuid()!=0 or subprocess.check_output(['findmnt','-n','-o','UUID','/srv/research']).decode().strip()!='524c4596-9d32-4f77-90c2-97274cff3c8d':raise SystemExit('Wrong host or research disk')
src=Path('/home/wmx/research_export_stage_20261030');runtime=Path('/opt/research-fetch');etc=Path('/etc/research-fetch');state=Path('/var/lib/research-fetch');cache=Path('/srv/research/research_exports')
try:user=pwd.getpwnam('research-fetch')
except KeyError:
 subprocess.run(['useradd','--system','--user-group','--no-create-home','--shell','/usr/sbin/nologin','research-fetch'],check=True);user=pwd.getpwnam('research-fetch')
try:group=grp.getgrnam('research-export-readers')
except KeyError:
 subprocess.run(['groupadd','--system','research-export-readers'],check=True);group=grp.getgrnam('research-export-readers')
subprocess.run(['usermod','-aG',group.gr_name,'research-dev'],check=True)
for path,mode,uid,gid in [(runtime,0o755,0,0),(etc,0o750,0,user.pw_gid),(state,0o700,user.pw_uid,user.pw_gid),(cache,0o2750,user.pw_uid,group.gr_gid)]:
 path.mkdir(parents=True,exist_ok=True);os.chown(path,uid,gid);os.chmod(path,mode)
subprocess.run(['setfacl','-m','u:research-dev:rx,d:u:research-dev:rx',str(cache)],check=True)
shutil.copytree(src/'src/research_export',runtime/'research_export',dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__'))
for path in (runtime/'research_export').glob('*.py'):os.chown(path,0,0);os.chmod(path,0o644)
for name in ('launch_fetch.py','client.py'):shutil.copyfile(src/'deploy/research_export'/name,runtime/name);os.chown(runtime/name,0,0);os.chmod(runtime/name,0o644)
for name in ('ca.crt','client217.crt','client217.key'):
 shutil.copyfile(src/name,etc/name);os.chown(etc/name,0,user.pw_gid);os.chmod(etc/name,0o640)
grant=json.loads((src/'commissioning_grant.json').read_text())
c={'server':'https://192.0.0.193:8443','ca':str(etc/'ca.crt'),'certificate':str(etc/'client217.crt'),'key':str(etc/'client217.key'),'grants':{grant['grant_id']:grant['export_id']},'clients':['research-dev'],'socket':'/run/research-fetch/request.sock','socket_group':'research-export-readers','downloads':str(cache),'state':str(state)}
(etc/'config.json').write_text(json.dumps(c)+'\n');os.chown(etc/'config.json',0,user.pw_gid);os.chmod(etc/'config.json',0o640)
for name in ('research-fetch.service','research-fetch-egress.service'):shutil.copyfile(src/'deploy/research_export'/name,Path('/etc/systemd/system')/name)
for source,dest in [('egress.py','/usr/local/libexec/research-fetch-egress'),('research-fetch','/usr/local/bin/research-fetch')]:
 shutil.copyfile(src/'deploy/research_export'/source,dest);os.chown(dest,0,0);os.chmod(dest,0o755)
subprocess.run(['ufw','allow','out','to','192.0.0.193','port','8443','proto','tcp','comment','RESEARCH_EXPORT_193_TLS'],check=True)
subprocess.run(['systemctl','daemon-reload'],check=True);subprocess.run(['systemctl','enable','--now','research-fetch-egress.service','research-fetch.service'],check=True)
(src/'client217.key').unlink();print('FETCH_SERVICE_INSTALLED')
