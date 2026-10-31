"""217 certificate-owning local service. Agent submits IDs, never URLs or keys."""
import errno,grp,subprocess,shutil
import http.client
import urllib.error
import argparse,hashlib,json,os,pwd,re,secrets,socket,ssl,stat,struct,threading,time,urllib.request
from pathlib import Path
ID=r'[0-9a-f]{32}'
def error_code(error):
 if isinstance(error,urllib.error.URLError) and isinstance(error.reason,ssl.SSLCertVerificationError):return 'TLS_CERTIFICATE_ERROR'
 if isinstance(error,http.client.HTTPException):return 'SERVICE_UNAVAILABLE'
 if isinstance(error,ssl.SSLCertVerificationError):return 'TLS_CERTIFICATE_ERROR'
 if isinstance(error,OSError) and error.errno==errno.ENOSPC:return 'DISK_PRESSURE'
 if isinstance(error,urllib.error.HTTPError):
  if error.code in (401,403,404):return 'ACCESS_DENIED_OR_NOT_READY'
  if error.code in (412,416):return 'VERSION_MISMATCH'
  if error.code in (429,503):return 'RETRY_LATER'
  return 'UPSTREAM_UNAVAILABLE'
 allowed={'unknown_grant','invalid_export','manifest_limit','manifest_identity','invalid_artifact','cache_path','cache_link','existing_cache_requires_review','partial_identity','range_identity','size_limit','content_digest','caller_denied','invalid_grant','invalid_job','unknown_job','job_scope','busy','job_limit','operation_denied','request_limit','reply_limit'}
 if isinstance(error,ValueError) and str(error) in allowed:return str(error).upper()
 if isinstance(error,OSError):return 'SERVICE_UNAVAILABLE'
 return 'REQUEST_FAILED'

class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None
class Fetch:
 def __init__(self,config):
  self.config=Path(config);self.lock=threading.Lock();self.busy=False;self.jobs={};self.started=[];self.leases={}
 def settings(self,gid):
  c=json.loads(self.config.read_text())
  if c['server']!='https://192.0.0.193:8443' or gid not in c['grants']:raise ValueError('unknown_grant')
  context=ssl.create_default_context(cafile=c['ca']);context.minimum_version=ssl.TLSVersion.TLSv1_2;context.load_cert_chain(c['certificate'],c['key'])
  opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=context),NoRedirect())
  return c,c['grants'][gid],opener
 def renew_lease(self,gid):
  old=self.leases.get(gid)
  if old and time.monotonic()<old['refresh_at']:return old.get('token')
  c,e,opener=self.settings(gid)
  with opener.open(urllib.request.Request(c['server']+'/v1/exports/'+e+'/lease',headers={'X-Export-Grant':gid}),timeout=30) as r:
   raw=r.read(4097)
  if len(raw)>4096:raise ValueError('manifest_limit')
  lease=json.loads(raw)
  if lease.get('required'):
   if not re.fullmatch('[0-9a-f]{64}',lease.get('token','')) or not 0<lease['valid_for']<=600:raise ValueError('invalid_grant')
   lease['refresh_at']=time.monotonic()+min(300,lease['valid_for']/2)
  else:lease['refresh_at']=time.monotonic()+300
  self.leases[gid]=lease;return lease.get('token')
 def request(self,gid,path,headers=None):
  token=self.renew_lease(gid);c,e,opener=self.settings(gid)
  h={'X-Export-Grant':gid,**(headers or {})}
  if token:h['X-Export-Lease']=token
  try:return opener.open(urllib.request.Request(c['server']+path,headers=h),timeout=30)
  except urllib.error.HTTPError as error:
   if error.code!=404 or not token:raise
   error.close();self.leases.pop(gid,None)
   # A stale lease gets one renewal attempt; revoked/expired approval still fails.
   h['X-Export-Lease']=self.renew_lease(gid)
   return opener.open(urllib.request.Request(c['server']+path,headers=h),timeout=30)
 def check_space(self,root,remaining=0):
  c=json.loads(self.config.read_text());floor=c.get('min_free_bytes',1024**3)
  if shutil.disk_usage(root).free<floor+remaining:raise OSError(errno.ENOSPC,'disk_pressure')
 def manifest(self,gid):
  c,e,_=self.settings(gid)
  if not re.fullmatch(ID,e):raise ValueError('invalid_export')
  with self.request(gid,'/v1/exports/'+e+'/manifest') as r:
   data=r.read(2*1024**2+1)
  if len(data)>2*1024**2:raise ValueError('manifest_limit')
  m=json.loads(data)
  if m['export_id']!=e:raise ValueError('manifest_identity')
  return m
 def download(self,gid,aid,jid):
  job=self.jobs[jid]
  try:
   m=self.manifest(gid);a=next(x for x in m['artifacts'] if x['artifact_id']==aid)
   if not re.fullmatch(r'\.(nii|nii\.gz|npz|png|xlsx)',a['format']) or not re.fullmatch('[0-9a-f]{64}',a['sha256']) or not 0<a['bytes']<=64*1024**3:raise ValueError('invalid_artifact')
   c,_,_=self.settings(gid);root=Path(c['downloads']);directory=root/m['export_id']
   if root.is_symlink() or directory.is_symlink():raise ValueError('cache_path')
   directory.mkdir(exist_ok=True,mode=0o750)
   path=directory/(aid+a['format']);partial=directory/(aid+a['format']+'.partial')
   if path.exists():
    if path.is_symlink() or path.stat().st_nlink!=1:raise ValueError('cache_link')
    h=hashlib.sha256()
    with path.open('rb') as cached:
     for block in iter(lambda:cached.read(1024*1024),b''):h.update(block)
    h=h.hexdigest()
    if h==a['sha256']:job.update(state='SUCCEEDED',path=str(path),sha256=h);return
    raise ValueError('existing_cache_requires_review')
   fd=os.open(partial,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
   with os.fdopen(fd,'r+b') as out:
    os.fchmod(out.fileno(),0o600)
    st=os.fstat(out.fileno())
    if st.st_nlink!=1 or not stat.S_ISREG(st.st_mode) or st.st_size>a['bytes']:raise ValueError('partial_identity')
    start=st.st_size
    if start<a['bytes']:
     self.check_space(root,a['bytes']-start)
     headers={'If-Match':'"'+a['sha256']+'"'}
     if start:headers['Range']='bytes='+str(start)+'-'
     with self.request(gid,'/v1/artifacts/'+aid+'/content',headers) as response:
      if response.status!=(206 if start else 200) or response.headers.get('ETag')!='"'+a['sha256']+'"':raise ValueError('range_identity')
      out.seek(start);total=start;checkpoint=start;checked=time.monotonic()
      job.update(received_bytes=start,total_bytes=a['bytes'])
      while True:
       if self.leases.get(gid,{}).get('required'):self.renew_lease(gid)
       self.check_space(root)
       b=response.read(256*1024)
       if not b:break
       total+=len(b)
       if total>a['bytes']:raise ValueError('size_limit')
       out.write(b)
       if total-checkpoint>=16*1024**2 or time.monotonic()-checked>=10:
        out.flush();os.fsync(out.fileno());job['received_bytes']=total;self.persist(jid)
        checkpoint=total;checked=time.monotonic()
      out.flush();os.fsync(out.fileno())
    if os.fstat(out.fileno()).st_size<a['bytes']:raise ConnectionError('incomplete_transfer')
    out.seek(0);h=hashlib.sha256()
    for b in iter(lambda:out.read(1024*1024),b''):h.update(b)
    if os.fstat(out.fileno()).st_size!=a['bytes'] or h.hexdigest()!=a['sha256']:raise ValueError('content_digest')
   os.chmod(partial,0o640);os.replace(partial,path);job.update(state='SUCCEEDED',path=str(path),sha256=a['sha256'],received_bytes=a['bytes'],total_bytes=a['bytes'])
  except Exception as e:job.update(state='FAILED',error=error_code(e))
  finally:
   with self.lock:self.busy=False
   self.persist(jid)
 def persist(self,jid):
  c=json.loads(self.config.read_text());p=Path(c['state'])/(jid+'.json');tmp=p.with_suffix('.tmp');tmp.write_text(json.dumps(self.jobs[jid])+'\n');os.chmod(tmp,0o640);tmp.replace(p)
 def call(self,request,uid):
  c=json.loads(self.config.read_text());allowed={pwd.getpwnam(name).pw_uid for name in c['clients']}
  if uid not in allowed:raise ValueError('caller_denied')
  op=request.get('operation');gid=request.get('grant_id','')
  if not re.fullmatch(ID,gid):raise ValueError('invalid_grant')
  self.settings(gid)
  if op=='manifest' and set(request)=={'operation','grant_id'}:return self.manifest(gid)
  if op=='job' and set(request)=={'operation','grant_id','job_id'}:
   jid=request['job_id']
   if not re.fullmatch(ID,jid):raise ValueError('invalid_job')
   job=self.jobs.get(jid)
   if job is None:
    try:job=json.loads((Path(c['state'])/(jid+'.json')).read_text())
    except OSError:raise ValueError('unknown_job')
    if job['state']=='RUNNING':job=dict(job,state='INTERRUPTED')
   if job['grant_id']!=gid:raise ValueError('job_scope')
   return job
  if op=='download' and set(request)=={'operation','grant_id','artifact_id'} and re.fullmatch(ID,request['artifact_id']):
   with self.lock:
    if self.busy:raise ValueError('busy')
    now=time.monotonic();self.started=[t for t in self.started if now-t<60]
    if len(self.started)>=30 or len(list(Path(c['state']).glob('*.json')))>=4096:raise ValueError('job_limit')
    if len(self.jobs)>=128:
     old=next((key for key,value in self.jobs.items() if value['state']!='RUNNING'),None)
     if old:self.jobs.pop(old)
     else:raise ValueError('job_limit')
    self.started.append(now)
    self.busy=True;jid=secrets.token_hex(16);self.jobs[jid]={'job_id':jid,'grant_id':gid,'state':'RUNNING'};self.persist(jid)
   threading.Thread(target=self.download,args=(gid,request['artifact_id'],jid),daemon=True).start();return dict(self.jobs[jid])
  raise ValueError('operation_denied')

def daemon(config):
 import fcntl
 from .queue import DurableFetch
 c=json.loads(Path(config).read_text());path=Path(c['socket'])
 guard=open(Path(c['state'])/'daemon.lock','a')
 fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
 if path.exists():
  st=path.lstat()
  if not stat.S_ISSOCK(st.st_mode) or st.st_uid!=os.getuid():raise RuntimeError('socket_already_exists')
  path.unlink()
 f=DurableFetch(config)
 with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as server:
  server.bind(str(path));os.chown(path,-1,grp.getgrnam(c['socket_group']).gr_gid);os.chmod(path,0o660)
  for name in c['clients']:
   uid=str(pwd.getpwnam(name).pw_uid)
   subprocess.run(['setfacl','-m','u:'+uid+':rx',str(path.parent)],check=True)
   subprocess.run(['setfacl','-m','u:'+uid+':rw',str(path)],check=True)
  server.listen(4);f.start()
  try:
   while True:
    conn,_=server.accept()
    with conn:
     try:
      conn.settimeout(5);_,uid,_=struct.unpack('3i',conn.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12));data=conn.recv(4097)
      if len(data)>4096:raise ValueError('request_limit')
      answer=f.call(json.loads(data),uid)
     except Exception as e:answer={'error':error_code(e)}
     conn.sendall(json.dumps(answer).encode()+b'\n')
  finally:f.stop();path.unlink(missing_ok=True)

def client(request,path='/run/research-fetch/request.sock'):
 with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
  s.settimeout(40);s.connect(path);s.sendall(json.dumps(request).encode());out=bytearray()
  while True:
   b=s.recv(65536)
   if not b:break
   out.extend(b)
   if len(out)>2*1024**2:raise ValueError('reply_limit')
 return json.loads(out)

def main():
 p=argparse.ArgumentParser();sub=p.add_subparsers(dest='op',required=True);a=sub.add_parser('serve');a.add_argument('--config',required=True)
 for name in ('manifest','download','batch','job','wait','resume'):
  a=sub.add_parser(name);a.add_argument('grant_id')
  if name=='download':a.add_argument('artifact_id')
  if name in ('job','wait','resume'):a.add_argument('job_id')
 args=p.parse_args()
 if args.op=='serve':daemon(args.config);return
 r={'operation':'job' if args.op=='wait' else args.op,'grant_id':args.grant_id}
 if args.op=='download':r['artifact_id']=args.artifact_id
 if args.op in ('job','wait','resume'):r['job_id']=args.job_id
 # Waiting is optional; the durable service continues independently.
 end=time.monotonic()+14*86400
 while True:
  try:reply=client(r)
  except (OSError,ValueError) as error:reply={'error':error_code(error)}
  if args.op!='wait' or reply.get('state') not in ('QUEUED','RUNNING','RETRY_WAIT') or time.monotonic()>end:break
  time.sleep(2)
 print(json.dumps(reply,indent=2))
 if 'error' in reply or reply.get('state') in ('FAILED','INTERRUPTED','BLOCKED'):raise SystemExit(1)
if __name__=='__main__':main()
