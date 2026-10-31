"""mTLS-only artifact gateway. Never imports legacy Flask/PACS code."""
from . import leases
from contextlib import contextmanager
import argparse,hashlib,json,os,re,socket,sqlite3,ssl,stat,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
ID=r'[0-9a-f]{32}'
class Denied(Exception):pass
class Busy(Denied):pass

class Store:
 def __init__(self,policy,published,state):
  self.policy=Path(policy);self.published=Path(published);self.state=Path(state);self.state.mkdir(exist_ok=True,mode=0o700);self.lock=threading.Lock();self.active={};self.verified={}
  with self.db() as db:
   db.execute('CREATE TABLE IF NOT EXISTS usage(grant_id TEXT PRIMARY KEY, bytes INTEGER NOT NULL)')
   db.execute('CREATE TABLE IF NOT EXISTS audit(at INTEGER, grant_id TEXT, action TEXT, result TEXT, bytes INTEGER)')
  leases.initialize(self)
 @contextmanager
 def db(self):
  db=sqlite3.connect(self.state/'usage.sqlite',timeout=5);db.execute('PRAGMA synchronous=FULL')
  try:
   with db:yield db
  finally:db.close()
 def grant(self,gid,peer):
  if not re.fullmatch(ID,gid):raise Denied()
  try:
   if self.policy.stat().st_size>1024*1024:raise Denied()
   p=json.loads(self.policy.read_text());g=p['grants'][gid];now=time.time()
   if g['status']!='APPROVED' or g['certificate_sha256']!=peer or not g['not_before']<=now<g['expires_at']:raise Denied()
   if not re.fullmatch(ID,g['export_id']) or not 1<=g['max_bytes']<=1024**4 or not 1<=g['rate_bytes_s']<=100*1024**2 or g['concurrency']!=1:raise Denied()
   if g.get('lease_seconds') and (g['lease_seconds']!=600 or not 0<g['expires_at']-g['not_before']<=14*86400):raise Denied()
   return g
  except (OSError,ValueError,KeyError,TypeError):raise Denied() from None
 def manifest(self,g):
  directory=self.published/g['export_id'];path=directory/'manifest.json'
  if directory.is_symlink() or path.is_symlink() or path.stat().st_size>2*1024**2:raise Denied()
  raw=path.read_bytes()
  if hashlib.sha256(raw).hexdigest()!=g['manifest_sha256']:raise Denied()
  m=json.loads(raw)
  if m['project_id']!=g['project_id'] or m['export_id']!=g['export_id'] or m['case_count']>g['max_cases']:raise Denied()
  return m
 def charge(self,gid,peer,n,action='content',expected=None,lease=None):
  g=self.grant(gid,peer);leases.check(self,gid,g,lease)
  if expected and any(g[k]!=expected[k] for k in ('export_id','manifest_sha256','project_id','certificate_sha256')):raise Denied()
  with self.lock,self.db() as db:
   db.execute('BEGIN IMMEDIATE');row=db.execute('SELECT bytes FROM usage WHERE grant_id=?',(gid,)).fetchone();used=row[0] if row else 0
   if used+n>g['max_bytes']:raise Denied()
   db.execute('INSERT OR REPLACE INTO usage VALUES (?,?)',(gid,used+n));db.execute('INSERT INTO audit VALUES (?,?,?,?,?)',(int(time.time()),gid,action,'ALLOW',n))
  return g
 def enter(self,gid):
  with self.lock:
   if self.active.get(gid,0):raise Busy()
   self.active[gid]=1
 def leave(self,gid):
  with self.lock:self.active.pop(gid,None)

class Handler(BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*_):pass
 def answer(self,code,body=b'{}',headers=None):
  self.send_response(code);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.send_header('Cache-Control','no-store');self.send_header('Connection','close');self.send_header('X-Content-Type-Options','nosniff')
  for k,v in (headers or {}).items():self.send_header(k,v)
  self.end_headers();self.close_connection=True
  if self.command!='HEAD':self.wfile.write(body)
 def do_POST(self):self.answer(405)
 do_PUT=do_DELETE=do_PATCH=do_OPTIONS=do_TRACE=do_CONNECT=do_POST
 def do_HEAD(self):self.do_GET()
 def do_GET(self):
  store=self.server.store;entered=False;gid=''
  try:
   if self.headers.get('Transfer-Encoding') or self.headers.get('Content-Length') not in (None,'0'):raise Denied()
   if len(self.headers.get_all('X-Export-Grant',[]))!=1:raise Denied()
   gid=self.headers['X-Export-Grant'];peer=hashlib.sha256(self.connection.getpeercert(binary_form=True)).hexdigest();g=store.grant(gid,peer);m=store.manifest(g)
   if self.path=='/v1/exports/'+g['export_id']+'/lease':
    if self.command!='GET':raise Denied()
    self.answer(200,json.dumps(leases.renew(store,gid,peer)).encode());return
   lease=self.headers.get('X-Export-Lease');leases.check(store,gid,g,lease)
   if self.path=='/v1/exports/'+g['export_id']:
    store.charge(gid,peer,0,'status',lease=lease);self.answer(200,json.dumps({'export_id':g['export_id'],'status':'READY','case_count':m['case_count'],'policy_version':m['policy_version']}).encode());return
   if self.path=='/v1/exports/'+g['export_id']+'/manifest':
    safe=dict(m);safe['artifacts']=[{k:v for k,v in a.items() if k!='file'} for a in m['artifacts']];body=json.dumps(safe).encode();store.charge(gid,peer,len(body),'manifest',lease=lease);self.answer(200,body);return
   match=re.fullmatch('/v1/artifacts/('+ID+')/content',self.path)
   if not match:raise Denied()
   a=next((a for a in m['artifacts'] if a['artifact_id']==match[1]),None)
   if not a or not re.fullmatch(ID+r'\.(nii|nii\.gz|npz|png|xlsx)',a['file']):raise Denied()
   store.enter(peer);entered=True
   folder=store.published/g['export_id']/'artifacts'
   if folder.is_symlink():raise Denied()
   fd=os.open(folder/a['file'],os.O_RDONLY|os.O_NOFOLLOW)
   with os.fdopen(fd,'rb') as f:
    st=os.fstat(f.fileno())
    if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or st.st_size!=a['bytes'] or st.st_mode&0o222:raise Denied()
    # Publisher freezes payloads; stat binding is checked again after hash and between chunks.
    identity=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns,a['sha256'])
    with store.lock:verified=identity in store.verified
    if not verified:
     h=hashlib.sha256()
     for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
     if h.hexdigest()!=a['sha256']:raise Denied()
     with store.lock:
      if len(store.verified)>=256:store.verified.clear()
      store.verified[identity]=True
    etag='"'+a['sha256']+'"';start=0;end=st.st_size-1;code=200
    if self.headers.get('If-Match') not in (None,etag):self.answer(412);return
    if self.headers.get('Range'):
     r=re.fullmatch(r'bytes=(\d+)-(\d*)',self.headers['Range'])
     if not r or self.headers.get('If-Match')!=etag:self.answer(416);return
     start=int(r[1]);end=min(end,int(r[2])) if r[2] else end
     if start>end or start>=st.st_size:self.answer(416);return
     code=206
    store.charge(gid,peer,0,'open',expected=g,lease=lease)
    self.send_response(code);self.send_header('Content-Type','application/octet-stream');self.send_header('Content-Length',str(end-start+1));self.send_header('ETag',etag);self.send_header('Accept-Ranges','bytes');self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff');self.send_header('Connection','close');self.send_header('Content-Disposition','attachment; filename="'+a['artifact_id']+a['format']+'"')
    if code==206:self.send_header('Content-Range',f'bytes {start}-{end}/{st.st_size}')
    self.end_headers();self.close_connection=True
    if self.command=='HEAD':return
    f.seek(start);remaining=end-start+1;sent=0;began=time.monotonic()
    while remaining:
     nowstat=os.fstat(f.fileno())
     if (nowstat.st_size,nowstat.st_mtime_ns,nowstat.st_ino)!=(st.st_size,st.st_mtime_ns,st.st_ino):raise Denied()
     b=f.read(min(256*1024,remaining))
     if not b:raise Denied()
     current=store.charge(gid,peer,len(b),expected=g,lease=lease);self.wfile.write(b);self.wfile.flush();remaining-=len(b);sent+=len(b)
     delay=sent/current['rate_bytes_s']-(time.monotonic()-began)
     while delay>0:
      time.sleep(min(delay,1));leases.check(store,gid,store.grant(gid,peer),lease);delay=sent/current['rate_bytes_s']-(time.monotonic()-began)
  except Busy:
   self.answer(429,headers={'Retry-After':'5'})
  except (Denied,OSError,ValueError,KeyError,TypeError):
   if entered:self.close_connection=True
   else:self.answer(404)
  finally:
   if entered:store.leave(peer)

class Server(ThreadingHTTPServer):
 daemon_threads=True;allow_reuse_address=True
 def __init__(self,address,store,context):self.store=store;self.context=context;self.slots=threading.BoundedSemaphore(8);super().__init__(address,Handler)
 def process_request(self,request,address):
  if not self.slots.acquire(blocking=False):request.close();return
  try:super().process_request(request,address)
  except BaseException:self.slots.release();raise
 def process_request_thread(self,request,address):
  try:
   request.settimeout(10)
   with self.context.wrap_socket(request,server_side=True) as tls:self.finish_request(tls,address)
  except (OSError,ssl.SSLError):pass
  finally:request.close();self.slots.release()
 def handle_error(self,*_):pass

def main():
 p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args();c=json.loads(Path(a.config).read_text())
 context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2;context.verify_mode=ssl.CERT_REQUIRED;context.load_cert_chain(c['certificate'],c['key']);context.load_verify_locations(c['ca'])
 store=Store(c['policy'],c['published'],c['state']);server=Server((c['host'],c['port']),store,context);server.serve_forever()
if __name__=='__main__':main()
