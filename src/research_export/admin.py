"""Root-side publication/grant CLI. No remote approval API."""
import argparse,hashlib,json,os,secrets,time
from pathlib import Path
from .publish import publish,POLICY

def atomic(path,value):
 path=Path(path);tmp=path.with_name(path.name+'.'+secrets.token_hex(4));tmp.write_text(json.dumps(value,sort_keys=True)+'\n');os.chmod(tmp,0o640)
 if path.exists():os.chown(tmp,path.stat().st_uid,path.stat().st_gid)
 tmp.replace(path)

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--policy',required=True);p.add_argument('--mapping-key',required=True);sub=p.add_subparsers(dest='op',required=True)
 a=sub.add_parser('synthetic');a.add_argument('--directory',required=True)
 a=sub.add_parser('publish');a.add_argument('--source',required=True);a.add_argument('--review',required=True);a.add_argument('--project',required=True)
 a=sub.add_parser('grant');a.add_argument('--days',type=int,default=0);a.add_argument('--export-id',required=True);a.add_argument('--certificate-sha256',required=True);a.add_argument('--hours',type=int,default=24);a.add_argument('--max-cases',type=int,default=100);a.add_argument('--max-bytes',type=int,default=20*1024**3);a.add_argument('--rate',type=int,default=5*1024**2)
 a=sub.add_parser('revoke');a.add_argument('grant_id')
 args=p.parse_args()
 if os.geteuid()!=0:p.error('Root console required')
 policy=Path(args.policy)
 if args.op=='synthetic':
  from .synthetic import generate
  generate(args.directory);print('SYNTHETIC_FIXTURE_GENERATED');return
 if args.op=='publish':
  review=json.loads(Path(args.review).read_text());key=Path(args.mapping_key).read_bytes()
  if len(key)<32:p.error('Mapping key too short')
  m=publish(args.source,review,Path(args.root),args.project,key)
  gid=policy.stat().st_gid
  directory=Path(args.root)/'published'/m['export_id']
  for folder,dirs,files in os.walk(directory):
   os.chown(folder,0,gid);os.chmod(folder,0o550)
   for name in files:os.chown(Path(folder)/name,0,gid);os.chmod(Path(folder)/name,0o440)
  print(json.dumps({'export_id':m['export_id'],'case_count':m['case_count'],'artifacts':len(m['artifacts'])}));return
 value=json.loads(policy.read_text())
 if args.op=='revoke':value['grants'][args.grant_id]['status']='REVOKED';atomic(policy,value);print('REVOKED');return
 import re
 if not re.fullmatch('[0-9a-f]{32}',args.export_id) or not re.fullmatch('[0-9a-f]{64}',args.certificate_sha256):p.error('Invalid identity')
 if not 0<=args.days<=14:p.error('Batch duration must be 1-14 days')
 duration=args.days*86400 if args.days else args.hours*3600
 if not 1<=args.hours<=24 or not 1<=args.max_cases<=1000 or not 1<=args.max_bytes<=1024**4 or not 1<=args.rate<=100*1024**2:p.error('Invalid grant limits')
 raw=(Path(args.root)/'published'/args.export_id/'manifest.json').read_bytes();m=json.loads(raw)
 if m['case_count']>args.max_cases:p.error('Case limit exceeded')
 gid=secrets.token_hex(16);now=int(time.time());value['grants'][gid]={'status':'APPROVED','project_id':m['project_id'],'export_id':args.export_id,'certificate_sha256':args.certificate_sha256,'manifest_sha256':hashlib.sha256(raw).hexdigest(),'not_before':now,'expires_at':now+duration,'max_cases':args.max_cases,'max_bytes':args.max_bytes,'rate_bytes_s':args.rate,'concurrency':1}
 if args.days:value['grants'][gid]['lease_seconds']=600
 atomic(policy,value);print(json.dumps({'grant_id':gid,'export_id':args.export_id,'expires_at':now+duration}))
if __name__=='__main__':main()
