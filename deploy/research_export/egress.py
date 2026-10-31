#!/usr/bin/python3
"""Owned chains only. No global flush or policy changes."""
import os,pwd,subprocess,sys
if os.geteuid()!=0 or len(sys.argv)!=2 or sys.argv[1] not in ('gateway','fetch'):raise SystemExit(2)
role=sys.argv[1];uid=str(pwd.getpwnam('research-export' if role=='gateway' else 'research-fetch').pw_uid);chain='REXP_GW_OUT' if role=='gateway' else 'REXP_FETCH_OUT'
def run(*args,check=True):return subprocess.run(args,check=check,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
for tool in ('iptables','ip6tables'):
 # Chain contents are exclusively managed by this named service; update idempotently.
 run(tool,'-w','-N',chain,check=False);run(tool,'-w','-F',chain)
 if role=='gateway':run(tool,'-w','-A',chain,'-m','owner','--uid-owner',uid,'-m','conntrack','--ctstate','NEW','-j','REJECT')
 else:
  if tool=='iptables':run(tool,'-w','-A',chain,'-m','owner','--uid-owner',uid,'-p','tcp','-d','192.0.0.193/32','--dport','8443','-j','RETURN')
  run(tool,'-w','-A',chain,'-m','owner','--uid-owner',uid,'-j','REJECT')
  if tool=='iptables':run(tool,'-w','-A',chain,'-p','tcp','-d','192.0.0.193/32','--dport','8443','-j','REJECT')
 run(tool,'-w','-A',chain,'-j','RETURN')
 if run(tool,'-w','-C','OUTPUT','-j',chain,check=False).returncode:run(tool,'-w','-I','OUTPUT','1','-j',chain)
