"""Durable, serial download queue. Contains no upstream task submission capability."""
import json
import os
import pwd
import re
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from .fetch import Fetch, ID, error_code

ACTIVE = ('QUEUED', 'RUNNING', 'RETRY_WAIT')
RETRYABLE = {'SERVICE_UNAVAILABLE', 'UPSTREAM_UNAVAILABLE', 'RETRY_LATER', 'DISK_PRESSURE'}


class DurableFetch(Fetch):
    def __init__(self, config):
        super().__init__(config)
        c = json.loads(self.config.read_text())
        self.database = Path(c['state']) / 'queue.sqlite'
        self.wake = threading.Event()
        self.shutdown = threading.Event()
        self.worker = None
        with self.db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, '
                       'grant_id TEXT, artifact_id TEXT, kind TEXT, batch_id TEXT, state TEXT, '
                       'next_at REAL, created_at REAL, payload TEXT)')
            db.execute('CREATE INDEX IF NOT EXISTS pending_jobs ON jobs(state,next_at)')
            db.execute('CREATE INDEX IF NOT EXISTS batch_jobs ON jobs(batch_id,state)')
            for (raw,) in db.execute("SELECT payload FROM jobs WHERE state='RUNNING' OR (state='SUCCEEDED' AND payload LIKE ?)", ('%"error"%',)).fetchall():
                job = json.loads(raw)
                if job['state']=='SUCCEEDED':
                    job.pop('error',None)
                    self.save(db,job)
                elif job['kind']=='artifact' or not job.get('expanded'):
                    job.update(state='QUEUED', next_at=0)
                    self.save(db, job)
        os.chmod(self.database, 0o600)

    def db(self):
        db = sqlite3.connect(self.database, timeout=10)
        db.execute('PRAGMA synchronous=FULL')
        # sqlite connection contexts do not close connections on their own.
        return _Database(db)

    @staticmethod
    def save(db, job):
        job['updated_at'] = time.time()
        db.execute('INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?,?,?,?)',
                   (job['job_id'], job['grant_id'], job.get('artifact_id'), job['kind'],
                    job.get('batch_id'), job['state'], job.get('next_at', 0), job['created_at'], json.dumps(job)))

    def persist(self, jid):
        with self.db() as db:
            self.save(db, self.jobs[jid])

    @staticmethod
    def new_job(gid, aid=None, batch_id=None):
        return dict(job_id=secrets.token_hex(16), grant_id=gid, artifact_id=aid,
                    kind='artifact' if aid else 'batch', batch_id=batch_id,
                    state='QUEUED', attempts=0, next_at=0, created_at=time.time())

    def call(self, request, uid):
        c = json.loads(self.config.read_text())
        if uid not in {pwd.getpwnam(n).pw_uid for n in c['clients']}:
            raise ValueError('caller_denied')
        gid = request.get('grant_id', '')
        if not re.fullmatch(ID, gid):
            raise ValueError('invalid_grant')
        # Local configured identity only. Offline enqueue must not require a TLS connection.
        if gid not in c['grants']:
            raise ValueError('unknown_grant')
        op = request.get('operation')
        if op == 'manifest' and set(request) == {'operation', 'grant_id'}:
            return self.manifest(gid)
        if op in ('job', 'resume') and set(request) == {'operation', 'grant_id', 'job_id'}:
            jid = request['job_id']
            if not re.fullmatch(ID, jid):
                raise ValueError('invalid_job')
            with self.lock, self.db() as db:
                row = db.execute('SELECT payload FROM jobs WHERE job_id=? AND grant_id=?', (jid, gid)).fetchone()
                if not row:
                    if op == 'job':
                        return super().call(request, uid)  # Stage1 status, no blind restart.
                    raise ValueError('unknown_job')
                job = json.loads(row[0])
                if op == 'resume' and job['state'] in ('BLOCKED', 'FAILED'):
                    # Reusing the same grant never changes server scope or quota.
                    job.update(state='QUEUED', next_at=0, attempts=0)
                    job.pop('error', None)
                    self.save(db, job)
                    if job['kind'] == 'batch' and job.get('expanded'):
                        for (raw,) in db.execute("SELECT payload FROM jobs WHERE batch_id=? AND state IN ('FAILED','BLOCKED')", (jid,)).fetchall():
                            child = json.loads(raw)
                            if child.get('batch_id') == jid and child['state'] in ('BLOCKED', 'FAILED'):
                                child.update(state='QUEUED', next_at=0, attempts=0)
                                self.save(db, child)
                        job['state'] = 'RUNNING'
                        self.save(db, job)
                self.wake.set()
                return job
        fields = {'operation', 'grant_id'} | ({'artifact_id'} if op == 'download' else set())
        if op not in ('download', 'batch') or set(request) != fields:
            raise ValueError('operation_denied')
        aid = request.get('artifact_id')
        if op == 'download' and not re.fullmatch(ID, aid or ''):
            raise ValueError('invalid_artifact')
        with self.lock, self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT payload FROM jobs WHERE grant_id=? AND '
                             'artifact_id IS ? AND state IN (?,?,?) LIMIT 1', (gid, aid, *ACTIVE)).fetchone()
            if row:
                return json.loads(row[0])
            now = time.monotonic()
            self.started = [t for t in self.started if now-t < 60]
            if len(self.started) >= 30 or db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0] >= 50000:
                raise ValueError('job_limit')
            self.started.append(now)
            job = self.new_job(gid, aid)
            self.save(db, job)
        self.wake.set()
        return job

    def expand(self, job):
        manifest = self.manifest(job['grant_id'])
        artifacts = manifest['artifacts']
        if not artifacts or len(artifacts) > 10000:
            raise ValueError('manifest_limit')
        ids = [a['artifact_id'] for a in artifacts]
        if len(ids) != len(set(ids)) or any(not re.fullmatch(ID, aid) for aid in ids):
            raise ValueError('invalid_artifact')
        with self.lock, self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0]+len(ids) > 50000:
                raise ValueError('job_limit')
            for aid in ids:
                child = self.new_job(job['grant_id'], aid, job['job_id'])
                child['approval_until'] = self.leases.get(job['grant_id'], {}).get('approval_until', job['created_at']+14*86400)
                self.save(db, child)
            job.update(expanded=True, state='RUNNING', total=len(ids), completed=0)
            job['approval_until']=self.leases.get(job['grant_id'],{}).get('approval_until',job['created_at']+14*86400)
            self.save(db, job)

    def aggregate(self):
        with self.db() as db:
            parents = db.execute("SELECT payload FROM jobs WHERE kind='batch' AND state='RUNNING'").fetchall()
            for (raw,) in parents:
                job = json.loads(raw)
                counts = dict(db.execute('SELECT state,COUNT(*) FROM jobs WHERE batch_id=? GROUP BY state', (job['job_id'],)))
                job['completed'] = counts.get('SUCCEEDED', 0)
                job['failed'] = counts.get('FAILED', 0)
                job['blocked'] = counts.get('BLOCKED', 0)
                if counts and not any(counts.get(state, 0) for state in ACTIVE):
                    job['state'] = 'FAILED' if job['failed'] else 'BLOCKED' if job['blocked'] else 'SUCCEEDED'
                self.save(db, job)

    def run_once(self):
        self.aggregate()
        with self.db() as db:
            row = db.execute("SELECT payload FROM jobs WHERE state IN ('QUEUED','RETRY_WAIT') "
                             "AND next_at<=? ORDER BY created_at LIMIT 1", (time.time(),)).fetchone()
            if not row:
                return False
            job = json.loads(row[0])
            job.update(state='RUNNING', attempts=job['attempts']+1)
            job.pop('error',None)
            self.save(db, job)
        try:
            if time.time() >= job.get('approval_until', job['created_at']+14*86400):
                job.update(state='BLOCKED', error='APPROVAL_EXPIRED')
                with self.db() as db:
                    self.save(db, job)
                return True
            if job['kind'] == 'batch':
                self.expand(job)
                return True
            self.jobs[job['job_id']] = job
            self.download(job['grant_id'], job['artifact_id'], job['job_id'])
        except Exception as error:
            job.update(state='FAILED', error=error_code(error))
        finally:
            self.jobs.pop(job['job_id'], None)
        if job['state'] == 'FAILED':
            deadline = self.leases.get(job['grant_id'], {}).get('approval_until', job.get('approval_until', job['created_at']+14*86400))
            job['approval_until'] = deadline
            if job.get('error') in RETRYABLE:
                delay = 300 if job['error'] == 'DISK_PRESSURE' else min(3600, 5*2**min(job['attempts']-1, 10))
                job.update(state='RETRY_WAIT', next_at=min(deadline, time.time()+delay))
            elif job.get('error') in ('ACCESS_DENIED_OR_NOT_READY', 'UNKNOWN_GRANT', 'TLS_CERTIFICATE_ERROR'):
                job['state'] = 'BLOCKED'
            with self.db() as db:
                self.save(db, job)
                if job['state'] == 'BLOCKED':
                    # Denied batch authority must not fan out into thousands of retries.
                    for (raw,) in db.execute('SELECT payload FROM jobs WHERE grant_id=? AND state IN (?,?,?)',
                                             (job['grant_id'], *ACTIVE)).fetchall():
                        other = json.loads(raw)
                        if other['kind'] == 'artifact' or not other.get('expanded'):
                            other.update(state='BLOCKED', error=job['error'])
                            self.save(db, other)
        self.aggregate()
        return True

    def start(self):
        def loop():
            while not self.shutdown.is_set():
                try:
                    if not self.run_once():
                        self.wake.wait(2)
                        self.wake.clear()
                except Exception:
                    os._exit(1)  # systemd restarts and recovers persisted jobs.
        self.worker = threading.Thread(target=loop, daemon=True)
        self.worker.start()

    def stop(self):
        self.shutdown.set()
        self.wake.set()


class _Database:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self.db

    def __exit__(self, kind, value, traceback):
        try:
            self.db.rollback() if kind else self.db.commit()
        finally:
            self.db.close()
