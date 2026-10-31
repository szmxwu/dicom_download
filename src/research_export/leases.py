"""Short renewable access leases within a root-approved immutable batch."""
import hashlib
import hmac
import json
import secrets
import time


def binding(grant):
    keys = ('project_id', 'export_id', 'manifest_sha256', 'certificate_sha256',
            'not_before', 'expires_at', 'max_cases', 'max_bytes', 'lease_seconds')
    return hashlib.sha256(json.dumps({k: grant[k] for k in keys}, sort_keys=True).encode()).hexdigest()


def initialize(store):
    with store.db() as db:
        db.execute('CREATE TABLE IF NOT EXISTS leases '
                   '(grant_id TEXT PRIMARY KEY, binding TEXT, token TEXT, until REAL)')


def renew(store, gid, peer):
    grant = store.grant(gid, peer)
    store.manifest(grant)
    if not grant.get('lease_seconds'):
        return {'required': False}
    scope = binding(grant)
    until = min(time.time() + grant['lease_seconds'], grant['expires_at'])
    with store.lock, store.db() as db:
        db.execute('BEGIN IMMEDIATE')
        usage=db.execute('SELECT bytes FROM usage WHERE grant_id=?',(gid,)).fetchone()
        if usage and usage[0]>=grant['max_bytes']:
            from .gateway import Denied
            raise Denied()
        row = db.execute('SELECT binding, token FROM leases WHERE grant_id=?', (gid,)).fetchone()
        token = row[1] if row and row[0] == scope else secrets.token_hex(32)
        db.execute('INSERT OR REPLACE INTO leases VALUES (?,?,?,?)', (gid, scope, token, until))
        db.execute('INSERT INTO audit VALUES (?,?,?,?,?)',
                   (int(time.time()), gid, 'lease', 'ALLOW', 0))
    # No policy writes and no reset of usage. Tokens never go to research agents.
    return {'required': True, 'token': token, 'valid_for': max(0, until-time.time()),
            'approval_until': grant['expires_at']}


def check(store, gid, grant, token):
    if not grant.get('lease_seconds'):
        return
    from .gateway import Denied
    if not isinstance(token, str) or len(token) != 64:
        raise Denied()
    with store.db() as db:
        row = db.execute('SELECT binding, token, until FROM leases WHERE grant_id=?', (gid,)).fetchone()
    if (not row or row[0] != binding(grant) or row[2] <= time.time()
            or not hmac.compare_digest(token, row[1])):
        raise Denied()
