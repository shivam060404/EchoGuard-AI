"""Create local demo credentials without printing bearer tokens."""
import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets


def configure(root: Path, rotate=False):
    credentials = root / 'data/local-auth.json'
    env = root / '.env'
    if credentials.exists() and not rotate:
        raise SystemExit('Credentials already exist. Use --rotate to replace them, then recreate the API container.')
    expires = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    tokens = {role: secrets.token_urlsafe(48) for role in ('operator', 'reviewer')}
    registry = {hashlib.sha256(token.encode()).hexdigest(): {
        'tenant_id': 'demo-tenant', 'actor_id': f'local-{role}', 'role': role, 'expires_at': expires,
    } for role, token in tokens.items()}
    credentials.parent.mkdir(parents=True, exist_ok=True)
    for path, content in [(credentials, json.dumps({'tokens': tokens, 'expires_at': expires}, indent=2))]:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w') as handle:
            handle.write(content + '\n')
    existing = env.read_text() if env.exists() else ''
    lines = [line for line in existing.splitlines() if not line.startswith('ECHOGUARD_AUTH_JSON=')]
    lines.append("ECHOGUARD_AUTH_JSON='" + json.dumps(registry, separators=(',', ':')) + "'")
    fd = os.open(env, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, 'w') as handle:
        handle.write('\n'.join(lines) + '\n')
    print('Local tokens saved to data/local-auth.json (mode 600); hashed registry saved to .env. Expires in 7 days.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rotate', action='store_true')
    configure(Path(__file__).resolve().parents[2], parser.parse_args().rotate)
