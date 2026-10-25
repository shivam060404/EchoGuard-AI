"""Launch or stop a repeatable isolated local incident workspace."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from backend.ingest.scenario import ScenarioConfig, write_scenario

ROOT = Path(__file__).resolve().parents[2]
PROFILES = ('stream', 'worker', 'knowledge', 'demo')
PROJECT = re.compile(r'^echoguard-(?:local|faculty)-[a-f0-9]{10}$')


def actor_name(tenant: str, role: str) -> str:
    # Retain the actor IDs of already-created faculty-prefixed sessions.
    return f'{"faculty" if tenant.startswith("faculty-") else "local"}-{role}'


def available_port(reserved: set[int]) -> int:
    while True:
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        if port not in reserved:
            reserved.add(port)
            return port


def credentials(tenant: str, directory: Path) -> tuple[dict[str, str], str]:
    tokens = {role: secrets.token_urlsafe(48) for role in ('operator', 'reviewer')}
    expires = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    registry = {hashlib.sha256(token.encode()).hexdigest(): {
        'tenant_id': tenant, 'actor_id': actor_name(tenant, role), 'role': role,
        'expires_at': expires} for role, token in tokens.items()}
    directory.mkdir(parents=True, mode=0o700)
    path = directory / 'credentials.json'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as handle:
        json.dump({'tenant_id': tenant, 'tokens': tokens, 'expires_at': expires}, handle, indent=2)
        handle.write('\n')
    return tokens, json.dumps(registry, separators=(',', ':'))


def registry_for_credentials(tenant: str, saved: dict) -> str:
    if saved.get('tenant_id') != tenant:
        raise ValueError('Credential file belongs to a different demo tenant')
    expires = datetime.fromisoformat(saved['expires_at'])
    if expires.tzinfo is None or expires <= datetime.now(timezone.utc):
        raise ValueError('Demo credentials expired; start a new isolated demo')
    tokens = saved['tokens']
    if set(tokens) != {'operator', 'reviewer'} or any(not isinstance(value, str) for value in tokens.values()):
        raise ValueError('Demo credential file is incomplete')
    registry = {hashlib.sha256(token.encode()).hexdigest(): {
        'tenant_id': tenant, 'actor_id': actor_name(tenant, role), 'role': role,
        'expires_at': saved['expires_at']} for role, token in tokens.items()}
    return json.dumps(registry, separators=(',', ':'))


def saved_project(project: str) -> tuple[dict, dict]:
    if not PROJECT.fullmatch(project):
        raise ValueError('Expected an echoguard-local-* project created by this launcher')
    directory = ROOT / 'data' / 'local_demo' / project
    run = json.loads((directory / 'run.json').read_text())
    saved = json.loads((directory / 'credentials.json').read_text())
    if run.get('project') != project:
        raise ValueError('Run metadata belongs to a different project')
    registry_for_credentials(run['tenant_id'], saved)
    return run, saved


def project_environment(run: dict, registry: str) -> dict[str, str]:
    web = urlsplit(run['web_url'])
    api = urlsplit(run['api_url'])
    if (web.scheme != 'http' or api.scheme != 'http'
            or web.hostname != 'localhost' or api.hostname != 'localhost'
            or web.port is None or api.port is None):
        raise ValueError('Demo URLs must use numbered localhost HTTP ports')
    return dict(os.environ, ECHOGUARD_AUTH_JSON=registry,
                ECHOGUARD_LOCAL_DEMO='true', ECHOGUARD_OUTCOME_WINDOW_SECONDS='60',
                ECHOGUARD_WEB_PORT=str(web.port), ECHOGUARD_API_PORT=str(api.port),
                ECHOGUARD_KAFKA_PORT=str(run.get('kafka_port', 9092)),
                ECHOGUARD_MONGO_PORT=str(run.get('mongo_port', 27017)),
                CONSOLE_ORIGIN=run['web_url'])


def compose(project: str, environment: dict[str, str], *arguments: str) -> None:
    command = ['docker', 'compose', '-p', project]
    for profile in PROFILES:
        command.extend(('--profile', profile))
    subprocess.run(command + list(arguments), cwd=ROOT, env=environment, check=True)


def incident_list(api_url: str, token: str) -> list[dict]:
    request = Request(f'{api_url}/incidents?limit=100',
                      headers={'Authorization': f'Bearer {token}'})
    with urlopen(request, timeout=10) as response:
        return json.load(response)['items']


def demo_status(api_url: str, token: str) -> dict:
    request = Request(f'{api_url}/demo/status',
                      headers={'Authorization': f'Bearer {token}'})
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def authentication_check(run: dict, token: str) -> dict:
    api = Request(run['api_url'] + '/me',
                  headers={'Authorization': f'Bearer {token}'})
    web = Request(run['web_url'] + '/api/session',
                  data=json.dumps({'token': token}).encode(),
                  headers={'Origin': run['web_url'], 'Content-Type': 'application/json'})
    result = {}
    for name, request in (('api', api), ('web_session', web)):
        try:
            with urlopen(request, timeout=10) as response:
                body = json.load(response)
                result[name] = {'status': response.status,
                                'tenant_match': body.get('tenant_id') == run['tenant_id'],
                                'role': body.get('role')}
        except HTTPError as exc:
            result[name] = {'status': exc.code}
        except URLError as exc:
            result[name] = {'network_error': type(exc.reason).__name__}
    return result


def require_authentication(run: dict, token: str) -> dict:
    result = authentication_check(run, token)
    if not all(result.get(name, {}).get('status') == 200
               and result[name].get('tenant_match') and result[name].get('role') == 'reviewer'
               for name in ('api', 'web_session')):
        raise RuntimeError(f'Demo reviewer authentication failed: {result}')
    return result


def complete_demo(info: dict, tokens: dict[str, str], registry: str,
                  *, resume: bool = False) -> None:
    project = info['project']
    tenant = info['tenant_id']
    expected_dataset = f'data/scenarios/{project}.jsonl'
    if info.get('dataset') != expected_dataset:
        raise ValueError('Demo dataset path does not match its project')
    dataset = ROOT / expected_dataset
    manifest = dataset.with_suffix('.manifest.json')
    receipt = dataset.with_suffix('.receipt.json')
    if not dataset.is_file() or not manifest.is_file():
        raise FileNotFoundError('Demo scenario or manifest is missing')
    environment = project_environment(info, registry)
    directory = ROOT / 'data' / 'local_demo' / project
    try:
        compose(project, environment, 'up', '-d', '--build', '--wait',
                'stream-job', 'case-writer', 'api', 'web')
        compose(project, environment, 'build', 'knowledge-tool', 'producer')
        compose(project, environment, 'run', '--rm', 'knowledge-tool',
                'python', '-m', 'backend.knowledge.seed', '--tenant', tenant)
        producer_args = ('--dataset', expected_dataset,
                         '--manifest', str(manifest.relative_to(ROOT)),
                         '--receipt', str(receipt.relative_to(ROOT)))
        if not resume or not receipt.is_file():
            compose(project, environment, 'run', '--rm', 'producer', 'publish',
                    *producer_args, '--rate', '0')
        compose(project, environment, 'run', '--rm', 'producer', 'verify', *producer_args)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            try:
                items = incident_list(info['api_url'], tokens['reviewer'])
                progress = demo_status(info['api_url'], tokens['reviewer'])
                codes = {item.get('error_code') for item in items}
                peaks = {item.get('error_code'): item.get('peak_count', 0) for item in items}
                if (progress['cases'] >= 250 and progress['knowledge_documents'] >= 3
                        and {'PAY-502', 'AUTH-401'}.issubset(codes)
                        and peaks.get('PAY-502', 0) >= 150
                        and peaks.get('AUTH-401', 0) >= 40):
                    require_authentication(info, tokens['reviewer'])
                    info['incidents'] = {code: next(item['_id'] for item in items
                                                     if item.get('error_code') == code)
                                         for code in ('PAY-502', 'AUTH-401')}
                    (directory / 'run.json').write_text(json.dumps(info, indent=2) + '\n')
                    print(json.dumps({'status': 'ready', **info}, indent=2), flush=True)
                    print('Open the web URL and sign in with the reviewer token in the credentials file.', flush=True)
                    print(f'To remove only this demo: python -m backend.demo.local stop {project}', flush=True)
                    return
            except (OSError, ValueError):
                pass
            time.sleep(3)
        raise TimeoutError('The two seeded incidents did not reach the API within five minutes')
    except Exception:
        print(f'Demo project {project} remains for inspection. Inspect with: '
              f'docker compose -p {project} ps', flush=True)
        print(f'Retry this project without deleting its data: '
              f'python -m backend.demo.local resume {project}', flush=True)
        raise


def launch() -> None:
    subprocess.run(['docker', 'info', '--format', '{{.ServerVersion}}'],
                   stdout=subprocess.DEVNULL, cwd=ROOT, check=True)
    suffix = uuid4().hex[:10]
    project = f'echoguard-local-{suffix}'
    tenant = f'local-{suffix}'
    directory = ROOT / 'data' / 'local_demo' / project
    tokens, registry = credentials(tenant, directory)
    reserved: set[int] = set()
    web_port, api_port, kafka_port, mongo_port = (available_port(reserved) for _ in range(4))
    web_url = f'http://localhost:{web_port}'
    api_url = f'http://localhost:{api_port}'
    now = datetime.now(timezone.utc)
    start = now.replace(minute=(now.minute // 30) * 30, second=0,
                        microsecond=0) - timedelta(minutes=30)
    name = f'{project}.jsonl'
    dataset = ROOT / 'data' / 'scenarios' / name
    manifest = dataset.with_suffix('.manifest.json')
    write_scenario(ScenarioConfig(tenant_id=tenant, start_time=start), dataset, manifest)
    info = {'project': project, 'tenant_id': tenant, 'web_url': web_url,
            'api_url': api_url, 'dataset': str(dataset.relative_to(ROOT)),
            'credentials': str((directory / 'credentials.json').relative_to(ROOT)),
            'kafka_port': kafka_port, 'mongo_port': mongo_port}
    (directory / 'run.json').write_text(json.dumps(info, indent=2) + '\n')
    print(f'Starting isolated local demo {project}. Main EchoGuard volumes are untouched.', flush=True)
    complete_demo(info, tokens, registry)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('start')
    stop = sub.add_parser('stop')
    stop.add_argument('project')
    check = sub.add_parser('check-auth')
    check.add_argument('project')
    repair = sub.add_parser('repair-auth')
    repair.add_argument('project')
    refresh = sub.add_parser('refresh')
    refresh.add_argument('project')
    resume = sub.add_parser('resume')
    resume.add_argument('project')
    args = parser.parse_args()
    if args.command == 'start':
        launch()
    elif args.command == 'stop':
        if not PROJECT.fullmatch(args.project):
            parser.error('Only echoguard-local-* projects created by this launcher can be stopped')
        compose(args.project, dict(os.environ), 'down', '-v', '--remove-orphans')
        print(f'Stopped isolated project {args.project}. Local demo files remain under data/local_demo/.')
    elif args.command == 'resume':
        run, saved = saved_project(args.project)
        print(f'Resuming isolated local demo {args.project} with its existing volumes.', flush=True)
        complete_demo(run, saved['tokens'], registry_for_credentials(run['tenant_id'], saved),
                      resume=True)
    else:
        run, saved = saved_project(args.project)
        if args.command in {'repair-auth', 'refresh'}:
            environment = project_environment(
                run, registry_for_credentials(run['tenant_id'], saved))
            compose(args.project, environment, 'up', '-d', '--build', '--no-deps',
                    '--force-recreate', '--wait', 'api')
            compose(args.project, environment, 'up', '-d', '--build', '--no-deps',
                    '--force-recreate', '--wait', 'web')
        result = authentication_check(run, saved['tokens']['reviewer'])
        print(json.dumps({'project': args.project, 'credential_expired': False,
                          'reviewer_authentication': result,
                          'updated_services': ['api', 'web'] if args.command in {'repair-auth', 'refresh'} else []},
                         sort_keys=True))
        if not all(result.get(name, {}).get('status') == 200
                   and result[name].get('tenant_match') for name in ('api', 'web_session')):
            raise SystemExit('Reviewer token is not accepted; inspect the project API/web logs')


if __name__ == '__main__':
    main()
