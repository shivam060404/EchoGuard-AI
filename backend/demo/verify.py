"""Optional full HTTP/proxy/Kafka/Spark/Mongo demo acceptance check."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from http.cookiejar import CookieJar
import json
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, Request, build_opener
from uuid import uuid4

from .local import PROJECT, ROOT


class NoUnusedIncident(RuntimeError):
    """Every seeded incident revision has already completed or authorized work."""


def action_authorized(item: dict) -> bool:
    if 'action_authorized' in item:
        return item['action_authorized'] is True
    # Older local API containers returned the internal claim in list records.
    claim = item.get('execution_claim')
    return isinstance(claim, dict) and claim.get('incident_version') == item.get('version')


def select_incident(items: list[dict]) -> dict:
    """Use a seeded revision that has not already authorized an action."""
    for code in ('PAY-502', 'AUTH-401'):
        match = next((item for item in items
                      if item.get('error_code') == code
                      and item.get('status') in {'candidate', 'reopened', 'investigating'}
                      and not action_authorized(item)), None)
        if match is not None:
            return match
    states = {code: [{'status': item.get('status'),
                      'action_already_authorized': action_authorized(item)}
                     for item in items if item.get('error_code') == code]
              for code in ('PAY-502', 'AUTH-401')}
    raise NoUnusedIncident(
        f'No unused seeded incident revision remains in this workspace ({states}). '
        'Start a fresh isolated workspace with: python -m backend.demo.local start'
    )


def verify(project: str) -> dict:
    if not PROJECT.fullmatch(project):
        raise ValueError('Expected a project created by backend.demo.local start')
    directory = ROOT / 'data' / 'local_demo' / project
    run = json.loads((directory / 'run.json').read_text())
    credentials = json.loads((directory / 'credentials.json').read_text())
    web = run['web_url'].rstrip('/')
    browser = build_opener(HTTPCookieProcessor(CookieJar()))

    with browser.open(web + '/', timeout=15) as response:
        page = response.read().decode('utf-8')
        assert response.status == 200
        assert 'text/html' in response.headers.get('Content-Type', '')
        assert 'EchoGuard' in page and 'See the issue.' in page

    def request(path: str, body=None, key: str | None = None):
        headers = {'Content-Type': 'application/json'}
        if body is not None:
            headers['Origin'] = web
        if key:
            headers['Idempotency-Key'] = key
        call = Request(web + path, headers=headers,
                       data=json.dumps(body).encode() if body is not None else None)
        try:
            with browser.open(call, timeout=65) as response:
                return json.load(response)
        except HTTPError as exc:
            raise AssertionError(f'{path} returned HTTP {exc.code}: {exc.read()[:300]!r}') from exc

    def traffic(incident_id: str, kind: str):
        key = f'acceptance-{kind}-{uuid4().hex}'
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            result = request('/api/proxy/demo/traffic',
                             {'incident_id': incident_id, 'kind': kind}, key)
            if result['status'] == 'ready':
                return result
            time.sleep(2)
        raise TimeoutError(f'{kind} traffic did not reach the case store')

    request('/api/session', {'token': credentials['tokens']['reviewer']})
    actor = request('/api/proxy/me')
    assert actor['role'] == 'reviewer' and actor['tenant_id'] == run['tenant_id']
    capability = request('/api/proxy/demo/status')
    assert capability['mode'] == 'local' and capability['tenant_id'] == run['tenant_id']
    assert capability['cases'] >= 250 and capability['incidents'] >= 2
    with browser.open(web + '/api/proxy/events', timeout=15) as response:
        assert 'text/event-stream' in response.headers.get('Content-Type', '')
        assert response.readline() == b'event: refresh\n'
    items = request('/api/proxy/incidents?limit=100')['items']
    incident = select_incident(items)
    identifier = incident['_id']
    detail = request(f'/api/proxy/incidents/{identifier}')
    assert detail['incident']['incident_id'] == identifier
    assert detail['evidence'] and detail['signals'] and detail['incident_hash']
    assert all('event_id' in entry and 'normalized_text' in entry for entry in detail['evidence'])
    assert all('current_count' in signal and 'window_start' in signal for signal in detail['signals'])
    state = request(f'/api/proxy/incidents/{identifier}/workflows',
                    {'expected_version': incident['version']}, f'verify-start-{uuid4().hex}')
    assert state['status'] == 'awaiting_approval' and state['proposal']['steps']
    run_id = state['run_id']
    current = request(f'/api/proxy/incidents/{identifier}')
    assert current['workflow']['_id'] == run_id
    assert current['workflow']['view']['status'] == 'awaiting_approval'
    assert isinstance(current['workflow']['audit'], list)
    body = {'expected_version': state['incident']['version'],
            'proposal_hash': state['proposal_hash']}
    decision = request(f'/api/proxy/workflows/{run_id}/decision',
                       {**body, 'decision': 'approve'}, f'verify-approval-{uuid4().hex}')
    assert decision['status'] == 'approved'
    current = request(f'/api/proxy/incidents/{identifier}')
    assert current['workflow']['view']['status'] == 'approved'
    before = traffic(identifier, 'baseline')
    assert before['projected_cases'] == 12
    visible_traffic = request(f'/api/proxy/demo/traffic?incident_id={identifier}')
    assert visible_traffic['baseline']['status'] == 'ready'
    assert visible_traffic['baseline']['expected_cases'] == before['expected_cases']
    action_key = f'verify-action-{uuid4().hex}'
    action = request(f'/api/proxy/workflows/{run_id}/action', body, action_key)
    assert action['status'] == 'succeeded' and action['result']['simulated'] is True
    assert request(f'/api/proxy/workflows/{run_id}/action', body, action_key) == action
    current = request(f'/api/proxy/incidents/{identifier}')
    assert current['workflow']['_id'] == run_id
    assert current['workflow']['action']['status'] == 'succeeded'
    first = request(f'/api/proxy/workflows/{run_id}/outcome')
    end = datetime.fromisoformat(first['window_end'].replace('Z', '+00:00'))
    delay = (end - datetime.now(timezone.utc)).total_seconds() + 2
    if delay > 0:
        time.sleep(delay)
    progress = traffic(identifier, 'recovery')
    assert progress['projected_cases'] == 3
    visible_traffic = request(f'/api/proxy/demo/traffic?incident_id={identifier}')
    assert visible_traffic['recovery']['status'] == 'ready'
    assert visible_traffic['recovery']['expected_cases'] == progress['expected_cases']
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        observation = request(f'/api/proxy/workflows/{run_id}/outcome')
        if observation['status'] == 'improving':
            break
        time.sleep(2)
    else:
        raise TimeoutError('Outcome did not show improving after recovery traffic')
    current = request(f'/api/proxy/incidents/{identifier}')
    closure = request(f'/api/proxy/incidents/{identifier}/resolve',
                      {'expected_version': current['incident']['version'],
                       'incident_hash': current['incident_hash']},
                      f'verify-close-{uuid4().hex}')
    assert closure['status'] == 'resolved'
    closed = request(f'/api/proxy/incidents/{identifier}')
    assert closed['incident']['status'] == 'resolved'
    assert any(item['kind'] == 'incident_resolved' for item in closed['lifecycle'])
    listed = request('/api/proxy/incidents?limit=100')['items']
    assert any(item['_id'] == identifier and item['status'] == 'resolved'
               for item in listed)
    return {'status': 'verified', 'project': project, 'run_id': run_id,
            'incident_code': incident['error_code'],
            'baseline_cases': before['projected_cases'],
            'recovery_cases': progress['projected_cases'],
            'outcome': observation['status'], 'closure': closure['status']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('project')
    args = parser.parse_args()
    try:
        result = verify(args.project)
    except NoUnusedIncident as exc:
        parser.exit(2, f'{exc}\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
