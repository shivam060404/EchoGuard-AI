"""Live API + Next.js session/proxy gate; uses only Python's standard library."""
import argparse
from http.cookiejar import CookieJar
import json
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, HTTPCookieProcessor
from uuid import uuid4


def verify(api_url, web_url, credential_file):
    tokens = json.loads(Path(credential_file).read_text())['tokens']
    browser = build_opener(HTTPCookieProcessor(CookieJar()))

    def request(base, path, body=None, token=None, expected=200, origin=None, key=None):
        headers = {'Content-Type': 'application/json'}
        if token: headers['Authorization'] = 'Bearer ' + token
        if origin: headers['Origin'] = origin
        if key: headers['Idempotency-Key'] = key
        req = Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers=headers)
        try:
            response = browser.open(req, timeout=65)
        except HTTPError as exc:
            response = exc
        payload = response.read()
        if response.code != expected:
            raise AssertionError(f'{path}: expected {expected}, received {response.code}: {payload[:300]!r}')
        return json.loads(payload) if payload else None

    deadline = time.monotonic() + 120
    while True:
        try:
            request(api_url, '/me', token=tokens['operator'])
            break
        except (URLError, AssertionError):
            if time.monotonic() > deadline: raise
            time.sleep(2)
    request(api_url, '/me', expected=401)
    request(web_url, '/api/proxy/me', expected=401)
    request(web_url, '/api/session', {'token': tokens['reviewer']}, origin='https://foreign.invalid', expected=403)
    request(web_url, '/api/session', {'token': tokens['reviewer']}, origin=web_url)
    actor = request(web_url, '/api/proxy/me')
    assert actor['role'] == 'reviewer'
    items = request(web_url, '/api/proxy/incidents')['items']
    incident = next((item for item in items if item['error_code'] == 'PAY-502'
                     and item['status'] != 'resolved' and not item.get('action_authorized')), None)
    if not incident:
        raise AssertionError('No unused PAY-502 incident revision; run backend/worker/demo.sh on a fresh workspace')
    identifier = incident['_id']
    prefix = '/api/proxy'
    key = 'phase6-' + uuid4().hex
    state = request(web_url, prefix + f'/incidents/{identifier}/workflows',
                    {'expected_version': incident['version']}, origin=web_url, key=key)
    assert state['status'] == 'awaiting_approval', state.get('reason')
    path = f"/workflows/{state['run_id']}"
    body = {'expected_version': state['incident']['version'], 'proposal_hash': state['proposal_hash']}
    request(api_url, path + '/action', body, token=tokens['reviewer'], key=key, expected=409)
    request(api_url, path + '/decision', {**body, 'decision': 'approve'}, token=tokens['operator'], key=key, expected=403)
    edited = request(web_url, prefix + path + '/proposal',
                     {**body, 'action': 'post_internal_notification', 'step_indices': [0]}, origin=web_url, key=key + '-edit')
    request(api_url, path + '/decision', {**body, 'decision': 'approve'}, token=tokens['reviewer'], key=key, expected=409)
    body['proposal_hash'] = edited['proposal_hash']
    for _ in range(2):
        result = request(web_url, prefix + path + '/decision', {**body, 'decision': 'approve'}, origin=web_url, key=key)
        assert result['status'] == 'approved'
    first = request(web_url, prefix + path + '/action', body, origin=web_url, key=key)
    second = request(web_url, prefix + path + '/action', body, origin=web_url, key=key)
    assert first == second and first['result']['simulated'] is True
    trend = request(web_url, prefix + path + '/outcome')
    print(json.dumps({'status': 'verified', 'run_id': state['run_id'], 'session_proxy': 'passed',
                      'approval_edit': 'passed', 'duplicate_action': 'idempotent', 'outcome': trend['status']}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api', default='http://localhost:8000')
    parser.add_argument('--web', default='http://localhost:3000')
    parser.add_argument('--credentials', default=str(Path(__file__).resolve().parents[2] / 'data/local-auth.json'))
    args = parser.parse_args()
    verify(args.api.rstrip('/'), args.web.rstrip('/'), args.credentials)
