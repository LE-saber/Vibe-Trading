import time

import pytest
from fastapi.testclient import TestClient

from vibe_trading.api import WebSettings, create_app
from vibe_trading.models import digest
from vibe_trading.progress import Reporter
from vibe_trading.store import ProcessLock, Store

HEADERS = {'origin': 'http://localhost', 'x-vibe-client': 'web'}


def login(client):
    response = client.post('/api/login', json={'password': 'demo'}, headers=HEADERS)
    assert response.status_code == 200, response.text
    return HEADERS | {'x-csrf-token': response.json()['csrf']}


def test_auth_csrf_no_arbitrary_trade_endpoint(bench):
    app = create_app(bench, WebSettings('demo', start_worker=False))
    with TestClient(app, base_url='http://localhost') as c:
        assert c.get('/').status_code == 200
        assert c.get('/api/dashboard').status_code == 401
        h = login(c)
        assert c.get('/api/dashboard').status_code == 200
        assert c.post('/api/runs', json={}, headers=HEADERS).status_code == 403
        assert c.post('/api/runs', json={}, headers=h | {'origin': 'http://evil.example'}).status_code == 403
        assert c.post('/api/buy', json={}, headers=h).status_code == 404
        assert 'HttpOnly' in c.post('/api/login', json={'password': 'demo'}, headers=HEADERS).headers['set-cookie']


def test_full_http_review_approve_execute(bench):
    app = create_app(bench, WebSettings('demo', start_worker=False))
    with TestClient(app, base_url='http://localhost') as c:
        h = login(c)
        r = c.post('/api/runs', json={}, headers=h).json()['run_id']
        for _ in range(100):
            run = c.get('/api/runs/'+r).json()
            if run['state'] == 'COMPLETED': break
            time.sleep(0.02)
        assert run['state'] == 'COMPLETED', run
        doc = c.get('/api/plans/'+run['plan_id']).json()
        url = '/api/plans/'+run['plan_id']+'/approve'
        assert c.post(url, json={'plan_hash': doc['hash'], 'acknowledge': False}, headers=h).status_code == 409
        assert c.post(url, json={'plan_hash': doc['hash'], 'acknowledge': 'true'}, headers=h).status_code == 422
        a = c.post(url, json={'plan_hash': doc['hash'], 'acknowledge': True}, headers=h).json()
        b = c.post('/api/approvals/'+a['approval_id']+'/execute', json={}, headers=h).json()['batch_id']
        for _ in range(15): bench.execution.tick(b)
        assert c.get('/api/batches/'+b).json()['state'] == 'COMPLETED'
        assert c.get('/api/runs/'+r+'/evidence').json()['review_context']['news']
        events = c.get('/api/events/'+r).json()
        assert len(events) > 20
        cursor = events[len(events)//2]['seq']
        assert all(e['seq'] > cursor for e in c.get('/api/events/'+r+'?after='+str(cursor)).json())
        assert c.post('/api/logout', json={}, headers=h).status_code == 200
        assert c.get('/api/dashboard').status_code == 401


def test_login_rate_limit_and_no_password_echo(bench):
    app = create_app(bench, WebSettings('demo', start_worker=False))
    with TestClient(app, base_url='http://localhost') as c:
        secret = 'DO_NOT_ECHO_PASSWORD'
        response = c.post('/api/login', json={'password': secret, 'extra': secret}, headers=HEADERS)
        assert response.status_code == 422 and secret not in response.text
        for _ in range(8): assert c.post('/api/login', json={'password': secret}, headers=HEADERS).status_code == 401
        assert c.post('/api/login', json={'password': secret}, headers=HEADERS).status_code == 429


def test_bad_origin_and_host_rejected(bench):
    with TestClient(create_app(bench, WebSettings('demo', start_worker=False)), base_url='http://localhost') as c:
        assert c.post('/api/login', json={'password': 'demo'}).status_code == 403
        assert c.get('/', headers={'host': 'attacker.example'}).status_code == 400


def test_real_counts_monotonic_progress_replay(planned):
    bench, plan = planned
    events = bench.store.events(plan.run_id)
    percentages = [e['body']['percent'] for e in events if e['kind'] == 'progress']
    assert percentages == sorted(percentages)
    assert max(percentages) == 99
    assert bench.store.get('run', plan.run_id)['percent'] == 100
    assert len({e['seq'] for e in events}) == len(events)


def test_progress_rejects_regression_and_skips(bench):
    bench.store.put('run', 'r', {'id': 'r', 'state': 'RUNNING', 'created_at': '2026-09-05T00:00:00+00:00'})
    report = Reporter(bench.store, 'r'); report('PREPARE', 0, 2, 'start'); report('PREPARE', 1, 2, 'item')
    with pytest.raises(ValueError): report('PREPARE', 0, 2, 'regression')
    with pytest.raises(ValueError): report('S1', 0, 1, 'skip')
    with pytest.raises(ValueError): report('PREPARE', 1, 3, 'changed denominator')
    with pytest.raises(ValueError): report('S0', 0, 1, 'premature next')


def test_cancelled_research_never_creates_orders(bench):
    bench.provider.delay = 0.005
    r = bench.create_run(); bench.cancel_run(r)
    for _ in range(100):
        run = bench.store.get('run', r)
        if run['state'] == 'CANCELLED': break
        time.sleep(0.01)
    assert run['state'] == 'CANCELLED'
    assert not bench.store.list('demo_order')


def test_bad_news_context_fails_research(bench, monkeypatch):
    original = bench.provider.research
    def bad(*args): return original(*args).model_copy(update={'news_context_hash': '0'*64})
    monkeypatch.setattr(bench.provider, 'research', bad)
    r = bench.create_run(asynchronous=False)
    assert bench.store.get('run', r)['state'] == 'FAILED'
    assert 'NEWS_CONTEXT_MISMATCH' in bench.store.get('run', r)['message']


def test_news_failure_not_empty_news_and_secrets_redacted(bench, monkeypatch):
    def fail(): raise ValueError('FAKE_SECRET_HEADER')
    monkeypatch.setattr(bench.provider, 'news', fail)
    dashboard = bench.dashboard()
    assert dashboard['news']['available'] is False
    assert 'FAKE_SECRET_HEADER' not in str(dashboard)
    r = bench.create_run(asynchronous=False)
    assert bench.store.get('run', r)['state'] == 'FAILED'
    assert 'FAKE_SECRET_HEADER' not in str(bench.store.get('run', r))


def test_process_lock_excludes_second_worker(tmp_path):
    a, b = ProcessLock(tmp_path/'process.lock'), ProcessLock(tmp_path/'process.lock')
    a.acquire()
    try:
        with pytest.raises(RuntimeError): b.acquire()
    finally: a.release()
    b.acquire(); b.release()


def test_provider_database_isolation(bench):
    with pytest.raises(RuntimeError): bench.store.bind_provider('ninef:NINEF_SIMULATION')


def test_restart_marks_research_interrupted(bench):
    bench.store.put('run', 'interrupted', {'id': 'interrupted', 'state': 'RUNNING'})
    bench.store.recover()
    assert bench.store.get('run', 'interrupted')['state'] == 'INTERRUPTED'


def test_future_database_version_not_silently_migrated(tmp_path):
    path = tmp_path/'future.db'; store = Store(path)
    with store.transaction() as conn: conn.execute('PRAGMA user_version=99')
    with pytest.raises(RuntimeError): Store(path)


def test_unhandled_provider_exception_not_re_raised_to_server_logs(planned, monkeypatch, caplog):
    bench, plan = planned
    app = create_app(bench, WebSettings('demo', start_worker=False))
    with TestClient(app, base_url='http://localhost') as client:
        headers = login(client)
        approval = client.post('/api/plans/'+plan.plan_id+'/approve',
                    json={'plan_hash': bench.store.get('plan', plan.plan_id)['hash'], 'acknowledge': True},
                    headers=headers).json()
        def fail(_): raise RuntimeError('FAKE_AUTHORIZATION_SECRET')
        monkeypatch.setattr(bench.provider, 'context', fail)
        result = client.post('/api/approvals/'+approval['approval_id']+'/execute', json={}, headers=headers)
        assert result.status_code == 503
        assert 'FAKE_AUTHORIZATION_SECRET' not in result.text
        assert 'FAKE_AUTHORIZATION_SECRET' not in caplog.text


def test_unknown_resolution_api_requires_consent_and_exact_read(planned, monkeypatch):
    bench, plan = planned
    original = bench.provider.submit
    accepted = []
    def ambiguous(order, key):
        receipt = original(order, key); accepted.append(receipt)
        raise TimeoutError('lost response')
    monkeypatch.setattr(bench.provider, 'submit', ambiguous)
    app = create_app(bench, WebSettings('demo', start_worker=False))
    with TestClient(app, base_url='http://localhost') as client:
        headers = login(client)
        approval = client.post('/api/plans/'+plan.plan_id+'/approve',
            json={'plan_hash': digest(plan), 'acknowledge': True}, headers=headers).json()
        batch = client.post('/api/approvals/'+approval['approval_id']+'/execute',
                            json={}, headers=headers).json()['batch_id']
        bench.execution.tick(batch)
        intent = bench.execution.snapshot(batch)['intents'][0]
        path = f"/api/batches/{batch}/intents/{intent['id']}/resolve"
        denied = client.post(path, json={'broker_order_id': accepted[0].order_id,
            'evidence_ref': 'local exact read', 'acknowledge': False}, headers=headers)
        assert denied.status_code == 409
        resolved = client.post(path, json={'broker_order_id': accepted[0].order_id,
            'evidence_ref': 'local exact read', 'acknowledge': True}, headers=headers)
        assert resolved.status_code == 200
        assert resolved.json()['broker_order_id'] == accepted[0].order_id
