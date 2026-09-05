import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from vibe_trading.api import WebSettings, create_app
from vibe_trading.cli import main
from vibe_trading.demo import DemoProvider


def test_sse_frame_last_event_id(planned):
    bench, plan = planned
    app = create_app(bench, WebSettings('demo', start_worker=False))
    with TestClient(app, base_url='http://localhost') as client:
        login = client.post('/api/login', json={'password': 'demo'}, headers={'origin': 'http://localhost', 'x-vibe-client': 'web'})
        assert login.status_code == 200
        events = bench.store.events(plan.run_id)
        async def connected(): return False
        request = SimpleNamespace(headers={'last-event-id': str(events[3]['seq'])},
                    cookies={'vibe_session': client.cookies.get('vibe_session')}, is_disconnected=connected)
        endpoint = next(r.endpoint for r in app.routes if getattr(r, 'path', '') == '/api/events/{scope}/stream')
        async def inspect():
            stream = await endpoint(plan.run_id, request)
            frame = await anext(stream.body_iterator)
            await stream.body_iterator.aclose()
            return frame
        frame = asyncio.run(inspect())
        assert frame.startswith(f"id: {events[4]['seq']}\nevent: update\ndata: ")
        assert frame.endswith('\n\n')
        request.headers = {'last-event-id': 'invalid'}
        assert asyncio.run(endpoint(plan.run_id, request)).status_code == 422


def test_readonly_doctor_no_balances_credentials(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr('sys.argv', ['vibe-trading', 'doctor', '--mode', 'demo', '--db', str(tmp_path/'doctor.db')])
    main()
    report = json.loads(capsys.readouterr().out)
    assert report['writes_attempted'] == 0
    assert all(v == 'READ_AND_CONTRACT_OK' for v in report['checks'].values())
    assert 'available_cash' not in str(report)


def test_local_cannot_silently_fall_back(tmp_path, monkeypatch):
    monkeypatch.delenv('VIBE_PROVIDER', raising=False)
    monkeypatch.setattr('sys.argv', ['vibe-trading', 'serve', '--mode', 'local', '--db', str(tmp_path/'local.db')])
    with pytest.raises(SystemExit): main()


def test_doctor_rejects_wrong_contract(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(DemoProvider, 'news', lambda self: [{'title': 'not a news contract'}])
    monkeypatch.setattr('sys.argv', ['vibe-trading', 'doctor', '--db', str(tmp_path/'bad.db')])
    main()
    report = json.loads(capsys.readouterr().out)
    assert report['checks']['news'] == 'FAILED_INSPECT_LOCALLY'
    assert report['writes_attempted'] == 0
