import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

url = os.environ.get('TEST_DATABASE_URL')
if not url:
    pytest.skip('Set TEST_DATABASE_URL to a disposable PostgreSQL database', allow_module_level=True)
os.environ['DATABASE_URL'] = url

from app.main import app
from app.db import SessionLocal
from app.models import AuditLog, Booking, Notification
from app.repositories import Repository
from app.services import BookingService
from app.schemas import BookingCreate


@pytest.fixture(scope='module')
def client():
    with TestClient(app) as c:
        yield c


def prepare(c):
    c.post('/experiments/worker/on').raise_for_status()
    c.post('/experiments/worker/delay', json={'delay_ms': 0}).raise_for_status()
    c.post('/experiments/reset').raise_for_status()
    c.post('/experiments/secondary/delay', json={'delay_ms': 0}).raise_for_status()
    r = c.post('/experiments/seed')
    r.raise_for_status()
    return r.json()


def payload(data, mode='hybrid_async', start=10, end=11):
    return dict(room_id=data['rooms'][0]['id'], user_id=data['users'][0]['id'],
                start_time=f'2026-04-20T{start:02}:00:00', end_time=f'2026-04-20T{end:02}:00:00', mode=mode)


def counts():
    with SessionLocal() as db:
        return tuple(db.scalar(select(func.count()).select_from(m)) for m in (Booking, AuditLog, Notification))


def test_repeated_reset_seed_and_modes(client):
    previous = 0
    for mode in ['centralized_sync', 'hybrid_async'] * 3:
        data = prepare(client)
        assert data['rooms'][0]['id'] > previous
        previous = data['rooms'][0]['id']
        assert client.post('/experiments/seed').status_code == 409
        assert len(client.get('/rooms').json()) == 3
        assert client.post('/bookings', json=payload(data, mode)).status_code == 200
        assert client.post('/bookings', json=payload(data, mode)).status_code == 409
        assert client.post('/bookings', json=payload(data, mode, 11, 12)).status_code == 200
        status = client.post('/experiments/drain').json()
        assert status['pending'] == status['failed'] == 0
        assert counts() == (2, 2, 2)


def test_concurrent_independent_transactions(client):
    data = prepare(client)
    # Call the service in separate threads/sessions, bypassing the HTTP gate.
    # PostgreSQL row locking must enforce the invariant itself.
    def book(_):
        from fastapi import HTTPException
        with SessionLocal() as db:
            try:
                BookingService(db).create_booking(BookingCreate(**payload(data, 'centralized_sync')))
                return 200
            except HTTPException as exc:
                return exc.status_code
    with ThreadPoolExecutor(max_workers=12) as pool:
        statuses = list(pool.map(book, range(20)))
    assert statuses.count(200) == 1
    assert statuses.count(409) == 19
    assert counts() == (1, 1, 1)


def test_reset_with_paused_and_delayed_worker(client):
    data = prepare(client)
    client.post('/experiments/worker/off').raise_for_status()
    assert client.post('/bookings', json=payload(data)).status_code == 200
    status = client.get('/experiments/worker').json()
    assert status['pending'] == 2
    assert client.post('/experiments/reset').status_code == 409
    assert counts() == (1, 0, 0)
    client.post('/experiments/worker/delay', json={'delay_ms': 150}).raise_for_status()
    client.post('/experiments/worker/on').raise_for_status()
    assert client.post('/experiments/reset?timeout=0.001').status_code == 409
    assert counts()[0] == 1
    client.post('/experiments/reset').raise_for_status()
    assert client.get('/experiments/worker').json()['pending'] == 0
    assert counts() == (0, 0, 0)
    data = client.post('/experiments/seed').json()
    assert client.post('/bookings', json=payload(data)).status_code == 200
    status = client.post('/experiments/drain').json()
    assert status['completed'] == 2 and status['failed'] == 0
    assert counts() == (1, 1, 1)


def test_validation_and_missing_entities(client):
    data = prepare(client)
    bad = payload(data, start=11, end=10)
    assert client.post('/bookings', json=bad).status_code == 400
    bad = payload(data); bad['room_id'] = -1
    assert client.post('/bookings', json=bad).status_code == 404
    bad = payload(data); bad['user_id'] = -1
    assert client.post('/bookings', json=bad).status_code == 404
    bad = payload(data); bad['start_time'] += 'Z'
    assert client.post('/bookings', json=bad).status_code == 422
    assert counts() == (0, 0, 0)


def test_failed_commit_never_enqueues(client, monkeypatch):
    data = prepare(client)
    def fail(*args, **kwargs):
        raise RuntimeError('injected metrics failure')
    with monkeypatch.context() as m:
        m.setattr(Repository, 'create_metrics_event', fail)
        assert client.post('/bookings', json=payload(data)).status_code == 500
    assert client.get('/experiments/worker').json()['pending'] == 0
    assert counts() == (0, 0, 0)


def test_worker_failure_is_reported_and_drain_finishes(client, monkeypatch):
    data = prepare(client)
    def fail(*args, **kwargs):
        raise RuntimeError('injected notification failure')
    with monkeypatch.context() as m:
        m.setattr(Repository, 'create_notification', fail)
        assert client.post('/bookings', json=payload(data)).status_code == 200
        status = client.post('/experiments/drain').json()
    assert status['pending'] == 0 and status['failed'] == 1 and status['completed'] == 1
    assert counts() == (1, 1, 0)
    assert any(e['result'] == 'worker_task_error' for e in client.get('/experiments/metrics').json())


def test_database_work_does_not_block_event_loop(client, monkeypatch):
    import threading
    data = prepare(client)
    entered, release = threading.Event(), threading.Event()
    original = BookingService.create_booking
    def slow(self, body):
        entered.set()
        assert release.wait(5)
        return original(self, body)
    with monkeypatch.context() as m, ThreadPoolExecutor(max_workers=2) as pool:
        m.setattr(BookingService, 'create_booking', slow)
        booking = pool.submit(client.post, '/bookings', json=payload(data, 'centralized_sync'))
        try:
            assert entered.wait(2)
            status = pool.submit(client.get, '/experiments/worker')
            assert status.result(timeout=1).status_code == 200
        finally:
            release.set()
        assert booking.result(timeout=5).status_code == 200


def test_load_script_stops_on_preparation_error():
    import asyncio
    import httpx
    from load.compare_modes import CompareClient
    calls = []
    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(500, json={'detail': 'injected reset error'})
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            with pytest.raises(httpx.HTTPStatusError):
                await CompareClient('http://test').reset_and_seed(http)
    asyncio.run(run())
    assert calls == ['/experiments/reset']


@pytest.mark.parametrize('delay_ms', [0, 10, 30])
def test_equal_secondary_cost_and_conflicts(client, monkeypatch, delay_ms):
    import app.secondary_actions as actions
    original_sleep = actions.time.sleep
    sleeps = []
    def measured_sleep(seconds):
        sleeps.append(seconds)
        original_sleep(seconds)
    # Capture only the helper's sleep, without patching the global time module.
    from types import SimpleNamespace
    with monkeypatch.context() as m:
        m.setattr(actions, 'time', SimpleNamespace(sleep=measured_sleep))
        for mode in ['centralized_sync', 'hybrid_async']:
            data = prepare(client)
            client.post('/experiments/secondary/delay', json={'delay_ms': delay_ms}).raise_for_status()
            sleeps.clear()
            assert client.post('/bookings', json=payload(data, mode)).status_code == 200
            client.post('/experiments/drain').raise_for_status()
            assert sleeps == ([delay_ms / 1000] * 2 if delay_ms else [])
            assert counts() == (1, 1, 1)
            assert client.post('/bookings', json=payload(data, mode)).status_code == 409
            client.post('/experiments/drain').raise_for_status()
            assert sleeps == ([delay_ms / 1000] * 2 if delay_ms else [])


def test_secondary_delay_change_waits_for_paused_tasks(client):
    data = prepare(client)
    client.post('/experiments/secondary/delay', json={'delay_ms': 10}).raise_for_status()
    client.post('/experiments/worker/off').raise_for_status()
    client.post('/bookings', json=payload(data)).raise_for_status()
    assert client.post('/experiments/secondary/delay', json={'delay_ms': 30}).status_code == 409
    assert client.get('/experiments/worker').json()['secondary_delay_ms'] == 10
    client.post('/experiments/worker/on').raise_for_status()
    client.post('/experiments/drain').raise_for_status()
    assert client.post('/experiments/secondary/delay', json={'delay_ms': -1}).status_code == 422
