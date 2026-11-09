from __future__ import annotations

import asyncio
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from app.coordination import gate
from app.db import get_db, SessionLocal
from app.queue_manager import queue_manager
from app.repositories import Repository
from app.schemas import MetricsRead, WorkerDelayRequest

router = APIRouter(prefix='/experiments', tags=['experiments'])


def worker_status() -> dict:
    return {'worker_enabled': queue_manager.worker_enabled, 'delay_ms': queue_manager.delay_ms,
            'secondary_delay_ms': queue_manager.secondary_delay_ms,
            'pending': queue_manager.pending, 'completed': queue_manager.completed,
            'failed': queue_manager.failed}


async def wait_for_worker(timeout: float) -> None:
    if queue_manager.pending and not queue_manager.worker_enabled:
        raise HTTPException(409, 'Worker is disabled with pending tasks; enable it and drain first')
    try:
        await asyncio.wait_for(queue_manager.queue.join(), timeout)
    except asyncio.TimeoutError:
        raise HTTPException(409, 'Worker drain timed out; data has not been reset')


def _reset() -> None:
    with SessionLocal() as db:
        Repository(db).clear_all()
        db.commit()


@router.post('/reset')
async def reset_data(timeout: float = Query(30, gt=0, le=300)) -> dict:
    async with gate.exclusive():
        await wait_for_worker(timeout)
        await run_in_threadpool(_reset)
        queue_manager.completed = queue_manager.failed = 0
        return {'status': 'ok'}


def _seed() -> dict:
    with SessionLocal() as db:
        try:
            data = Repository(db).seed_defaults()
            db.commit()
            return {'status': 'ok', **data}
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc


@router.post('/seed')
async def seed_data() -> dict:
    async with gate.exclusive():
        return await run_in_threadpool(_seed)


@router.post('/drain')
async def drain(timeout: float = Query(30, gt=0, le=300)) -> dict:
    async with gate.exclusive():
        await wait_for_worker(timeout)
        return worker_status()


@router.get('/metrics', response_model=list[MetricsRead])
def get_metrics(db: Session = Depends(get_db)) -> list[MetricsRead]:
    return [MetricsRead.model_validate(item) for item in Repository(db).list_metrics()]


@router.get('/worker')
async def get_worker() -> dict:
    return worker_status()


@router.post('/worker/on')
async def worker_on() -> dict:
    queue_manager.worker_enabled = True
    return worker_status()


@router.post('/worker/off')
async def worker_off() -> dict:
    queue_manager.worker_enabled = False
    return worker_status()


@router.post('/worker/delay')
async def worker_delay(payload: WorkerDelayRequest) -> dict:
    queue_manager.delay_ms = payload.delay_ms
    return worker_status()


@router.post('/secondary/delay')
async def secondary_delay(payload: WorkerDelayRequest) -> dict:
    async with gate.exclusive():
        await wait_for_worker(30)
        queue_manager.secondary_delay_ms = payload.delay_ms
        return worker_status()
