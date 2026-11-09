from __future__ import annotations

from starlette.concurrency import run_in_threadpool

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db, SessionLocal
from app.coordination import gate
from app.queue_manager import queue_manager
from app.repositories import Repository
from app.schemas import BookingCreate, BookingRead
from app.services import BookingService

router = APIRouter(tags=['bookings'])


@router.get('/bookings', response_model=list[BookingRead])
def get_bookings(db: Session = Depends(get_db)) -> list[BookingRead]:
    repo = Repository(db)
    return [BookingRead.model_validate(item) for item in repo.list_bookings()]


def _create(payload: BookingCreate, secondary_delay_ms: int) -> dict:
    with SessionLocal() as db:
        return BookingService(db, secondary_delay_ms).create_booking(payload)


@router.post('/bookings')
async def create_booking(payload: BookingCreate) -> dict:
    async with gate.booking():
        result = await run_in_threadpool(_create, payload, queue_manager.secondary_delay_ms)
        for task in result.pop('tasks'):
            queue_manager.put_nowait(task)
        return result
