from __future__ import annotations

import asyncio

from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool
from contextlib import asynccontextmanager
import logging

from app.api.bookings import router as bookings_router
from app.api.experiments import router as experiments_router
from app.api.rooms import router as rooms_router
from app.config import settings
from app.db import Base, engine
from app.queue_manager import queue_manager
from app.worker import run_worker

@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(Base.metadata.create_all, bind=engine)
    queue_manager.delay_ms = settings.worker_delay_ms
    queue_manager.secondary_delay_ms = settings.secondary_delay_ms
    worker_task = asyncio.create_task(run_worker())
    try:
        yield
    finally:
        queue_manager.worker_enabled = True
        try:
            await asyncio.wait_for(queue_manager.queue.join(), 30)
        except asyncio.TimeoutError:
            logging.error('Shutdown timed out with %s pending tasks', queue_manager.pending)
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(rooms_router)
app.include_router(bookings_router)
app.include_router(experiments_router)


@app.get('/')
def health() -> dict:
    return {'status': 'ok', 'service': settings.app_name}
