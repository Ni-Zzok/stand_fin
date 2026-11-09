from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from starlette.concurrency import run_in_threadpool

from app.db import SessionLocal
from app.queue_manager import queue_manager
from app.repositories import Repository
from app.secondary_actions import perform_action


def process_task(task) -> bool:
    try:
        with SessionLocal() as db:
            repo = Repository(db)
            perform_action(repo, task.task_type, task.booking_id, task.recipient,
                           task.secondary_delay_ms, 'async')
            now = datetime.now(UTC).replace(tzinfo=None)
            repo.create_metrics_event(
                task.request_id, task.scenario, task.mode, task.enqueued_at, now,
                int((now - task.enqueued_at).total_seconds() * 1000),
                'worker_task_success', f'processed:{task.task_type}; includes queue wait and delay')
            db.commit()
        return True
    except Exception:
        logging.exception('Background task failed: %s', task.task_type)
        try:
            with SessionLocal() as db:
                now = datetime.now(UTC).replace(tzinfo=None)
                Repository(db).create_metrics_event(
                    task.request_id, task.scenario, task.mode, task.enqueued_at, now,
                    int((now - task.enqueued_at).total_seconds() * 1000),
                    'worker_task_error', f'failed:{task.task_type}; see server log')
                db.commit()
        except Exception:
            logging.exception('Could not persist background failure metric')
        return False


async def run_worker() -> None:
    while True:
        task = await queue_manager.get()
        try:
            while not queue_manager.worker_enabled:
                await asyncio.sleep(0.05)
            await asyncio.sleep(queue_manager.delay_ms / 1000)
            success = await run_in_threadpool(process_task, task)
            if success:
                queue_manager.completed += 1
            else:
                queue_manager.failed += 1
        finally:
            queue_manager.task_done()
