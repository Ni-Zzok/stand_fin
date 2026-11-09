from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal


TaskType = Literal['audit', 'notification']


@dataclass(slots=True)
class AsyncTask:
    task_type: TaskType
    booking_id: int
    recipient: str | None
    request_id: str
    scenario: str
    mode: str
    enqueued_at: datetime
    secondary_delay_ms: int = 0


class QueueManager:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[AsyncTask] = asyncio.Queue()
        self.worker_enabled: bool = True
        self.delay_ms: int = 0
        self.secondary_delay_ms: int = 0
        self.pending: int = 0
        self.completed: int = 0
        self.failed: int = 0

    async def put(self, task: AsyncTask) -> None:
        self.put_nowait(task)

    def put_nowait(self, task: AsyncTask) -> None:
        task.enqueued_at = datetime.now(UTC).replace(tzinfo=None)
        self.queue.put_nowait(task)
        self.pending += 1

    async def get(self) -> AsyncTask:
        return await self.queue.get()

    def task_done(self) -> None:
        self.pending -= 1
        self.queue.task_done()


queue_manager = QueueManager()
