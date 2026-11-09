"""Single-process experiment barrier; booking requests may run concurrently."""
import asyncio
from contextlib import asynccontextmanager


class ExperimentGate:
    def __init__(self):
        self.condition = asyncio.Condition()
        self.active = 0
        self.maintenance = False

    @asynccontextmanager
    async def booking(self):
        async with self.condition:
            await self.condition.wait_for(lambda: not self.maintenance)
            self.active += 1
        try:
            yield
        finally:
            async with self.condition:
                self.active -= 1
                self.condition.notify_all()

    @asynccontextmanager
    async def exclusive(self):
        async with self.condition:
            await self.condition.wait_for(lambda: not self.maintenance)
            self.maintenance = True
        try:
            async with self.condition:
                await self.condition.wait_for(lambda: self.active == 0)
            yield
        finally:
            async with self.condition:
                self.maintenance = False
                self.condition.notify_all()


gate = ExperimentGate()
