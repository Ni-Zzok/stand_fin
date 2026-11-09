from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, NaiveDatetime

BookingMode = Literal['centralized_sync', 'hybrid_async']


class RoomRead(BaseModel):
    id: int
    name: str
    status: str

    model_config = {'from_attributes': True}


class BookingCreate(BaseModel):
    room_id: int
    user_id: int
    start_time: NaiveDatetime
    end_time: NaiveDatetime
    mode: BookingMode
    scenario: str = Field(default='api_manual')


class BookingRead(BaseModel):
    id: int
    room_id: int
    user_id: int
    start_time: NaiveDatetime
    end_time: NaiveDatetime
    status: str
    mode: str
    created_at: datetime

    model_config = {'from_attributes': True}


class MetricsRead(BaseModel):
    id: int
    request_id: str
    scenario: str
    mode: str
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    result: str
    notes: str | None

    model_config = {'from_attributes': True}


class WorkerDelayRequest(BaseModel):
    delay_ms: int = Field(ge=0)
