from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4
import logging

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.queue_manager import AsyncTask
from app.repositories import Repository
from app.secondary_actions import perform_action
from app.schemas import BookingCreate, BookingRead


class BookingService:
    def __init__(self, db: Session, secondary_delay_ms: int = 0):
        self.secondary_delay_ms = secondary_delay_ms
        self.db = db
        self.repo = Repository(db)

    def create_booking(self, payload: BookingCreate) -> dict:
        started_at = datetime.now(UTC).replace(tzinfo=None)
        request_id = str(uuid4())

        try:
            if payload.end_time <= payload.start_time:
                raise HTTPException(status_code=400, detail='end_time must be greater than start_time')

            room = self.repo.get_room(payload.room_id)
            if room is None:
                raise HTTPException(status_code=404, detail='Room not found')

            user = self.repo.get_user(payload.user_id)
            if user is None:
                raise HTTPException(status_code=404, detail='User not found')

            if self.repo.has_conflict(payload.room_id, payload.start_time, payload.end_time):
                finished_at = datetime.now(UTC).replace(tzinfo=None)
                duration_ms = int((finished_at - started_at).total_seconds() * 1000)
                self.repo.create_metrics_event(
                    request_id=request_id,
                    scenario=payload.scenario,
                    mode=payload.mode,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_ms=duration_ms,
                    result='conflict',
                    notes='overlap_detected',
                )
                self.db.commit()
                raise HTTPException(status_code=409, detail='Booking conflict for selected room and time interval')

            booking = self.repo.create_booking(
                room_id=payload.room_id,
                user_id=payload.user_id,
                start_time=payload.start_time,
                end_time=payload.end_time,
                mode=payload.mode,
            )

            if payload.mode == 'centralized_sync':
                for kind in ('audit', 'notification'):
                    perform_action(self.repo, kind, booking.id, user.name, self.secondary_delay_ms, 'sync')
            elif payload.mode == 'hybrid_async':
                pass  # Enqueue on the event loop, only after this transaction commits.
            else:
                raise HTTPException(status_code=400, detail='Unsupported mode')

            finished_at = datetime.now(UTC).replace(tzinfo=None)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)
            self.repo.create_metrics_event(
                request_id=request_id,
                scenario=payload.scenario,
                mode=payload.mode,
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
                result='success',
                notes=f'booking_id={booking.id}',
            )
            self.db.commit()
            self.db.refresh(booking)

            return {
                'request_id': request_id,
                'booking': BookingRead.model_validate(booking).model_dump(),
                'tasks': [AsyncTask(kind, booking.id, user.name, request_id, payload.scenario,
                                    payload.mode, datetime.now(UTC).replace(tzinfo=None),
                                    secondary_delay_ms=self.secondary_delay_ms)
                          for kind in ('audit', 'notification')]
                         if payload.mode == 'hybrid_async' else [],
            }
        except HTTPException:
            if self.db.in_transaction():
                self.db.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            if self.db.in_transaction():
                self.db.rollback()
            now = datetime.now(UTC).replace(tzinfo=None)
            try:
                self.repo.create_metrics_event(
                    request_id=request_id,
                    scenario=payload.scenario,
                    mode=payload.mode,
                    started_at=started_at,
                    finished_at=now,
                    duration_ms=int((now - started_at).total_seconds() * 1000),
                    result='error',
                    notes=str(exc),
                )
                self.db.commit()
            except Exception:
                self.db.rollback()
                logging.exception('Could not persist booking failure metric')
            raise HTTPException(status_code=500, detail='Internal booking error') from exc
