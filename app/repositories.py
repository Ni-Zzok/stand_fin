from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app import models


class Repository:
    def __init__(self, db: Session):
        self.db = db

    def list_rooms(self) -> list[models.Room]:
        return list(self.db.scalars(select(models.Room).order_by(models.Room.id)).all())

    def list_bookings(self) -> list[models.Booking]:
        return list(self.db.scalars(select(models.Booking).order_by(models.Booking.id)).all())

    def list_metrics(self) -> list[models.MetricsEvent]:
        return list(self.db.scalars(select(models.MetricsEvent).order_by(models.MetricsEvent.id)).all())

    def get_room(self, room_id: int) -> models.Room | None:
        return self.db.scalar(select(models.Room).where(models.Room.id == room_id).with_for_update())

    def get_user(self, user_id: int) -> models.User | None:
        return self.db.get(models.User, user_id)

    def has_conflict(self, room_id: int, start_time: datetime, end_time: datetime) -> bool:
        stmt = (
            select(models.Booking.id)
            .where(models.Booking.room_id == room_id)
            .where(models.Booking.start_time < end_time)
            .where(models.Booking.end_time > start_time)
            .limit(1)
        )
        return self.db.execute(stmt).first() is not None

    def create_booking(
        self,
        room_id: int,
        user_id: int,
        start_time: datetime,
        end_time: datetime,
        mode: str,
    ) -> models.Booking:
        booking = models.Booking(
            room_id=room_id,
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            status='confirmed',
            mode=mode,
        )
        self.db.add(booking)
        self.db.flush()
        return booking

    def create_audit_log(self, booking_id: int, event_type: str) -> models.AuditLog:
        audit = models.AuditLog(booking_id=booking_id, event_type=event_type)
        self.db.add(audit)
        self.db.flush()
        return audit

    def create_notification(self, booking_id: int, recipient: str, status: str = 'created') -> models.Notification:
        notification = models.Notification(booking_id=booking_id, recipient=recipient, status=status)
        self.db.add(notification)
        self.db.flush()
        return notification

    def create_metrics_event(
        self,
        request_id: str,
        scenario: str,
        mode: str,
        started_at: datetime,
        finished_at: datetime,
        duration_ms: int,
        result: str,
        notes: str | None,
    ) -> models.MetricsEvent:
        event = models.MetricsEvent(
            request_id=request_id,
            scenario=scenario,
            mode=mode,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            result=result,
            notes=notes,
        )
        self.db.add(event)
        self.db.flush()
        return event

    def clear_all(self) -> None:
        for table in (
            models.Notification,
            models.AuditLog,
            models.Booking,
            models.MetricsEvent,
            models.Room,
            models.User,
        ):
            self.db.execute(delete(table))

    def seed_defaults(self) -> dict:
        rooms = [
            models.Room(name='Blue Room', status='available'),
            models.Room(name='Green Room', status='available'),
            models.Room(name='Studio Hall', status='available'),
        ]
        users = [
            models.User(name='Alice'),
            models.User(name='Bob'),
            models.User(name='Charlie'),
        ]
        if self.db.scalar(select(models.Room.id).limit(1)) is not None or self.db.scalar(select(models.User.id).limit(1)) is not None:
            raise ValueError('Reset before seeding: rooms or users already exist')
        self.db.add_all(rooms + users)
        self.db.flush()
        return {'rooms': [{'id': r.id, 'name': r.name} for r in rooms],
                'users': [{'id': u.id, 'name': u.name} for u in users]}
