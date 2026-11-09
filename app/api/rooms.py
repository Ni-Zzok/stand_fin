from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.repositories import Repository
from app.schemas import RoomRead

router = APIRouter(tags=['rooms'])


@router.get('/rooms', response_model=list[RoomRead])
def get_rooms(db: Session = Depends(get_db)) -> list[RoomRead]:
    repo = Repository(db)
    return [RoomRead.model_validate(item) for item in repo.list_rooms()]
