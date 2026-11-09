from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = 'Booking Processing Lab'
    database_url: str = Field(
        default='postgresql+psycopg://postgres:postgres@localhost:5432/stand_db',
        alias='DATABASE_URL',
    )
    secondary_delay_ms: int = Field(default=0, ge=0, alias='SECONDARY_DELAY_MS')
    worker_delay_ms: int = Field(default=0, ge=0, alias='WORKER_DELAY_MS')


settings = Settings()
