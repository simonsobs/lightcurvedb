"""
Configuration for lightcurvedb.
"""

from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


def get_backend(settings):
    """
    Context manager to get a backend instance based on settings.
    """
    backend_type = settings.backend_type

    if backend_type == "postgres":
        from lightcurvedb.storage.postgres.backend import postgres_backend

        return postgres_backend(settings)
    elif backend_type == "timescale":
        from lightcurvedb.storage.timescale.backend import timescale_backend

        return timescale_backend(settings)
    elif backend_type == "parquet":
        from lightcurvedb.storage.parquet.backend import pandas_backend

        return pandas_backend(settings)


class Settings(BaseSettings):
    postgres_user: str = "postgres"
    postgres_password: str = "password"
    postgres_port: int = 5432
    postgres_host: str = "127.0.0.1"
    postgres_db: str = "lightcurvedb"

    parquet_base_path: Path = "./data"

    backend_type: Literal["postgres", "timescale", "parquet"] = "postgres"

    model_config = SettingsConfigDict(env_prefix="LIGHTCURVEDB_")

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def backend(self):
        return get_backend(self)


settings = Settings()
