"""
Configuration for lightcurvedb.
"""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_user: str = "postgres"
    postgres_password: str = "password"
    postgres_port: int = 5432
    postgres_host: str = "127.0.0.1"
    postgres_db: str = "lightcurvedb"

    backend_type: Literal["postgres", "timescaledb", "numpy"] = "postgres"

    postgres_partition_count: int = 4

    model_config = SettingsConfigDict(env_prefix="LIGHTCURVEDB_")

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


settings = Settings()
