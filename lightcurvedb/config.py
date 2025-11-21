"""
Configuration options for the lightcurvedb when running in a fixed
environment.
"""
from enum import Enum

from pydantic_settings import BaseSettings, SettingsConfigDict

from .managers import AsyncSessionManager, SyncSessionManager

class ConnectionType(str, Enum):
    SQLALCHEMY = "sqlalchemy"
    PSYCOPG = "psycopg"

class Settings(BaseSettings):
    postgres_user: str = "postgres"
    postgres_password: str = "password"
    postgres_port: int = 5432
    postgres_host: str = "127.0.0.1"
    postgres_db: str = "lightcurvedb"

    postgres_echo: bool = False
    connection_type: ConnectionType = ConnectionType.PSYCOPG

    model_config = SettingsConfigDict(env_prefix="LIGHTCURVEDB_")

    @property
    def postgres_uri(self) -> str:
        return f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def psycopg_uri(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    def sync_manager(self) -> SyncSessionManager:
        return SyncSessionManager(
            connection_url=self.postgres_uri, echo=self.postgres_echo
        )

    @property
    def async_postgres_uri(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    def async_manager(self) -> AsyncSessionManager:
        return AsyncSessionManager(
            connection_url=self.async_postgres_uri, echo=self.postgres_echo
        )


settings = Settings()
