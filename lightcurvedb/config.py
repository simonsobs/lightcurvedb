"""
Configuration options for the lightcurvedb when running in a fixed
environment.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict

from .managers import AsyncSessionManager, SyncSessionManager


class Settings(BaseSettings):
    postgres_user: str = "postgres"
    postgres_password: str = "password"
    postgres_port: int = 5432
    postgres_host: str = "127.0.0.1"
    postgres_db: str = "lightcurvedb"

    postgres_echo: bool = False

    model_config = SettingsConfigDict(env_prefix="LIGHTCURVEDB_")

    @property
    def postgres_uri(self) -> str:
        return f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    def sync_manager(self) -> SyncSessionManager:
        return SyncSessionManager(
            connection_url=self.postgres_uri, echo=self.postgres_echo
        )

    @property
    def async_postgres_uri(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    def async_manager(self) -> SyncSessionManager:
        return AsyncSessionManager(
            connection_url=self.postgres_uri, echo=self.postgres_echo
        )


settings = Settings()
