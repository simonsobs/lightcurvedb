"""
Configuration options for the lightcurvedb
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_user: str = "postgres"
    postgres_password: str = "password"
    postgres_port: int = 5432
    postgres_host: str = "localhost"
    postgres_db: str = "lightcurvedb"

    postgres_echo: bool = False

    model_config = SettingsConfigDict(env_prefix="LIGHTCURVEDB_")

    @property
    def postgres_uri(self) -> str:
        return f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def async_postgres_uri(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


settings = Settings()