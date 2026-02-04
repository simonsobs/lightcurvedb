"""
Storage backend.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from psycopg import AsyncConnection

from lightcurvedb.config import Settings
from lightcurvedb.protocols.storage import FluxStorageBackend
from lightcurvedb.storage.postgres.backend import PostgresBackend


@asynccontextmanager
async def get_storage(
    settings: Settings | None = None,
) -> AsyncIterator[FluxStorageBackend]:
    """
    Get storage backend.
    """
    if settings is None:
        settings = Settings()

    async with await AsyncConnection.connect(settings.database_url) as conn:
        if settings.backend_type == "postgres":
            yield PostgresBackend(conn)
        else:
            raise ValueError(f"Unknown backend type: {settings.backend_type}")


__all__ = ["get_storage"]
