"""
Storage backend.
"""

from typing import AsyncIterator
from contextlib import asynccontextmanager
from psycopg import AsyncConnection

from lightcurvedb.protocols.storage import FluxStorageBackend
from lightcurvedb.storage.postgres.backend import PostgresBackend
from lightcurvedb.config import settings


@asynccontextmanager
async def get_storage() -> AsyncIterator[FluxStorageBackend]:
    """
    Get storage backend.
    """
    async with await AsyncConnection.connect(settings.database_url) as conn:
        if settings.backend_type == "postgres":
            yield PostgresBackend(conn)
        else:
            raise ValueError(f"Unknown backend type: {settings.backend_type}")


__all__ = ["get_storage"]