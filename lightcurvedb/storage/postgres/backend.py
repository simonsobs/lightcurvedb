"""
PostgreSQL storage backend.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from psycopg import AsyncConnection

from lightcurvedb.config import Settings
from lightcurvedb.storage.postgres.instrument import PostgresInstrumentStorage
from lightcurvedb.storage.postgres.cutout import PostgresCutoutStorage
from lightcurvedb.storage.postgres.flux import PostgresFluxMeasurementStorage
from lightcurvedb.storage.postgres.source import PostgresSourceStorage
from lightcurvedb.storage.prototype.backend import Backend


async def generate_postgres_backend(conn: AsyncConnection) -> Backend:
    backend = Backend(
        sources=PostgresSourceStorage(conn),
        instruments=PostgresInstrumentStorage(conn),
        fluxes=PostgresFluxMeasurementStorage(conn),
        cutouts=PostgresCutoutStorage(conn),
    )

    await backend.setup()

    return backend


@asynccontextmanager
async def postgres_backend(settings: Settings) -> AsyncIterator[Backend]:
    """
    Get a PostgreSQL storage backend.
    """
    async with await AsyncConnection.connect(settings.database_url) as conn:
        backend = await generate_postgres_backend(conn)
        yield backend
