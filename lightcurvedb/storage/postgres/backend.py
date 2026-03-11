"""
PostgreSQL storage backend.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from psycopg_pool import AsyncConnectionPool

from lightcurvedb.config import Settings
from lightcurvedb.storage.postgres.analysis import PostgresAnalysisProvider
from lightcurvedb.storage.postgres.cutout import PostgresCutoutStorage
from lightcurvedb.storage.postgres.flux import PostgresFluxMeasurementStorage
from lightcurvedb.storage.postgres.instrument import PostgresInstrumentStorage
from lightcurvedb.storage.postgres.lightcurves import PostgresLightcurveProvider
from lightcurvedb.storage.postgres.source import PostgresSourceStorage
from lightcurvedb.storage.prototype.backend import Backend


async def generate_postgres_backend(pool: AsyncConnectionPool) -> Backend:
    fluxes = PostgresFluxMeasurementStorage(pool)
    lightcurves = PostgresLightcurveProvider(flux_storage=fluxes)
    analysis = PostgresAnalysisProvider(
        flux_storage=fluxes,
        lightcurve_provider=lightcurves,
    )

    backend = Backend(
        sources=PostgresSourceStorage(pool),
        instruments=PostgresInstrumentStorage(pool),
        fluxes=fluxes,
        cutouts=PostgresCutoutStorage(pool),
        lightcurves=lightcurves,
        analysis=analysis,
    )

    await backend.setup()

    return backend


@asynccontextmanager
async def postgres_backend(settings: Settings) -> AsyncIterator[Backend]:
    """
    Get a PostgreSQL storage backend.
    """
    async with AsyncConnectionPool(conninfo=settings.database_url) as conn:
        backend = await generate_postgres_backend(conn)
        yield backend
