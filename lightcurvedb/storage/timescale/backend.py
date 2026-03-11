"""
PostgreSQL storage backend.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from psycopg_pool import AsyncConnectionPool

from lightcurvedb.config import Settings
from lightcurvedb.storage.postgres.analysis import PostgresAnalysisProvider
from lightcurvedb.storage.postgres.instrument import PostgresInstrumentStorage
from lightcurvedb.storage.postgres.source import PostgresSourceStorage
from lightcurvedb.storage.prototype.backend import Backend
from lightcurvedb.storage.timescale.cutout import TimescaleCutoutStorage
from lightcurvedb.storage.timescale.flux import TimescaleFluxMeasurementStorage
from lightcurvedb.storage.timescale.lightcurves import TimescaleLightcurveProvider


async def generate_timescale_backend(pool: AsyncConnectionPool) -> Backend:
    fluxes = TimescaleFluxMeasurementStorage(pool)
    lightcurves = TimescaleLightcurveProvider(flux_storage=fluxes)
    analysis = PostgresAnalysisProvider(
        flux_storage=fluxes,
        lightcurve_provider=lightcurves,
    )

    backend = Backend(
        sources=PostgresSourceStorage(pool),
        instruments=PostgresInstrumentStorage(pool),
        fluxes=fluxes,
        cutouts=TimescaleCutoutStorage(pool),
        lightcurves=lightcurves,
        analysis=analysis,
    )

    await backend.setup()

    return backend


@asynccontextmanager
async def timescale_backend(settings: Settings) -> AsyncIterator[Backend]:
    """
    Get a TimescaleDB storage backend.
    """
    async with AsyncConnectionPool(conninfo=settings.database_url) as pool:
        backend = await generate_timescale_backend(pool)
        yield backend
