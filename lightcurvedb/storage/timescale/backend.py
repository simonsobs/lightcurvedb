"""
PostgreSQL storage backend.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from psycopg import AsyncConnection

from lightcurvedb.config import Settings
from lightcurvedb.storage.postgres.analysis import PostgresAnalysisProvider
from lightcurvedb.storage.postgres.instrument import PostgresInstrumentStorage
from lightcurvedb.storage.postgres.source import PostgresSourceStorage
from lightcurvedb.storage.prototype.backend import Backend
from lightcurvedb.storage.timescale.cutout import TimescaleCutoutStorage
from lightcurvedb.storage.timescale.flux import TimescaleFluxMeasurementStorage
from lightcurvedb.storage.timescale.lightcurves import TimescaleLightcurveProvider


async def generate_timescale_backend(conn: AsyncConnection) -> Backend:
    fluxes = TimescaleFluxMeasurementStorage(conn)
    lightcurves = TimescaleLightcurveProvider(flux_storage=fluxes)
    analysis = PostgresAnalysisProvider(
        flux_storage=fluxes,
        lightcurve_provider=lightcurves,
    )

    backend = Backend(
        sources=PostgresSourceStorage(conn),
        instruments=PostgresInstrumentStorage(conn),
        fluxes=fluxes,
        cutouts=TimescaleCutoutStorage(conn),
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
    async with await AsyncConnection.connect(settings.database_url) as conn:
        backend = await generate_timescale_backend(conn)
        yield backend
