"""
PostgreSQL storage backend.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from opentelemetry import metrics, trace
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
    tracer = trace.get_tracer("lightcurvedb-timescale-backend")
    meter = metrics.get_meter("lightcurvedb-timescale-backend")

    fluxes = TimescaleFluxMeasurementStorage(pool, tracer=tracer, meter=meter)
    lightcurves = TimescaleLightcurveProvider(
        flux_storage=fluxes, tracer=tracer, meter=meter
    )
    analysis = PostgresAnalysisProvider(
        flux_storage=fluxes,
        lightcurve_provider=lightcurves,
        tracer=tracer,
        meter=meter,
    )

    backend = Backend(
        sources=PostgresSourceStorage(pool, tracer=tracer, meter=meter),
        instruments=PostgresInstrumentStorage(pool, tracer=tracer, meter=meter),
        fluxes=fluxes,
        cutouts=TimescaleCutoutStorage(pool, tracer=tracer, meter=meter),
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
    try:
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

        PsycopgInstrumentor().instrument()
    except ImportError:
        pass

    async with AsyncConnectionPool(conninfo=settings.database_url) as pool:
        backend = await generate_timescale_backend(pool)

        yield backend
