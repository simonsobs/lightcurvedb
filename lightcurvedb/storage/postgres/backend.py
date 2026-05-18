"""
PostgreSQL storage backend.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from opentelemetry import metrics, trace
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
    tracer = trace.get_tracer("lightcurvedb-postgres-backend")
    meter = metrics.get_meter("lightcurvedb-postgres-backend")

    fluxes = PostgresFluxMeasurementStorage(pool, tracer=tracer, meter=meter)
    lightcurves = PostgresLightcurveProvider(
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
        cutouts=PostgresCutoutStorage(pool, tracer=tracer, meter=meter),
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
    try:
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

        PsycopgInstrumentor().instrument()
    except ImportError:
        pass

    async with AsyncConnectionPool(conninfo=settings.database_url) as conn:
        backend = await generate_postgres_backend(conn)
        yield backend
