"""
Sets up a testcontainer with a few lightcurves and cut-outs in it for
testing purposes.
"""

from typing import AsyncGenerator

from pytest import fixture as sync_fixture
from pytest_asyncio import fixture as async_fixture
from testcontainers.postgres import PostgresContainer

from lightcurvedb.config import Settings
from lightcurvedb.storage.pandas.backend import pandas_backend
from lightcurvedb.storage.postgres.backend import postgres_backend
from lightcurvedb.storage.prototype.backend import Backend
from lightcurvedb.storage.timescale.backend import timescale_backend


@sync_fixture(scope="session")
def postgres_database():
    """
    Sets up a testcontainer PostgreSQL database for the session.
    """
    with PostgresContainer(
        image="postgres:18",
        username="postgres",
        password="password",
        dbname="test_lightcurvedb",
    ) as container:
        yield container


@sync_fixture(scope="session")
def timescale_database():
    """
    Sets up a testcontainer PostgreSQL database for the session.
    """
    with PostgresContainer(
        image="timescale/timescaledb:latest-pg18",
        username="postgres",
        password="password",
        dbname="test_lightcurvedb",
    ) as container:
        yield container


@async_fixture(scope="session", params=["pandas", "postgres", "timescale"])
async def backend(
    request, postgres_database, timescale_database, tmp_path_factory
) -> AsyncGenerator[Backend, None, None]:
    if request.param == "postgres":
        async with postgres_backend(
            settings=Settings(
                postgres_host=postgres_database.get_container_host_ip(),
                postgres_port=postgres_database.get_exposed_port(5432),
                postgres_user="postgres",
                postgres_password="password",
                postgres_db="test_lightcurvedb",
                backend_type="postgres",
            )
        ) as backend:
            yield backend
    elif request.param == "timescale":
        async with timescale_backend(
            settings=Settings(
                postgres_host=timescale_database.get_container_host_ip(),
                postgres_port=timescale_database.get_exposed_port(5432),
                postgres_user="postgres",
                postgres_password="password",
                postgres_db="test_lightcurvedb",
                backend_type="timescale",
            )
        ) as backend:
            yield backend
    elif request.param == "pandas":
        async with pandas_backend(
            settings=Settings(
                pandas_base_path=tmp_path_factory.mktemp("pandas_test_data"),
                backend_type="pandas",
            )
        ) as backend:
            yield backend


@async_fixture(scope="session", autouse=True)
async def setup_test_data(backend: Backend):
    import random
    from datetime import datetime, timedelta

    from lightcurvedb.models.instrument import Instrument
    from lightcurvedb.simulation import cutouts as sim_cutouts
    from lightcurvedb.simulation import fluxes as sim_fluxes
    from lightcurvedb.simulation import sources as sim_sources

    test_instruments = [
        Instrument(
            frequency=band_frequency,
            module="i1",
            telescope="lat",
            instrument="latr",
            details={
                "comissioning_date": "2023-05-14",
            },
        )
        for band_frequency in [27, 39, 93, 145, 225, 280]
    ]
    await backend.instruments.create_batch(test_instruments)

    source_ids = await sim_sources.create_fixed_sources(64, backend=backend)

    instruments = await backend.instruments.get_all()

    for source_id in source_ids:
        source = await backend.sources.get(source_id)
        usable_bands = random.sample(instruments, k=4)

        await sim_fluxes.generate_fluxes_fixed_source(
            source=source,
            instruments=usable_bands,
            backend=backend,
            start_time=datetime.now(),
            cadence=timedelta(days=1),
            number=random.randint(10, 16),
        )

    # Generate cutouts for only the first source.
    for frequency in [27, 39, 93, 145, 225, 280]:
        fluxes = await backend.lightcurves.get_frequency_lightcurve(
            source_id=source_ids[0], frequency=frequency, limit=1024
        )

        if len(fluxes) == 0:
            continue

        cutouts = [sim_cutouts.create_cutout(nside=32, flux=flux) for flux in fluxes]
        _ = await backend.cutouts.create_batch(cutouts)

    yield source_ids
