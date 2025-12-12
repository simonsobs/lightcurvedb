"""
Sets up a testcontainer with a few lightcurves and cut-outs in it for
testing purposes.
"""

import os
from pytest import fixture as sync_fixture
from pytest_asyncio import fixture as async_fixture
from testcontainers.postgres import PostgresContainer


@sync_fixture(scope="session")
def test_database():
    """
    Sets up a testcontainer PostgreSQL database for the session.
    """
    with PostgresContainer(
        image="postgres:16-alpine",
        username="postgres",
        password="password",
        dbname="test_lightcurvedb",
    ).with_bind_ports(5432, 5432) as container:
        os.environ["LIGHTCURVEDB_POSTGRES_HOST"] = container.get_container_host_ip()
        os.environ["LIGHTCURVEDB_POSTGRES_PORT"] = str(container.get_exposed_port(5432))
        os.environ["LIGHTCURVEDB_POSTGRES_USER"] = "postgres"
        os.environ["LIGHTCURVEDB_POSTGRES_PASSWORD"] = "password"
        os.environ["LIGHTCURVEDB_POSTGRES_DB"] = "test_lightcurvedb"
        os.environ["LIGHTCURVEDB_BACKEND_TYPE"] = "postgres"

        yield container


@sync_fixture(scope="session")
def get_backend(test_database):
    from lightcurvedb.storage import get_storage
    return get_storage


@async_fixture(scope="session", autouse=True)
async def setup_test_data(test_database):
    import random
    from datetime import datetime, timedelta
    from lightcurvedb.storage import get_storage
    from lightcurvedb.models.band import Band
    from lightcurvedb.simulation import sources as sim_sources
    from lightcurvedb.simulation import fluxes as sim_fluxes

    async with get_storage() as backend:
        await backend.create_schema()

        test_bands = [
            Band(
                name=f"f{band_frequency:03d}",
                frequency=float(band_frequency),
                instrument="LATR",
                telescope="SOLAT",
            )
            for band_frequency in [27, 39, 93, 145, 225, 280]
        ]
        await backend.bands.create_batch(test_bands)

        source_ids = await sim_sources.create_fixed_sources(64, backend=backend)

        bands = await backend.bands.get_all()
        for source_id in source_ids:
            source = await backend.sources.get(source_id)
            usable_bands = random.sample(bands, k=4)

            await sim_fluxes.generate_fluxes_fixed_source(
                source=source,
                bands=usable_bands,
                backend=backend,
                start_time=datetime.now(),
                cadence=timedelta(days=1),
                number=random.randint(10, 16),
            )

        await backend.conn.commit()

    yield source_ids