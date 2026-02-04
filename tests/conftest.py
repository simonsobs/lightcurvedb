"""
Sets up a testcontainer with a few lightcurves and cut-outs in it for
testing purposes.
"""

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
    ) as container:
        yield container


@sync_fixture(scope="session")
def get_backend(test_database):
    from lightcurvedb.config import Settings
    from lightcurvedb.storage import get_storage

    def factory():
        return get_storage(
            Settings(
                postgres_host=test_database.get_container_host_ip(),
                postgres_port=test_database.get_exposed_port(5432),
                postgres_user="postgres",
                postgres_password="password",
                postgres_db="test_lightcurvedb",
                backend_type="postgres",
                postgres_partition_count=4,
            )
        )

    yield factory


@async_fixture(scope="session", autouse=True)
async def setup_test_data(test_database):
    import random
    from datetime import datetime, timedelta

    from lightcurvedb.config import Settings
    from lightcurvedb.models.band import Band
    from lightcurvedb.simulation import fluxes as sim_fluxes
    from lightcurvedb.simulation import sources as sim_sources
    from lightcurvedb.storage import get_storage

    settings = Settings(
        postgres_host=test_database.get_container_host_ip(),
        postgres_port=test_database.get_exposed_port(5432),
        postgres_user="postgres",
        postgres_password="password",
        postgres_db="test_lightcurvedb",
        backend_type="postgres",
        postgres_partition_count=4,
    )

    async with get_storage(settings) as backend:
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
