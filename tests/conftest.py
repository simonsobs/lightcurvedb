"""
Sets up a testcontainer with a few lightcurves and cut-outs in it for
testing purposes.
"""

from pytest import fixture as sync_fixture
from pytest_asyncio import fixture as async_fixture
from testcontainers.postgres import PostgresContainer

from lightcurvedb.config import Settings
from lightcurvedb.storage.postgres.backend import postgres_backend
from lightcurvedb.storage.prototype.backend import Backend


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


@async_fixture(scope="session")
async def backend(test_database):
    async with postgres_backend(
        settings=Settings(
            postgres_host=test_database.get_container_host_ip(),
            postgres_port=test_database.get_exposed_port(5432),
            postgres_user="postgres",
            postgres_password="password",
            postgres_db="test_lightcurvedb",
            backend_type="postgres",
            postgres_partition_count=4,
        )
    ) as backend_instance:
        yield backend_instance


@async_fixture(scope="session", autouse=True)
async def setup_test_data(backend: Backend):
    import random
    from datetime import datetime, timedelta

    from lightcurvedb.models.band import Band
    from lightcurvedb.simulation import cutouts as sim_cutouts
    from lightcurvedb.simulation import fluxes as sim_fluxes
    from lightcurvedb.simulation import sources as sim_sources

    test_bands = [
        Band(
            band_name=f"f{band_frequency:03d}",
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

    # Generate cutouts for only the first source.
    for band_frequency in [27, 39, 93, 145, 225, 280]:
        band_name = f"f{band_frequency:03d}"

        fluxes = await backend.fluxes.get_recent_measurements(
            source_id=source_ids[0], limit=1024, band_name=band_name
        )

        if len(fluxes) == 0:
            continue

        cutouts = [sim_cutouts.create_cutout(nside=32, flux=flux) for flux in fluxes]
        _ = await backend.cutouts.create_batch(cutouts)

    yield source_ids
