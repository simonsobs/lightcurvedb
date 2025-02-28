"""
Sets up a testcontainer with a few lightcurves and cut-outs in it for
testing purposes.
"""

from pytest import fixture as sync_fixture
from pytest_asyncio import fixture as async_fixture
from testcontainers.postgres import PostgresContainer

from lightcurvedb.managers import AsyncSessionManager, SyncSessionManager


@sync_fixture(scope="session")
def base_server():
    """
    Sets up a server (completely empty).
    """

    with PostgresContainer() as container:
        conn_url = container.get_connection_url()

        yield conn_url


@sync_fixture(scope="session")
def sync_client(base_server):
    manager = SyncSessionManager(base_server.replace("psycopg2", "psycopg"))

    manager.create_all()

    yield manager

    manager.drop_all()


@sync_fixture(scope="session")
def source_ids(sync_client):
    import random
    from datetime import datetime, timedelta

    from sqlalchemy import select

    from lightcurvedb.models.band import BandTable
    from lightcurvedb.models.flux import FluxMeasurementTable
    from lightcurvedb.models.source import SourceTable
    from lightcurvedb.simulation import cutouts, fluxes, sources

    source_ids = sources.create_fixed_sources(8, manager=sync_client)

    with sync_client.session() as session:
        bands = [
            BandTable(
                name=f"f{band_frequency:03d}",
                frequency=band_frequency,
                instrument="LATR",
                telescope="SOLAT",
            )
            for band_frequency in [27, 39, 93, 145, 225, 280]
        ]

        session.add_all(bands)
        session.commit()

        sources = [session.get(SourceTable, source_id) for source_id in source_ids]
        bands = session.execute(select(BandTable)).scalars().all()

        for source in sources:
            # Not all sources should have all bands.
            usable_band_ids = set(random.choices(range(len(bands)), k=4))
            usable_bands = [bands[x] for x in usable_band_ids]

            fluxes.generate_fluxes_fixed_source(
                source=source,
                bands=usable_bands,
                start_time=datetime.now(),
                cadence=timedelta(days=1),
                number=random.randint(10, 16),
                session=session,
            )

            # Get the most recent 30 flux measurements.
            useful_fluxes = (
                session.query(FluxMeasurementTable)
                .filter(FluxMeasurementTable.source_id == source.id)
                .order_by(FluxMeasurementTable.time.desc())
                .limit(random.randint(3, 9))
                .all()
            )

            for flux in useful_fluxes:
                cutouts.create_cutout(
                    nside=64,
                    flux=flux,
                    session=session,
                )

        yield source_ids

        with sync_client.session() as session:
            for source_id in source_ids:
                session.delete(session.get(SourceTable, source_id))
            session.commit()


@async_fixture(loop_scope="session", scope="session")
async def client(base_server, source_ids):
    manager = AsyncSessionManager(base_server.replace("psycopg2", "asyncpg"))
    yield manager
