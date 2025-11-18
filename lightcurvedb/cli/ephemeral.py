"""
For generating an ephemeral light curve server using testcontainers.
"""

import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from time import sleep

import tqdm
from loguru import logger
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

# from lightcurvedb.analysis.aggregates import create_continuous_aggregates


@contextmanager
def core(number: int = 128, probability_of_flare: float = 0.1):
    with PostgresContainer(
        image="timescale/timescaledb:latest-pg16",
        port=5432,
        username="postgres",
        password="password",
        dbname="lightcurvedb",
    ) as postgres:
        print("----- Postgres connection details -----")
        print(f"Host: {postgres.get_container_host_ip()}")
        print(f"Port: {postgres.get_exposed_port(5432)}")
        print("Username: postgres")
        print("Password: password")
        print("Database: lightcurvedb")

        # Set environment variables
        os.environ["LIGHTCURVEDB_POSTGRES_USER"] = "postgres"
        os.environ["LIGHTCURVEDB_POSTGRES_PASSWORD"] = "password"
        os.environ["LIGHTCURVEDB_POSTGRES_DB"] = "lightcurvedb"
        os.environ["LIGHTCURVEDB_POSTGRES_HOST"] = postgres.get_container_host_ip()
        os.environ["LIGHTCURVEDB_POSTGRES_PORT"] = str(postgres.get_exposed_port(5432))

        from lightcurvedb.config import settings
        from lightcurvedb.models import BandTable, FluxMeasurementTable, SourceTable
        from lightcurvedb.simulation import cutouts, fluxes, sources

        manager = settings.sync_manager()

        # Create tables
        manager.create_all()
        # Enable TimescaleDB extension
        with manager.session() as session:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
            session.execute(text("""
                    SELECT create_hypertable(
                        'flux_measurements',
                        'time',
                        chunk_time_interval => INTERVAL '7 days',
                        if_not_exists => TRUE
                    );
                     """))
            session.commit()
            #create_continuous_aggregates(session)
        logger.warning("Test TimescaleDB setup")
        source_ids = sources.create_fixed_sources(number, manager=manager)

        # Create bands
        bands = [
            BandTable(
                name=f"f{band_frequency:03d}",
                frequency=band_frequency,
                instrument="LATR",
                telescope="SOLAT",
            )
            for band_frequency in [27, 39, 93, 145, 225, 280]
        ]

        with manager.session() as session:
            session.add_all(bands)
            session.commit()

        with manager.session() as session:
            sources = [session.get(SourceTable, source_id) for source_id in source_ids]
            bands = session.query(BandTable).all()

            for source in tqdm.tqdm(sources):
                fluxes.generate_fluxes_fixed_source(
                    source=source,
                    bands=bands,
                    start_time=datetime.now()- timedelta(days=1865), 
                    cadence=timedelta(days=1),
                    number=1865,  
                    session=session,
                    probability_of_flare=probability_of_flare,
                )

                # Get the most recent 30 flux measurements.
                useful_fluxes = (
                    session.query(FluxMeasurementTable)
                    .filter(FluxMeasurementTable.source_id == source.id)
                    .order_by(FluxMeasurementTable.time.desc())
                    .limit(30)
                    .all()
                )

                for flux in useful_fluxes:
                    cutouts.create_cutout(
                        nside=64,
                        flux=flux,
                        session=session,
                    )




        yield postgres


def main():
    import argparse as ap

    parser = ap.ArgumentParser(
        description="Run the ephemeral light curve server for testing"
    )
    parser.add_argument(
        "--number",
        type=int,
        default=128,
        help="The number of sources to generate",
    )
    parser.add_argument(
        "--keepalive",
        action="store_true",
        help="Keep the server alive indefinitely; generally required to be true",
    )

    args = parser.parse_args()

    with core(number=args.number) as _:
        while args.keepalive:
            sleep(10)
