"""
For generating an ephemeral light curve server using testcontainers.
"""

import os
from datetime import datetime, timedelta
from time import sleep

import tqdm
from pydantic_settings import BaseSettings, CliApp
from testcontainers.postgres import PostgresContainer


class CLISettings(BaseSettings):
    """
    Generate an ephemeral lightcurve server. On startup, it will print out
    connection details that you can use to connect to the server and explore
    the data.
    """

    number: int = 128

    def cli_cmd(self) -> None:
        with PostgresContainer(
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
            os.environ["LIGHTCURVEDB_POSTGRES_PORT"] = str(
                postgres.get_exposed_port(5432)
            )

            from lightcurvedb.config import settings
            from lightcurvedb.models import BandTable, FluxMeasurementTable, SourceTable
            from lightcurvedb.simulation import cutouts, fluxes, sources

            manager = settings.sync_manager()

            # Create tables
            manager.create_all()

            source_ids = sources.create_fixed_sources(self.number, manager=manager)

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
                sources = [
                    session.get(SourceTable, source_id) for source_id in source_ids
                ]
                bands = session.query(BandTable).all()

                for source in tqdm.tqdm(sources):
                    fluxes.generate_fluxes_fixed_source(
                        source=source,
                        bands=bands,
                        start_time=datetime.now(),
                        cadence=timedelta(days=1),
                        number=365,
                        session=session,
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

            # Keep that session alive!
            while True:
                sleep(10)


def main():
    _ = CliApp.run(CLISettings)
