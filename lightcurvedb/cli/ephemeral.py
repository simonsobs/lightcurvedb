"""
For generating an ephemeral light curve server using testcontainers.
"""

import asyncio
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from time import sleep

import tqdm
from loguru import logger
from testcontainers.postgres import PostgresContainer


def _setup_backend_env(backend_type: str, container=None):
    """
    Set environment variables for backend.
    """
    os.environ["LIGHTCURVEDB_BACKEND_TYPE"] = backend_type

    if container is not None:
        os.environ["LIGHTCURVEDB_POSTGRES_USER"] = "postgres"
        os.environ["LIGHTCURVEDB_POSTGRES_PASSWORD"] = "password"
        os.environ["LIGHTCURVEDB_POSTGRES_DB"] = "lightcurvedb"
        os.environ["LIGHTCURVEDB_POSTGRES_HOST"] = container.get_container_host_ip()
        os.environ["LIGHTCURVEDB_POSTGRES_PORT"] = str(container.get_exposed_port(5432))


def _get_container_for_backend(backend_type: str):
    if backend_type == "postgres":
        return PostgresContainer(
            image="postgres:18-alpine",
            port=5432,
            username="postgres",
            password="password",
            dbname="lightcurvedb",
        )
    elif backend_type == "timescaledb":
        return PostgresContainer(
            image="timescale/timescaledb:latest-pg16",
            port=5432,
            username="postgres",
            password="password",
            dbname="lightcurvedb",
        )
    elif backend_type == "numpy":
        return None
    else:
        raise ValueError(f"Unknown backend: {backend_type}")


@contextmanager
def core(
    backend_type: str = "postgres", number: int = 128, probability_of_flare: float = 0.1
):
    from lightcurvedb.config import Settings
    from lightcurvedb.models.instrument import Instrument
    from lightcurvedb.simulation.fluxes import generate_fluxes_fixed_source
    from lightcurvedb.simulation.sources import create_fixed_sources
    from lightcurvedb.storage.postgres.backend import postgres_backend

    async def setup_and_simulate():
        async with postgres_backend(Settings()) as backend:
            logger.info(f"Schema created for {backend_type}")

            # Create bands
            bands_data = [
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
            await backend.instruments.create_batch(bands_data)
            logger.info(f"Created {len(bands_data)} bands")

            # Create sources
            source_ids = await create_fixed_sources(number, backend)
            logger.info(f"Created {len(source_ids)} sources")

            # Get bands for flux generation
            bands = await backend.instruments.get_all()

            # Generate fluxes for each source
            start_time = datetime.now() - timedelta(days=1865)
            cadence = timedelta(days=1)
            num_measurements = 1865

            for source_id in tqdm.tqdm(source_ids, desc="Generating fluxes"):
                source = await backend.sources.get(source_id)
                _ = await generate_fluxes_fixed_source(
                    source=source,
                    instruments=bands,
                    backend=backend,
                    start_time=start_time,
                    cadence=cadence,
                    number=num_measurements,
                    probability_of_flare=probability_of_flare,
                )

            logger.info(f"Generated flux measurements for {len(source_ids)} sources")

    container = _get_container_for_backend(backend_type)

    if container is None:
        print(f"----- {backend_type.upper()} Backend -----")
        _setup_backend_env(backend_type)
        asyncio.run(setup_and_simulate())
        logger.warning(f"Ephemeral {backend_type} backend ready with {number} sources")
        yield None

    else:
        with container as db:
            print(f"----- {backend_type.upper()} Backend -----")
            print(f"Host: {db.get_container_host_ip()}")
            print(f"Port: {db.get_exposed_port(5432)}")
            print("Database: lightcurvedb")

            _setup_backend_env(backend_type, db)
            asyncio.run(setup_and_simulate())
            logger.warning(
                f"Ephemeral {backend_type} backend ready with {number} sources"
            )
            yield db


def main():
    import argparse as ap

    parser = ap.ArgumentParser(
        description="Run the ephemeral light curve server for testing"
    )
    parser.add_argument(
        "--backend",
        type=str,
        default="postgres",
        choices=["postgres", "timescaledb", "numpy"],
        help="Backend type to use",
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

    with core(backend_type=args.backend, number=args.number) as _:
        while args.keepalive:
            sleep(10)
