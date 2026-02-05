"""
Create the database tables if they do not exist.
"""

import asyncio

from loguru import logger

from lightcurvedb.config import Settings
from asyncio import sleep


async def setup_database():
    settings = Settings()

    logger.info(f"Setting up database with backend: {settings.backend_type}")
    logger.info(f"Database URL: {settings.database_url}")

    if settings.backend_type == "postgres":
        from lightcurvedb.storage.postgres.backend import postgres_backend
        with postgres_backend(settings) as _:
            # Backend 'auto' sets up.
            await sleep(0.1)


def main():
    asyncio.run(setup_database())
