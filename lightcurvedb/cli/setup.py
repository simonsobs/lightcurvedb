"""
Create the database tables if they do not exist.
"""

import asyncio

from loguru import logger

from lightcurvedb.config import Settings
from lightcurvedb.storage import get_storage


async def setup_database():
    settings = Settings()

    logger.info(f"Setting up database with backend: {settings.backend_type}")
    logger.info(f"Database URL: {settings.database_url}")

    async with get_storage(settings) as backend:
        logger.info("Creating schema...")
        await backend.create_schema()
        logger.success("Schema created successfully!")


def main():
    asyncio.run(setup_database())
