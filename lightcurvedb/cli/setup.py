"""
Create the database tables if they do not exist.
"""

import asyncio
from loguru import logger
from lightcurvedb.storage import get_storage
from lightcurvedb.config import settings


async def setup_database():
    logger.info(f"Setting up database with backend: {settings.backend_type}")
    logger.info(f"Database URL: {settings.database_url}")

    async with get_storage() as backend:
        logger.info("Creating schema...")
        await backend.create_schema()
        logger.success("Schema created successfully!")


def main():
    asyncio.run(setup_database())