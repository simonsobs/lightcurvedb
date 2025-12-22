"""
PostgreSQL storage backend.
"""

from psycopg import AsyncConnection
from lightcurvedb.config import settings
from lightcurvedb.storage.postgres.source import PostgresSourceStorage
from lightcurvedb.storage.postgres.band import PostgresBandStorage
from lightcurvedb.storage.postgres.flux import PostgresFluxMeasurementStorage
from lightcurvedb.storage.base.schema import SOURCES_TABLE, BANDS_TABLE
from lightcurvedb.storage.postgres.schema import (
    FLUX_MEASUREMENTS_TABLE,
    generate_flux_partitions,
    FLUX_INDEXES,
)


class PostgresBackend:

    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.sources = PostgresSourceStorage(conn)
        self.bands = PostgresBandStorage(conn)
        self.fluxes = PostgresFluxMeasurementStorage(conn)
        self.partition_count = settings.postgres_partition_count

    async def create_schema(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(SOURCES_TABLE)
            await cur.execute(BANDS_TABLE)
            await cur.execute(FLUX_MEASUREMENTS_TABLE)
            partitions_sql = generate_flux_partitions(self.partition_count)
            await cur.execute(partitions_sql)
            await cur.execute(FLUX_INDEXES)

        await self.conn.commit()
