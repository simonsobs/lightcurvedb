"""
PostgreSQL storage backend.
"""

from psycopg import AsyncConnection
from lightcurvedb.storage.postgres.source import PostgresSourceStorage
from lightcurvedb.storage.postgres.band import PostgresBandStorage
from lightcurvedb.storage.postgres.flux import PostgresFluxMeasurementStorage
from lightcurvedb.storage.base.schema import ALL_TABLES


class PostgresBackend:

    def __init__(self, conn: AsyncConnection):
        self.conn = conn
        self.sources = PostgresSourceStorage(conn)
        self.bands = PostgresBandStorage(conn)
        self.fluxes = PostgresFluxMeasurementStorage(conn)

    async def create_schema(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(ALL_TABLES)
        await self.conn.commit()
