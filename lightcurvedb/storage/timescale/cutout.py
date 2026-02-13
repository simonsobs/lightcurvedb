"""
Store cutouts directly in a postgres array.
"""

from psycopg import AsyncConnection

from lightcurvedb.storage.postgres.cutout import PostgresCutoutStorage
from lightcurvedb.storage.timescale.schema import CUTOUT_INDEXES, CUTOUT_SCHEMA


class TimescaleCutoutStorage(PostgresCutoutStorage):
    """
    PostgreSQL cutout storage with array aggregations.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def setup(self) -> None:
        """
        Set up the cutout storage system (e.g. create the tables).
        """
        async with self.conn.cursor() as cur:
            await cur.execute(CUTOUT_SCHEMA)
            await cur.execute(CUTOUT_INDEXES)
