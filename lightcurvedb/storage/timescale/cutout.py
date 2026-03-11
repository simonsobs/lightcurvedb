"""
Store cutouts directly in a postgres array.
"""

from lightcurvedb.storage.postgres.cutout import PostgresCutoutStorage
from lightcurvedb.storage.timescale.schema import CUTOUT_INDEXES, CUTOUT_SCHEMA


class TimescaleCutoutStorage(PostgresCutoutStorage):
    """
    PostgreSQL cutout storage with array aggregations.
    """

    async def setup(self) -> None:
        """
        Set up the cutout storage system (e.g. create the tables).
        """
        async with self.cursor() as cur:
            await cur.execute(CUTOUT_SCHEMA)
            await cur.execute(CUTOUT_INDEXES)
