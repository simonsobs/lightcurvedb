"""
Store cutouts directly in a postgres array.
"""

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from lightcurvedb.models import Cutout
from lightcurvedb.storage.postgres.schema import CUTOUT_INDEXES, CUTOUT_SCHEMA
from lightcurvedb.storage.prototype.cutout import ProvidesCutoutStorage


class PostgresCutoutStorage(ProvidesCutoutStorage):
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

    async def create(self, cutout: Cutout) -> int:
        """
        Store a cutout for a given source and band.
        """
        query = """
            INSERT INTO cutouts (source_id, flux_id, band_name, cutout_data,
            time, units) VALUES (%(source_id)s, %(flux_id)s, %(band_name)s,
            %(data)s, %(time)s, %(units)s)
            RETURNING cutout_id
        """
        params = cutout.model_dump()

        async with self.conn.cursor() as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return row[0]

    async def create_batch(self, cutouts: list[Cutout]) -> list[int]:
        """
        Store a cutout for a given source and band.
        """
        query = """
            INSERT INTO cutouts (source_id, flux_id, band_name, cutout_data,
            time, units) VALUES (%(source_id)s, %(flux_id)s, %(band_name)s,
            %(data)s, %(time)s, %(units)s)
            RETURNING cutout_id
        """
        params_list = [c.model_dump() for c in cutouts]

        async with self.conn.cursor() as cur:
            await cur.executemany(query, params_list)

        return []

    async def retrieve_cutout(self, source_id: int, flux_id: int) -> Cutout:
        """
        Retrieve a cutout for a given source and band.
        """
        query = """
            SELECT cutout_id, source_id, flux_id, band_name, cutout_data as data, time, units
            FROM cutouts
            WHERE source_id = %(source_id)s AND flux_id = %(flux_id)s
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                query,
                {
                    "source_id": source_id,
                    "flux_id": flux_id,
                },
            )
            row = await cur.fetchone()
            return Cutout(**row)

    async def retrieve_cutouts_for_source(self, source_id: int) -> list[Cutout]:
        """
        Retrieve cutouts for a given source.
        """
        query = """
            SELECT cutout_id, source_id, flux_id, band_name, cutout_data as data, time, units
            FROM cutouts
            WHERE source_id = %(source_id)s
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                query,
                {
                    "source_id": source_id,
                },
            )
            rows = await cur.fetchall()
            return [Cutout(**row) for row in rows]

    async def delete(self, cutout_id: int) -> None:
        """
        Delete a cutout by ID.
        """
        query = """
            DELETE FROM cutouts
            WHERE cutout_id = %(cutout_id)s
        """

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"cutout_id": cutout_id})
