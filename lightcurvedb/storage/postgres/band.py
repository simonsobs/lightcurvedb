"""
PostgreSQL implementation of BandStorage protocol.
"""

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from lightcurvedb.models.band import Band


class PostgresBandStorage:
    """
    PostgreSQL band storage.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def create(self, band: Band) -> Band:
        """
        Create a band.
        """
        query = """
            INSERT INTO bands (name, telescope, instrument, frequency)
            VALUES (%(name)s, %(telescope)s, %(instrument)s, %(frequency)s)
            RETURNING name, telescope, instrument, frequency
        """

        params = band.model_dump()

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return Band(**row)

    async def create_batch(self, bands: list[Band]) -> int:
        """
        Bulk insert bands.
        """
        query = """
            INSERT INTO bands (name, telescope, instrument, frequency)
            VALUES (%(name)s, %(telescope)s, %(instrument)s, %(frequency)s)
            ON CONFLICT (name) DO NOTHING
        """

        params_list = [b.model_dump() for b in bands]

        async with self.conn.cursor() as cur:
            await cur.executemany(query, params_list)

        return len(bands)

    async def get(self, band_name: str) -> Band:
        """Get band by name."""
        query = """
            SELECT name, telescope, instrument, frequency
            FROM bands
            WHERE name = %(band_name)s
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, {"band_name": band_name})
            row = await cur.fetchone()

            if not row:
                from lightcurvedb.models.exceptions import BandNotFoundException
                raise BandNotFoundException(f"Band {band_name} not found")

            return Band(**row)

    async def get_all(self) -> list[Band]:
        """Get all bands."""
        query = """
            SELECT name, telescope, instrument, frequency
            FROM bands
            ORDER BY name
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return [Band(**row) for row in rows]
