"""
PostgreSQL implementation of BandStorage protocol.
"""

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from lightcurvedb.models.band import Band
from lightcurvedb.storage.base.schema import BANDS_TABLE
from lightcurvedb.storage.prototype.band import ProvidesBandStorage


class PostgresBandStorage(ProvidesBandStorage):
    """
    PostgreSQL band storage.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def setup(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(BANDS_TABLE)

    async def create(self, band: Band) -> Band:
        """
        Create a band.
        """
        query = """
            INSERT INTO bands (band_name, telescope, instrument, frequency)
            VALUES (%(band_name)s, %(telescope)s, %(instrument)s, %(frequency)s)
            RETURNING band_name, telescope, instrument, frequency
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
            INSERT INTO bands (band_name, telescope, instrument, frequency)
            VALUES (%(band_name)s, %(telescope)s, %(instrument)s, %(frequency)s)
            ON CONFLICT (band_name) DO NOTHING
        """

        params_list = [b.model_dump() for b in bands]

        async with self.conn.cursor() as cur:
            await cur.executemany(query, params_list)

        return len(bands)

    async def get(self, band_name: str) -> Band:
        """Get band by name."""
        query = """
            SELECT band_name, telescope, instrument, frequency
            FROM bands
            WHERE band_name = %(band_name)s
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
            SELECT band_name, telescope, instrument, frequency
            FROM bands
            ORDER BY band_name
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return [Band(**row) for row in rows]

    async def delete(self, band_name: str) -> None:
        """
        Delete a band by name.
        """

        try:
            await self.get(band_name)
        except Exception:
            from lightcurvedb.models.exceptions import BandNotFoundException

            raise BandNotFoundException(f"Band {band_name} not found")

        query = "DELETE FROM bands WHERE band_name = %(band_name)s"

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"band_name": band_name})
