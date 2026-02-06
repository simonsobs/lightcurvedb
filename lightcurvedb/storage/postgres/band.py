"""
PostgreSQL implementation of BandStorage protocol.
"""

from collections import defaultdict

from psycopg import AsyncConnection
from psycopg.rows import class_row

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

    async def create(self, band: Band) -> str:
        """
        Create a band.
        """
        query = """
            INSERT INTO bands (band_name, telescope, instrument, frequency)
            VALUES (%(band_name)s, %(telescope)s, %(instrument)s, %(frequency)s)
            RETURNING band_name
        """

        params = band.model_dump()

        async with self.conn.cursor() as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return row[0]

    async def create_batch(self, bands: list[Band]) -> int:
        """
        Bulk insert bands.
        """
        query = """
            INSERT INTO bands (band_name, telescope, instrument, frequency)
            SELECT *
            FROM UNNEST(
                %(band_name)s::text[],
                %(telescope)s::text[],
                %(instrument)s::text[],
                %(frequency)s::double precision[]
            )
            ON CONFLICT (band_name) DO NOTHING
            RETURNING band_name
        """

        data = defaultdict(list)

        for band in bands:
            band_dict = band.model_dump()
            for key, value in band_dict.items():
                data[key].append(value)

        async with self.conn.cursor() as cur:
            await cur.execute(query, data)
            response = await cur.fetchall()
            inserted_band_names = {row[0] for row in response}

        return len(inserted_band_names)

    async def get(self, band_name: str) -> Band:
        """Get band by name."""
        query = """
            SELECT band_name, telescope, instrument, frequency
            FROM bands
            WHERE band_name = %(band_name)s
        """

        async with self.conn.cursor(row_factory=class_row(Band)) as cur:
            await cur.execute(query, {"band_name": band_name})
            row = await cur.fetchone()

            if not row:
                from lightcurvedb.models.exceptions import BandNotFoundException

                raise BandNotFoundException(f"Band {band_name} not found")

            return row

    async def get_all(self) -> list[Band]:
        """Get all bands."""
        query = """
            SELECT band_name, telescope, instrument, frequency
            FROM bands
            ORDER BY band_name
        """

        async with self.conn.cursor(row_factory=class_row(Band)) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return rows

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
