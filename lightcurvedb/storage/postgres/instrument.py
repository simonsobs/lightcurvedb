"""
PostgreSQL implementation of BandStorage protocol.
"""

import json
from collections import defaultdict

from psycopg import AsyncConnection
from psycopg.rows import class_row

from lightcurvedb.models.instrument import Instrument
from lightcurvedb.storage.base.schema import INSTRUMENTS_TABLE
from lightcurvedb.storage.prototype.instrument import ProvidesInstrumentStorage


class PostgresInstrumentStorage(ProvidesInstrumentStorage):
    """
    PostgreSQL instrument storage.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def setup(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(INSTRUMENTS_TABLE)

    async def create(self, instrument: Instrument) -> str:
        """
        Create an instrument.
        """
        query = """
            INSERT INTO instruments (frequency, module, telescope, instrument, details)
            VALUES (%(frequency)s, %(module)s, %(telescope)s, %(instrument)s, %(details)s)
            RETURNING instrument
        """

        params = instrument.model_dump()

        if params["details"] is not None:
            params["details"] = json.dumps(params["details"])

        async with self.conn.cursor() as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return row[0]

    async def create_batch(self, instruments: list[Instrument]) -> int:
        """
        Bulk insert instruments.
        """
        query = """
            INSERT INTO instruments (frequency, module, telescope, instrument, details)
            SELECT *
            FROM UNNEST(
                %(frequency)s::integer[],
                %(module)s::text[],
                %(telescope)s::text[],
                %(instrument)s::text[],
                %(details)s::jsonb[]
            )
            ON CONFLICT (frequency, module) DO NOTHING
        """

        data = defaultdict(list)

        for instrument in instruments:
            instrument_dict = instrument.model_dump()
            for key, value in instrument_dict.items():
                if key == "details" and value is not None:
                    value = json.dumps(value)
                data[key].append(value)

        async with self.conn.cursor() as cur:
            await cur.execute(query, data)

        return len(instruments)

    async def get(self, frequency: int, module: str) -> Instrument:
        """Get instrument by frequency and module."""
        query = """
            SELECT frequency, module, telescope, instrument, details
            FROM instruments
            WHERE frequency = %(frequency)s AND module = %(module)s
        """

        async with self.conn.cursor(row_factory=class_row(Instrument)) as cur:
            await cur.execute(query, {"frequency": frequency, "module": module})
            row = await cur.fetchone()

            if not row:
                from lightcurvedb.models.exceptions import InstrumentNotFoundException

                raise InstrumentNotFoundException(
                    f"Instrument with frequency {frequency} and module {module} not found"
                )

            return row

    async def get_all(self) -> list[Instrument]:
        """Get all instruments."""
        query = """
            SELECT frequency, module, telescope, instrument, details
            FROM instruments
            ORDER BY frequency, module
        """

        async with self.conn.cursor(row_factory=class_row(Instrument)) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return rows

    async def delete(self, frequency: int, module: str) -> None:
        """
        Delete an instrument by frequency and module.
        """

        try:
            await self.get(frequency, module)
        except Exception:
            from lightcurvedb.models.exceptions import InstrumentNotFoundException

            raise InstrumentNotFoundException(
                f"Instrument with frequency {frequency} and module {module} not found"
            )

        query = "DELETE FROM instruments WHERE frequency = %(frequency)s AND module = %(module)s"

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"frequency": frequency, "module": module})
