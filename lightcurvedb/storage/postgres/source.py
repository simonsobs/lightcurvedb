"""
PostgreSQL implementation of SourceStorage protocol.
"""

import json
from collections import defaultdict
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import class_row

from lightcurvedb.models.source import Source
from lightcurvedb.storage.base.schema import SOURCES_TABLE
from lightcurvedb.storage.prototype.source import ProvidesSourceStorage


class PostgresSourceStorage(ProvidesSourceStorage):
    """
    PostgreSQL source storage.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def setup(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(SOURCES_TABLE)

    async def create(self, source: Source) -> int:
        """
        Create a source.
        """

        query = """
            INSERT INTO sources (
                source_id, socat_id, name, ra, dec, variable, extra
            )
            VALUES (
                %(source_id)s,
                %(socat_id)s,
                %(name)s,
                %(ra)s,
                %(dec)s,
                %(variable)s,
                %(extra)s
            )
            RETURNING source_id
        """

        params = source.model_dump()

        if params["extra"] is not None:
            params["extra"] = json.dumps(params["extra"])

        async with self.conn.cursor() as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()

        return row[0]

    async def create_batch(self, sources: list[Source]) -> list[int]:
        """
        Bulk insert sources, returns created source IDs.
        """
        query = """
            INSERT INTO sources (source_id, name, ra, dec, variable, extra)
            SELECT *
            FROM UNNEST(
                %(source_id)s::uuid[],
                %(name)s::text[],
                %(ra)s::double precision[],
                %(dec)s::double precision[],
                %(variable)s::boolean[],
                %(extra)s::jsonb[]
            )
            RETURNING source_id
        """

        data = defaultdict(list)

        for source in sources:
            source_dict = source.model_dump()
            if source_dict["extra"] is not None:
                source_dict["extra"] = json.dumps(source_dict["extra"])
            for key, value in source_dict.items():
                data[key].append(value)

        async with self.conn.cursor() as cur:
            await cur.execute(query, data)
            response = await cur.fetchall()
            source_ids = [row[0] for row in response]

        return source_ids

    async def get(self, source_id: UUID) -> Source:
        """
        Get source by ID.
        """
        query = """
            SELECT source_id, socat_id, name, ra, dec, variable, extra
            FROM sources
            WHERE source_id = %(source_id)s
        """

        async with self.conn.cursor(row_factory=class_row(Source)) as cur:
            await cur.execute(query, {"source_id": source_id})
            row = await cur.fetchone()

            if not row:
                from lightcurvedb.models.exceptions import SourceNotFoundException
                raise SourceNotFoundException(f"Source {source_id} not found")

            return row

    async def get_by_socat_id(self, socat_id: int) -> Source:
        """
        Get source by SOcat ID.
        """
        query = """
            SELECT source_id, socat_id, name, ra, dec, variable, extra
            FROM sources
            WHERE socat_id = %(socat_id)s
        """

        async with self.conn.cursor(row_factory=class_row(Source)) as cur:
            await cur.execute(query, {"socat_id": socat_id})
            row = await cur.fetchone()

            if not row:
                from lightcurvedb.models.exceptions import SourceNotFoundException

                raise SourceNotFoundException(
                    f"Source with SOcat ID {socat_id} not found"
                )

            return row

    async def get_all(self) -> list[Source]:
        """Get all sources."""
        query = """
            SELECT source_id, socat_id, name, ra, dec, variable, extra
            FROM sources
            ORDER BY source_id
        """

        async with self.conn.cursor(row_factory=class_row(Source)) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            return rows

    async def delete(self, source_id: UUID) -> None:
        """
        Delete a source by ID.
        """
        # Check if source exists first
        try:
            await self.get(source_id)
        except Exception:
            from lightcurvedb.models.exceptions import SourceNotFoundException

            raise SourceNotFoundException(f"Source {source_id} not found")

        query = "DELETE FROM sources WHERE source_id = %(source_id)s"

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"source_id": source_id})

    async def get_in_bounds(
        self, ra_min: float, ra_max: float, dec_min: float, dec_max: float
    ) -> list[Source]:
        """
        Get all sources within rectangular RA/Dec bounds.
        """
        query = """
            SELECT source_id, socat_id, name, ra, dec, variable, extra
            FROM sources
            WHERE ra > %(ra_min)s
              AND ra < %(ra_max)s
              AND dec > %(dec_min)s
              AND dec < %(dec_max)s
        """

        params = {
            "ra_min": ra_min,
            "ra_max": ra_max,
            "dec_min": dec_min,
            "dec_max": dec_max,
        }

        async with self.conn.cursor(row_factory=class_row(Source)) as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()

            return rows
