"""
PostgreSQL implementation of SourceStorage protocol.
"""

from psycopg import AsyncConnection
from psycopg.rows import dict_row
import json

from lightcurvedb.models.source import Source, SourceCreate, SourceMetadata


class PostgresSourceStorage:
    """
    PostgreSQL source storage.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def create(self, source: SourceCreate) -> Source:
        """
        Create a source.
        """
        query = """
            INSERT INTO sources (name, ra, dec, variable, extra)
            VALUES (%(name)s, %(ra)s, %(dec)s, %(variable)s, %(extra)s)
            RETURNING id, name, ra, dec, variable, extra
        """

        params = source.model_dump()
        if params['extra'] is not None:
            params['extra'] = json.dumps(params['extra'])

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()

            if row['extra']:
                row['extra'] = SourceMetadata(**row['extra'])

            return Source(**row)

    async def create_batch(self, sources: list[SourceCreate]) -> list[int]:
        """
        Bulk insert sources, returns created source IDs.
        """
        query = """
            INSERT INTO sources (name, ra, dec, variable, extra)
            VALUES (%(name)s, %(ra)s, %(dec)s, %(variable)s, %(extra)s)
            RETURNING id
        """

        params_list = []
        for s in sources:
            params = s.model_dump()
            if params['extra'] is not None:
                params['extra'] = json.dumps(params['extra'])
            params_list.append(params)

        source_ids = []
        async with self.conn.cursor() as cur:
            for params in params_list:
                await cur.execute(query, params)
                row = await cur.fetchone()
                source_ids.append(row[0])

        return source_ids

    async def get(self, source_id: int) -> Source:
        """
        Get source by ID.
        """
        query = """
            SELECT id, name, ra, dec, variable, extra
            FROM sources
            WHERE id = %(source_id)s
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, {"source_id": source_id})
            row = await cur.fetchone()

            if not row:
                from lightcurvedb.models.exceptions import SourceNotFoundException
                raise SourceNotFoundException(f"Source {source_id} not found")

            if row['extra']:
                row['extra'] = SourceMetadata(**row['extra'])

            return Source(**row)

    async def get_all(self) -> list[Source]:
        """Get all sources."""
        query = """
            SELECT id, name, ra, dec, variable, extra
            FROM sources
            ORDER BY id
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()

            sources = []
            for row in rows:
                if row['extra']:
                    row['extra'] = SourceMetadata(**row['extra'])
                sources.append(Source(**row))

            return sources
