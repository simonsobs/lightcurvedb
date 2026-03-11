"""
A base class providing a set-up employing connection pools to generate
cursors.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg import AsyncClientCursor
from psycopg.cursor_async import AsyncRowFactory, Row
from psycopg_pool import AsyncConnectionPool


class PostgresPoolUser:
    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    @asynccontextmanager
    async def cursor(
        self, *, row_factory: AsyncRowFactory[Row] | None = None, **kwargs
    ) -> AsyncIterator[AsyncClientCursor[Row]]:
        async with self.pool.connection() as conn:
            async with conn.cursor(row_factory=row_factory, **kwargs) as cur:
                yield cur
