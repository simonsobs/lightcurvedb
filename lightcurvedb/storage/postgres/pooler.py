"""
A base class providing a set-up employing connection pools to generate
cursors.
"""

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, overload

from opentelemetry import metrics, trace
from psycopg import AsyncClientCursor
from psycopg.rows import BaseRowFactory, Row
from psycopg_pool import AsyncConnectionPool


class PostgresPoolUser:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        tracer: trace.Tracer | None = None,
        meter: metrics.Meter | None = None,
    ):
        self.pool = pool
        self.tracer = tracer or trace.get_tracer("lightcurvedb-postgres-pool-user")
        self.meter = meter or metrics.get_meter("lightcurvedb-postgres-pool-user")

    @overload
    def cursor(
        self,
        *,
        row_factory: BaseRowFactory[Row],
    ) -> AbstractAsyncContextManager[AsyncClientCursor[Row]]: ...

    @overload
    def cursor(self) -> AbstractAsyncContextManager[AsyncClientCursor[Any]]: ...

    @asynccontextmanager  # type: ignore[misc]
    async def cursor(
        self, *, row_factory: BaseRowFactory[Row] | None = None
    ) -> AsyncIterator[AsyncClientCursor[Any]]:
        async with self.pool.connection() as conn:
            if row_factory is not None:
                async with conn.cursor(row_factory=row_factory) as cur:
                    yield cur
            else:
                async with conn.cursor() as cur:
                    yield cur
