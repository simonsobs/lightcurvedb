"""
PostgreSQL implementation of unassigned source storage.
"""

import json
from typing import Literal
from uuid import UUID

from psycopg.rows import class_row

from lightcurvedb.models import UnassignedSource
from lightcurvedb.models.exceptions import UnassignedSourceNotFoundException
from lightcurvedb.storage.postgres.pooler import PostgresPoolUser
from lightcurvedb.storage.postgres.schema import UNASSIGNED_SOURCES_TABLE
from lightcurvedb.storage.prototype.unassigned_source import (
    ProvidesUnassignedSourceStorage,
)


class PostgresUnassignedSourceStorage(
    ProvidesUnassignedSourceStorage, PostgresPoolUser
):
    """
    PostgreSQL storage for sources awaiting cross-match review.
    """

    async def setup(self) -> None:
        async with self.cursor() as cur:
            await cur.execute(UNASSIGNED_SOURCES_TABLE)

    async def create(self, source: UnassignedSource) -> UUID:
        """
        Create an unassigned source.
        """
        query = """
            INSERT INTO unassigned_sources (
                source_id, ra, dec, first_seen, last_seen, status, version, extra
            )
            VALUES (
                %(source_id)s, %(ra)s, %(dec)s, %(first_seen)s, %(last_seen)s,
                %(status)s, %(version)s, %(extra)s
            )
            RETURNING source_id
        """

        with self.tracer.start_as_current_span("create_unassigned_source") as span:
            span.set_attribute("unassigned_source.source_id", str(source.source_id))

            params = source.model_dump()
            if params["extra"] is not None:
                params["extra"] = json.dumps(params["extra"])

            async with self.cursor() as cur:
                await cur.execute(query, params)
                row = await cur.fetchone()

            if row is None:
                raise ValueError("INSERT RETURNING source_id returned no row")
            return row[0]

    async def get(self, source_id: UUID) -> UnassignedSource:
        """
        Retrieve an unassigned source by ID.
        """
        query = """
            SELECT source_id, ra, dec, first_seen, last_seen, status, version, extra
            FROM unassigned_sources
            WHERE source_id = %(source_id)s
        """

        with self.tracer.start_as_current_span("get_unassigned_source") as span:
            span.set_attribute("unassigned_source.source_id", str(source_id))

            async with self.cursor(row_factory=class_row(UnassignedSource)) as cur:
                await cur.execute(query, {"source_id": source_id})
                row = await cur.fetchone()

            if row is None:
                raise UnassignedSourceNotFoundException(
                    f"Unassigned source {source_id} not found"
                )
            return row

    async def get_all(
        self,
        status: Literal["unmatched", "merged", "external_match", "novel", "noise"]
        | None = None,
    ) -> list[UnassignedSource]:
        """
        Retrieve all unassigned sources, optionally filtered by status.
        """
        query = """
            SELECT source_id, ra, dec, first_seen, last_seen, status, version, extra
            FROM unassigned_sources
        """
        params = {}
        if status is not None:
            query += " WHERE status = %(status)s"
            params["status"] = status
        query += " ORDER BY last_seen DESC, source_id"

        with self.tracer.start_as_current_span("get_all_unassigned_sources"):
            async with self.cursor(row_factory=class_row(UnassignedSource)) as cur:
                await cur.execute(query, params)
                return await cur.fetchall()

    async def get_in_radius(
        self,
        *,
        ra: float,
        dec: float,
        radius_arcmin: float,
        status: Literal["unmatched", "merged", "external_match", "novel", "noise"]
        | None = None,
    ) -> list[UnassignedSource]:
        """Retrieve sources in an ICRS great-circle cone ordered by separation."""
        query = """
            WITH positioned_sources AS (
                SELECT
                    source_id, ra, dec, first_seen, last_seen, status, version, extra,
                    degrees(2 * asin(least(1.0, sqrt(
                        power(sin(radians((dec - %(dec)s) / 2)), 2)
                        + cos(radians(%(dec)s)) * cos(radians(dec))
                        * power(sin(radians((ra - %(ra)s) / 2)), 2)
                    )))) * 60.0 AS separation_arcmin
                FROM unassigned_sources
            )
            SELECT source_id, ra, dec, first_seen, last_seen, status, version, extra
            FROM positioned_sources
            WHERE separation_arcmin <= %(radius_arcmin)s
        """
        params = {
            "ra": ra % 360.0,
            "dec": dec,
            "radius_arcmin": radius_arcmin,
        }
        if status is not None:
            query += " AND status = %(status)s"
            params["status"] = status
        query += " ORDER BY separation_arcmin, source_id"

        with self.tracer.start_as_current_span("get_unassigned_sources_in_radius"):
            async with self.cursor(row_factory=class_row(UnassignedSource)) as cur:
                await cur.execute(query, params)
                return await cur.fetchall()

    async def delete(self, source_id: UUID) -> None:
        """
        Delete an unassigned source by ID.
        """
        with self.tracer.start_as_current_span("delete_unassigned_source") as span:
            span.set_attribute("unassigned_source.source_id", str(source_id))
            await self.get(source_id)

            async with self.cursor() as cur:
                await cur.execute(
                    "DELETE FROM unassigned_sources WHERE source_id = %(source_id)s",
                    {"source_id": source_id},
                )
