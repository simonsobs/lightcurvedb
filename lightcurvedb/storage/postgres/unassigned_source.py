"""
PostgreSQL implementation of unassigned source storage.
"""

import json
from typing import Literal
from uuid import UUID

from psycopg.rows import class_row, dict_row

from lightcurvedb.models import CandidateReviewConflictError, UnassignedSource
from lightcurvedb.models.exceptions import UnassignedSourceNotFoundException
from lightcurvedb.models.review import (
    CandidateDecisionCommand,
    CandidateMerge,
    CandidateMergeCommand,
    CandidateReviewDecision,
    decision_metadata,
)
from lightcurvedb.storage.postgres.pooler import PostgresPoolUser
from lightcurvedb.storage.postgres.schema import (
    UNASSIGNED_SOURCES_TABLE,
)
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
            SELECT source_id, ra, dec, first_seen, last_seen, status, version,
                   reviewed_by, reviewed_at, review_metadata, extra
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

    async def replace(self, source: UnassignedSource) -> None:
        """
        Replace an unassigned source record.
        """

        query = """
            UPDATE unassigned_sources
            SET ra = %(ra)s, dec = %(dec)s, first_seen = %(first_seen)s,
                last_seen = %(last_seen)s, status = %(status)s,
                version = %(version)s, reviewed_by = %(reviewed_by)s,
                reviewed_at = %(reviewed_at)s,
                review_metadata = %(review_metadata)s::jsonb, extra = %(extra)s
            WHERE source_id = %(source_id)s
        """

        with self.tracer.start_as_current_span("replace_unassigned_source") as span:
            span.set_attribute("unassigned_source.source_id", str(source.source_id))

            params = source.model_dump()

            if params["extra"] is not None:
                params["extra"] = json.dumps(params["extra"])

            if params["review_metadata"] is not None:
                params["review_metadata"] = json.dumps(params["review_metadata"])

            async with self.cursor() as cur:
                await cur.execute(query, params)

    async def merge(self, command: CandidateMergeCommand) -> CandidateMerge:
        """
        Merge one unmatched source into another in a single transaction.
        """
        if command.source_id == command.target_source_id:
            raise CandidateReviewConflictError(
                "An unassigned source cannot be merged into itself"
            )

        locked_sources_query = """
            WITH locked_sources AS (
                SELECT source_id, status, version, first_seen, last_seen
                FROM unassigned_sources
                WHERE source_id = ANY(%(source_ids)s)
                ORDER BY source_id
                FOR UPDATE
            )
            SELECT * FROM locked_sources
        """
        move_measurements_query = """
            UPDATE unassigned_flux_measurements
            SET source_id = %(target_source_id)s
            WHERE source_id = %(source_id)s
        """
        update_source_query = """
            UPDATE unassigned_sources
            SET status = 'merged', version = version + 1,
                reviewed_by = %(reviewer)s, reviewed_at = CURRENT_TIMESTAMP,
                review_metadata = %(review_metadata)s::jsonb
            WHERE source_id = %(source_id)s
        """
        update_target_query = """
            UPDATE unassigned_sources
            SET first_seen = LEAST(first_seen, %(source_first_seen)s),
                last_seen = GREATEST(last_seen, %(source_last_seen)s),
                version = version + 1
            WHERE source_id = %(target_source_id)s
        """

        with self.tracer.start_as_current_span("merge_unassigned_source") as span:
            span.set_attribute("unassigned_source.source_id", str(command.source_id))
            span.set_attribute(
                "unassigned_source.target_source_id", str(command.target_source_id)
            )

            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute(
                            locked_sources_query,
                            {
                                "source_ids": sorted(
                                    (command.source_id, command.target_source_id),
                                    key=str,
                                )
                            },
                        )
                        locked = {
                            row["source_id"]: row for row in await cursor.fetchall()
                        }
                        source = locked.get(command.source_id)
                        target = locked.get(command.target_source_id)

                        if source is None or target is None:
                            raise CandidateReviewConflictError(
                                "Unassigned source no longer exists"
                            )

                        self._validate_unmatched(
                            source["status"],
                            source["version"],
                            command.expected_version,
                        )

                        if target["status"] != "unmatched":
                            raise CandidateReviewConflictError(
                                "Only unmatched sources may be merge targets"
                            )

                        await cursor.execute(
                            move_measurements_query,
                            {
                                "source_id": command.source_id,
                                "target_source_id": command.target_source_id,
                            },
                        )

                        await cursor.execute(
                            update_source_query,
                            {
                                "source_id": command.source_id,
                                "reviewer": command.reviewer,
                                "review_metadata": json.dumps(
                                    {
                                        "target_source_id": str(
                                            command.target_source_id
                                        ),
                                        "reason": command.reason,
                                    }
                                ),
                            },
                        )

                        await cursor.execute(
                            update_target_query,
                            {
                                "target_source_id": command.target_source_id,
                                "source_first_seen": source["first_seen"],
                                "source_last_seen": source["last_seen"],
                            },
                        )

        return CandidateMerge(
            source_id=command.source_id,
            target_source_id=command.target_source_id,
        )

    async def decide(
        self, command: CandidateDecisionCommand
    ) -> CandidateReviewDecision:
        """
        Record one terminal source decision in a single transaction.
        """
        locked_source_query = """
            SELECT source_id, status, version
            FROM unassigned_sources
            WHERE source_id = %(source_id)s
            FOR UPDATE
        """
        canonical_source_query = """
            SELECT source_id
            FROM sources
            WHERE source_id = %(source_id)s
            FOR KEY SHARE
        """
        materialize_measurements_query = """
            INSERT INTO flux_measurements (
                measurement_id, frequency, module, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty, flux, flux_err, extra
            )
            SELECT
                measurement_id, frequency, module, %(canonical_source_id)s,
                time, ra, dec, ra_uncertainty, dec_uncertainty, flux, flux_err, extra
            FROM unassigned_flux_measurements
            WHERE source_id = %(source_id)s
        """
        update_source_query = """
            UPDATE unassigned_sources
            SET status = %(outcome)s, version = version + 1,
                reviewed_by = %(reviewer)s, reviewed_at = CURRENT_TIMESTAMP,
                review_metadata = %(review_metadata)s::jsonb
            WHERE source_id = %(source_id)s
        """

        with self.tracer.start_as_current_span("decide_unassigned_source") as span:
            span.set_attribute("unassigned_source.source_id", str(command.source_id))
            span.set_attribute("unassigned_source.outcome", command.outcome)

            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute(
                            locked_source_query,
                            {"source_id": command.source_id},
                        )
                        source = await cursor.fetchone()

                        if source is None:
                            raise CandidateReviewConflictError(
                                "Unassigned source no longer exists"
                            )

                        self._validate_unmatched(
                            source["status"],
                            source["version"],
                            command.expected_version,
                        )

                        if command.canonical_source_id is not None:
                            await cursor.execute(
                                canonical_source_query,
                                {"source_id": command.canonical_source_id},
                            )

                            if await cursor.fetchone() is None:
                                raise CandidateReviewConflictError(
                                    "Canonical source no longer exists"
                                )

                            await cursor.execute(
                                materialize_measurements_query,
                                {
                                    "source_id": command.source_id,
                                    "canonical_source_id": command.canonical_source_id,
                                },
                            )

                        await cursor.execute(
                            update_source_query,
                            {
                                "source_id": command.source_id,
                                "outcome": command.outcome,
                                "reviewer": command.reviewer,
                                "review_metadata": json.dumps(decision_metadata(command)),
                            },
                        )

        return CandidateReviewDecision(
            source_id=command.source_id,
            outcome=command.outcome,
            canonical_source_id=command.canonical_source_id,
        )

    @staticmethod
    def _validate_unmatched(status: str, version: int, expected_version: int) -> None:
        if status != "unmatched":
            raise CandidateReviewConflictError(
                "Only unmatched sources may receive a review decision"
            )

        if version != expected_version:
            raise CandidateReviewConflictError("Unassigned source review is stale")

    async def get_all(
        self,
        status: Literal["unmatched", "merged", "external_match", "novel", "noise"]
        | None = None,
    ) -> list[UnassignedSource]:
        """
        Retrieve all unassigned sources, optionally filtered by status.
        """
        query = """
            SELECT source_id, ra, dec, first_seen, last_seen, status, version,
                   reviewed_by, reviewed_at, review_metadata, extra
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
                    source_id, ra, dec, first_seen, last_seen, status, version,
                    reviewed_by, reviewed_at, review_metadata, extra,
                    degrees(2 * asin(least(1.0, sqrt(
                        power(sin(radians((dec - %(dec)s) / 2)), 2)
                        + cos(radians(%(dec)s)) * cos(radians(dec))
                        * power(sin(radians((ra - %(ra)s) / 2)), 2)
                    )))) * 60.0 AS separation_arcmin
                FROM unassigned_sources
            )
            SELECT source_id, ra, dec, first_seen, last_seen, status, version,
                   reviewed_by, reviewed_at, review_metadata, extra
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
