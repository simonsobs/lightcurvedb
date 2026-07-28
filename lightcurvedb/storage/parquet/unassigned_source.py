"""
Parquet implementation of unassigned source storage.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

import astropy.units as u
import pandas as pd
from astropy.coordinates import SkyCoord
from asyncer import asyncify

from lightcurvedb.models import (
    CandidateReviewConflictError,
    FluxMeasurement,
    Source,
    UnassignedSource,
)
from lightcurvedb.models.exceptions import UnassignedSourceNotFoundException
from lightcurvedb.models.review import (
    CandidateDecisionCommand,
    CandidateMerge,
    CandidateMergeCommand,
    CandidateReviewDecision,
    decision_metadata,
)
from lightcurvedb.storage.prototype.flux import ProvidesFluxMeasurementStorage
from lightcurvedb.storage.prototype.source import ProvidesSourceStorage
from lightcurvedb.storage.prototype.unassigned_flux import (
    ProvidesUnassignedFluxMeasurementStorage,
)
from lightcurvedb.storage.prototype.unassigned_source import (
    ProvidesUnassignedSourceStorage,
)


class PandasUnassignedSourceStorage(ProvidesUnassignedSourceStorage):
    """
    Parquet storage for sources awaiting cross-match review.
    """

    def __init__(
        self,
        path: Path,
        *,
        sources: ProvidesSourceStorage,
        fluxes: ProvidesFluxMeasurementStorage,
        unassigned_fluxes: ProvidesUnassignedFluxMeasurementStorage,
    ):
        self.path = path
        self.sources = sources
        self.fluxes = fluxes
        self.unassigned_fluxes = unassigned_fluxes

        self._read_file = asyncify(self._read_file_sync)
        self._write_file = asyncify(self._write_file_sync)

    async def setup(self) -> None:
        """
        Set up the unassigned source storage system.
        """
        pass

    def _read_file_sync(self) -> pd.DataFrame | None:
        if not self.path.exists():
            return None
        return pd.read_parquet(self.path)

    def _write_file_sync(self, table: pd.DataFrame) -> None:
        table.to_parquet(self.path)

    @staticmethod
    def _source_data(row, source_id: UUID) -> dict:
        data = row.to_dict()

        for field in ("reviewed_by", "reviewed_at", "review_metadata"):
            value = data.get(field)

            if value is None or (
                not isinstance(value, (dict, list)) and pd.isna(value)
            ):
                data[field] = None

        data["source_id"] = str(source_id)

        return data

    async def create(self, source: UnassignedSource) -> UUID:
        """
        Create an unassigned source and return its ID.
        """
        new_table = pd.DataFrame([source.model_dump()])
        new_table["source_id"] = new_table["source_id"].astype(str)
        new_table.set_index("source_id", inplace=True)

        if (table := await self._read_file()) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(new_table)

        return source.source_id

    async def get(self, source_id: UUID) -> UnassignedSource:
        """
        Retrieve an unassigned source by ID.
        """
        if (table := await self._read_file()) is None:
            raise UnassignedSourceNotFoundException("Table not found")

        try:
            row = table.loc[str(source_id)]
        except KeyError:
            raise UnassignedSourceNotFoundException(
                f"Unassigned source {source_id} not found"
            )

        return UnassignedSource.model_validate(self._source_data(row, source_id))

    async def replace(self, source: UnassignedSource) -> None:
        """
        Replace one source row for the non-transactional local backend.
        """
        if (table := await self._read_file()) is None:
            raise UnassignedSourceNotFoundException("Table not found")

        source_id = str(source.source_id)

        if source_id not in table.index:
            raise UnassignedSourceNotFoundException(
                f"Unassigned source {source.source_id} not found"
            )

        for field, value in source.model_dump().items():
            if field != "source_id":
                table.at[source_id, field] = value

        await self._write_file(table)

    async def merge(self, command: CandidateMergeCommand) -> CandidateMerge:
        """
        Merge one unmatched source into another with best-effort local writes.
        """
        if command.source_id == command.target_source_id:
            raise CandidateReviewConflictError(
                "An unassigned source cannot be merged into itself"
            )

        source = await self.get(command.source_id)
        target = await self.get(command.target_source_id)

        self._validate_unmatched(
            source.status,
            source.version,
            command.expected_version,
        )

        if target.status != "unmatched":
            raise CandidateReviewConflictError(
                "Only unmatched sources may be merge targets"
            )

        await self.unassigned_fluxes.move_to_source(
            source_id=source.source_id,
            target_source_id=target.source_id,
        )

        await self.replace(
            source.model_copy(
                update={
                    "status": "merged",
                    "version": source.version + 1,
                    "reviewed_by": command.reviewer,
                    "reviewed_at": datetime.now(UTC),
                    "review_metadata": {
                        "target_source_id": str(target.source_id),
                        "reason": command.reason,
                    },
                }
            )
        )

        await self.replace(
            target.model_copy(
                update={
                    "first_seen": min(source.first_seen, target.first_seen),
                    "last_seen": max(source.last_seen, target.last_seen),
                    "version": target.version + 1,
                }
            )
        )

        return CandidateMerge(
            source_id=source.source_id,
            target_source_id=target.source_id,
        )

    async def decide(
        self, command: CandidateDecisionCommand
    ) -> CandidateReviewDecision:
        """
        Record one terminal decision with best-effort local writes.
        """
        source = await self.get(command.source_id)

        self._validate_unmatched(
            source.status,
            source.version,
            command.expected_version,
        )

        canonical_source_id = None

        if command.outcome != "noise":
            source_name = command.novel_name or (
                command.external_evidence.identifier
                if command.external_evidence is not None
                else None
            )
            canonical_source = Source(
                name=source_name,
                ra=source.ra,
                dec=source.dec,
            )
            await self.sources.create(canonical_source)
            canonical_source_id = canonical_source.source_id

            measurements = await self.unassigned_fluxes.get_for_source(source.source_id)

            if measurements:
                await self.fluxes.create_batch(
                    [
                        FluxMeasurement.model_validate(
                            {
                                **measurement.model_dump(),
                                "source_id": canonical_source_id,
                            }
                        )
                        for measurement in measurements
                    ]
                )

        await self.replace(
            source.model_copy(
                update={
                    "status": command.outcome,
                    "version": source.version + 1,
                    "reviewed_by": command.reviewer,
                    "reviewed_at": datetime.now(UTC),
                    "review_metadata": decision_metadata(
                        command,
                        canonical_source_id=canonical_source_id,
                    ),
                }
            )
        )

        return CandidateReviewDecision(
            source_id=source.source_id,
            outcome=command.outcome,
            canonical_source_id=canonical_source_id,
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
        if (table := await self._read_file()) is None:
            return []

        if status is not None:
            table = table.loc[table["status"] == status]

        table = table.sort_values(["last_seen"], ascending=False)
        sources = []

        for source_id, row in table.iterrows():
            sources.append(
                UnassignedSource.model_validate(self._source_data(row, UUID(source_id)))
            )

        return sources

    async def get_in_radius(
        self,
        *,
        ra: float,
        dec: float,
        radius_arcmin: float,
        status: Literal["unmatched", "merged", "external_match", "novel", "noise"]
        | None = None,
    ) -> list[UnassignedSource]:
        """Filter the Parquet source table with great-circle separations."""
        centre = SkyCoord(ra=ra % 360.0 * u.deg, dec=dec * u.deg, frame="icrs")
        matches = []
        for source in await self.get_all(status=status):
            coordinate = SkyCoord(
                ra=source.ra % 360.0 * u.deg, dec=source.dec * u.deg, frame="icrs"
            )
            separation = float(centre.separation(coordinate).to_value(u.arcmin))
            if separation <= radius_arcmin:
                matches.append((separation, source))
        return [source for _, source in sorted(matches, key=lambda match: match[0])]

    async def delete(self, source_id: UUID) -> None:
        """
        Delete an unassigned source by ID.
        """
        if (table := await self._read_file()) is None:
            raise UnassignedSourceNotFoundException("Table not found")

        try:
            table.drop(str(source_id), axis=0, inplace=True)
        except KeyError:
            raise UnassignedSourceNotFoundException(
                f"Unassigned source {source_id} not found"
            )

        await self._write_file(table)
