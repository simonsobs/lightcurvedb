"""
Parquet implementation of unassigned source storage.
"""

from pathlib import Path
from typing import Literal
from uuid import UUID

import astropy.units as u
import pandas as pd
from astropy.coordinates import SkyCoord
from asyncer import asyncify

from lightcurvedb.models import UnassignedSource
from lightcurvedb.models.exceptions import UnassignedSourceNotFoundException
from lightcurvedb.storage.prototype.unassigned_source import (
    ProvidesUnassignedSourceStorage,
)


class PandasUnassignedSourceStorage(ProvidesUnassignedSourceStorage):
    """
    Parquet storage for sources awaiting cross-match review.
    """

    def __init__(self, path: Path):
        self.path = path

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

        data = row.to_dict()
        data["source_id"] = str(source_id)
        return UnassignedSource.model_validate(data)

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
            data = row.to_dict()
            data["source_id"] = str(source_id)
            sources.append(UnassignedSource.model_validate(data))
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
