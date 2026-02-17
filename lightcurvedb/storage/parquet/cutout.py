"""
Prototype for cutout storage interactions
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Iterable
from uuid import UUID

import pandas as pd
from asyncer import asyncify

from lightcurvedb.models import Cutout
from lightcurvedb.models.exceptions import CutoutNotFoundException
from lightcurvedb.storage.prototype.cutout import ProvidesCutoutStorage


class PandasCutoutStorage(ProvidesCutoutStorage):
    def __init__(self, path: Path):
        if path.suffix == ".parquet":
            path = path.with_suffix("")

        self.base_path = path

        self._read_file = asyncify(self._read_file_sync)
        self._write_file = asyncify(self._write_file_sync)

    def _source_path(self, source_id: UUID) -> Path:
        return self.base_path / f"{source_id}.parquet"

    def _read_file_sync(self, source_id: UUID) -> pd.DataFrame | None:
        path = self._source_path(source_id)
        if not path.exists():
            return None
        table = pd.read_parquet(path)
        return table

    def _write_file_sync(self, source_id: UUID, table: pd.DataFrame) -> None:
        self.base_path.mkdir(parents=True, exist_ok=True)
        table.to_parquet(self._source_path(source_id))

    def _new_table(self, cutouts: Iterable[Cutout]) -> pd.DataFrame:
        rows = []
        for cutout in cutouts:
            row = cutout.model_dump()
            row["measurement_id"] = str(cutout.measurement_id)
            row["source_id"] = str(cutout.source_id)

            rows.append(row)

        table = pd.DataFrame(rows)
        table.set_index("measurement_id", inplace=True)
        table["source_id"] = table["source_id"].astype(str)
        return table

    async def setup(self) -> None:
        """
        Set up the cutout storage system (e.g. create the tables).
        """
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def create(self, cutout: Cutout) -> int:
        """
        Store a cutout for a given source and band.
        """
        new_table = self._new_table([cutout])

        if (table := await self._read_file(cutout.source_id)) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(cutout.source_id, new_table)

        return cutout.measurement_id

    async def create_batch(self, cutouts: list[Cutout]) -> list[int]:
        """
        Store a cutout for a given source and band.
        """
        if not cutouts:
            return []

        grouped: dict[UUID, list[Cutout]] = defaultdict(list)
        for cutout in cutouts:
            grouped[cutout.source_id].append(cutout)

        for source_id, group in grouped.items():
            new_table = self._new_table(group)
            if (table := await self._read_file(source_id)) is not None:
                new_table = pd.concat([table, new_table])
            await self._write_file(source_id, new_table)

        return [cutout.measurement_id for cutout in cutouts if cutout.measurement_id]

    async def retrieve_cutouts_for_source(self, source_id: int) -> list[Cutout]:
        """
        Retrieve cutouts for a given source.
        """
        if not isinstance(source_id, UUID):
            source_id = UUID(str(source_id))

        if (table := await self._read_file(source_id)) is None:
            return []

        cutouts = []
        for measurement_id, row in table.iterrows():
            data = row.to_dict()
            data["measurement_id"] = measurement_id
            cutouts.append(Cutout.model_validate(data))

        return cutouts

    async def retrieve_cutout(self, source_id: int, measurement_id: int) -> Cutout:
        """
        Retrieve a cutout for a given source and band.
        """
        if not isinstance(source_id, UUID):
            source_id = UUID(str(source_id))

        if (table := await self._read_file(source_id)) is None:
            raise CutoutNotFoundException("Cutout table not found")

        measurement_id_str = str(measurement_id)
        if measurement_id_str not in table.index:
            raise CutoutNotFoundException(
                f"Cutout with measurement ID {measurement_id} not found"
            )

        row = table.loc[measurement_id_str].to_dict()
        row["measurement_id"] = measurement_id_str
        return Cutout.model_validate(row)

    async def delete(self, cutout_id: int) -> None:
        """
        Delete a cutout by ID.
        """
        cutout_id_str = str(cutout_id)
        if not self.base_path.exists():
            return

        for path in self.base_path.glob("*.parquet"):
            source_id = UUID(path.stem)
            table = await self._read_file(source_id)
            if table is None or cutout_id_str not in table.index:
                continue

            table.drop(cutout_id_str, axis=0, inplace=True)
            await self._write_file(source_id, table)
            return
