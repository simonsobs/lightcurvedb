from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Iterable
from uuid import UUID

import pandas as pd
from asyncer import asyncify
from uuid_extensions import uuid7

from lightcurvedb.models import FluxMeasurementCreate
from lightcurvedb.storage.prototype.flux import ProvidesFluxMeasurementStorage


class PandasFluxMeasurementStorage(ProvidesFluxMeasurementStorage):
    def __init__(self, base_path: Path):
        if base_path.suffix == ".parquet":
            base_path = base_path.with_suffix("")

        self.base_path = base_path

        self._read_file = asyncify(self._read_file_sync)
        self._write_file = asyncify(self._write_file_sync)

    def _source_path(self, source_id: UUID) -> Path:
        return self.base_path / f"{source_id}.parquet"

    def _read_file_sync(self, source_id: UUID) -> pd.DataFrame | None:
        path = self._source_path(source_id)
        if not path.exists():
            return None
        table = pd.read_parquet(path)
        return self._normalize_table(table)

    def _write_file_sync(self, source_id: UUID, table: pd.DataFrame) -> None:
        self.base_path.mkdir(parents=True, exist_ok=True)
        table.to_parquet(self._source_path(source_id))

    def _normalize_table(self, table: pd.DataFrame) -> pd.DataFrame:
        if "measurement_id" in table.columns:
            table = table.set_index("measurement_id")

        table.index = table.index.astype(str)

        if "source_id" in table.columns:
            table["source_id"] = table["source_id"].astype(str)

        if "time" in table.columns:
            table["time"] = pd.to_datetime(table["time"], utc=False)

        return table

    def _new_table(self, measurements: Iterable[FluxMeasurementCreate]) -> pd.DataFrame:
        table = pd.DataFrame(
            [
                {
                    **measurement.model_dump(),
                    "measurement_id": str(uuid7()),
                    "source_id": str(measurement.source_id),
                }
                for measurement in measurements
            ]
        )

        table.set_index("measurement_id", inplace=True)

        return table

    async def setup(self) -> None:
        """
        Set up the flux storage system (e.g. create the tables).
        """
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def create(self, measurement: FluxMeasurementCreate) -> int:
        """
        Insert single measurement.
        """
        new_table = self._new_table([measurement])
        new_id = new_table.index.tolist()[0]

        if (table := await self._read_file(measurement.source_id)) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(measurement.source_id, new_table)

        return new_id

    async def create_batch(
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[int]:
        """
        Bulk insert
        """

        grouped_measurements = defaultdict(list)

        for measurement in measurements:
            grouped_measurements[measurement.source_id].append(measurement)

        measurement_ids = []

        for source_id, group in grouped_measurements.items():
            new_table = self._new_table(group)
            measurement_ids.extend(new_table.index.tolist())

            if (table := await self._read_file(source_id)) is not None:
                new_table = pd.concat([table, new_table])

            await self._write_file(source_id, new_table)

        return measurement_ids

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete a flux measurement by ID.
        """
        measurement_id = str(measurement_id)
        if not self.base_path.exists():
            return

        for path in self.base_path.glob("*.parquet"):
            source_id = UUID(path.stem)
            table = await self._read_file(source_id)
            if table is None:
                continue

            if measurement_id not in table.index:
                continue

            table.drop(measurement_id, axis=0, inplace=True)
            await self._write_file(source_id, table)
            return
