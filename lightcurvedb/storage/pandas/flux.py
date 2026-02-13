from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Iterable
from uuid import UUID

import pandas as pd
from asyncer import asyncify
from uuid_extensions import uuid7

from lightcurvedb.models import FluxMeasurementCreate
from lightcurvedb.models.statistics import SourceStatistics
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

    def _new_table(
        self, measurements: Iterable[FluxMeasurementCreate], measurement_ids: list[UUID]
    ) -> pd.DataFrame:
        rows = []
        for measurement, measurement_id in zip(measurements, measurement_ids):
            row = measurement.model_dump()
            row["measurement_id"] = str(measurement_id)
            row["source_id"] = str(measurement.source_id)
            rows.append(row)

        table = pd.DataFrame(rows)
        table.set_index("measurement_id", inplace=True)
        table["source_id"] = table["source_id"].astype(str)
        return table

    def _parse_band_name(self, band_name: str) -> tuple[str, int]:
        if "_" in band_name:
            module, frequency_str = band_name.split("_", 1)
            return module, int(frequency_str)
        if band_name.startswith("f") and band_name[1:].isdigit():
            return "all", int(band_name[1:])
        if band_name.isdigit():
            return "all", int(band_name)

        raise ValueError(
            "band_name must be in the form '<module>_<frequency>' or 'f<frequency>'"
        )

    async def setup(self) -> None:
        """
        Set up the flux storage system (e.g. create the tables).
        """
        self.base_path.mkdir(parents=True, exist_ok=True)

    async def create(self, measurement: FluxMeasurementCreate) -> int:
        """
        Insert single measurement.
        """
        measurement_id = uuid7()

        new_table = self._new_table([measurement], [measurement_id])

        if (table := await self._read_file(measurement.source_id)) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(measurement.source_id, new_table)

        return measurement_id

    async def create_batch(
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[int]:
        """
        Bulk insert
        """
        if not measurements:
            return []

        measurement_ids: list[UUID] = [uuid7() for _ in measurements]

        grouped: dict[UUID, list[tuple[FluxMeasurementCreate, UUID]]] = defaultdict(
            list
        )

        for measurement, measurement_id in zip(measurements, measurement_ids):
            grouped[measurement.source_id].append((measurement, measurement_id))

        for source_id, group in grouped.items():
            group_measurements = [m for m, _ in group]
            group_ids = [m_id for _, m_id in group]
            new_table = self._new_table(group_measurements, group_ids)

            if (table := await self._read_file(source_id)) is not None:
                new_table = pd.concat([table, new_table])

            await self._write_file(source_id, new_table)

        return measurement_ids

    async def get_statistics(
        self,
        source_id: int,
        band_name: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Retrieve statistical summary of flux measurements for a specific band and source.
        """
        if not isinstance(source_id, UUID):
            source_id = UUID(str(source_id))

        if (table := await self._read_file(source_id)) is None:
            raise ValueError(f"No flux data for source {source_id}")

        module, frequency = self._parse_band_name(band_name)
        filtered = table

        if module != "all":
            filtered = filtered[filtered["module"] == module]
        filtered = filtered[filtered["frequency"] == frequency]

        if start_time is not None:
            filtered = filtered[filtered["time"] >= start_time]
        if end_time is not None:
            filtered = filtered[filtered["time"] <= end_time]

        if filtered.empty:
            raise ValueError(
                f"No flux data for source {source_id} and band {band_name}"
            )

        flux = filtered["flux"]
        flux_err = filtered["flux_err"].replace(0, pd.NA)
        valid_err = flux_err.notna()

        weights = 1.0 / (flux_err[valid_err] ** 2)
        if weights.empty or weights.sum() == 0:
            weighted_mean = float("nan")
            weighted_error = float("nan")
        else:
            weighted_mean = (
                flux[valid_err] / (flux_err[valid_err] ** 2)
            ).sum() / weights.sum()
            weighted_error = 1.0 / weights.sum() ** 0.5

        return SourceStatistics(
            source_id=source_id,
            module=module,
            frequency=frequency,
            start_time=filtered["time"].min(),
            end_time=filtered["time"].max(),
            measurement_count=int(filtered.shape[0]),
            min_flux=float(flux.min()),
            max_flux=float(flux.max()),
            mean_flux=float(flux.mean()),
            stddev_flux=float(flux.std()),
            median_flux=float(flux.median()),
            weighted_mean_flux=float(weighted_mean),
            weighted_error_on_mean_flux=float(weighted_error),
        )

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
