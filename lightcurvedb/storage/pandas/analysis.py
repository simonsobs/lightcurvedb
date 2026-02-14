"""
Analysis of flux measurements and lightcurves.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from uuid import UUID

import pandas as pd

from lightcurvedb.models.statistics import SourceStatistics
from lightcurvedb.storage.pandas.flux import PandasFluxMeasurementStorage
from lightcurvedb.storage.prototype.analysis import ProvidesAnalysis


class PandasAnalysis(ProvidesAnalysis):
    def __init__(self, flux_storage: PandasFluxMeasurementStorage):
        self.flux_storage = flux_storage

    async def setup(self) -> None:
        """
        Setup any necessary tables or indexes for analysis.
        """
        return None

    async def get_source_statistics_for_frequency_and_module(
        self,
        source_id: UUID,
        module: str,
        frequency: int,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Get statistics for a given source for a specific frequency and module.
        Supports "module = 'all'" to get statistics across all modules for the
        given frequency.
        """

        if (table := await self.flux_storage._read_file(source_id)) is None:
            raise ValueError(f"No flux data for source {source_id}")

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
                f"No flux data for source {source_id} and combination {module} {frequency}"
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

    async def get_source_statistics_for_frequency(
        self,
        source_id: UUID,
        frequency: int,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Get source statistics for a given frequency.
        """
        return await self.get_source_statistics_for_frequency_and_module(
            source_id=source_id,
            module="all",
            frequency=frequency,
            start_time=start_time,
            end_time=end_time,
        )

    async def get_source_statistics(
        self,
        source_id: UUID,
        collate_modules: bool = False,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> dict[str, SourceStatistics]:
        """
        Get source statistics across all frequencies and modules.
        """
        table = await self.flux_storage._read_file(source_id)
        if table is None or table.empty:
            return {}

        module_frequency_pairs = (
            table[["module", "frequency"]]
            .dropna()
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )

        pairs = [
            (module, int(frequency)) for module, frequency in module_frequency_pairs
        ]

        if collate_modules:
            unique_frequencies = sorted({frequency for _, frequency in pairs})
            pairs = [("all", freq) for freq in unique_frequencies]

        statistics = await asyncio.gather(
            *[
                self.get_source_statistics_for_frequency_and_module(
                    source_id=source_id,
                    module=module,
                    frequency=frequency,
                    start_time=start_time,
                    end_time=end_time,
                )
                for module, frequency in pairs
            ]
        )

        if collate_modules:
            return {str(stats.frequency): stats for stats in statistics}

        return {f"{stats.module}_{stats.frequency}": stats for stats in statistics}
