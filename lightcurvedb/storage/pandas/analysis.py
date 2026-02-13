"""
Analysis of flux measurements and lightcurves.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from uuid import UUID

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
        band_name = f"{module}_{frequency}" if module != "all" else f"f{frequency}"
        return await self.flux_storage.get_statistics(
            source_id=source_id,
            band_name=band_name,
            start_time=start_time,
            end_time=end_time,
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
