"""
Analysis of flux measurements and lightcurves.
"""

from datetime import datetime
from typing import Protocol

from lightcurvedb.models.statistics import SourceStatistics


class ProvidesAnalysis(Protocol):
    async def setup(self) -> None:
        """
        Setup any necessary tables or indexes for analysis.
        """
        ...

    async def get_source_statistics_for_frequency_and_module(
        self,
        source_id: int,
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
        ...

    async def get_source_statistics_for_frequency(
        self,
        source_id: int,
        frequency: int,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Get source statistics for a given frequency.
        """
        ...

    async def get_source_statistics(
        self,
        source_id: int,
        collate_modules: bool = False,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> dict[str, SourceStatistics]:
        """
        Get source statistics across all frequencies and modules.
        """
        ...
