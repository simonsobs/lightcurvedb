"""
Provider for lightcurves from postgres data stores.
"""

import datetime
from typing import Literal, Protocol, overload
from uuid import UUID

from lightcurvedb.models.lightcurves import (
    BinnedFrequencyLightcurve,
    BinnedInstrumentLightcurve,
    FrequencyLightcurve,
    InstrumentLightcurve,
    SourceLightcurveBinnedFrequency,
    SourceLightcurveBinnedInstrument,
    SourceLightcurveFrequency,
    SourceLightcurveInstrument,
)


class ProvidesLightcurves(Protocol):
    """
    Provides lightcurves from a PostgreSQL data store.
    """

    async def setup(self) -> None:
        """
        Set up the instrument storage system (e.g. create the tables).
        """
        ...

    async def get_instrument_lightcurve(
        self, source_id: UUID, module: str, frequency: int
    ) -> InstrumentLightcurve:
        """
        Get a lightcurve for a specific source, module, and frequency.
        """
        ...

    async def get_frequency_lightcurve(
        self, source_id: UUID, frequency: int
    ) -> FrequencyLightcurve:
        """
        Get a lightcurve for a specific source andd frequency, for all modules.
        """
        ...

    async def get_binned_instrument_lightcurve(
        self,
        source_id: UUID,
        module: str,
        frequency: int,
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> BinnedInstrumentLightcurve:
        """
        Get a binned lightcurve for a specific source, module, and frequency.
        """
        ...

    async def get_binned_frequency_lightcurve(
        self,
        source_id: UUID,
        frequency: int,
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> BinnedFrequencyLightcurve:
        """
        Get a binned lightcurve for a specific source and frequency, for all modules.
        """
        ...

    async def get_frequencies_for_source(self, source_id: UUID) -> list[int]:
        """
        Get all frequencies for a given source.
        """
        ...

    async def get_module_frequency_pairs_for_source(
        self, source_id: UUID
    ) -> list[tuple[str, int]]:
        """
        Get all modules for a given source.
        """
        ...

    @overload
    async def get_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency"],
    ) -> SourceLightcurveFrequency: ...

    @overload
    async def get_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["instrument"],
    ) -> SourceLightcurveInstrument: ...

    async def get_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency", "instrument"],
    ) -> SourceLightcurveInstrument | SourceLightcurveFrequency:
        """
        Get a lightcurve for a specific source, with the given strategy and binning.
        """
        ...

    @overload
    async def get_binned_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency"],
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> SourceLightcurveBinnedFrequency: ...

    @overload
    async def get_binned_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["instrument"],
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> SourceLightcurveBinnedInstrument: ...

    async def get_binned_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency", "instrument"],
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ) -> SourceLightcurveBinnedFrequency | SourceLightcurveBinnedInstrument:
        """
        Get a binned lightcurve for a specific source, with the given strategy and binning.
        """
        ...
