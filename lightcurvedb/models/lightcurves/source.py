from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from .frequency import BinnedFrequencyLightcurve, FrequencyLightcurve
from .instrument import BinnedInstrumentLightcurve, InstrumentLightcurve


class SourceLightcurve(BaseModel):
    source_id: UUID
    selection_strategy: Literal["none", "frequency", "instrument"] = "none"
    binning_strategy: Literal["none", "1 day", "7 days", "30 days"] = "none"

    lightcurves: list[
        InstrumentLightcurve
        | FrequencyLightcurve
        | BinnedInstrumentLightcurve
        | BinnedFrequencyLightcurve
    ]

    def __len__(self):
        return len(self.lightcurves)

    def __iter__(self):
        for lc in self.lightcurves:
            yield lc

    def __getitem__(
        self, index: int
    ) -> (
        InstrumentLightcurve
        | FrequencyLightcurve
        | BinnedInstrumentLightcurve
        | BinnedFrequencyLightcurve
    ):
        return self.lightcurves[index]


class SourceLightcurveFrequency(SourceLightcurve):
    selection_strategy: Literal["frequency"] = "frequency"
    binning_strategy: Literal["none"] = "none"

    lightcurves: list[FrequencyLightcurve]


class SourceLightcurveInstrument(SourceLightcurve):
    selection_strategy: Literal["instrument"] = "instrument"
    binning_strategy: Literal["none"] = "none"

    lightcurves: list[InstrumentLightcurve]


class SourceLightcurveBinnedFrequency(SourceLightcurve):
    selection_strategy: Literal["frequency"] = "frequency"
    binning_strategy: Literal["1 day", "7 days", "30 days"]
    start_time: datetime
    end_time: datetime

    lightcurves: list[BinnedFrequencyLightcurve]


class SourceLightcurveBinnedInstrument(SourceLightcurve):
    selection_strategy: Literal["instrument"] = "instrument"
    binning_strategy: Literal["1 day", "7 days", "30 days"]
    start_time: datetime
    end_time: datetime

    lightcurves: list[BinnedInstrumentLightcurve]
