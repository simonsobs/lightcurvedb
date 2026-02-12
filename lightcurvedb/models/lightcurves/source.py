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

    lightcurves: dict[
        str | int,
        InstrumentLightcurve
        | FrequencyLightcurve
        | BinnedInstrumentLightcurve
        | BinnedFrequencyLightcurve,
    ]

    def __len__(self):
        return len(self.lightcurves)

    def __iter__(self):
        for lc in self.lightcurves.values():
            yield lc

    def items(self):
        return self.lightcurves.items()

    def __getitem__(
        self, index: str | int
    ) -> (
        InstrumentLightcurve
        | FrequencyLightcurve
        | BinnedInstrumentLightcurve
        | BinnedFrequencyLightcurve
    ):
        try:
            return self.lightcurves[index]
        except KeyError as e:
            if isinstance(index, str):
                if index.startswith("f"):
                    return self.lightcurves[int(index[1:])]
                else:
                    return self.lightcurves[int(index)]
            raise e


class SourceLightcurveFrequency(SourceLightcurve):
    selection_strategy: Literal["frequency"] = "frequency"
    binning_strategy: Literal["none"] = "none"

    lightcurves: dict[int, FrequencyLightcurve]


class SourceLightcurveInstrument(SourceLightcurve):
    selection_strategy: Literal["instrument"] = "instrument"
    binning_strategy: Literal["none"] = "none"

    lightcurves: dict[str, InstrumentLightcurve]


class SourceLightcurveBinnedFrequency(SourceLightcurve):
    selection_strategy: Literal["frequency"] = "frequency"
    binning_strategy: Literal["1 day", "7 days", "30 days"]
    start_time: datetime
    end_time: datetime

    lightcurves: dict[int, BinnedFrequencyLightcurve]


class SourceLightcurveBinnedInstrument(SourceLightcurve):
    selection_strategy: Literal["instrument"] = "instrument"
    binning_strategy: Literal["1 day", "7 days", "30 days"]
    start_time: datetime
    end_time: datetime

    lightcurves: dict[str, BinnedInstrumentLightcurve]
