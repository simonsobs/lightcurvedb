import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class IndividualLightcurve(BaseModel):
    source_id: UUID
    measurement_id: list[UUID]
    time: list[datetime.datetime]
    ra: list[float]
    dec: list[float]
    flux: list[float]
    flux_err: list[float | None]
    extra: list[dict | None]

    def __len__(self):
        return len(self.measurement_id)

    def __iter__(self):
        for i in range(len(self.measurement_id)):
            yield {
                "source_id": self.source_id,
                "measurement_id": self.measurement_id[i],
                "time": self.time[i],
                "ra": self.ra[i],
                "dec": self.dec[i],
                "flux": self.flux[i],
                "flux_err": self.flux_err[i],
                "extra": self.extra[i],
            }

    def __getitem__(self, index: int) -> dict:
        return {
            "source_id": self.source_id,
            "measurement_id": self.measurement_id[index],
            "time": self.time[index],
            "ra": self.ra[index],
            "dec": self.dec[index],
            "flux": self.flux[index],
            "flux_err": self.flux_err[index],
            "extra": self.extra[index],
        }


class BinnedLightcurve(BaseModel):
    source_id: UUID

    time: list[datetime.datetime]
    ra: list[float]
    dec: list[float]
    flux: list[float]
    flux_err: list[float | None]

    binning_strategy: Literal["1 day", "7 days", "30 days"]
    start_time: datetime.datetime
    end_time: datetime.datetime

    def __len__(self):
        return len(self.time)

    def __iter__(self):
        for i in range(len(self.time)):
            yield {
                "source_id": self.source_id,
                "time": self.time[i],
                "ra": self.ra[i],
                "dec": self.dec[i],
                "flux": self.flux[i],
                "flux_err": self.flux_err[i],
            }

    def __getitem__(self, index: int) -> dict:
        return {
            "source_id": self.source_id,
            "time": self.time[index],
            "ra": self.ra[index],
            "dec": self.dec[index],
            "flux": self.flux[index],
            "flux_err": self.flux_err[index],
        }


class InstrumentLightcurve(IndividualLightcurve):
    module: str
    frequency: int


class FrequencyLightcurve(IndividualLightcurve):
    frequency: int


class BinnedInstrumentLightcurve(BinnedLightcurve):
    module: str
    frequency: int


class BinnedFrequencyLightcurve(BinnedLightcurve):
    frequency: int


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
