from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from lightcurvedb.models.flux import FluxMeasurement


class InstrumentLightcurve(BaseModel):
    module: str
    frequency: int
    source_id: UUID
    measurement_id: list[UUID]
    time: list[datetime]
    ra: list[float]
    dec: list[float]
    flux: list[float]
    flux_err: list[float | None]
    extra: list[dict | None]

    def __len__(self):
        return len(self.measurement_id)

    def _measurement(self, index: int) -> FluxMeasurement:
        return FluxMeasurement(
            frequency=self.frequency,
            module=self.module,
            source_id=self.source_id,
            measurement_id=self.measurement_id[index],
            time=self.time[index],
            ra=self.ra[index],
            dec=self.dec[index],
            flux=self.flux[index],
            flux_err=self.flux_err[index],
            extra=self.extra[index],
            ra_uncertainty=None,
            dec_uncertainty=None,
        )

    def __iter__(self):
        for i in range(len(self.measurement_id)):
            yield self._measurement(i)

    def __getitem__(self, index: int) -> FluxMeasurement:
        return self._measurement(index)


class BinnedInstrumentLightcurve(BaseModel):
    frequency: int
    module: str
    source_id: UUID
    time: list[datetime]
    ra: list[float]
    dec: list[float]
    flux: list[float]
    flux_err: list[float | None]

    binning_strategy: Literal["1 day", "7 days", "30 days"]
    start_time: datetime
    end_time: datetime

    def __len__(self):
        return len(self.measurement_id)

    def _measurement(self, index: int) -> FluxMeasurement:
        return FluxMeasurement(
            frequency=self.frequency,
            source_id=self.source_id,
            measurement_id=None,
            module=self.module,
            time=self.time[index],
            ra=self.ra[index],
            dec=self.dec[index],
            flux=self.flux[index],
            flux_err=self.flux_err[index],
            extra=None,
            ra_uncertainty=None,
            dec_uncertainty=None,
        )

    def __iter__(self):
        for i in range(len(self.measurement_id)):
            yield self._measurement(i)

    def __getitem__(self, index: int) -> FluxMeasurement:
        return self._measurement(index)
