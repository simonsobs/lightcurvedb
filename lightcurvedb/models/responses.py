"""
Response models.
"""

from datetime import datetime

from pydantic import BaseModel

from lightcurvedb.models.flux import FluxMeasurement
from lightcurvedb.models.instrument import Instrument
from lightcurvedb.models.source import Source


class LightcurveBandData(BaseModel):
    """
    Time series data for a single band.
    """

    band_name: str
    source_id: int

    measurement_ids: list[int]
    times: list[datetime]
    ra: list[float]
    dec: list[float]
    ra_uncertainty: list[float | None]
    dec_uncertainty: list[float | None]
    flux: list[float]
    flux_err: list[float | None]

    def __iter__(self):
        for i in range(len(self.measurement_ids)):
            yield FluxMeasurement(
                measurement_id=self.measurement_ids[i],
                time=self.times[i],
                ra=self.ra[i],
                dec=self.dec[i],
                ra_uncertainty=self.ra_uncertainty[i],
                dec_uncertainty=self.dec_uncertainty[i],
                flux=self.flux[i],
                flux_err=self.flux_err[i],
                band_name=self.band_name,
                source_id=self.source_id,
            )

    def __len__(self):
        return len(self.measurement_ids)

    def __getitem__(self, index: int) -> FluxMeasurement:
        return FluxMeasurement(
            measurement_id=self.measurement_ids[index],
            time=self.times[index],
            ra=self.ra[index],
            dec=self.dec[index],
            ra_uncertainty=self.ra_uncertainty[index],
            dec_uncertainty=self.dec_uncertainty[index],
            flux=self.flux[index],
            flux_err=self.flux_err[index],
            band_name=self.band_name,
            source_id=self.source_id,
        )


class LightcurveBandResult(LightcurveBandData):
    source: Source
    band: Instrument


class LightcurveResult(BaseModel):
    source: Source
    bands: list[LightcurveBandData]


class SourceStatistics(BaseModel):
    """
    Statistical summary of flux measurements.
    """

    measurement_count: int
    min_flux: float | None
    max_flux: float | None
    mean_flux: float | None
    stddev_flux: float | None
    median_flux: float | None
    weighted_mean_flux: float | None
    weighted_error_on_mean_flux: float | None
    start_time: datetime | None
    end_time: datetime | None
