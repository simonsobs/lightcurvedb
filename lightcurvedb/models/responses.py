"""
Response models.
"""

from datetime import datetime

from pydantic import BaseModel

from lightcurvedb.models.band import Band
from lightcurvedb.models.flux import FluxMeasurement
from lightcurvedb.models.source import Source


class LightcurveBandData(BaseModel):
    """
    Time series data for a single band.
    """

    band_name: str
    source_id: int

    ids: list[int]
    times: list[datetime]
    ra: list[float]
    dec: list[float]
    ra_uncertainty: list[float | None]
    dec_uncertainty: list[float | None]
    i_flux: list[float]
    i_uncertainty: list[float | None]

    def __iter__(self):
        for i in range(len(self.ids)):
            yield FluxMeasurement(
                id=self.ids[i],
                time=self.times[i],
                ra=self.ra[i],
                dec=self.dec[i],
                ra_uncertainty=self.ra_uncertainty[i],
                dec_uncertainty=self.dec_uncertainty[i],
                i_flux=self.i_flux[i],
                i_uncertainty=self.i_uncertainty[i],
                band_name=self.band_name,
                source_id=self.source_id,
            )

    def __len__(self):
        return len(self.ids)

    def __getitem__(self, index: int) -> FluxMeasurement:
        return FluxMeasurement(
            id=self.ids[index],
            time=self.times[index],
            ra=self.ra[index],
            dec=self.dec[index],
            ra_uncertainty=self.ra_uncertainty[index],
            dec_uncertainty=self.dec_uncertainty[index],
            i_flux=self.i_flux[index],
            i_uncertainty=self.i_uncertainty[index],
            band_name=self.band_name,
            source_id=self.source_id,
        )


class LightcurveBandResult(LightcurveBandData):
    source: Source
    band: Band


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
