"""
Response models.
"""

from datetime import datetime

from pydantic import BaseModel

from lightcurvedb.models.band import Band
from lightcurvedb.models.source import Source


class LightcurveBandData(BaseModel):
    """
    Time series data for a single band.
    """

    ids: list[int]
    times: list[datetime]
    ra: list[float]
    dec: list[float]
    ra_uncertainty: list[float | None]
    dec_uncertainty: list[float | None]
    i_flux: list[float]
    i_uncertainty: list[float | None]


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
