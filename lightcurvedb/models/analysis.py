"""
Server-side analysis functions.
"""
from datetime import datetime

from pydantic import BaseModel


class BandStatistics(BaseModel):
    """
    Statistics over a band.
    """
    weighted_mean_flux: float | None
    weighted_error_on_mean_flux: float | None
    min_flux: float | None
    max_flux: float | None
    data_points: int | None
    variance_flux: float | None


class BandTimeSeries(BaseModel):
    """
    Timeseries data for a band.
    """
    timestamps: list[datetime]
    mean_flux: list[float]