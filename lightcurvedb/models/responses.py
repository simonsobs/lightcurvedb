"""
Response models.
"""

from datetime import datetime

from pydantic import BaseModel


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
