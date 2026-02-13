from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class SourceStatistics(BaseModel):
    """
    Summary of flux measurements for a given source, module, and frequency.
    Module can be "all" to get statistics across all modules for the given frequency.
    """

    source_id: UUID
    module: str
    frequency: int

    start_time: datetime
    end_time: datetime
    measurement_count: int

    min_flux: float
    max_flux: float
    mean_flux: float
    stddev_flux: float
    median_flux: float

    weighted_mean_flux: float
    weighted_error_on_mean_flux: float
