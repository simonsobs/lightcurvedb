"""
Server-side analysis functions.
"""
from pydantic import BaseModel

class BandStatistics(BaseModel):
    """
    Statistics over a band.
    """

    mean_flux: float | None
    min_flux: float | None
    max_flux: float | None
    data_points: int | None

