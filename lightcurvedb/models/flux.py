"""
Individual flux measurements.
"""

from datetime import datetime

from pydantic import BaseModel
from pydantic import Field as PydanticField


class MeasurementMetadata(BaseModel):
    """
    Additional metadata about flux measurements stored as a JSONB
    column.
    """

    flags: list[str] = PydanticField(default=[])


class FluxMeasurementCreate(BaseModel):
    band_name: str
    source_id: int
    time: datetime
    ra: float
    dec: float
    ra_uncertainty: float
    dec_uncertainty: float
    i_flux: float
    i_uncertainty: float
    extra: MeasurementMetadata | None = None


class FluxMeasurement(FluxMeasurementCreate):
    """
    Flux measurement domain model.
    """

    id: int | None = None
