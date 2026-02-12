"""
Individual flux measurements.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel
from pydantic import Field as PydanticField


class MeasurementMetadata(BaseModel):
    """
    Additional metadata about flux measurements stored as a JSONB
    column.
    """

    flags: list[str] = PydanticField(default=[])


class FluxMeasurementCreate(BaseModel):
    frequency: int
    module: str
    source_id: UUID
    time: datetime
    ra: float
    dec: float
    ra_uncertainty: float | None
    dec_uncertainty: float | None
    flux: float
    flux_err: float
    extra: MeasurementMetadata | None = None


class FluxMeasurement(FluxMeasurementCreate):
    """
    Flux measurement domain model.
    """

    measurement_id: UUID | None = None
    module: str | None = None
