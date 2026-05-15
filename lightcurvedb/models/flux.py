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


class FluxMeasurement(BaseModel):
    measurement_id: UUID
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

    def model_dump_tuple(self) -> tuple:
        """
        Dump the model as a tuple of values, in the order of the fields.
        """
        return tuple(self.model_dump().values())

    def model_dump_sub_as_json(self) -> dict:
        """
        Dump the model as a dict, but with the `extra` field dumped as JSON.
        """
        d = self.model_dump()
        if d["extra"] is not None:
            d["extra"] = self.extra.model_dump_json()
        return d
