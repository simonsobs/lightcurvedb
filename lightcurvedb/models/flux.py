"""
Individual flux measurements.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from pydantic import Field as PydanticField
from sqlmodel import Field, Relationship, SQLModel

from .json import JSONEncodedPydantic

if TYPE_CHECKING:
    from .band import BandTable
    from .source import SourceTable


class MeasurementMetadata(BaseModel):
    """
    Additional metadata about flux measurements stored as a JSONB
    column.
    """

    flags: list[str] = PydanticField(default=[])


class FluxMeasurement(BaseModel):
    """
    An individual flux measurement, potentially registered to a source.
    """

    id: int | None = None
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


class FluxMeasurementTable(FluxMeasurement, SQLModel, table=True):
    __tablename__ = "flux_measurements"
    id: int = Field(primary_key=True)

    source_id: int | None = Field(default=None, foreign_key="sources.id")
    source: "SourceTable" = Relationship(back_populates="flux_measurements")

    band_name: str | None = Field(default=None, foreign_key="bands.name")
    band: "BandTable" = Relationship()

    extra: MeasurementMetadata | None = Field(
        default=None, sa_type=JSONEncodedPydantic(MeasurementMetadata)
    )
