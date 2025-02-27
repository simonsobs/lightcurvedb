"""
Individual flux measurements.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .band import BandTable
    from .source import SourceTable


class FluxMeasurement(BaseModel):
    """
    An individual flux measurement, potentially registered to a source.
    """

    id: int
    band: str  # Band should not be str. It should be an enumeration or a linked table

    time: datetime

    i_flux: float
    i_uncertainty: float

    q_flux: float
    q_uncertainty: float

    u_flux: float
    u_uncertainty: float


class FluxMeasurementTable(FluxMeasurement, SQLModel, table=True):
    __tablename__ = "flux_measurements"
    id: int = Field(primary_key=True)

    source_id: int | None = Field(default=None, foreign_key="sources.id")
    source: "SourceTable" = Relationship(back_populates="flux_measurements")

    band_name: str | None = Field(default=None, foreign_key="bands.name")
    band: "BandTable" = Relationship()
