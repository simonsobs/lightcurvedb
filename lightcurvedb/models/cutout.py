"""
Cut-outs around sources.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy.types import ARRAY, FLOAT
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .band import BandTable
    from .flux import FluxMeasurementTable


class Cutout(BaseModel):
    id: int
    band: str

    time: datetime

    data: list[list[float]]
    units: str


class CutoutTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    data: list[list[float]] = Field(sa_type=ARRAY(FLOAT))

    band_name: str | None = Field(default=None, foreign_key="bands.name")
    band: "BandTable" = Relationship()

    flux_id: int = Field(foreign_key="flux_measurements.id")
    flux: "FluxMeasurementTable" = Relationship()
