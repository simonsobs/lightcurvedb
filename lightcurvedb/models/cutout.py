"""
Cut-outs around sources.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy.types import ARRAY, FLOAT
from sqlmodel import Field, Relationship, SQLModel
from sqlalchemy.schema import ForeignKeyConstraint
if TYPE_CHECKING:
    from .band import BandTable
    from .flux import FluxMeasurementTable


class Cutout(BaseModel):
    id: int | None = None
    data: list[list[float]]

    time: datetime

    units: str

    band_name: str | None = None
    flux_id: int | None = None


class CutoutTable(SQLModel, Cutout, table=True):
    id: int = Field(primary_key=True)
    data: list[list[float]] = Field(sa_type=ARRAY(FLOAT))

    band_name: str | None = Field(default=None, foreign_key="bands.name")
    band: "BandTable" = Relationship()

    flux_id: int = Field()
    flux: "FluxMeasurementTable" = Relationship()
    __table_args__ = (
        ForeignKeyConstraint(
            ["flux_id", "time"],
            ["flux_measurements.id", "flux_measurements.time"],
        ),
    )

    def to_model(self) -> Cutout:
        return Cutout(
            id=self.id,
            data=self.data,
            band_name=self.band_name,
            flux_id=self.flux_id,
            time=self.time,
            units=self.units,
        )
