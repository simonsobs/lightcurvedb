"""
Cut-outs around sources.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.types import ARRAY, FLOAT
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .band import BandTable
    from .flux import FluxMeasurementTable


class Cutout(BaseModel):
    id: int | None = None
    data: list[list[float]]

    time: datetime

    units: str

    source_id: int | None = None
    band_name: str | None = None
    flux_id: int | None = None

