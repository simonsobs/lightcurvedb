"""
Source information, reproduced from the source catalog
"""

from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from .flux import FluxMeasurementTable


class Source(BaseModel):
    """
    A source as tracked in socat
    """

    id: int
    # The ID in socat

    ra: float | None
    dec: float | None

    variable: bool
    # Whether the source has variable position; ra, dec should be None in this instance and you
    # will have to ask socat for the information


class SourceTable(SQLModel, Source, table=True):
    __tablename__ = "sources"

    id: int = Field(primary_key=True)
    flux_measurements: list["FluxMeasurementTable"] = Relationship(
        back_populates="source"
    )
