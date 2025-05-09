"""
Source information, reproduced from the source catalog
"""

from typing import TYPE_CHECKING

from pydantic import BaseModel
from pydantic import Field as PydanticField
from sqlmodel import Field, Relationship, SQLModel

from .json import JSONEncodedPydantic

if TYPE_CHECKING:
    from .flux import FluxMeasurementTable


class CrossMatch(BaseModel):
    """
    A cross match between this source and another one.
    """

    name: str


class SourceMetadata(BaseModel):
    """
    Additional metadata about sources stored as a JSONB
    column.
    """

    cross_matches: list[CrossMatch] = PydanticField(default=[])


class Source(BaseModel):
    """
    A source as tracked in socat
    """

    id: int | None = None
    # The ID in socat

    ra: float | None
    dec: float | None

    variable: bool
    # Whether the source has variable position; ra, dec should be None in this instance and you
    # will have to ask socat for the information

    extra: SourceMetadata | None = None


class SourceTable(SQLModel, Source, table=True):
    __tablename__ = "sources"

    id: int = Field(primary_key=True)
    flux_measurements: list["FluxMeasurementTable"] = Relationship(
        back_populates="source"
    )
    extra: SourceMetadata | None = Field(
        sa_type=JSONEncodedPydantic(SourceMetadata), default=None
    )

    def to_model(self) -> Source:
        return Source(**self.model_dump())
