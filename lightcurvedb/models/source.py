"""
Source information
"""

from uuid import UUID

from pydantic import BaseModel, Field
from pydantic import Field as PydanticField
from uuid_extensions import uuid7


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
    socat_id: UUID | None = None


class SourceProperties(BaseModel):
    """
    Additional properties about sources stored as a JSONB
    column.
    """

    # Keyed as f"{module}_{frequency}", e.g. "i1_90".
    median_flux: dict[str, float]


class Source(BaseModel):
    """
    Input model for creating sources.
    """

    source_id: UUID = Field(default_factory=uuid7)
    socat_id: UUID | None = None
    name: str | None = "No Name"
    ra: float | None
    dec: float | None
    variable: bool = False
    extra: SourceMetadata | None = None
    properties: SourceProperties | None = None
