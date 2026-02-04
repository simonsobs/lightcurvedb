"""
Source information
"""

from pydantic import BaseModel
from pydantic import Field as PydanticField


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


class SourceCreate(BaseModel):
    """
    Input model for creating sources.
    """

    name: str | None = None
    ra: float | None
    dec: float | None
    variable: bool = False
    extra: SourceMetadata | None = None


class Source(SourceCreate):
    id: int | None = None
