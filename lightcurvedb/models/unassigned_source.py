"""
Unassigned source information.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel
from pydantic import Field as PydanticField


class UnassignedSourceMetadata(BaseModel):
    """
    Additional metadata about unassigned sources stored as a JSONB column.
    """

    flags: list[str] = PydanticField(default=[])


class UnassignedSource(BaseModel):
    """
    A source which has not yet been registered in the source catalog.
    """

    source_id: UUID
    ra: float
    dec: float
    first_seen: datetime
    last_seen: datetime
    status: Literal["unmatched", "merged", "external_match", "novel", "noise"] = (
        "unmatched"
    )
    version: int = 1
    extra: UnassignedSourceMetadata | None = None
