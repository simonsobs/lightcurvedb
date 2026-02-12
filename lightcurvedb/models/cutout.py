"""
Cut-outs around sources.
"""

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel

if TYPE_CHECKING:
    pass


class Cutout(BaseModel):
    measurement_id: UUID | None = None

    data: list[list[float]]

    time: datetime
    units: str

    frequency: int
    module: str

    source_id: UUID | None = None
