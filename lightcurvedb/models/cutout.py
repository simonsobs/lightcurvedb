"""
Cut-outs around sources.
"""

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    pass


class Cutout(BaseModel):
    id: int | None = None
    data: list[list[float]]

    time: datetime

    units: str

    source_id: int | None = None
    band_name: str | None = None
    flux_id: int | None = None
