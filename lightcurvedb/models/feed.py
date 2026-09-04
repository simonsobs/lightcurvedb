"""
Responses from the feed.
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from lightcurvedb.models.instrument import band_name as format_band_name


class FeedResultItem(BaseModel):
    source_id: UUID
    source_name: str | None = None
    ra: float
    dec: float

    time: list[datetime]
    flux: list[float]


class FeedResult(BaseModel):
    items: list[FeedResultItem]

    start: int
    stop: int

    frequency: int

    total_number_of_sources: int

    @property
    def band_name(self) -> str:
        return format_band_name(self.frequency)
