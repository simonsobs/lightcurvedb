"""
Responses from the feed.
"""

from datetime import datetime

from pydantic import BaseModel


class FeedResultItem(BaseModel):
    source_id: int
    source_name: str | None = None
    ra: float
    dec: float

    time: list[datetime]
    flux: list[float]


class FeedResult(BaseModel):
    items: list[FeedResultItem]

    start: int
    stop: int

    band_name: str

    total_number_of_sources: int
