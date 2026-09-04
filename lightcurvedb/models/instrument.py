"""
Instrument model.
"""

from typing import Any

from pydantic import BaseModel


class Instrument(BaseModel):
    frequency: int
    module: str
    telescope: str
    instrument: str
    details: dict[str, Any]


def band_name(frequency: int) -> str:
    """
    Format a frequency (GHz) as its band name, e.g. 90 -> "f090".
    """
    return f"f{frequency:03d}"
