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
