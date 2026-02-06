"""
Band model.
"""

from pydantic import BaseModel


class Band(BaseModel):
    band_name: str
    telescope: str
    instrument: str
    frequency: float
