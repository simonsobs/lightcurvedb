"""
Band model.
"""

from pydantic import BaseModel


class Band(BaseModel):
    name: str
    telescope: str
    instrument: str
    frequency: float
