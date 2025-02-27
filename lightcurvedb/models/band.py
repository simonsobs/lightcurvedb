"""
Band model and information.
"""

from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class Band(BaseModel):
    name: str
    telescope: str
    instrument: str
    frequency: float


class BandTable(SQLModel, Band, table=True):
    __tablename__ = "bands"

    name: str = Field(primary_key=True)

    def to_model(self) -> Band:
        return Band(**self.model_dump())