"""
Cut-outs around sources.
"""

from datetime import datetime

import numpy as np
from pydantic import BaseModel
from pydantic_numpy import np_array_pydantic_annotated_typing
from sqlalchemy.types import ARRAY, FLOAT
from sqlmodel import Field, SQLModel


class Cutout(BaseModel):
    id: int
    band: str

    time: datetime

    data: np_array_pydantic_annotated_typing(data_type=np.float32, dimensions=2)
    units: str


class CutoutTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    data: np_array_pydantic_annotated_typing(data_type=np.float32, dimensions=2) = Field(sa_type=ARRAY(FLOAT))

    flux_id: int = Field(foreign_key="flux_measurements.id")
