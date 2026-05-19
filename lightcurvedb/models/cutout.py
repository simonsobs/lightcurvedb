"""
Cut-outs around sources.
"""

from datetime import datetime
from json import dumps as json_dumps
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel, field_serializer

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

    @field_serializer("data")
    def serialize_data(self, value, info):
        target = (info.context or {}).get("target")

        if target == "postgres":
            # Convert to Postgres array literal
            return json_dumps(value).replace("[", "{").replace("]", "}")

        # default: JSON-friendly list
        return value
