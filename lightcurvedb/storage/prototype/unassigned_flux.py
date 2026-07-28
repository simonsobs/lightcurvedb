from typing import Protocol
from uuid import UUID

from lightcurvedb.models import UnassignedFluxMeasurement


class ProvidesUnassignedFluxMeasurementStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the unassigned flux storage system.
        """

    async def create(self, measurement: UnassignedFluxMeasurement) -> UUID:
        """
        Insert an unassigned flux measurement.
        """
        ...

    async def get(self, measurement_id: UUID) -> UnassignedFluxMeasurement:
        """
        Retrieve an unassigned flux measurement by ID.
        """
        ...

    async def get_for_source(self, source_id: UUID) -> list[UnassignedFluxMeasurement]:
        """
        Retrieve all unassigned flux measurements for a source.
        """
        ...

    async def move_to_source(self, *, source_id: UUID, target_source_id: UUID) -> None:
        """
        Move every measurement from one unassigned source to another.
        """
        ...

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete an unassigned flux measurement by ID.
        """
        ...
