from typing import Protocol
from uuid import UUID

from lightcurvedb.models import FluxMeasurement, FluxMeasurementCreate


class ProvidesFluxMeasurementStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the flux storage system (e.g. create the tables).
        """

    async def create(self, measurement: FluxMeasurementCreate) -> UUID:
        """
        Insert single measurement.
        """
        ...

    async def get(self, measurement_id: UUID) -> FluxMeasurement:
        """
        Retrieve a flux measurement by ID.
        """
        ...

    async def create_batch(
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[int]:
        """
        Bulk insert
        """
        ...

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete a flux measurement by ID.
        """
        ...
