from typing import Protocol
from uuid import UUID

from lightcurvedb.models import Source


class ProvidesSourceStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the source storage system (e.g. create the tables).
        """

    async def create(self, source: Source) -> UUID:
        """
        Create a new source and return its ID.
        """
        ...

    async def create_batch(self, sources: list[Source]) -> list[UUID]:
        """
        Bulk insert sources, returns created source IDs.
        """
        ...

    async def get(self, source_id: UUID) -> Source:
        """
        Retrieve source details by ID.
        """
        ...

    async def get_by_socat_id(self, socat_id: int) -> Source:
        """
        Retrieve source details by SoCat ID.
        """
        ...

    async def get_all(self) -> list[Source]:
        """
        Retrieve all sources.
        """
        ...

    async def delete(self, source_id: UUID) -> None:
        """
        Delete a source by ID.
        """
        ...

    async def get_in_bounds(
        self, ra_min: float, ra_max: float, dec_min: float, dec_max: float
    ) -> list[Source]:
        """
        Retrieve sources within specified RA/Dec bounds.
        """
        ...
