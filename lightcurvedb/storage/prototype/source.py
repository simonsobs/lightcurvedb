from typing import Protocol

from lightcurvedb.models import Source, SourceCreate


class ProvidesSourceStorage(Protocol):
    async def create(self, source: SourceCreate) -> int:
        """
        Create a new source and return its ID.
        """
        ...

    async def create_batch(self, sources: list[SourceCreate]) -> list[int]:
        """
        Bulk insert sources, returns created source IDs.
        """
        ...

    async def get(self, source_id: int) -> Source:
        """
        Retrieve source details by ID.
        """
        ...

    async def get_all(self) -> list[Source]:
        """
        Retrieve all sources.
        """
        ...

    async def delete(self, source_id: int) -> None:
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
