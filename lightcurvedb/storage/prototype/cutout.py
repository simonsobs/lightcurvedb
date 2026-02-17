"""
Prototype for cutout storage interactions
"""

from typing import Protocol
from uuid import UUID

from lightcurvedb.models import Cutout


class ProvidesCutoutStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the cutout storage system (e.g. create the tables).
        """
        ...

    async def create(self, cutout: Cutout) -> int:
        """
        Store a cutout for a given source and band.
        """
        ...

    async def create_batch(self, cutouts: list[Cutout]) -> list[int]:
        """
        Store a cutout for a given source and band.
        """
        ...

    async def retrieve_cutouts_for_source(self, source_id: UUID) -> list[Cutout]:
        """
        Retrieve cutouts for a given source.
        """
        ...

    async def retrieve_cutout(self, source_id: UUID, measurement_id: UUID) -> Cutout:
        """
        Retrieve a cutout for a given source and band.
        """
        ...

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete a cutout by ID.
        """
        ...
