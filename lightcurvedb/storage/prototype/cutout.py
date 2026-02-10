"""
Prototype for cutout storage interactions
"""

from typing import Protocol

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

    async def retrieve_cutouts_for_source(self, source_id: int) -> list[Cutout]:
        """
        Retrieve cutouts for a given source.
        """
        ...

    async def retrieve_cutout(self, source_id: int, flux_id: int) -> Cutout:
        """
        Retrieve a cutout for a given source and band.
        """
        ...

    async def delete(self, cutout_id: int) -> None:
        """
        Delete a cutout by ID.
        """
        ...
