from typing import Literal, Protocol
from uuid import UUID

from lightcurvedb.models import UnassignedSource


class ProvidesUnassignedSourceStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the unassigned source storage system.
        """

    async def create(self, source: UnassignedSource) -> UUID:
        """
        Create an unassigned source and return its ID.
        """
        ...

    async def get(self, source_id: UUID) -> UnassignedSource:
        """
        Retrieve an unassigned source by ID.
        """
        ...

    async def get_all(
        self,
        status: Literal["unmatched", "merged", "external_match", "novel", "noise"]
        | None = None,
    ) -> list[UnassignedSource]:
        """
        Retrieve all unassigned sources, optionally filtered by status.
        """
        ...

    async def get_in_radius(
        self,
        *,
        ra: float,
        dec: float,
        radius_arcmin: float,
        status: Literal["unmatched", "merged", "external_match", "novel", "noise"]
        | None = None,
    ) -> list[UnassignedSource]:
        """Retrieve sources within an ICRS great-circle radius."""
        ...

    async def delete(self, source_id: UUID) -> None:
        """
        Delete an unassigned source by ID.
        """
        ...
