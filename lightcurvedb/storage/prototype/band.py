from typing import Protocol

from lightcurvedb.models import Band


class ProvidesBandStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the band storage system (e.g. create the tables).
        """

    async def create(self, band: Band) -> str:
        """
        Create a single band in the storage system.
        """
        ...

    async def create_batch(self, bands: list[Band]) -> list[str]:
        """
        Bulk insert bands.
        """
        ...

    async def get(self, band_name: str) -> Band:
        """
        Get band by name.
        """
        ...

    async def get_all(self) -> list[Band]:
        """
        Get all bands.
        """
        ...

    async def delete(self, band_name: str) -> None:
        """
        Delete band by name.
        """
        ...
