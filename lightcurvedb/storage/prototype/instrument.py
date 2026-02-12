from typing import Protocol

from lightcurvedb.models import Instrument


class ProvidesInstrumentStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the instrument storage system (e.g. create the tables).
        """

    async def create(self, instrument: Instrument) -> str:
        """
        Create a single instrument in the storage system.
        """
        ...

    async def create_batch(self, instruments: list[Instrument]) -> list[str]:
        """
        Bulk insert instrument.
        """
        ...

    async def get(self, frequency: int, module: str) -> Instrument:
        """
        Get instrument by frequency and module.
        """
        ...

    async def get_all(self) -> list[Instrument]:
        """
        Get all registered instruments.
        """
        ...

    async def delete(self, frequency: int, module: str) -> None:
        """
        Delete instrument by frequency and module.
        """
        ...
