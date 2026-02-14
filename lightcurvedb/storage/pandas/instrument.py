from pathlib import Path

import pandas as pd
from asyncer import asyncify

from lightcurvedb.models import Instrument
from lightcurvedb.models.exceptions import InstrumentNotFoundException
from lightcurvedb.storage.prototype.instrument import ProvidesInstrumentStorage


class PandasInstrumentStorage(ProvidesInstrumentStorage):
    def __init__(self, path: Path):
        self.path = path

        self._read_file = asyncify(self._read_file_sync)
        self._write_file = asyncify(self._write_file_sync)

    async def setup(self) -> None:
        """
        Set up the instrument storage system (e.g. create the tables).
        """
        pass

    def _read_file_sync(self) -> pd.DataFrame | None:
        if not self.path.exists():
            return None
        return pd.read_parquet(self.path)

    def _write_file_sync(self, table: pd.DataFrame) -> None:
        table.to_parquet(self.path)

    async def create(self, instrument: Instrument) -> str:
        """
        Create a single instrument in the storage system.
        """
        index = pd.MultiIndex.from_tuples(
            [(instrument.frequency, instrument.module)],
            names=["frequency", "module"],
        )
        new_table = pd.DataFrame([instrument.model_dump()], index=index)

        if (table := await self._read_file()) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(new_table)

        return instrument.instrument

    async def create_batch(self, instruments: list[Instrument]) -> list[str]:
        """
        Bulk insert instrument.
        """
        index = pd.MultiIndex.from_tuples(
            [(instrument.frequency, instrument.module) for instrument in instruments],
            names=["frequency", "module"],
        )
        new_table = pd.DataFrame(
            [instrument.model_dump() for instrument in instruments], index=index
        )

        if (table := await self._read_file()) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(new_table)

        return [i.instrument for i in instruments]

    async def get(self, frequency: int, module: str) -> Instrument:
        """
        Get instrument by frequency and module.
        """
        if (table := await self._read_file()) is None:
            raise InstrumentNotFoundException("Table not found")

        try:
            row = table.loc[(frequency, module)]
        except KeyError:
            raise InstrumentNotFoundException(
                f"Instrument with frequency {frequency} and module {module} not found"
            )

        return Instrument.model_validate(row.to_dict())

    async def get_all(self) -> list[Instrument]:
        """
        Get all registered instruments.
        """
        if (table := await self._read_file()) is None:
            return []

        return [Instrument.model_validate(row.to_dict()) for _, row in table.iterrows()]

    async def delete(self, frequency: int, module: str) -> None:
        """
        Delete instrument by frequency and module.
        """
        if (table := await self._read_file()) is None:
            return

        try:
            table.drop((frequency, module), axis=0, inplace=True)
        except KeyError:
            raise InstrumentNotFoundException(
                f"Instrument with frequency {frequency} and module {module} not found"
            )

        await self._write_file(table)
