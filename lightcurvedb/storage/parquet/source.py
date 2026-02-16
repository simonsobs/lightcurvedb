from pathlib import Path
from uuid import UUID

import pandas as pd
from asyncer import asyncify

from lightcurvedb.models import Source
from lightcurvedb.models.exceptions import SourceNotFoundException
from lightcurvedb.storage.prototype.source import ProvidesSourceStorage


class PandasSourceStorage(ProvidesSourceStorage):
    def __init__(self, path: Path):
        self.path = path

        self._read_file = asyncify(self._read_file_sync)
        self._write_file = asyncify(self._write_file_sync)

    async def setup(self):
        """
        Set up the source storage system (e.g. create the tables).
        """
        pass

    def _read_file_sync(self) -> pd.DataFrame | None:
        if not self.path.exists():
            return None
        return pd.read_parquet(self.path)

    def _write_file_sync(self, table: pd.DataFrame) -> None:
        table.to_parquet(self.path)

    async def create(self, source: Source) -> UUID:
        """
        Create a new source and return its ID.
        """
        new_table = pd.DataFrame([source.model_dump()])
        # Parquet writers don't natively support UUIDs.
        # Use strings, they're friendlier (though less compact) than bytes.
        new_table["source_id"] = new_table["source_id"].astype(str)
        new_table.set_index("source_id", inplace=True)

        if (table := await self._read_file()) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(new_table)

        return source.source_id

    async def create_batch(self, sources: list[Source]) -> list[UUID]:
        """
        Bulk insert sources, returns created source IDs.
        """
        new_table = pd.DataFrame([s.model_dump() for s in sources])
        new_table["source_id"] = new_table["source_id"].astype(str)
        new_table.set_index("source_id", inplace=True)

        if (table := await self._read_file()) is not None:
            new_table = pd.concat([table, new_table])

        await self._write_file(new_table)

        return [source.source_id for source in sources]

    async def get(self, source_id: UUID) -> Source:
        """
        Retrieve source details by ID.
        """
        if (table := await self._read_file()) is None:
            raise SourceNotFoundException("Table not found")

        try:
            row = table.loc[str(source_id)]
        except KeyError:
            raise SourceNotFoundException(f"Source with ID {source_id} not found")

        data = row.to_dict()
        data["source_id"] = str(source_id)
        return Source.model_validate(data)

    async def get_by_socat_id(self, socat_id: int) -> Source:
        """
        Retrieve source details by SoCat ID.
        """
        if (table := await self._read_file()) is None:
            raise SourceNotFoundException("Table not found")

        row = table.loc[table["socat_id"] == socat_id]

        if row.empty:
            raise SourceNotFoundException(f"Source with SoCat ID {socat_id} not found")

        data = row.iloc[0].to_dict()
        data["source_id"] = str(row.index[0])
        return Source.model_validate(data)

    async def get_all(self) -> list[Source]:
        """
        Retrieve all sources.
        """
        if (table := await self._read_file()) is None:
            return []

        sources = []
        for source_id, row in table.iterrows():
            data = row.to_dict()
            data["source_id"] = str(source_id)
            sources.append(Source.model_validate(data))
        return sources

    async def delete(self, source_id: UUID) -> None:
        """
        Delete a source by ID.
        """
        if (table := await self._read_file()) is None:
            raise SourceNotFoundException("Table not found")

        try:
            table.drop(str(source_id), axis=0, inplace=True)
        except KeyError:
            raise SourceNotFoundException(f"Source with ID {source_id} not found")

        await self._write_file(table)

    async def get_in_bounds(
        self, ra_min: float, ra_max: float, dec_min: float, dec_max: float
    ) -> list[Source]:
        """
        Retrieve sources within specified RA/Dec bounds.
        """
        if (table := await self._read_file()) is None:
            return []

        in_bounds = table[
            (table["ra"] >= ra_min)
            & (table["ra"] <= ra_max)
            & (table["dec"] >= dec_min)
            & (table["dec"] <= dec_max)
        ]

        sources = []
        for source_id, row in in_bounds.iterrows():
            data = row.to_dict()
            data["source_id"] = str(source_id)
            sources.append(Source.model_validate(data))
        return sources
