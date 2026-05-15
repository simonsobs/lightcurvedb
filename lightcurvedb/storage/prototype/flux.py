from io import BytesIO
from typing import Literal, Protocol
from uuid import UUID

from lightcurvedb.models import FluxMeasurement


class ProvidesFluxMeasurementStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the flux storage system (e.g. create the tables).
        """

    async def create(self, measurement: FluxMeasurement) -> UUID:
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
        self,
        measurements: list[FluxMeasurement],
        bulk_insert_mode: Literal["unnest", "json", "csv"] | None = None,
    ) -> None:
        """
        Bulk insert
        """
        ...

    async def ingest_dataframe(
        self,
        parquet_bytes: BytesIO,
        parquet_ingest_mode: Literal["csv", "duckdb"] | None = None,
    ) -> None:
        """
        Bulk insert from a DataFrame, usually a transferred Parquet file.
        """
        ...

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete a flux measurement by ID.
        """
        ...
