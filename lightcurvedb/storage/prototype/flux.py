from datetime import datetime
from typing import Protocol

from lightcurvedb.models import (
    FluxMeasurementCreate,
)
from lightcurvedb.models.responses import LightcurveBandData, SourceStatistics


class ProvidesFluxMeasurementStorage(Protocol):
    async def setup(self) -> None:
        """
        Set up the flux storage system (e.g. create the tables).
        """

    async def create(self, measurement: FluxMeasurementCreate) -> int:
        """
        Insert single measurement.
        """
        ...

    async def create_batch(
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[int]:
        """
        Bulk insert
        """
        ...

    async def get_band_data(
        self,
        source_id: int,
        band_name: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> LightcurveBandData:
        """
        Retrieve lightcurve data for a specific band and source.
        """

    async def get_statistics(
        self,
        source_id: int,
        band_name: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Retrieve statistical summary of flux measurements for a specific band and source.
        """
        ...

    async def delete(self, id: int) -> None:
        """
        Delete a flux measurement by ID.
        """
        ...

    async def get_bands_for_source(self, source_id: int) -> list[str]:
        """
        Get all band names associated with a source.
        """
        ...

    async def get_recent_measurements(
        self, source_id: int, band_name: str, limit: int
    ) -> LightcurveBandData:
        """
        Get the most recent flux measurements for a source and band.
        """
        ...
