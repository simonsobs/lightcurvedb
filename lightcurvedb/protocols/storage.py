"""
Storage protocol.
"""

from datetime import datetime
from typing import Protocol
from lightcurvedb.models.flux import FluxMeasurement, FluxMeasurementCreate
from lightcurvedb.models.responses import LightcurveBandData, SourceStatistics
from lightcurvedb.models.source import Source, SourceCreate
from lightcurvedb.models.band import Band


class FluxMeasurementStorage(Protocol):
    """
    Protocol for flux measurement storage operations.
    """

    async def create(self, measurement: FluxMeasurementCreate) -> FluxMeasurement:
        """
        Insert single measurement.
        """
        ...

    async def create_batch(
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[int]:
        """
        Bulk insert measurements, returns souce ids inserted.
        """
        ...

    async def get_band_data(
        self, source_id: int, band_name: str
    ) -> LightcurveBandData:
        """
        Get all measurements for source/band as arrays
        """
        ...

    async def get_time_range(
        self,
        source_id: int,
        band_name: str,
        start_time: datetime,
        end_time: datetime,
    ) -> LightcurveBandData:
        """
        Get measurements in time range.
        """
        ...

    async def get_statistics(
        self, source_id: int, band_name: str
    ) -> SourceStatistics:
        """
        Compute statistics (database-side aggregation).
        """
        ...

    async def delete(self, id: int) -> None:
        """
        Delete a flux measurement by ID.
        """
        ...

    async def get_bands_for_source(self, source_id: int) -> list[str]:
        """
        Get distinct band names that have measurements for a given source.
        """
        ...


class DatabaseSetup(Protocol):
    """
    Protocol for database initialization.
    """

    async def create_schema(self) -> None:
        """
        Create all tables.
        """
        ...


class SourceStorage(Protocol):
    """
    Protocol for source storage operations.
    """

    async def create(self, source: SourceCreate) -> Source:
        """
        Create a source.
        """
        ...

    async def create_batch(self, sources: list[SourceCreate]) -> list[int]:
        """
        Bulk insert sources, returns list of created source IDs.
        """
        ...

    async def get(self, source_id: int) -> Source:
        """
        Get source by ID.
        """
        ...

    async def get_all(self) -> list[Source]:
        """
        Get all sources.
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
        Get all sources within rectangular RA/Dec bounds.
        """
        ...


class BandStorage(Protocol):
    """
    Protocol for band storage operations.
    """

    async def create(self, band: Band) -> Band:
        """
        Create a band.
        """
        ...

    async def create_batch(self, bands: list[Band]) -> int:
        """
        Bulk insert bands, returns count inserted.
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
        Delete a band by name.
        """
        ...


class FluxStorageBackend(Protocol):
    """
    Storage backend interface.
    """

    sources: SourceStorage
    bands: BandStorage
    fluxes: FluxMeasurementStorage

    async def create_schema(self) -> None:
        """
        Create all tables.
        """
        ...