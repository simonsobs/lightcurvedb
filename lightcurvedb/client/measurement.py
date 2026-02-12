"""
For individual measurements.
"""

import datetime

from pydantic import BaseModel

from lightcurvedb.models.flux import FluxMeasurement
from lightcurvedb.storage.prototype.backend import Backend


class MeasurementSummaryResult(BaseModel):
    source_id: int
    band_name: str
    start: datetime.datetime
    end: datetime.datetime
    count: int


async def measurement_flux_add(measurement: FluxMeasurement, backend: Backend) -> int:
    """
    Add a flux measurement.
    """
    created = await backend.fluxes.create(measurement)
    return created


async def measurement_flux_delete(id: int, backend: Backend) -> None:
    """
    Delete a flux measurement by ID.
    """
    await backend.fluxes.delete(id)


async def measurement_summary(
    source_id: int, band_name: str, backend: Backend
) -> MeasurementSummaryResult:
    """
    Get a measurement summary for a specific band and source ID.
    """
    stats = await backend.fluxes.get_statistics(source_id, band_name)

    return MeasurementSummaryResult(
        source_id=source_id,
        band_name=band_name,
        start=stats.start_time,
        end=stats.end_time,
        count=stats.measurement_count,
    )
