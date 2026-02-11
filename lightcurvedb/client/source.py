"""
Extensions to core for sources.
"""

from math import cos, pi

from pydantic import BaseModel

from lightcurvedb.client.measurement import (
    MeasurementSummaryResult,
    measurement_summary,
)
from lightcurvedb.models.instrument import Instrument
from lightcurvedb.models.source import Source
from lightcurvedb.storage.prototype.backend import Backend


class SourceSummaryResult(BaseModel):
    source: Source
    bands: list[Instrument]
    measurements: list[MeasurementSummaryResult]


async def source_read(id: int, backend: Backend) -> Source:
    """
    Read core metadata about a source.
    """
    return await backend.sources.get(id)


async def source_read_bands(id: int, backend: Backend) -> list[str]:
    """
    Read the bands names that are available for a source.
    """
    return await backend.fluxes.get_bands_for_source(id)


async def source_read_all(backend: Backend) -> list[Source]:
    """
    Read all sources available in the system.
    """
    return await backend.sources.get_all()


async def source_read_summary(id: int, backend: Backend) -> SourceSummaryResult:
    """
    Read the full summary for an individual source, including number of
    observations.
    """
    source = await source_read(id=id, backend=backend)
    available_bands = await source_read_bands(id=id, backend=backend)

    band_info = [await backend.bands.get(band_name=x) for x in available_bands]
    measurements = [
        await measurement_summary(source_id=id, band_name=x, backend=backend)
        for x in available_bands
    ]

    return SourceSummaryResult(
        source=source, bands=band_info, measurements=measurements
    )


async def source_read_in_radius(
    center: tuple[float, float], radius: float, backend: Backend
) -> list[Source]:
    """
    Read all sources within a square of 'radius' (degrees) of center (ra, dec, degrees,
    -180 < ra < 180, -90 < dec < 90). Can further be filtered to a circle if required.
    Takes into account geometry near the poles.
    """
    # Declination does not need wrapping; all wrapping happens in the RA dimension.
    ra, dec = center

    # Bounds checking - ra and dec must be in the right place!
    if not ((ra <= 180.0 and ra > -180.0) and (dec <= 90.0 and dec > -90.0)):
        raise ValueError(f"Ra, dec out of bounds {ra}, {dec}")

    if radius <= 0:
        raise ValueError(
            f"Radius value {radius} unacceptable, must be strictly positive"
        )

    cos_dec = cos(pi * dec / 180.0)

    bottom_left = (ra - radius / cos_dec, dec - radius)
    top_right = (ra + radius / cos_dec, dec + radius)

    # Swap top and bottom in extreme cases:
    if bottom_left[0] < -180.0:
        bottom_left = (bottom_left[0] + 360, bottom_left[1])

    if top_right[0] > 180.0:
        top_right = (top_right[0] - 360, top_right[1])

    return await backend.sources.get_in_bounds(
        ra_min=bottom_left[0],
        ra_max=top_right[0],
        dec_min=bottom_left[1],
        dec_max=top_right[1],
    )


async def source_add(source: Source, backend: Backend) -> int:
    """
    Add a source, returning its primary key.
    """
    created = await backend.sources.create(source)
    return created


async def source_delete(id: int, backend: Backend) -> None:
    """
    Delete a source (and all of its measurements!) from the system.
    """
    await backend.sources.delete(id)
