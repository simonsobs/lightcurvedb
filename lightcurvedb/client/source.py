"""
Extensions to core for sources.
"""

import asyncio
from math import cos, pi

from lightcurvedb.models.instrument import band_name
from lightcurvedb.models.source import Source, SourceProperties
from lightcurvedb.storage.prototype.backend import Backend


async def source_read_all(backend: Backend) -> list[Source]:
    """
    Read all sources, with computed properties (e.g. median flux per band)
    merged in. Sources with no flux measurements are returned with
    `properties` left unset.
    """
    sources, median_flux_by_source = await asyncio.gather(
        backend.sources.get_all(),
        backend.analysis.get_median_flux_for_all_sources(),
    )

    for source in sources:
        per_band = median_flux_by_source.get(source.source_id)
        if per_band:
            source.properties = SourceProperties(
                median_flux={
                    band_name(frequency): value
                    for frequency, value in per_band.items()
                }
            )

    return sources


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
