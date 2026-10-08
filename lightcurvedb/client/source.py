"""
Extensions to core for sources.
"""

import asyncio
from math import cos, pi

from lightcurvedb.models.source import Source, SourceProperties
from lightcurvedb.storage.prototype.backend import Backend


async def source_read_all(backend: Backend) -> list[Source]:
    """
    Read all sources, with computed properties (e.g. median flux per module
    and frequency) merged in. Sources with no flux measurements are returned
    with `properties` left unset.
    """
    sources, median_flux_by_source = await asyncio.gather(
        backend.sources.get_all(),
        backend.analysis.get_median_flux_for_all_sources(),
    )

    for source in sources:
        per_module_frequency = median_flux_by_source.get(source.source_id)
        if per_module_frequency:
            source.properties = SourceProperties(median_flux=per_module_frequency)

    return sources


async def source_read_in_radius(
    center: tuple[float, float], radius: float, backend: Backend
) -> list[Source]:
    """
    Read all sources within a square of 'radius' (degrees) of center (ra, dec, degrees,
    -90 < dec < 90). ra may be given in either the [-180, 180] or [0, 360) convention --
    get_in_bounds normalizes ra mod 360 on both sides of the comparison, so the box
    computed here doesn't need to be pre-wrapped into any particular range itself.
    Can further be filtered to a circle if required. Takes into account geometry
    near the poles.
    """
    ra, dec = center

    if not (dec <= 90.0 and dec > -90.0):
        raise ValueError(f"Dec out of bounds {dec}")

    if radius <= 0:
        raise ValueError(
            f"Radius value {radius} unacceptable, must be strictly positive"
        )

    cos_dec = cos(pi * dec / 180.0)

    ra_min = ra - radius / cos_dec
    ra_max = ra + radius / cos_dec

    return await backend.sources.get_in_bounds(
        ra_min=ra_min,
        ra_max=ra_max,
        dec_min=dec - radius,
        dec_max=dec + radius,
    )
