"""
Extensions to core for sources.
"""

from math import cos, pi

from lightcurvedb.models.source import Source
from lightcurvedb.storage.prototype.backend import Backend


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
