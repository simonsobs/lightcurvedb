"""
Integration tools with SOCat, used for upserting the sources that were used
for forced photometry.
"""

from math import isclose

import astropy.units as u
from astropy.coordinates import ICRS
from socat.client.core import ClientBase
from tqdm import tqdm

from lightcurvedb.models.exceptions import SourceNotFoundException
from lightcurvedb.models.source import Source
from lightcurvedb.storage.prototype.source import ProvidesSourceStorage


def clamp_ra(ra: float) -> float:
    if ra > 180.0:
        return ra - 360.0
    elif ra < -180.0:
        return ra + 360.0
    else:
        return ra


async def upsert_sources(
    client: ClientBase, backend: ProvidesSourceStorage, progress_bar: bool = False
) -> tuple[int, int]:
    """
    Upserts all sources that lightcurvedb knows about. Effectively
    synchronises the source definitions in `socat` and `lightcurvedb`.

    Right now this is an expensive and poorly optimized operation, but
    that need not be the case in the future. Right now this is a simple
    loop over all the sources, but we can improve performance significantly
    by using actual UPSERT statements.

    Parameters
    ----------
    client: ClientBase
        The SOCat client to use.
    backend: ProvidesSourceStorage
        Backend providing source storage for lightcurvedb to upsert.
    progress_bar: bool = False
        Whether to display a progress bar.

    Returns
    -------
    sources_added: int
        Number of sources added to the database.
    sources_modified: int
        Number of sources modified in the database.
    """

    all_sources = client.get_box(
        lower_left=ICRS(ra=0.0 * u.deg, dec=-90.0 * u.deg),
        upper_right=ICRS(ra=359.999999 * u.deg, dec=90.0 * u.deg),
    )

    if progress_bar:
        all_sources = tqdm(all_sources, desc="Upserting sources")

    sources_added = 0
    sources_modified = 0

    for source in all_sources:
        try:
            print(source.source_id)
            lightcurvedb_source = await backend.get(source.source_id)
        except SourceNotFoundException:
            await backend.create(
                Source(
                    source_id=source.source_id,
                    name=source.name,
                    ra=clamp_ra(source.position.ra.to_value("deg")),
                    dec=source.position.dec.to_value("deg"),
                    variable=False,
                )
            )
            sources_added += 1

            continue

        ra_equal = isclose(
            lightcurvedb_source.ra,
            clamp_ra(source.position.ra.to_value("deg")),
            abs_tol=1 / 3600.0 / 100.0,
        )
        dec_equal = isclose(
            lightcurvedb_source.dec,
            source.position.dec.to_value("deg"),
            abs_tol=1 / 3600.0 / 100.0,
        )
        name_equal = lightcurvedb_source.name == source.name

        if not (ra_equal and dec_equal and name_equal):
            lightcurvedb_source.ra = clamp_ra(source.position.ra.to_value("deg"))
            lightcurvedb_source.dec = source.position.dec.to_value("deg")
            lightcurvedb_source.name = source.name

            if lightcurvedb_source.extra is not None:
                raise ValueError(
                    f"Unable to handle upsert for source {lightcurvedb_source.source_id} due to "
                    f"presence of extra: {lightcurvedb_source.extra}"
                )

            sources_modified += 1

            continue

    return sources_added, sources_modified
