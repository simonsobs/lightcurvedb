"""
Integration tools with SOCat, used for upserting the sources that were used
for forced photometry.
"""

import astropy.units as u
from astropy.coordinates import ICRS
from socat.client.core import ClientBase
from sqlalchemy.orm import Session
from tqdm import tqdm

from lightcurvedb.models.source import SourceTable


def upsert_sources(
    client: ClientBase, session: Session, progress_bar: bool = False
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
    session: Session
        SQLAlchemy session for the lightcurvedb to upsert.
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
        lightcurvedb_source = session.get(SourceTable, source.id)

        if lightcurvedb_source is None:
            session.add(
                SourceTable(
                    id=source.id,
                    name=source.name,
                    ra=source.position.ra.to_value("deg"),
                    dec=source.position.dec.to_value("deg"),
                    variable=False,
                )
            )
            sources_added += 1

            continue

        ra_equal = lightcurvedb_source.ra == source.position.ra.to_value("deg")
        dec_equal = lightcurvedb_source.dec == source.position.dec.to_value("deg")
        name_equal = lightcurvedb_source.name == source.name

        if not (ra_equal and dec_equal and name_equal):
            lightcurvedb_source.ra = source.position.ra.to_value("deg")
            lightcurvedb_source.dec = source.position.dec.to_value("deg")
            lightcurvedb_source.name = source.name

            if lightcurvedb_source.extra is not None:
                raise ValueError(
                    f"Unable to handle upsert for source {lightcurvedb_source.id} due to "
                    f"presence of extra: {lightcurvedb_source.extra}"
                )

            session.add(lightcurvedb_source)
            sources_modified += 1

            continue

    session.commit()

    return sources_added, sources_modified
