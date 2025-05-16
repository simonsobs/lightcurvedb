"""
Tools for reading a 'feed' of all sources in a fixed ordering
based upon some algorithm. For now, we just return sources ordered
by ID descending in a paginated way.
"""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection

from lightcurvedb.models.feed import FeedResult, FeedResultItem
from lightcurvedb.models.flux import FluxMeasurementTable
from lightcurvedb.models.source import SourceTable


async def feed_read(
    start: int, number: int, band_name: str, conn: AsyncConnection
) -> FeedResult:
    """
    Reads the 'feed' of sources. Currently just orders by
    source id.

    Parameters
    ----------
    start: int
        ID to start reading at (suggest 0)
    number: int
        Number of results to return (suggest 10 or 16)
    band_name: str
        Band to use
    conn: AsyncConnection
        Database session

    Returns
    -------
    result: FeedResult
        The feed result with additional metadata
    """

    results = []

    for source_id in range(start, start + number):
        query = (
            select(
                FluxMeasurementTable.time,
                FluxMeasurementTable.i_flux,
                FluxMeasurementTable.ra,
                FluxMeasurementTable.dec,
            )
            .filter(
                FluxMeasurementTable.source_id == source_id,
                FluxMeasurementTable.band_name == band_name,
            )
            .order_by(FluxMeasurementTable.time.desc())
            .limit(30)
        )

        scalar = (await conn.execute(query)).all()

        if not scalar:
            continue

        ras = [x.ra for x in scalar]
        decs = [x.dec for x in scalar]

        if len(ras) <= 1:
            continue

        results.append(
            FeedResultItem(
                time=[x.time for x in scalar],
                flux=[x.i_flux for x in scalar],
                ra=sum(ras) / len(ras),
                dec=sum(decs) / len(decs),
                source_id=source_id,
            )
        )

    total_number_of_sources = (
        await conn.execute(select(func.count()).select_from(SourceTable))
    ).scalar_one()

    return FeedResult(
        items=results,
        start=start,
        stop=start + number,
        band_name=band_name,
        total_number_of_sources=total_number_of_sources,
    )
