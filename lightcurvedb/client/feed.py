"""
Tools for reading a 'feed' of all sources in a fixed ordering
based upon some algorithm. For now, we just return sources ordered
by ID descending in a paginated way.
"""

from lightcurvedb.models.feed import FeedResult, FeedResultItem
from lightcurvedb.storage.prototype.backend import Backend


async def feed_read(
    start: int, number: int, frequency: int, backend: Backend
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
    frequency: int
        Band to use
    backend: Backend
        Storage backend

    Returns
    -------
    result: FeedResult
        The feed result with additional metadata
    """

    results = []

    sources = await backend.sources.get_all()
    source_ids = [source.source_id for source in sources[start : start + number]]

    for source_id in source_ids:
        measurements = await backend.lightcurves.get_frequency_lightcurve(
            source_id, frequency=frequency, limit=30
        )

        if len(measurements) <= 1:
            continue

        source = await backend.sources.get(source_id)

        results.append(
            FeedResultItem(
                time=measurements.time,
                flux=measurements.flux,
                ra=sum(measurements.ra) / len(measurements.ra),
                dec=sum(measurements.dec) / len(measurements.dec),
                source_id=source_id,
                source_name=source.name,
            )
        )

    all_sources = await backend.sources.get_all()
    total_number_of_sources = len(all_sources)

    return FeedResult(
        items=results,
        start=start,
        stop=start + number,
        frequency=frequency,
        total_number_of_sources=total_number_of_sources,
    )
