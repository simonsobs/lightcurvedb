"""
Tools for reading a 'feed' of all sources in a fixed ordering
based upon some algorithm. For now, we just return sources ordered
by ID descending in a paginated way.
"""

from lightcurvedb.models.feed import FeedResult, FeedResultItem
from lightcurvedb.protocols.storage import FluxStorageBackend


async def feed_read(
    start: int, number: int, band_name: str, backend: FluxStorageBackend
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
    backend: FluxStorageBackend
        Storage backend

    Returns
    -------
    result: FeedResult
        The feed result with additional metadata
    """

    results = []

    for source_id in range(start, start + number):
        measurements = await backend.fluxes.get_recent_measurements(
            source_id, band_name, limit=30
        )

        if len(measurements) <= 1:
            continue

        source = await backend.sources.get(source_id)

        results.append(
            FeedResultItem(
                time=measurements.times,
                flux=measurements.i_flux,
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
        band_name=band_name,
        total_number_of_sources=total_number_of_sources,
    )
