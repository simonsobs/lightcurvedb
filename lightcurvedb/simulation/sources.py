"""
Source generation.
"""

from random import randint, random

from lightcurvedb.models.source import CrossMatch, SourceCreate, SourceMetadata
from lightcurvedb.protocols.storage import FluxStorageBackend


async def create_fixed_sources(number: int, backend: FluxStorageBackend) -> list[int]:
    """
    Create a number of fixed sources in the database.

    Parameters
    ----------
    number : int
        Number of sources to create
    backend : FluxStorageBackend
        Storage backend from factory

    Returns
    -------
    list[int]
        The IDs of the created sources.
    """
    sources = [
        SourceCreate(
            name=f"SIM-{i:05d}",
            ra=random() * 360.0 - 180.0,
            dec=random() * 180.0 - 90.0,
            variable=False,
            extra=SourceMetadata(
                cross_matches=[CrossMatch(name=f"ACT-{randint(0, 10_000):05d}")]
            ),
        )
        for i in range(number)
    ]

    return await backend.sources.create_batch(sources)
