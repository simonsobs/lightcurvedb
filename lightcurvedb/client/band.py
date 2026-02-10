"""
Client functions for band operations.
"""

from lightcurvedb.models.band import Band
from lightcurvedb.protocols.storage import FluxStorageBackend


async def band_read(name: str, backend: FluxStorageBackend) -> Band:
    """
    Read core metadata about a band.
    """
    return await backend.bands.get(name)


async def band_read_all(backend: FluxStorageBackend) -> list[Band]:
    """
    Get the list of all bands in use throughout the system.
    """
    return await backend.bands.get_all()


async def band_add(band: Band, backend: FluxStorageBackend) -> str:
    """
    Add a band to the system.
    """
    created = await backend.bands.create(band)
    return created


async def band_delete(name: str, backend: FluxStorageBackend) -> None:
    """
    Delete a band from the system.
    """
    await backend.bands.delete(name)
