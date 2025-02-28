"""
Tests for band client interface.
"""

import pytest

from lightcurvedb.client.band import (
    BandNotFound,
    band_add,
    band_delete,
    band_read,
    band_read_all,
)
from lightcurvedb.models.band import Band


@pytest.mark.asyncio(loop_scope="session")
async def test_band_read_all(client):
    async with client.session() as conn:
        bands = await band_read_all(conn)

    assert len(bands) > 0
    assert isinstance(bands[0], Band)


@pytest.mark.asyncio(loop_scope="session")
async def test_band_creation_deletion_flow(client):
    band = Band(
        name="test_band",
        telescope="hubble",
        instrument="hypersupremecam",
        frequency=9.99,
    )

    async with client.session() as conn:
        await band_add(band=band, conn=conn)

    async with client.session() as conn:
        read_band = await band_read(band.name, conn=conn)
        assert read_band == band

    async with client.session() as conn:
        await band_delete(name=band.name, conn=conn)

    with pytest.raises(BandNotFound):
        async with client.session() as conn:
            await band_read(name=band.name, conn=conn)
