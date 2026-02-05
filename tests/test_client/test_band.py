"""
Tests for band client interface.
"""

import pytest

from lightcurvedb.client.band import (
    band_add,
    band_delete,
    band_read,
    band_read_all,
)
from lightcurvedb.models.band import Band
from lightcurvedb.models.exceptions import BandNotFoundException


@pytest.mark.asyncio(loop_scope="session")
async def test_band_read_all(backend):
    bands = await band_read_all(backend=backend)

    assert len(bands) > 0
    assert isinstance(bands[0], Band)


@pytest.mark.asyncio(loop_scope="session")
async def test_band_creation_deletion_flow(backend):
    band = Band(
        name="test_band",
        telescope="hubble",
        instrument="hypersupremecam",
        frequency=9.99,
    )

    # Add band
    band_name = await band_add(band=band, backend=backend)
    assert band_name == band.name

    # Read band back
    read_band = await band_read(band.name, backend=backend)
    assert read_band.name == band.name
    assert read_band.telescope == band.telescope
    assert read_band.instrument == band.instrument
    assert read_band.frequency == band.frequency

    # Delete band
    await band_delete(name=band.name, backend=backend)

    with pytest.raises(BandNotFoundException):
        await band_read(name=band.name, backend=backend)
