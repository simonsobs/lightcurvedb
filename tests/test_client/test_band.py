"""
Tests for band client interface.
"""

import pytest

from lightcurvedb.models.band import Band
from lightcurvedb.models.exceptions import BandNotFoundException
from lightcurvedb.storage.prototype.backend import Backend


@pytest.mark.asyncio(loop_scope="session")
async def test_band_read_all(backend: Backend):
    bands = await backend.bands.get_all()

    assert len(bands) > 0
    assert isinstance(bands[0], Band)


@pytest.mark.asyncio(loop_scope="session")
async def test_band_creation_deletion_flow(backend: Backend):
    band = Band(
        band_name="test_band",
        telescope="hubble",
        instrument="hypersupremecam",
        frequency=9.99,
    )

    # Add band
    band_name = await backend.bands.create(band=band)
    assert band_name == band.band_name

    # Read band back
    read_band = await backend.bands.get(band.band_name)
    assert read_band.band_name == band.band_name
    assert read_band.telescope == band.telescope
    assert read_band.instrument == band.instrument
    assert read_band.frequency == band.frequency

    # Delete band
    await backend.bands.delete(band.band_name)

    with pytest.raises(BandNotFoundException):
        await backend.bands.get(band.band_name)
