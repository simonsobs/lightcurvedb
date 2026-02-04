"""
Test whether we can get a random lightcurve.
"""

import random

import pytest

from lightcurvedb.client.lightcurve import lightcurve_read_band, lightcurve_read_source


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_source(get_backend, setup_test_data):
    source_ids = setup_test_data
    async with get_backend() as backend:
        for source_id in random.choices(source_ids, k=4):
            result = await lightcurve_read_source(id=source_id, backend=backend)
            assert result.source.id == source_id
            assert len(result.bands) > 0


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_band(get_backend, setup_test_data):
    source_ids = setup_test_data
    async with get_backend() as backend:
        source_id = random.choice(source_ids)

        band_names = await backend.fluxes.get_bands_for_source(source_id)
        band_name = random.choice(band_names)

        result = await lightcurve_read_band(
            id=source_id, band_name=band_name, backend=backend
        )

        assert result.source.id == source_id
        assert result.band.name == band_name
        assert len(result.ids) > 0
