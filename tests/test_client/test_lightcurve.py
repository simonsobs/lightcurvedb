"""
Test whether we can get a random lightcurve.
"""

import random

import pytest

from lightcurvedb.client.lightcurve import lightcurve_read_band, lightcurve_read_source


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_source(backend, setup_test_data):
    source_ids = setup_test_data
    for source_id in random.choices(source_ids, k=4):
        result = await lightcurve_read_source(id=source_id, backend=backend)
        assert result.source.source_id == source_id
        assert len(result.bands) > 0


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_band(backend, setup_test_data):
    source_ids = setup_test_data
    source_id = random.choice(source_ids)

    band_names = await backend.fluxes.get_bands_for_source(source_id)
    band_name = random.choice(band_names)

    result = await lightcurve_read_band(
        id=source_id, band_name=band_name, backend=backend
    )

    assert result.source.source_id == source_id
    assert result.band.band_name == band_name
    assert len(result) > 0
