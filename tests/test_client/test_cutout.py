"""
Test for grabbing cutouts.
"""

import pytest

from lightcurvedb.storage.prototype.backend import Backend


@pytest.mark.asyncio(loop_scope="session")
async def test_cutout_read(backend: Backend, setup_test_data):
    source_ids = setup_test_data
    source_id = source_ids[0]

    cutouts = await backend.cutouts.retrieve_cutouts_for_source(source_id)

    assert len(cutouts) > 0
    assert cutouts[0].source_id == source_id
