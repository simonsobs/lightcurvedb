"""
Test for grabbing cutouts.
"""

import pytest

from lightcurvedb.models.cutout import Cutout
from lightcurvedb.storage.prototype.backend import Backend


@pytest.mark.asyncio(loop_scope="session")
async def test_cutout_read(backend: Backend, setup_test_data):
    source_ids = setup_test_data
    source_id = source_ids[0]

    cutouts = await backend.cutouts.retrieve_cutouts_for_source(source_id)

    assert len(cutouts) > 0
    assert cutouts[0].source_id == source_id


@pytest.mark.asyncio(loop_scope="session")
async def test_cutout_write_and_delete(backend: Backend, setup_test_data):
    source_ids = setup_test_data
    source_id = source_ids[1]

    bands = await backend.fluxes.get_bands_for_source(source_id=source_id)
    fluxes = await backend.fluxes.get_recent_measurements(
        source_id, limit=100, band_name=bands[0]
    )

    # Create a new cutout
    flux_id = await backend.cutouts.create(
        cutout=Cutout(
            source_id=source_id,
            flux_id=fluxes.flux_ids[0],
            time=fluxes.times[0],
            band_name=fluxes.band_name,
            data=[[0.1, 0.2], [0.3, 0.4]],
            units="mJy",
        )
    )

    # Retrieve the cutout
    retrieved_cutouts = await backend.cutouts.retrieve_cutouts_for_source(source_id)
    assert any(cutout.flux_id == flux_id for cutout in retrieved_cutouts)

    # Retrieve a single cutout
    retrieved_cutout = await backend.cutouts.retrieve_cutout(
        source_id=source_id, flux_id=fluxes[0].flux_id
    )
    assert retrieved_cutout is not None
    assert retrieved_cutout.flux_id == flux_id

    # Delete the cutout
    await backend.cutouts.delete(flux_id)

    # Verify deletion
    retrieved_cutouts_after_deletion = (
        await backend.cutouts.retrieve_cutouts_for_source(source_id)
    )
    assert all(cutout.flux_id != flux_id for cutout in retrieved_cutouts_after_deletion)
