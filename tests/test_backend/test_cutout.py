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
    source_id = setup_test_data[1]

    lightcurve = await backend.lightcurves.get_source_lightcurve(
        source_id=source_id, selection_strategy="instrument"
    )
    fluxes = next(iter(lightcurve.lightcurves.values()))

    # Create a new cutout
    measurement_id = await backend.cutouts.create(
        cutout=Cutout(
            source_id=source_id,
            measurement_id=fluxes.measurement_id[0],
            time=fluxes.time[0],
            frequency=fluxes.frequency,
            module=fluxes.module,
            data=[[0.1, 0.2], [0.3, 0.4]],
            units="mJy",
        )
    )

    # Retrieve the cutout
    retrieved_cutouts = await backend.cutouts.retrieve_cutouts_for_source(source_id)
    assert any(cutout.measurement_id == measurement_id for cutout in retrieved_cutouts)

    # Retrieve a single cutout
    retrieved_cutout = await backend.cutouts.retrieve_cutout(
        source_id=source_id, measurement_id=fluxes[0].measurement_id
    )
    assert retrieved_cutout is not None
    assert retrieved_cutout.measurement_id == measurement_id

    # Delete the cutout
    await backend.cutouts.delete(measurement_id)

    # Verify deletion
    retrieved_cutouts_after_deletion = (
        await backend.cutouts.retrieve_cutouts_for_source(source_id)
    )
    assert all(
        cutout.measurement_id != measurement_id
        for cutout in retrieved_cutouts_after_deletion
    )


@pytest.mark.parametrize("bulk_insert_mode", ["text", "json", "csv", None])
@pytest.mark.asyncio(loop_scope="session")
async def test_cutout_bulk_write_and_delete(
    backend: Backend, setup_test_data, bulk_insert_mode
):
    source_id = setup_test_data[1]

    lightcurve = await backend.lightcurves.get_source_lightcurve(
        source_id=source_id, selection_strategy="instrument"
    )
    fluxes = lightcurve.lightcurves.values()

    lightcurve = next(iter(fluxes))

    # Create 5 cutouts:
    cutouts = [
        Cutout(
            source_id=source_id,
            measurement_id=lightcurve[i].measurement_id,
            time=lightcurve[i].time,
            frequency=lightcurve[i].frequency,
            module=lightcurve[i].module,
            data=[[0.1, 0.2] * 16, [0.3, 0.4] * 16] * 16,
            units="mJy",
        )
        for i in range(5)
    ]

    await backend.cutouts.create_batch(
        cutouts=cutouts, bulk_insert_mode=bulk_insert_mode
    )

    # Retrieve the cutouts
    retrieved_cutouts = await backend.cutouts.retrieve_cutouts_for_source(source_id)
    retrieved_measurement_ids = set(
        cutout.measurement_id for cutout in retrieved_cutouts
    )
    for cutout in cutouts:
        assert cutout.measurement_id in retrieved_measurement_ids

    # Delete the cutouts
    for cutout in cutouts:
        await backend.cutouts.delete(cutout.measurement_id)

    # Verify deletion
    retrieved_cutouts_after_deletion = (
        await backend.cutouts.retrieve_cutouts_for_source(source_id)
    )
    assert all(
        cutout.measurement_id not in retrieved_measurement_ids
        for cutout in retrieved_cutouts_after_deletion
    )
