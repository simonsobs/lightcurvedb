"""
Test whether we can get a random lightcurve.
"""

import datetime
import random
from uuid import UUID

import pytest

from lightcurvedb.storage.prototype.backend import Backend


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_source(backend: Backend, setup_test_data: list[UUID]):
    for source_id in random.choices(setup_test_data, k=4):
        result = await backend.lightcurves.get_source_lightcurve(
            source_id=source_id, selection_strategy="frequency"
        )
        assert result.source_id == source_id
        assert len(result.lightcurves) > 0


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_array(backend: Backend, setup_test_data: list[UUID]):
    source_id = random.choice(setup_test_data)

    result = await backend.lightcurves.get_source_lightcurve(
        source_id=source_id, selection_strategy="instrument"
    )

    assert result.source_id == source_id
    assert len(result.lightcurves) > 0


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_source_binned(
    backend: Backend, setup_test_data: list[UUID]
):
    for source_id in random.choices(setup_test_data, k=4):
        for binning_strategy in ["1 day", "7 days", "30 days"]:
            result = await backend.lightcurves.get_binned_source_lightcurve(
                source_id=source_id,
                selection_strategy="frequency",
                binning_strategy=binning_strategy,
                start_time=datetime.datetime.now() - datetime.timedelta(days=90),
                end_time=datetime.datetime.now(),
            )
            assert result.source_id == source_id
            assert len(result.lightcurves) > 0
            assert result.binning_strategy == binning_strategy


@pytest.mark.asyncio(loop_scope="session")
async def test_lightcurve_read_array_binned(
    backend: Backend, setup_test_data: list[UUID]
):
    for source_id in random.choices(setup_test_data, k=4):
        for binning_strategy in ["1 day", "7 days", "30 days"]:
            result = await backend.lightcurves.get_binned_source_lightcurve(
                source_id=source_id,
                selection_strategy="instrument",
                binning_strategy=binning_strategy,
                start_time=datetime.datetime.now() - datetime.timedelta(days=90),
                end_time=datetime.datetime.now(),
            )
            assert result.source_id == source_id
            assert len(result.lightcurves) > 0
            assert result.binning_strategy == binning_strategy
