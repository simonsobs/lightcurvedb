"""Tests that unassigned-source fixtures persist through every backend."""

from datetime import datetime, timedelta, timezone

import pytest

from lightcurvedb.simulation.unassigned import create_unassigned_source_fixtures


@pytest.mark.asyncio(loop_scope="session")
async def test_create_unassigned_source_fixtures(backend):
    instruments = await backend.instruments.get_all()
    source_ids = await create_unassigned_source_fixtures(
        backend=backend,
        instruments=instruments,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        cadence=timedelta(days=1),
        measurements_per_source=2,
        source_count=6,
        seed=1,
    )

    assert len(source_ids) == 6
    for source_id in source_ids:
        source = await backend.unassigned_sources.get(source_id)
        measurements = await backend.unassigned_fluxes.get_for_source(source_id)
        assert source.extra.simulation_scenario is not None
        assert len(measurements) == 2 * len(instruments)
        assert all(
            measurement.extra.simulation_scenario == source.extra.simulation_scenario
            for measurement in measurements
        )
