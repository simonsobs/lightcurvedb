"""
Tests for database-side statistical analysis functions.
"""

import datetime
from datetime import timezone

import pytest

from lightcurvedb.models.flux import FluxMeasurementCreate
from lightcurvedb.models.instrument import Instrument
from lightcurvedb.models.source import Source


@pytest.mark.asyncio(loop_scope="session")
async def test_weighted_statistics(backend):
    """
    Test flux statistics.
    """
    # Create source and band
    source = await backend.sources.create(
        Source(name="WEIGHTED-TEST-001", ra=150.0, dec=30.0, variable=True)
    )
    _ = await backend.instruments.create(
        Instrument(
            frequency=999,
            module="test-weighted",
            telescope="TEST",
            instrument="TEST-CAM",
            details={},
        )
    )

    base_time = datetime.datetime(2024, 1, 1, tzinfo=timezone.utc)
    measurements = [
        FluxMeasurementCreate(
            module="test-weighted",
            frequency=999,
            source_id=source,
            time=base_time + datetime.timedelta(days=i),
            ra=150.0,
            dec=30.0,
            ra_uncertainty=0.1,
            dec_uncertainty=0.1,
            flux=10.0,
            flux_err=2.0,
        )
        for i in range(5)
    ]
    measurement_ids = await backend.fluxes.create_batch(measurements)

    # Get statistics
    stats = await backend.analysis.get_source_statistics_for_frequency_and_module(
        source_id=source,
        module="test-weighted",
        frequency=999,
        start_time=base_time,
        end_time=base_time + datetime.timedelta(days=10),
    )

    assert stats.measurement_count == 5
    assert stats.min_flux == 10.0
    assert stats.max_flux == 10.0

    for measurement_id in measurement_ids:
        await backend.fluxes.delete(measurement_id=measurement_id)

    await backend.sources.delete(source_id=source)
