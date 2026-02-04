"""
Tests for database-side statistical analysis functions.
"""

import datetime
from datetime import timezone

import pytest

from lightcurvedb.models.band import Band
from lightcurvedb.models.flux import FluxMeasurementCreate
from lightcurvedb.models.source import SourceCreate


@pytest.mark.asyncio(loop_scope="session")
async def test_weighted_statistics(get_backend, setup_test_data):
    """
    Test flux statistics.
    """
    async with get_backend() as backend:
        # Create source and band
        source = await backend.sources.create(
            SourceCreate(name="WEIGHTED-TEST-001", ra=150.0, dec=30.0, variable=True)
        )
        band = await backend.bands.create(
            Band(
                name="test-weighted",
                telescope="TEST",
                instrument="TEST-CAM",
                frequency=500.0,
            )
        )

        base_time = datetime.datetime(2024, 1, 1, tzinfo=timezone.utc)
        measurements = [
            FluxMeasurementCreate(
                band_name="test-weighted",
                source_id=source.id,
                time=base_time + datetime.timedelta(days=i),
                ra=150.0,
                dec=30.0,
                ra_uncertainty=0.1,
                dec_uncertainty=0.1,
                i_flux=10.0,
                i_uncertainty=2.0,
            )
            for i in range(5)
        ]
        await backend.fluxes.create_batch(measurements)
        await backend.conn.commit()

        # Get statistics
        stats = await backend.fluxes.get_statistics(
            source_id=source.id, band_name="test-weighted"
        )

        assert stats.measurement_count == 5
        assert stats.min_flux == 10.0
        assert stats.max_flux == 10.0
