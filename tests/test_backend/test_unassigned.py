"""
Tests for unassigned source storage.
"""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lightcurvedb.models import (
    UnassignedFluxMeasurement,
    UnassignedMeasurementMetadata,
    UnassignedSource,
    UnassignedSourceMetadata,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_unassigned_source_and_measurements(backend):
    assert backend.unassigned_sources is not None
    assert backend.unassigned_fluxes is not None

    now = datetime.now(timezone.utc)
    source = UnassignedSource(
        source_id=uuid4(),
        ra=12.5,
        dec=-34.5,
        first_seen=now,
        last_seen=now,
        extra=UnassignedSourceMetadata(flags=["blind_search"]),
    )

    source_id = await backend.unassigned_sources.create(source)
    source_read = await backend.unassigned_sources.get(source_id)

    assert source_read == source
    assert source_read.status == "unmatched"

    measurement = UnassignedFluxMeasurement(
        measurement_id=uuid4(),
        source_id=source_id,
        frequency=27,
        module="i1",
        time=now,
        ra=12.5,
        dec=-34.5,
        ra_uncertainty=0.01,
        dec_uncertainty=0.01,
        flux=1.5,
        flux_err=0.2,
        extra=UnassignedMeasurementMetadata(flags=["candidate"], map_id="test-map"),
    )

    measurement_id = await backend.unassigned_fluxes.create(measurement)
    measurement_read = await backend.unassigned_fluxes.get(measurement_id)
    measurements = await backend.unassigned_fluxes.get_for_source(source_id)

    assert measurement_read == measurement
    assert measurements == [measurement]

    await backend.unassigned_fluxes.delete(measurement_id)
    await backend.unassigned_sources.delete(source_id)
