"""
Tests for unassigned source models.
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


@pytest.fixture(autouse=True)
def setup_test_data():
    """
    Override the backend fixture because these are pure model tests.
    """
    yield


def test_unassigned_source_and_measurement_models():
    now = datetime.now(timezone.utc)
    source = UnassignedSource(
        source_id=uuid4(),
        ra=1.0,
        dec=-1.0,
        first_seen=now,
        last_seen=now,
        extra=UnassignedSourceMetadata(flags=["blind_search"]),
    )

    measurement = UnassignedFluxMeasurement(
        measurement_id=uuid4(),
        source_id=source.source_id,
        frequency=27,
        module="i1",
        time=now,
        ra=source.ra,
        dec=source.dec,
        ra_uncertainty=None,
        dec_uncertainty=None,
        flux=1.0,
        flux_err=0.1,
        extra=UnassignedMeasurementMetadata(map_id="map-1"),
    )

    assert source.status == "unmatched"
    assert measurement.extra.map_id == "map-1"
