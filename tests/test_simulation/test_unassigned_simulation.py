"""Tests for deterministic unassigned-source simulation fixtures."""

from datetime import datetime, timedelta, timezone

import pytest

from lightcurvedb.models import Instrument
from lightcurvedb.simulation.unassigned import build_unassigned_source_fixtures


@pytest.fixture(autouse=True)
def setup_test_data():
    """Override the database fixture because these are pure simulator tests."""
    yield


@pytest.fixture
def instruments():
    return [
        Instrument(
            frequency=frequency,
            module="i1",
            telescope="lat",
            instrument="latr",
            details={},
        )
        for frequency in [27, 93]
    ]


def test_unassigned_source_fixtures_cover_review_scenarios(instruments):
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sources, measurements = build_unassigned_source_fixtures(
        instruments=instruments,
        start_time=start_time,
        cadence=timedelta(days=7),
        measurements_per_source=4,
        source_count=8,
        seed=12,
    )

    scenarios = {source.extra.simulation_scenario for source in sources}
    assert {
        "3c273-external-match",
        "m31-external-match",
        "moving-unmatched",
        "merge-primary",
        "merge-secondary",
        "fixed-unmatched",
    } <= scenarios
    assert len(sources) == 8
    assert len(measurements) == 8 * 4 * len(instruments)
    assert all(source.status == "unmatched" for source in sources)
    assert all(-180.0 <= measurement.ra < 180.0 for measurement in measurements)
    assert all(-90.0 <= measurement.dec <= 90.0 for measurement in measurements)
    assert all(measurement.time.tzinfo is not None for measurement in measurements)
    assert all(measurement.flux > 0.0 for measurement in measurements)
    assert all(measurement.flux_err > 0.0 for measurement in measurements)
    assert all(source.source_id.version == 7 for source in sources)
    assert all(measurement.measurement_id.version == 7 for measurement in measurements)

    moving_measurements = [
        measurement
        for measurement in measurements
        if measurement.extra.simulation_scenario == "moving-unmatched"
        and measurement.frequency == instruments[0].frequency
    ]
    assert moving_measurements[0].ra != moving_measurements[-1].ra
    assert moving_measurements[0].dec != moving_measurements[-1].dec
    assert any(measurement.ra < 0.0 for measurement in moving_measurements)


def test_unassigned_source_fixture_values_are_deterministic(instruments):
    kwargs = {
        "instruments": instruments,
        "start_time": datetime(2024, 1, 1),
        "cadence": timedelta(days=1),
        "measurements_per_source": 2,
        "source_count": 6,
        "seed": 123,
    }

    first_sources, first_measurements = build_unassigned_source_fixtures(**kwargs)
    second_sources, second_measurements = build_unassigned_source_fixtures(**kwargs)

    assert [source.model_dump(exclude={"source_id"}) for source in first_sources] == [
        source.model_dump(exclude={"source_id"}) for source in second_sources
    ]
    assert [
        measurement.model_dump(exclude={"measurement_id", "source_id"})
        for measurement in first_measurements
    ] == [
        measurement.model_dump(exclude={"measurement_id", "source_id"})
        for measurement in second_measurements
    ]


def test_unassigned_source_fixtures_require_all_review_scenarios(instruments):
    with pytest.raises(ValueError, match="at least 6"):
        build_unassigned_source_fixtures(
            instruments=instruments,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            cadence=timedelta(days=1),
            measurements_per_source=1,
            source_count=5,
        )
