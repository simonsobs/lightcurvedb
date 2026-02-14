"""
Tests adding and removing a measurement.
"""

import datetime
import random
import uuid

import pytest

from lightcurvedb.models.flux import FluxMeasurement
from lightcurvedb.storage.prototype.backend import Backend


@pytest.mark.asyncio(loop_scope="session")
async def test_measurement_add_and_delete(backend: Backend, setup_test_data):
    source_ids = setup_test_data
    source_id = random.choice(source_ids)

    bands = await backend.lightcurves.get_module_frequency_pairs_for_source(
        source_id=source_id
    )
    band = random.choice(bands)

    measurement = FluxMeasurement(
        source_id=source_id,
        module=band[0],
        frequency=band[1],
        time=datetime.datetime.now(tz=datetime.timezone.utc),
        flux=0.0,
        flux_err=0.0,
        ra=0.0,
        dec=0.0,
        ra_uncertainty=0.0,
        dec_uncertainty=0.0,
        extra={"bad_measurement": True},
    )

    measurement_id = await backend.fluxes.create(measurement=measurement)

    # Grab the lightcurve for this source and ensure that the measurement is there.
    measurements = await backend.lightcurves.get_instrument_lightcurve(
        source_id=source_id, module=band[0], frequency=band[1]
    )

    print(measurements, measurement_id)

    measurement_recovered = [
        m for m in measurements if m.measurement_id == measurement_id
    ][0]

    assert len(measurements) > 1, "Expected at least two measurement in the lightcurve"

    for x, y in zip(
        sorted(measurement.model_dump().items()),
        sorted(measurement_recovered.model_dump().items()),
    ):
        if x[0] in ["measurement_id", "dec_uncertainty", "ra_uncertainty"]:
            continue

        assert x == y, f"Expected {x} but got {y}"

    await backend.fluxes.delete(measurement_id=measurement_id)


@pytest.mark.asyncio(loop_scope="session")
async def test_get_band_data(backend, setup_test_data):
    source_ids = setup_test_data
    source_id = random.choice(source_ids)

    bands = await backend.lightcurves.get_module_frequency_pairs_for_source(
        source_id=source_id
    )
    band = random.choice(bands)

    measurements = await backend.lightcurves.get_frequency_lightcurve(
        source_id=source_id, frequency=band[1], limit=100
    )

    for measurement in measurements:
        assert measurement.source_id == source_id
        assert measurement.frequency == band[1]


@pytest.mark.asyncio(loop_scope="session")
async def test_recent_measurements_for_non_existent_source(backend):
    measurements = await backend.lightcurves.get_frequency_lightcurve(
        source_id=uuid.uuid4(), frequency=123415, limit=100
    )
    assert len(measurements.flux) == 0
