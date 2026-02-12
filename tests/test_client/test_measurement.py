"""
Tests adding and removing a measurement.
"""

import datetime
import random
import uuid

import pytest

from lightcurvedb.client.measurement import (
    measurement_flux_add,
    measurement_flux_delete,
)
from lightcurvedb.client.source import source_read_bands
from lightcurvedb.models.flux import FluxMeasurement


@pytest.mark.asyncio(loop_scope="session")
async def test_measurement_add_and_delete(backend, setup_test_data):
    source_ids = setup_test_data
    source_id = random.choice(source_ids)

    bands = await source_read_bands(source_id, backend=backend)
    band = random.choice(bands)

    measurement = FluxMeasurement(
        band_name=band,
        source_id=source_id,
        module=band[0],
        frequency=band[1],
        time=datetime.datetime.now(),
        flux=0.0,
        flux_err=0.0,
        ra=0.0,
        dec=0.0,
        ra_uncertainty=0.0,
        dec_uncertainty=0.0,
        extra={"bad_measurement": True},
    )

    measurement_id = await measurement_flux_add(
        measurement=measurement, backend=backend
    )

    await measurement_flux_delete(id=measurement_id, backend=backend)


@pytest.mark.asyncio(loop_scope="session")
async def test_get_band_data(backend, setup_test_data):
    source_ids = setup_test_data
    source_id = random.choice(source_ids)

    bands = await source_read_bands(source_id, backend=backend)
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
