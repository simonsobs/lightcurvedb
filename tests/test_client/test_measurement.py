"""
Tests adding and removing a measurement.
"""

import datetime
import random

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

    time_start = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
        days=30
    )
    time_end = datetime.datetime.now(tz=datetime.timezone.utc)

    measurements = await backend.fluxes.get_band_data(
        source_id=source_id, band_name=band, start_time=time_start, end_time=time_end
    )

    for measurement in measurements:
        assert measurement.source_id == source_id
        assert measurement.band_name == band
        assert time_start <= measurement.time <= time_end


@pytest.mark.asyncio(loop_scope="session")
async def test_recent_measurements_for_non_existent_source(backend):
    measurements = await backend.fluxes.get_recent_measurements(
        source_id=9999999, limit=5, band_name="non_existent"
    )
    assert len(measurements.flux) == 0
