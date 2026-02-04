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
async def test_measurement_add_and_delete(get_backend, setup_test_data):
    source_ids = setup_test_data
    async with get_backend() as backend:
        source_id = random.choice(source_ids)

        bands = await source_read_bands(source_id, backend=backend)
        band = random.choice(bands)

        measurement = FluxMeasurement(
            band_name=band,
            source_id=source_id,
            time=datetime.datetime.now(),
            i_flux=0.0,
            i_uncertainty=0.0,
            ra=0.0,
            dec=0.0,
            ra_uncertainty=0.0,
            dec_uncertainty=0.0,
        )

        measurement_id = await measurement_flux_add(
            measurement=measurement, backend=backend
        )
        await backend.conn.commit()

        await measurement_flux_delete(id=measurement_id, backend=backend)
        await backend.conn.commit()
