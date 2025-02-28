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
async def test_lightcurve_read_source(client, source_ids):
    source_id = random.choice(source_ids)

    async with client.session() as conn:
        bands = await source_read_bands(source_id, conn=conn)
        band = random.choice(bands)

        measurement = FluxMeasurement(
            band_name=band,
            source_id=source_id,
            time=datetime.datetime.now(),
            i_flux=0.0,
            i_uncertainty=0.0,
            q_flux=0.0,
            q_uncertainty=0.0,
            u_flux=0.0,
            u_uncertainty=0.0,
        )

        measurement_id = await measurement_flux_add(measurement=measurement, conn=conn)

    async with client.session() as conn:
        await measurement_flux_delete(id=measurement_id, conn=conn)
