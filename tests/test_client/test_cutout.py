"""
Test for grabbing cutouts.
"""

import pytest

from lightcurvedb.client.cutouts import cutout_read, cutout_read_from_flux_id
from lightcurvedb.client.lightcurve import lightcurve_read_source


@pytest.mark.asyncio(loop_scope="session")
async def test_cutout_readers(client, source_ids):
    # First, grab our flux measurements.
    async with client.session() as conn:
        full_lightcurve = await lightcurve_read_source(id=source_ids[-1], conn=conn)

        flux_measurement_id = full_lightcurve.bands[0].id[-1]

        cutout = await cutout_read_from_flux_id(flux_measurement_id, conn=conn)

        # The previous row should still exist.
        read_previous = await cutout_read(cutout.id - 1, conn=conn)

        assert cutout.band_name == full_lightcurve.bands[0].band.name
        assert len(read_previous.data[0]) == len(read_previous.data)
