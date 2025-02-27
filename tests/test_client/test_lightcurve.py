"""
Test whether we can get a random lightcurve.
"""

import random

import pytest

from lightcurvedb.client.lightcurve import lightcurve_read_source


@pytest.mark.asyncio(scope="session")
async def test_lightcurve_read_source(client, source_ids):
    for source_id in random.choices(source_ids, k=4):
        async with client.session() as conn:
            lightcurve = await lightcurve_read_source(id=source_id, conn=conn)

            assert lightcurve.source.id == source_id
