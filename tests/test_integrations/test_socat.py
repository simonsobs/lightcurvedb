"""
Tests the SOCat integration.
"""

import random

import pytest
from astropy import units as u
from astropy.coordinates import ICRS
from socat.client.mock import Client

from lightcurvedb.integrations.socat import upsert_sources
from lightcurvedb.storage.prototype.backend import Backend


@pytest.mark.asyncio(loop_scope="session")
async def test_insert(backend: Backend):
    socat_client = Client()
    # Do not use the low-number source IDs, these are already used.
    socat_client.n = 123456

    def make_source(i):
        return {
            "position": ICRS(
                ra=random.random() * 360.0 * u.deg,
                dec=(random.random() - 0.5) * 180.0 * u.deg,
            ),
            "name": f"Test Upsert Source {i}",
            "flux": random.random() * u.Jy,
        }

    sources = [make_source(i) for i in range(5)]

    source_ids = []

    for source in sources:
        input_source = socat_client.create_source(**source)
        source_ids.append(input_source.source_id)

    added, modified = await upsert_sources(client=socat_client, backend=backend.sources)

    assert added == len(sources)
    assert modified == 0

    # Try again...

    added, modified = await upsert_sources(client=socat_client, backend=backend.sources)

    assert added == 0
    assert modified == 0

    # Modify one and add a single new one.
    socat_client.update_source(source_id=source_ids[0], name="New Name!")
    new_source = socat_client.create_source(**make_source(len(source_ids) + 1))
    source_ids.append(new_source.source_id)

    added, modified = await upsert_sources(client=socat_client, backend=backend.sources)

    assert added == 1
    assert modified == 1

    # Remove them all!
    for id in source_ids:
        source = await backend.sources.get_by_socat_id(socat_id=id)
        await backend.sources.delete(source.source_id)

    return
