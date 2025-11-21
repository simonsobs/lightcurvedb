"""
Tests the SOCat integration.
"""

import random

from astropy import units as u
from astropy.coordinates import ICRS
from socat.client.mock import Client

from lightcurvedb.integrations.socat import upsert_sources
from lightcurvedb.models.source import SourceTable


def test_insert(sync_client):
    socat_client = Client()

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
        input_source = socat_client.create(**source)
        source_ids.append(input_source.id)

    with sync_client.session() as session:
        added, modified = upsert_sources(client=socat_client, session=session)

        assert added == len(sources)
        assert modified == 0

    # Try again...

    with sync_client.session() as session:
        added, modified = upsert_sources(client=socat_client, session=session)

        assert added == 0
        assert modified == 0

    # Modify one and add a single new one.
    socat_client.update_source(id=source_ids[0], name="New Name!")
    new_source = socat_client.create(**make_source(len(source_ids) + 1))
    source_ids.append(new_source.id)

    with sync_client.session() as session:
        added, modified = upsert_sources(client=socat_client, session=session)

        assert added == 1
        assert modified == 1

    # Remove them all!
    with sync_client.session() as session:
        for id in source_ids:
            session.delete(session.get(SourceTable, id))
        session.commit()

    return
