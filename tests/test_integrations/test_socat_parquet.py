"""
Regression coverage for the socat_id UUID fix (Source.socat_id/get_by_socat_id
used to be typed `int`, but socat's real source IDs are UUIDs) that doesn't
depend on Docker/testcontainers.

test_socat.py::test_insert already exercises this same integration, but only
via the session-scoped `backend` fixture in conftest.py, which unconditionally
requires the `postgres_database`/`timescale_database` fixtures as function
arguments regardless of which backend is actually parametrized -- so even the
"parquet" parametrization fails immediately without a Docker daemon. This
test builds a Pandas/parquet backend directly, so it can run anywhere.
"""

import random
from pathlib import Path

import pytest
from astropy import units as u
from astropy.coordinates import ICRS
from socat.client.mock import Client

from lightcurvedb.integrations.socat import upsert_sources
from lightcurvedb.storage.parquet.backend import generate_pandas_backend


@pytest.mark.asyncio
async def test_insert_and_round_trip_by_socat_id(tmp_path: Path):
    backend = await generate_pandas_backend(tmp_path)

    socat_client = Client()
    socat_client.n = 987654

    def make_source(i):
        return {
            "position": ICRS(
                ra=random.random() * 360.0 * u.deg,
                dec=(random.random() - 0.5) * 180.0 * u.deg,
            ),
            "name": f"Test Source {i}",
            "flux": random.random() * u.Jy,
        }

    sources = [make_source(i) for i in range(3)]
    socat_ids = [socat_client.create_source(**s).source_id for s in sources]

    added, modified = await upsert_sources(client=socat_client, backend=backend.sources)
    assert added == len(sources)
    assert modified == 0

    for socat_id in socat_ids:
        lc_source = await backend.sources.get_by_socat_id(socat_id=socat_id)
        assert lc_source.socat_id == socat_id

    # Idempotent: re-running with no socat-side changes adds/modifies nothing.
    added, modified = await upsert_sources(client=socat_client, backend=backend.sources)
    assert added == 0
    assert modified == 0
