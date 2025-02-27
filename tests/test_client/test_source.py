"""
Tests the source reading.
"""

import pytest

from lightcurvedb.client.source import source_read


@pytest.mark.asyncio(loop_scope="session")
async def test_read_source(client_full):
    async with client_full.session() as conn:
        source = await source_read(id=1, conn=conn)

    assert source.ra == 44.4
