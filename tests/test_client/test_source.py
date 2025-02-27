"""
Tests the source reading.
"""

import pytest

from lightcurvedb.client.source import source_read
from lightcurvedb.models.source import SourceTable


@pytest.mark.asyncio(loop_scope="session")
async def test_read_source(client):
    async with client.session() as conn:
        source = await source_read(id=1, conn=conn)

    assert isinstance(source, SourceTable)
    assert source.ra is not None
    assert source.dec is not None
