"""
Tests the source reading.
"""

import pytest

from lightcurvedb.client.source import (
    SourceNotFound,
    source_add,
    source_delete,
    source_read,
)
from lightcurvedb.models.source import Source


@pytest.mark.asyncio(loop_scope="session")
async def test_read_source(client):
    async with client.session() as conn:
        source = await source_read(id=1, conn=conn)

    assert isinstance(source, Source)
    assert source.ra is not None
    assert source.dec is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_read_source_fails(client):
    with pytest.raises(SourceNotFound):
        async with client.session() as conn:
            await source_read(id=1000000, conn=conn)


@pytest.mark.asyncio(loop_scope="session")
async def test_add_read_remove_source(client):
    source = Source(ra=2.134, dec=89.37, variable=True)
    async with client.session() as conn:
        id = await source_add(source=source, conn=conn)

    async with client.session() as conn:
        read_source = await source_read(id=id, conn=conn)

        assert read_source.ra == source.ra
        assert read_source.dec == source.dec
        assert read_source.variable == source.variable

    async with client.session() as conn:
        await source_delete(id=id, conn=conn)

    with pytest.raises(SourceNotFound):
        async with client.session() as conn:
            await source_read(id=id, conn=conn)
