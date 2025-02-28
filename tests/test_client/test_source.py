"""
Tests the source reading.
"""

import random

import pytest

from lightcurvedb.client.source import (
    SourceNotFound,
    source_add,
    source_delete,
    source_read,
    source_read_all,
    source_read_summary,
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
async def test_read_all_sources(client, source_ids):
    async with client.session() as conn:
        all_sources = await source_read_all(conn)

    found_source_ids = set(x.id for x in all_sources)
    expected_source_ids = set(source_ids)

    assert found_source_ids == expected_source_ids


@pytest.mark.asyncio(loop_scope="session")
async def test_source_read_summary(client, source_ids):
    for source_id in random.choices(source_ids, k=4):
        async with client.session() as conn:
            source_summary = await source_read_summary(id=source_id, conn=conn)

            assert source_summary.source.id == source_id
            assert len(source_summary.bands) > 0
            assert (
                source_summary.measurements[0].end
                > source_summary.measurements[0].start
            )
            assert source_summary.measurements[0].count > 0


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
