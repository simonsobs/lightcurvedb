"""
Tests the source reading.
"""

import random

import pytest

from lightcurvedb.client.source import (
    source_add,
    source_delete,
    source_read,
    source_read_all,
    source_read_in_radius,
    source_read_summary,
)
from lightcurvedb.models.source import Source
from lightcurvedb.models.exceptions import SourceNotFoundException


@pytest.mark.asyncio(loop_scope="session")
async def test_read_source(get_backend):
    async with get_backend() as backend:
        source = await source_read(id=1, backend=backend)

        assert isinstance(source, Source)
        assert source.ra is not None
        assert source.dec is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_read_all_sources(get_backend):
    async with get_backend() as backend:
        all_sources = await source_read_all(backend=backend)
        assert len(all_sources) == 64


@pytest.mark.asyncio(loop_scope="session")
async def test_source_read_summary(get_backend):
    async with get_backend() as backend:

        all_sources = await backend.sources.get_all()
        source_ids = [s.id for s in all_sources]

        for source_id in random.choices(source_ids, k=4):
            source_summary = await source_read_summary(id=source_id, backend=backend)

            assert source_summary.source.id == source_id
            assert len(source_summary.bands) > 0
            assert (
                source_summary.measurements[0].end
                > source_summary.measurements[0].start
            )
            assert source_summary.measurements[0].count > 0


@pytest.mark.asyncio(loop_scope="session")
async def test_source_read_in_radius(get_backend):
    async with get_backend() as backend:
        sources_in_radius = await source_read_in_radius((0, 0), 80.0, backend=backend)
        assert len(sources_in_radius) > 0

        for source in sources_in_radius:
            assert source.ra > -80.0
            assert source.ra < 80.0
            assert source.dec > -80.0
            assert source.dec < 80.0

        with pytest.raises(ValueError):
            await source_read_in_radius((0, 0), -80.0, backend=backend)

        with pytest.raises(ValueError):
            await source_read_in_radius((0, -100), 1.0, backend=backend)


@pytest.mark.asyncio(loop_scope="session")
async def test_read_source_fails(get_backend):
    async with get_backend() as backend:
        with pytest.raises(SourceNotFoundException):
            await source_read(id=1000000, backend=backend)


@pytest.mark.asyncio(loop_scope="session")
async def test_add_read_remove_source(get_backend):
    async with get_backend() as backend:
        source = Source(ra=2.134, dec=89.37, variable=True)
        id = await source_add(source=source, backend=backend)
        await backend.conn.commit()

        read_source = await source_read(id=id, backend=backend)

        assert read_source.ra == source.ra
        assert read_source.dec == source.dec
        assert read_source.variable == source.variable

        await source_delete(id=id, backend=backend)
        await backend.conn.commit()

        with pytest.raises(SourceNotFoundException):
            await source_read(id=id, backend=backend)
