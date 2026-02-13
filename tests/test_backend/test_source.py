"""
Tests the source reading.
"""

import uuid

import pytest

from lightcurvedb.client.source import (
    source_read_in_radius,
)
from lightcurvedb.models.exceptions import SourceNotFoundException
from lightcurvedb.models.source import Source


@pytest.mark.asyncio(loop_scope="session")
async def test_read_source(backend, setup_test_data):
    source = await backend.sources.get(setup_test_data[0])

    assert isinstance(source, Source)
    assert source.ra is not None
    assert source.dec is not None


@pytest.mark.asyncio(loop_scope="session")
async def test_read_all_sources(backend):
    all_sources = await backend.sources.get_all()
    assert len(all_sources) == 64


@pytest.mark.asyncio(loop_scope="session")
async def test_source_read_in_radius(backend):
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
async def test_read_source_fails(backend):
    with pytest.raises(SourceNotFoundException):
        await backend.sources.get(uuid.uuid4())


@pytest.mark.asyncio(loop_scope="session")
async def test_add_read_remove_source(backend):
    source = Source(ra=2.134, dec=89.37, variable=True, extra={"haha": "hoho"})
    id = await backend.sources.create(source=source)

    read_source = await backend.sources.get(id)

    assert read_source.ra == source.ra
    assert read_source.dec == source.dec
    assert read_source.variable == source.variable

    await backend.sources.delete(id)

    with pytest.raises(SourceNotFoundException):
        await backend.sources.get(id)

    with pytest.raises(SourceNotFoundException):
        await backend.sources.delete(id)
