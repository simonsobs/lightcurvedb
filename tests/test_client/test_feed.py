"""
Test the feed items
"""

import pytest

from lightcurvedb.client.feed import feed_read


@pytest.mark.asyncio(loop_scope="session")
async def test_read_feed(get_backend):
    async with get_backend() as backend:
        feed = await feed_read(start=0, number=8, band_name="f145", backend=backend)

        assert len(feed.items) > 0
        assert feed.band_name == "f145"
        assert feed.start == 0
        assert feed.stop == 8
