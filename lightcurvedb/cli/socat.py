"""
Update the sources registered in lightcurvedb to match those from socat.
"""

import asyncio

from lightcurvedb.integrations.socat import upsert_sources


async def core(lightcurvedb_settings, socat_settings):
    async with lightcurvedb_settings.backend as backend:
        await upsert_sources(
            client=socat_settings.client,
            backend=backend,
            progress_bar=True,
        )


def main():
    from socat.client import settings as socat_settings

    from lightcurvedb.config import settings as lightcurvedb_settings

    asyncio.run(core(lightcurvedb_settings, socat_settings))


if __name__ == "__main__":
    main()
