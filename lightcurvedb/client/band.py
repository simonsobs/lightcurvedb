"""
Extensions to core for sources.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lightcurvedb.models.band import Band, BandTable


class BandNotFound(Exception):
    pass


async def band_read(name: str, conn: AsyncSession) -> BandTable:
    """
    Read core metadata about a band.
    """
    res = await conn.get(BandTable, name)

    if res is None:
        raise BandNotFound

    return res.to_model()


async def band_read_all(conn: AsyncSession) -> list[BandTable]:
    """
    Get the list of all bands in use throughout the system.
    """
    query = select(BandTable)

    res = await conn.execute(query)

    return [x.to_model() for x in res.scalars().all()]


async def band_add(band: Band, conn: AsyncSession) -> str:
    """
    Add a band to the system.
    """

    table = BandTable(
        name=band.name,
        telescope=band.telescope,
        instrument=band.instrument,
        frequency=band.frequency,
    )

    conn.add(table)
    await conn.commit()
    await conn.refresh(table)

    return table.name


async def band_delete(name: str, conn: AsyncSession):
    """
    Delete a band from the system.
    """

    res = await conn.get(BandTable, name)

    if res is None:  # pragma: no cover
        raise BandNotFound

    await conn.delete(res)
    await conn.commit()
