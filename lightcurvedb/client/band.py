"""
Extensions to core for sources.
"""


from sqlalchemy.ext.asyncio import AsyncSession
from lightcurvedb.models.band import BandTable, Band
from lightcurvedb.models.flux import FluxMeasurementTable
from sqlalchemy import select

class BandNotFound(Exception):
    pass

async def band_read(name: str, conn: AsyncSession) -> BandTable:
    """
    Read core metadata about a band.
    """
    res = await conn.get(BandTable, id)

    if res is None:
        raise BandNotFound

    return res

async def band_read_all(conn: AsyncSession) -> list[BandTable]:
    """
    Get the list of all bands in use throughout the system.
    """
    query = select(BandTable)

    res = await conn.execute(query)

    return res.all()


async def band_add(band: Band, conn: AsyncSession) -> str:
    """
    Add a band to the system.
    """

    table = BandTable(
        name=band.name,
        telescope=band.telescope,
        instrument=band.instrument,
        frequency=band.frequency
    )

    conn.add(table)
    await conn.commit()

    return table.name


async def band_delete(band_name: str, conn: AsyncSession):
    """
    Delete a band from the system.
    """

    res = await band_read(band_name)

    await conn.delete(res)
    await conn.commit()