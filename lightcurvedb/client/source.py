"""
Extensions to core for sources.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lightcurvedb.models import Source, SourceTable
from lightcurvedb.models.flux import FluxMeasurementTable


class SourceNotFound(Exception):
    pass


async def source_read(id: int, conn: AsyncSession) -> Source:
    """
    Read core metadata about a source.
    """
    res = await conn.get(SourceTable, id)

    if res is None:
        raise SourceNotFound

    return res.to_model()


async def source_read_bands(id: int, conn: AsyncSession) -> list[str]:
    """
    Read the bands that are available for a source.
    """
    query = select(FluxMeasurementTable.band_name)

    query = query.filter(
        FluxMeasurementTable.source_id == id,
    )

    query = query.distinct()

    result = await conn.execute(query)

    return result.scalars().all()


async def source_add(source: Source, conn: AsyncSession) -> int:
    """
    Add a source, returning its primary key.
    """
    table = SourceTable(ra=source.ra, dec=source.dec, variable=source.variable)

    conn.add(table)
    await conn.commit()
    await conn.refresh(table)

    return table.id


async def source_delete(id: int, conn: AsyncSession) -> int:
    """
    Delete a source (and all of its measurements!) from the system.
    """
    res = await source_read(id=id, conn=conn)

    await conn.delete(res)
    await conn.commit()

    return
