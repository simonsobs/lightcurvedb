"""
Cut-outs from the database.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lightcurvedb.models.cutout import Cutout, CutoutTable


class CutoutNotFound(Exception):
    pass


async def cutout_read(id: int, conn: AsyncSession) -> Cutout:
    """
    Read a single cut-out.
    """

    table = await conn.get(CutoutTable, id)

    if table is None:
        raise CutoutNotFound

    return table.to_model()


async def cutout_add(cutout: Cutout, conn: AsyncSession) -> int:
    """
    Add a cutout to the database.
    """

    table = CutoutTable(
        **cutout.model_dump(),
    )

    conn.add(table)
    await conn.commit()
    await conn.refresh(table)

    return table.id


async def cutout_read_from_flux_id(
    flux_measurement_id: int, conn: AsyncSession
) -> Cutout:
    """
    Read a single cut-out, but from its flux measurement ID.
    """

    query = select(CutoutTable).filter(CutoutTable.flux_id == flux_measurement_id)

    table = await conn.execute(query)
    table = table.scalar_one_or_none()

    if table is None:
        raise CutoutNotFound

    return table.to_model()
