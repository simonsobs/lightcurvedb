"""
Extensions to core for sources.
"""

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from lightcurvedb.client.band import band_read
from lightcurvedb.client.measurement import (
    MeasurementSummaryResult,
    measurement_summary,
)
from lightcurvedb.models import Band, Source, SourceTable
from lightcurvedb.models.flux import FluxMeasurementTable


class SourceNotFound(Exception):
    pass


class SourceSummaryResult(BaseModel):
    source: Source
    bands: list[Band]
    measurements: list[MeasurementSummaryResult]


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
    Read the bands names that are available for a source.
    """
    query = select(FluxMeasurementTable.band_name)

    query = query.filter(
        FluxMeasurementTable.source_id == id,
    )

    query = query.distinct()

    result = await conn.execute(query)

    return result.scalars().all()


async def source_read_all(conn: AsyncSession) -> list[Source]:
    """
    Read all sources available in the system.
    """

    query = select(SourceTable)

    res = await conn.execute(query)

    return [x.to_model() for x in res.scalars().all()]


async def source_read_summary(id: int, conn: AsyncSession) -> SourceSummaryResult:
    """
    Read the full summary for an individual source, including number of
    observations.
    """

    source = await source_read(id=id, conn=conn)
    available_bands = await source_read_bands(id=id, conn=conn)

    band_info = [await band_read(x, conn) for x in available_bands]
    measurements = [
        await measurement_summary(source_id=id, band_name=x, conn=conn)
        for x in available_bands
    ]

    return SourceSummaryResult(
        source=source, bands=band_info, measurements=measurements
    )


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

    res = await conn.get(SourceTable, id)

    if res is None:  # pragma: no cover
        raise SourceNotFound

    await conn.delete(res)
    await conn.commit()

    return
