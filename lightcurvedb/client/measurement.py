"""
For individual measurements.
"""

import datetime

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from lightcurvedb.models.flux import FluxMeasurement, FluxMeasurementTable


class MeasurementSummaryResult(BaseModel):
    source_id: int
    band_name: str
    start: datetime.datetime
    end: datetime.datetime
    count: int


async def measurement_flux_add(measurement: FluxMeasurement, conn: AsyncSession) -> int:
    table = FluxMeasurementTable(**measurement.model_dump())

    conn.add(table)
    await conn.commit()
    await conn.refresh(table)

    return table.id


async def measurement_flux_delete(id: int, conn: AsyncSession):
    table = await conn.get(FluxMeasurementTable, id)

    await conn.delete(table)
    await conn.commit()

    return


async def measurement_summary(
    source_id: int, band_name: str, conn: AsyncSession
) -> MeasurementSummaryResult:
    """
    Get a measurement summary for a specific band and source ID. Returns information
    like the range of observation times, and how many observations there was in this
    time range.
    """

    def apply_limit(query):
        return conn.execute(
            query.filter(
                FluxMeasurementTable.source_id == source_id,
                FluxMeasurementTable.band_name == band_name,
            )
        )

    start_time = (
        await apply_limit(select(func.min(FluxMeasurementTable.time)))
    ).scalar()
    end_time = (await apply_limit(select(func.max(FluxMeasurementTable.time)))).scalar()
    count = (await apply_limit(select(func.count(FluxMeasurementTable.id)))).scalar()

    return MeasurementSummaryResult(
        source_id=source_id,
        band_name=band_name,
        start=start_time,
        end=end_time,
        count=count,
    )
