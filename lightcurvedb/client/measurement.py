"""
For individual measurements.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from lightcurvedb.models.flux import FluxMeasurement, FluxMeasurementTable


async def measurement_flux_add(measurement: FluxMeasurement, conn: AsyncSession) -> int:
    table = FluxMeasurementTable(**measurement)

    conn.add(table)
    await conn.commit()
    await conn.refresh(table)

    return table.id


async def measurement_flux_delete(id: int, conn: AsyncSession):
    table = await conn.get(FluxMeasurementTable, id)

    await conn.delete(table)
    await conn.commit()

    return
