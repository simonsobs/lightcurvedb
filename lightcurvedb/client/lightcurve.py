"""
A client for extracting complete light-curves.
"""

from psycopg import AsyncConnection
from pydantic import BaseModel
import asyncio

from lightcurvedb.models.source import SourceTable
from lightcurvedb.models.flux import FluxMeasurementTable
from lightcurvedb.models.band import Band, BandTable
from lightcurvedb.models.source import Source
from sqlalchemy import select
from datetime import datetime

BAND_RESULT_ITEMS = [
    "time", "i_flux", "i_uncertainty", "q_flux", "q_uncertainty", "u_flux", "u_uncertainty"
]

class LightcurveBandResult(BaseModel):
    band: Band

    time: list[datetime]

    i_flux: list[float]
    i_uncertainty: list[float]

    q_flux: list[float]
    q_uncertainty: list[float]

    u_flux: list[float]
    u_uncertainty: list[float]


class LightcurveResult(BaseModel):
    source: Source
    bands: list[LightcurveBandResult]

async def lightcurve_read_band(id: int, band_name: str, conn: AsyncConnection) -> LightcurveBandResult:
    query = select(FluxMeasurementTable)

    query = query.filter(FluxMeasurementTable.source_id == id)
    query = query.filter(FluxMeasurementTable.band_name == band_name)

    query.order_by(FluxMeasurementTable.time)

    res = await conn.execute(query)

    outputs = {x: [] for x in BAND_RESULT_ITEMS}

    for item in res.all():
        for x in BAND_RESULT_ITEMS:
            outputs[x].append(getattr(item, x))

    band = await conn.get(BandTable, band_name)

    return LightcurveBandResult(
        band=band.to_model(),
        **outputs
    )

async def lightcurve_read_source(id: int, conn: AsyncConnection) -> LightcurveResult:
    query = select(FluxMeasurementTable.band_name)
    query = query.filter(FluxMeasurementTable.source_id == id)
    query = query.distinct()

    band_names = (await conn.execute(query)).all()

    bands = [lightcurve_read_band(id=id, band_name=b, conn=conn) for b in band_names]
    bands = await asyncio.wait(bands)

    source = await conn.get(SourceTable, id)

    return LightcurveResult(source=source.to_model(), bands=bands)

