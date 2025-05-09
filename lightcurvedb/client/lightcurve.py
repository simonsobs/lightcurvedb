"""
A client for extracting complete light-curves.
"""

import asyncio
from datetime import datetime

from psycopg import AsyncConnection
from pydantic import BaseModel
from sqlalchemy import select

from lightcurvedb.client.band import BandNotFound
from lightcurvedb.client.source import SourceNotFound
from lightcurvedb.models.band import Band, BandTable
from lightcurvedb.models.flux import FluxMeasurementTable, MeasurementMetadata
from lightcurvedb.models.source import Source, SourceTable

BAND_RESULT_ITEMS = [
    "id",
    "time",
    "i_flux",
    "i_uncertainty",
    "ra",
    "dec",
    "ra_uncertainty",
    "dec_uncertainty",
    "extra",
]


class LightcurveBandResult(BaseModel):
    band: Band

    id: list[int]
    time: list[datetime]

    i_flux: list[float]
    i_uncertainty: list[float]

    ra: list[float]
    dec: list[float]

    ra_uncertainty: list[float]
    dec_uncertainty: list[float]

    extra: list[MeasurementMetadata | None]


class LightcurveResult(BaseModel):
    source: Source
    bands: list[LightcurveBandResult]


async def lightcurve_read_band(
    id: int, band_name: str, conn: AsyncConnection
) -> LightcurveBandResult:
    band = await conn.get(BandTable, band_name)

    if band is None:
        raise BandNotFound

    query = select(FluxMeasurementTable)

    query = query.filter(FluxMeasurementTable.source_id == id)
    query = query.filter(FluxMeasurementTable.band_name == band_name)

    query.order_by(FluxMeasurementTable.time)

    res = (await conn.execute(query)).scalars().all()

    if len(res) == 0:
        raise SourceNotFound

    outputs = {x: [] for x in BAND_RESULT_ITEMS}

    for item in res:
        for x in BAND_RESULT_ITEMS:
            outputs[x].append(getattr(item, x))

    return LightcurveBandResult(band=band.to_model(), **outputs)


async def lightcurve_read_source(id: int, conn: AsyncConnection) -> LightcurveResult:
    source = await conn.get(SourceTable, id)

    if source is None:
        raise SourceNotFound

    query = select(FluxMeasurementTable.band_name)
    query = query.filter(FluxMeasurementTable.source_id == id)
    query = query.distinct()

    band_names = (await conn.execute(query)).scalars().all()

    bands = [lightcurve_read_band(id=id, band_name=b, conn=conn) for b in band_names]
    bands = await asyncio.gather(*bands)

    return LightcurveResult(source=source.to_model(), bands=bands)
