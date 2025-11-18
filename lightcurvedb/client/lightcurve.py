"""
A client for extracting complete light-curves.
"""

import asyncio
from datetime import datetime

from psycopg import AsyncConnection
from psycopg.rows import dict_row
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

class LightcurveBandData(BaseModel):
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

class LightcurveBandResult(LightcurveBandData):
    source: Source

class LightcurveResult(BaseModel):
    source: Source
    bands: list[LightcurveBandData]

async def _read_lightcurve_band_data(
    id: int, band_name: str, conn: AsyncConnection
) -> LightcurveBandData:
    band = await conn.get(BandTable, band_name)

    if band is None:
        raise BandNotFound

    query = select(FluxMeasurementTable)
    query = query.filter(FluxMeasurementTable.source_id == id)
    query = query.filter(FluxMeasurementTable.band_name == band_name)
    query = query.order_by(FluxMeasurementTable.time)

    res = (await conn.execute(query)).scalars().all()

    if len(res) == 0:
        raise SourceNotFound

    outputs = {x: [] for x in BAND_RESULT_ITEMS}

    for item in res:
        for x in BAND_RESULT_ITEMS:
            outputs[x].append(getattr(item, x))

    return LightcurveBandData(band=band.to_model(), **outputs)

async def lightcurve_read_band(
    id: int, band_name: str, conn: AsyncConnection
) -> LightcurveBandResult:
    source = await conn.get(SourceTable, id)

    if source is None:
        raise SourceNotFound
    band_data = await _read_lightcurve_band_data(id=id, band_name=band_name, conn=conn)

    return LightcurveBandResult(source=source.to_model(), **band_data.model_dump())

async def lightcurve_read_source(id: int, conn: AsyncConnection) -> LightcurveResult:
    source = await conn.get(SourceTable, id)

    if source is None:
        raise SourceNotFound

    query = select(FluxMeasurementTable.band_name)
    query = query.filter(FluxMeasurementTable.source_id == id)
    query = query.distinct()

    band_names = (await conn.execute(query)).scalars().all()

    bands = [_read_lightcurve_band_data(id=id, band_name=b, conn=conn) for b in band_names]
    band_data_list = await asyncio.gather(*bands)

    return LightcurveResult(source=source.to_model(),bands=band_data_list)


async def _get_band_sql(band_name: str, conn: AsyncConnection) -> Band:
    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            "SELECT * FROM bands WHERE name = %s",
            [band_name]
        )
        row = await cursor.fetchone()
        if not row:
            raise BandNotFound
        return Band(**row)


async def _get_source_sql(source_id: int, conn: AsyncConnection) -> Source:
    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            "SELECT * FROM sources WHERE id = %s",
            [source_id]
        )
        row = await cursor.fetchone()
        if not row:
            raise SourceNotFound
        return Source(**row)


async def _get_band_names_sql(source_id: int, conn: AsyncConnection) -> list[str]:
    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """
            SELECT DISTINCT band_name
            FROM flux_measurements
            WHERE source_id = %s
            ORDER BY band_name
            """,
            [source_id]
        )
        rows = await cursor.fetchall()
        return [row['band_name'] for row in rows]


async def _read_lightcurve_band_data_sql(
    id: int, band_name: str, conn: AsyncConnection
) -> LightcurveBandData:
    band = await _get_band_sql(band_name, conn)
    _LIGHTCURVE_QUERY = """
        SELECT
            ARRAY_AGG(id ORDER BY time) as id,
            ARRAY_AGG(time ORDER BY time) as time,
            ARRAY_AGG(i_flux ORDER BY time) as i_flux,
            ARRAY_AGG(i_uncertainty ORDER BY time) as i_uncertainty,
            ARRAY_AGG(ra ORDER BY time) as ra,
            ARRAY_AGG(dec ORDER BY time) as dec,
            ARRAY_AGG(ra_uncertainty ORDER BY time) as ra_uncertainty,
            ARRAY_AGG(dec_uncertainty ORDER BY time) as dec_uncertainty,
            ARRAY_AGG(extra ORDER BY time) as extra
        FROM flux_measurements
        WHERE source_id = %s AND band_name = %s
    """
    async with conn.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(_LIGHTCURVE_QUERY, [id, band_name])
        row = await cursor.fetchone()

        if not row:
            raise BandNotFound

    return LightcurveBandData(band=band, **row)


async def lightcurve_read_band_sql(
    id: int, band_name: str, conn: AsyncConnection
) -> LightcurveBandResult:
    source = await _get_source_sql(id, conn)
    band_data = await _read_lightcurve_band_data_sql(id, band_name, conn)
    return LightcurveBandResult(source=source, **band_data.model_dump())


async def lightcurve_read_source_sql(
    id: int, conn: AsyncConnection
) -> LightcurveResult:
    source = await _get_source_sql(id, conn)
    band_names = await _get_band_names_sql(id, conn)

    bands = []
    for band_name in band_names:
        band_data = await _read_lightcurve_band_data_sql(id, band_name, conn)
        bands.append(band_data)

    return LightcurveResult(source=source, bands=bands)
