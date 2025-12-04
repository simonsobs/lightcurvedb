"""
A client for extracting complete light-curves.
"""

import asyncio
from datetime import datetime
from typing import Protocol
from pydantic import BaseModel
from lightcurvedb.client.band import BandNotFound
from lightcurvedb.client.source import SourceNotFound
from lightcurvedb.models.band import Band
from lightcurvedb.models.flux import MeasurementMetadata
from lightcurvedb.models.source import Source

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


class LightcurveConnection(Protocol):

    async def get_source(self, source_id: int) -> Source:
        ...

    async def get_band(self, band_name: str) -> Band:
        ...

    async def get_band_names(self, source_id: int) -> list[str]:
        ...

    async def read_band_data(
        self, source_id: int, band_name: str
    ) -> LightcurveBandData:
        ...

class SQLAlchemyLightcurveConnection:

    def __init__(self, session):
        from sqlalchemy.ext.asyncio import AsyncSession
        from lightcurvedb.models.band import BandTable
        from lightcurvedb.models.flux import FluxMeasurementTable
        from lightcurvedb.models.source import SourceTable

        self._session = session
        self._BandTable = BandTable
        self._FluxMeasurementTable = FluxMeasurementTable
        self._SourceTable = SourceTable

    async def get_source(self, source_id: int) -> Source:
        res = await self._session.get(self._SourceTable, source_id)
        if res is None:
            raise SourceNotFound
        return res.to_model()

    async def get_band(self, band_name: str) -> Band:
        res = await self._session.get(self._BandTable, band_name)
        if res is None:
            raise BandNotFound
        return res.to_model()

    async def get_band_names(self, source_id: int) -> list[str]:
        from sqlalchemy import select

        query = select(self._FluxMeasurementTable.band_name)
        query = query.filter(self._FluxMeasurementTable.source_id == source_id)
        query=query.distinct()
        result = await self._session.execute(query)
        return result.scalars().all()

    async def read_band_data(
        self, source_id: int, band_name: str
    ) -> LightcurveBandData:

        from sqlalchemy import select

        band = await self.get_band(band_name)

        query = select(self._FluxMeasurementTable)
        query = query.filter(self._FluxMeasurementTable.source_id == source_id)
        query = query.filter(self._FluxMeasurementTable.band_name == band_name)
        query = query.order_by(self._FluxMeasurementTable.time)

        res = (await self._session.execute(query)).scalars().all()

        if len(res) == 0:
            raise SourceNotFound

        outputs = {x: [] for x in BAND_RESULT_ITEMS}
        for item in res:
            for x in BAND_RESULT_ITEMS:
                outputs[x].append(getattr(item, x))

        return LightcurveBandData(band=band, **outputs)

class PsycopgLightcurveConnection:
    def __init__(self, conn):
        from psycopg import AsyncConnection

        self._conn = conn

    async def get_source(self, source_id: int) -> Source:
        from psycopg.rows import dict_row

        async with self._conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                "SELECT * FROM sources WHERE id = %s",
                [source_id],
            )
            row = await cursor.fetchone()
            if not row:
                raise SourceNotFound
            return Source(**row)

    async def get_band(self, band_name: str) -> Band:
        from psycopg.rows import dict_row

        async with self._conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                "SELECT * FROM bands WHERE name = %s",
                [band_name],
            )
            row = await cursor.fetchone()
            if not row:
                raise BandNotFound
            return Band(**row)

    async def get_band_names(self, source_id: int) -> list[str]:
        from psycopg.rows import dict_row

        async with self._conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                SELECT DISTINCT band_name
                FROM flux_measurements
                WHERE source_id = %s
                ORDER BY band_name
                """,
                [source_id],
            )
            rows = await cursor.fetchall()
            return [row["band_name"] for row in rows]

    async def read_band_data(
        self, source_id: int, band_name: str
    ) -> LightcurveBandData:

        from psycopg.rows import dict_row

        band = await self.get_band(band_name)

        async with self._conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
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
                """,
                [source_id, band_name],
            )
            row = await cursor.fetchone()

            if not row or not row["id"]:
                raise SourceNotFound

        return LightcurveBandData(band=band, **row)



async def lightcurve_read_band(
    id: int, band_name: str, conn: LightcurveConnection
) -> LightcurveBandResult:

    source = await conn.get_source(id)
    band_data = await conn.read_band_data(id, band_name)
    return LightcurveBandResult(source=source, **band_data.model_dump())


async def lightcurve_read_source(id: int, conn: LightcurveConnection) -> LightcurveResult:
    source = await conn.get_source(id)
    band_names = await conn.get_band_names(id)


    band_data_list = await asyncio.gather(
        *[conn.read_band_data(id, band_name) for band_name in band_names]
    )

    return LightcurveResult(source=source, bands=band_data_list)
