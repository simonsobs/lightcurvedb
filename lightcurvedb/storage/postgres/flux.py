"""
PostgreSQL implementation of FluxMeasurementStorage protocol.
"""

import json
from datetime import datetime

import psycopg
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from lightcurvedb.models.flux import (
    FluxMeasurement,
    FluxMeasurementCreate,
    MeasurementMetadata,
)
from lightcurvedb.models.responses import LightcurveBandData, SourceStatistics
from lightcurvedb.storage.postgres.schema import FLUX_INDEXES, FLUX_MEASUREMENTS_TABLE
from lightcurvedb.storage.prototype.flux import ProvidesFluxMeasurementStorage


class PostgresFluxMeasurementStorage(ProvidesFluxMeasurementStorage):
    """
    PostgreSQL flux measurement storage with array aggregations.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

    async def setup(self) -> None:
        async with self.conn.cursor() as cur:
            await cur.execute(FLUX_MEASUREMENTS_TABLE)
            await cur.execute(FLUX_INDEXES)

    async def create(self, measurement: FluxMeasurementCreate) -> FluxMeasurement:
        """
        Insert single measurement.
        """
        query = """
            INSERT INTO flux_measurements (
                band_name, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty,
                i_flux, i_uncertainty, extra
            )
            VALUES (
                %(band_name)s, %(source_id)s, %(time)s, %(ra)s, %(dec)s,
                %(ra_uncertainty)s, %(dec_uncertainty)s,
                %(i_flux)s, %(i_uncertainty)s, %(extra)s
            )
            RETURNING flux_id, band_name, source_id, time, ra, dec,
                      ra_uncertainty, dec_uncertainty, i_flux, i_uncertainty, extra
        """

        params = measurement.model_dump()

        if params["extra"] is not None:
            params["extra"] = json.dumps(params["extra"])

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()

            if row["extra"]:
                row["extra"] = MeasurementMetadata(**row["extra"])

            return FluxMeasurement(**row)

    async def create_batch(self, measurements: list[FluxMeasurementCreate]) -> int:
        """
        Bulk insert
        """
        query = """
            INSERT INTO flux_measurements (
                band_name, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty,
                i_flux, i_uncertainty, extra
            )
            VALUES (
                %(band_name)s, %(source_id)s, %(time)s, %(ra)s, %(dec)s,
                %(ra_uncertainty)s, %(dec_uncertainty)s,
                %(i_flux)s, %(i_uncertainty)s, %(extra)s
            )
        """

        params_list = []
        for m in measurements:
            params = m.model_dump()
            if params["extra"] is not None:
                params["extra"] = json.dumps(params["extra"])
            params_list.append(params)

        async with self.conn.cursor() as cur:
            await cur.executemany(query, params_list)

        return len(measurements)

    async def get_band_data(
        self,
        source_id: int,
        band_name: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> LightcurveBandData:
        """
        Get measurements as arrays using database-side aggregation.
        Optionally filter by time range.
        """
        where_clauses = ["source_id = %(source_id)s", "band_name = %(band_name)s"]
        params: dict[str, int | str | datetime] = {
            "source_id": source_id,
            "band_name": band_name,
        }

        if start_time is not None:
            where_clauses.append("time >= %(start_time)s")
            params["start_time"] = start_time
        if end_time is not None:
            where_clauses.append("time <= %(end_time)s")
            params["end_time"] = end_time

        query = f"""
            SELECT
                COALESCE(ARRAY_AGG(flux_id), ARRAY[]::INTEGER[]) AS flux_ids,
                COALESCE(ARRAY_AGG(time), ARRAY[]::TIMESTAMPTZ[]) AS times,
                COALESCE(ARRAY_AGG(ra), ARRAY[]::REAL[]) AS ra,
                COALESCE(ARRAY_AGG(dec), ARRAY[]::REAL[]) AS dec,
                COALESCE(ARRAY_AGG(ra_uncertainty), ARRAY[]::REAL[]) AS ra_uncertainty,
                COALESCE(ARRAY_AGG(dec_uncertainty), ARRAY[]::REAL[]) AS dec_uncertainty,
                COALESCE(ARRAY_AGG(i_flux), ARRAY[]::REAL[]) AS i_flux,
                COALESCE(ARRAY_AGG(i_uncertainty), ARRAY[]::REAL[]) AS i_uncertainty
            FROM (
                SELECT *
                FROM flux_measurements
                ORDER BY time
            ) t
            WHERE {" AND ".join(where_clauses)}
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return LightcurveBandData(source_id=source_id, band_name=band_name, **row)

    async def get_statistics(
        self,
        source_id: int,
        band_name: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Compute statistics using database-side aggregations.
        """
        where_clauses = ["source_id = %(source_id)s", "band_name = %(band_name)s"]
        params: dict[str, int | str | datetime] = {
            "source_id": source_id,
            "band_name": band_name,
        }

        if start_time is not None:
            where_clauses.append("time >= %(start_time)s")
            params["start_time"] = start_time
        if end_time is not None:
            where_clauses.append("time <= %(end_time)s")
            params["end_time"] = end_time

        query = f"""
            SELECT
                COUNT(*) as measurement_count,
                MIN(i_flux) as min_flux,
                MAX(i_flux) as max_flux,
                AVG(i_flux) as mean_flux,
                STDDEV(i_flux) as stddev_flux,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i_flux) as median_flux,
                SUM(i_flux / NULLIF(POWER(i_uncertainty, 2), 0)) /
                    NULLIF(SUM(1.0 / NULLIF(POWER(i_uncertainty, 2), 0)), 0)
                    AS weighted_mean_flux,
                1.0 / SQRT(NULLIF(SUM(1.0 / NULLIF(POWER(i_uncertainty, 2), 0)), 0))
                    AS weighted_error_on_mean_flux,
                MIN(time) as start_time,
                MAX(time) as end_time
            FROM flux_measurements
            WHERE {" AND ".join(where_clauses)}
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return SourceStatistics(**row)

    async def delete(self, flux_id: int) -> None:
        """
        Delete a flux measurement by ID.
        """
        query = "DELETE FROM flux_measurements WHERE flux_id = %(flux_id)s"

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"flux_id": flux_id})

    async def get_bands_for_source(self, source_id: int) -> list[str]:
        """
        Get distinct band names that have measurements for a given source.
        """
        query = """
            SELECT DISTINCT band_name
            FROM flux_measurements
            WHERE source_id = %(source_id)s
        """

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"source_id": source_id})
            rows = await cur.fetchall()
            return [row[0] for row in rows]

    async def get_recent_measurements(
        self, source_id: int, band_name: str, limit: int
    ) -> LightcurveBandData:
        """
        Get most recent N measurements for source/band, ordered by time DESC.
        """
        query = """
            SELECT
                COALESCE(ARRAY_AGG(flux_id), ARRAY[]::INTEGER[]) AS flux_ids,
                COALESCE(ARRAY_AGG(time), ARRAY[]::TIMESTAMPTZ[]) AS times,
                COALESCE(ARRAY_AGG(ra), ARRAY[]::REAL[]) AS ra,
                COALESCE(ARRAY_AGG(dec), ARRAY[]::REAL[]) AS dec,
                COALESCE(ARRAY_AGG(ra_uncertainty), ARRAY[]::REAL[]) AS ra_uncertainty,
                COALESCE(ARRAY_AGG(dec_uncertainty), ARRAY[]::REAL[]) AS dec_uncertainty,
                COALESCE(ARRAY_AGG(i_flux), ARRAY[]::REAL[]) AS i_flux,
                COALESCE(ARRAY_AGG(i_uncertainty), ARRAY[]::REAL[]) AS i_uncertainty
            FROM (
                SELECT *
                FROM flux_measurements
                WHERE source_id = %(source_id)s
                AND band_name = %(band_name)s
                ORDER BY time DESC
                LIMIT %(limit)s
            ) subquery;
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                query, {"source_id": source_id, "band_name": band_name, "limit": limit}
            )
            try:
                row = await cur.fetchone()
            except psycopg.ProgrammingError:
                # No data found
                row = {
                    "flux_ids": [],
                    "times": [],
                    "ra": [],
                    "dec": [],
                    "ra_uncertainty": [],
                    "dec_uncertainty": [],
                    "i_flux": [],
                    "i_uncertainty": [],
                }
            return LightcurveBandData(source_id=source_id, band_name=band_name, **row)
