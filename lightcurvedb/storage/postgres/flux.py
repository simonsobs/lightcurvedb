"""
PostgreSQL implementation of FluxMeasurementStorage protocol.
"""

from psycopg import AsyncConnection
from psycopg.rows import dict_row
import json
from datetime import datetime

from lightcurvedb.models.flux import FluxMeasurement, FluxMeasurementCreate, MeasurementMetadata
from lightcurvedb.models.responses import LightcurveBandData, SourceStatistics


class PostgresFluxMeasurementStorage:
    """
    PostgreSQL flux measurement storage with array aggregations.
    """

    def __init__(self, conn: AsyncConnection):
        self.conn = conn

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
            RETURNING id, band_name, source_id, time, ra, dec,
                      ra_uncertainty, dec_uncertainty, i_flux, i_uncertainty, extra
        """

        params = measurement.model_dump()

        if params['extra'] is not None:
            params['extra'] = json.dumps(params['extra'])

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()

            if row['extra']:
                row['extra'] = MeasurementMetadata(**row['extra'])

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
            if params['extra'] is not None:
                params['extra'] = json.dumps(params['extra'])
            params_list.append(params)

        async with self.conn.cursor() as cur:
            await cur.executemany(query, params_list)

        return len(measurements)

    async def get_band_data(self, source_id: int, band_name: str) -> LightcurveBandData:
        """
        Get all measurements as arrays using database-side aggregation.
        """
        query = """
            SELECT
                COALESCE(ARRAY_AGG(id ORDER BY time), ARRAY[]::INTEGER[]) as ids,
                COALESCE(ARRAY_AGG(time ORDER BY time), ARRAY[]::TIMESTAMPTZ[]) as times,
                COALESCE(ARRAY_AGG(ra ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as ra,
                COALESCE(ARRAY_AGG(dec ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as dec,
                COALESCE(ARRAY_AGG(ra_uncertainty ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as ra_uncertainty,
                COALESCE(ARRAY_AGG(dec_uncertainty ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as dec_uncertainty,
                COALESCE(ARRAY_AGG(i_flux ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as i_flux,
                COALESCE(ARRAY_AGG(i_uncertainty ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as i_uncertainty
            FROM flux_measurements
            WHERE source_id = %(source_id)s AND band_name = %(band_name)s
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, {"source_id": source_id, "band_name": band_name})
            row = await cur.fetchone()
            return LightcurveBandData(**row)

    async def get_time_range(
        self,
        source_id: int,
        band_name: str,
        start_time: datetime,
        end_time: datetime
    ) -> LightcurveBandData:
        """
        Get measurements in time.
        """
        query = """
            SELECT
                COALESCE(ARRAY_AGG(id ORDER BY time), ARRAY[]::INTEGER[]) as ids,
                COALESCE(ARRAY_AGG(time ORDER BY time), ARRAY[]::TIMESTAMPTZ[]) as times,
                COALESCE(ARRAY_AGG(ra ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as ra,
                COALESCE(ARRAY_AGG(dec ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as dec,
                COALESCE(ARRAY_AGG(ra_uncertainty ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as ra_uncertainty,
                COALESCE(ARRAY_AGG(dec_uncertainty ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as dec_uncertainty,
                COALESCE(ARRAY_AGG(i_flux ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as i_flux,
                COALESCE(ARRAY_AGG(i_uncertainty ORDER BY time), ARRAY[]::DOUBLE PRECISION[]) as i_uncertainty
            FROM flux_measurements
            WHERE source_id = %(source_id)s
              AND band_name = %(band_name)s
              AND time BETWEEN %(start_time)s AND %(end_time)s
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, {
                "source_id": source_id,
                "band_name": band_name,
                "start_time": start_time,
                "end_time": end_time
            })
            row = await cur.fetchone()
            return LightcurveBandData(**row)

    async def get_statistics(self, source_id: int, band_name: str) -> SourceStatistics:
        """
        Compute statistics using database-side aggregations.
        """
        query = """
            SELECT
                COUNT(*) as measurement_count,
                MIN(i_flux) as min_flux,
                MAX(i_flux) as max_flux,
                AVG(i_flux) as mean_flux,
                STDDEV(i_flux) as stddev_flux,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i_flux) as median_flux,
                MIN(time) as start_time,
                MAX(time) as end_time
            FROM flux_measurements
            WHERE source_id = %(source_id)s AND band_name = %(band_name)s
        """

        async with self.conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, {"source_id": source_id, "band_name": band_name})
            row = await cur.fetchone()
            return SourceStatistics(**row)

    async def delete(self, id: int) -> None:
        """
        Delete a flux measurement by ID.
        """
        query = "DELETE FROM flux_measurements WHERE id = %(id)s"

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"id": id})

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
