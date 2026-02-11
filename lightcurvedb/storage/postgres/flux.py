"""
PostgreSQL implementation of FluxMeasurementStorage protocol.
"""

import json
from collections import defaultdict
from datetime import datetime

from psycopg import AsyncConnection
from psycopg.rows import class_row

from lightcurvedb.models.flux import (
    FluxMeasurementCreate,
)
from lightcurvedb.models.responses import SourceStatistics
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

    async def create(self, measurement: FluxMeasurementCreate) -> int:
        """
        Insert single measurement.
        """
        query = """
            INSERT INTO flux_measurements (
                frequency, module, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty,
                flux, flux_err, extra
            )
            VALUES (
                %(frequency)s, %(module)s, %(source_id)s, %(time)s,
                %(ra)s, %(dec)s, %(ra_uncertainty)s, %(dec_uncertainty)s,
                %(flux)s, %(flux_err)s, %(extra)s
            )
            RETURNING measurement_id
        """

        params = measurement.model_dump()

        if params["extra"] is not None:
            params["extra"] = json.dumps(params["extra"])

        async with self.conn.cursor() as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return row[0]

    async def create_batch(
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[int]:
        """
        Bulk insert
        """
        query = """
            INSERT INTO flux_measurements (frequency, module, source_id, time, ra,
            dec, ra_uncertainty, dec_uncertainty, flux, flux_err, extra)
            SELECT * 
            FROM UNNEST(
                %(frequency)s::integer[],
                %(module)s::text[],
                %(source_id)s::uuid[],
                %(time)s::timestamptz[],
                %(ra)s::real[],
                %(dec)s::real[],
                %(ra_uncertainty)s::real[],
                %(dec_uncertainty)s::real[],
                %(flux)s::real[],
                %(flux_err)s::real[],
                %(extra)s::jsonb[]
            )
            RETURNING measurement_id
        """

        data = defaultdict(list)

        for measurement in measurements:
            measurement_dict = measurement.model_dump()
            if measurement_dict["extra"] is not None:
                measurement_dict["extra"] = json.dumps(measurement_dict["extra"])
            for key, value in measurement_dict.items():
                data[key].append(value)

        async with self.conn.cursor() as cur:
            await cur.execute(query, data)
            response = await cur.fetchall()
            inserted_measurement_ids = [row[0] for row in response]

        return inserted_measurement_ids

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
                MIN(flux) as min_flux,
                MAX(flux) as max_flux,
                AVG(flux) as mean_flux,
                STDDEV(flux) as stddev_flux,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY flux) as median_flux,
                SUM(flux / NULLIF(POWER(flux_err, 2), 0)) /
                    NULLIF(SUM(1.0 / NULLIF(POWER(flux_err, 2), 0)), 0)
                    AS weighted_mean_flux,
                1.0 / SQRT(NULLIF(SUM(1.0 / NULLIF(POWER(flux_err, 2), 0)), 0))
                    AS weighted_error_on_mean_flux,
                MIN(time) as start_time,
                MAX(time) as end_time
            FROM flux_measurements
            WHERE {" AND ".join(where_clauses)}
        """

        async with self.conn.cursor(row_factory=class_row(SourceStatistics)) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return row

    async def delete(self, measurement_id: int) -> None:
        """
        Delete a flux measurement by ID.
        """
        query = (
            "DELETE FROM flux_measurements WHERE measurement_id = %(measurement_id)s"
        )

        async with self.conn.cursor() as cur:
            await cur.execute(query, {"measurement_id": measurement_id})
