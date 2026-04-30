"""
TimescaleDB implementation of FluxMeasurementStorage protocol.
"""

import json
from collections import defaultdict
from uuid import UUID

from lightcurvedb.models.flux import FluxMeasurementCreate
from lightcurvedb.storage.postgres.flux import PostgresFluxMeasurementStorage
from lightcurvedb.storage.timescale.schema import FLUX_INDEXES, FLUX_MEASUREMENTS_TABLE


class TimescaleFluxMeasurementStorage(PostgresFluxMeasurementStorage):
    """
    TimescaleDB flux measurement storage with array aggregations.
    """

    async def setup(self) -> None:
        async with self.cursor() as cur:
            await cur.execute(FLUX_MEASUREMENTS_TABLE)
            await cur.execute(FLUX_INDEXES)

    async def create(self, measurement: FluxMeasurementCreate) -> UUID:
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
            ON CONFLICT (time, frequency, module, source_id) DO NOTHING
            RETURNING measurement_id 
        """

        params = measurement.model_dump()

        if params["extra"] is not None:
            params["extra"] = json.dumps(params["extra"])

        async with self.cursor() as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            if row is None:
                raise ValueError("INSERT RETURNING measurement_id returned no row")
            return row[0]

    async def create_batch(
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[UUID]:
        """
        Bulk insert
        """
        data = defaultdict(list)

        for measurement in measurements:
            measurement_dict = measurement.model_dump()
            if measurement_dict["extra"] is not None:
                measurement_dict["extra"] = json.dumps(measurement_dict["extra"])
            for key, value in measurement_dict.items():
                data[key].append(value)

        return await self._insert_batch_data(data)

    async def _insert_batch_data(self, data: dict[str, list]) -> list[UUID]:
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
            ON CONFLICT (time, frequency, module, source_id) DO NOTHING
            RETURNING measurement_id 
        """

        async with self.cursor() as cur:
            await cur.execute(query, data)
            response = await cur.fetchall()
            inserted_measurement_ids = [row[0] for row in response]

        return inserted_measurement_ids
