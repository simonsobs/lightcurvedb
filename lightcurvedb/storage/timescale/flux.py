"""
TimescaleDB implementation of FluxMeasurementStorage protocol.
"""

import json
from uuid import UUID

from lightcurvedb.models.flux import FluxMeasurement
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

    async def create(self, measurement: FluxMeasurement) -> UUID:
        """
        Insert single measurement.
        """
        query = """
            INSERT INTO flux_measurements (
                measurement_id, frequency, module, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty,
                flux, flux_err, extra
            )
            VALUES (
                %(measurement_id)s, %(frequency)s, %(module)s, %(source_id)s, %(time)s,
                %(ra)s, %(dec)s, %(ra_uncertainty)s, %(dec_uncertainty)s,
                %(flux)s, %(flux_err)s, %(extra)s
            )
            ON CONFLICT (time, frequency, module, source_id) DO NOTHING
            RETURNING measurement_id 
        """

        with self.tracer.start_as_current_span("create_flux_measurement") as span:
            span.set_attribute("flux.num_measurements", 1)

            params = measurement.model_dump()

            if params["extra"] is not None:
                params["extra"] = json.dumps(params["extra"])

            async with self.cursor() as cur:
                await cur.execute(query, params)
                row = await cur.fetchone()
                if row is None:
                    raise ValueError("INSERT RETURNING measurement_id returned no row")
                return row[0]
