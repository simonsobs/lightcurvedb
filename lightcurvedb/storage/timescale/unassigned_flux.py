"""
TimescaleDB implementation of unassigned flux measurement storage.
"""

import json
from uuid import UUID

from lightcurvedb.models.unassigned_flux import UnassignedFluxMeasurement
from lightcurvedb.storage.postgres.unassigned_flux import (
    PostgresUnassignedFluxMeasurementStorage,
)
from lightcurvedb.storage.timescale.schema import (
    UNASSIGNED_FLUX_INDEXES,
    UNASSIGNED_FLUX_MEASUREMENTS_TABLE,
)


class TimescaleUnassignedFluxMeasurementStorage(
    PostgresUnassignedFluxMeasurementStorage
):
    """
    TimescaleDB storage for measurements awaiting cross-match review.
    """

    async def setup(self) -> None:
        async with self.cursor() as cur:
            await cur.execute(UNASSIGNED_FLUX_MEASUREMENTS_TABLE)
            await cur.execute(UNASSIGNED_FLUX_INDEXES)

    async def create(self, measurement: UnassignedFluxMeasurement) -> UUID:
        """
        Insert an unassigned flux measurement.
        """
        query = """
            INSERT INTO unassigned_flux_measurements (
                measurement_id, frequency, module, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty, flux, flux_err, extra
            )
            VALUES (
                %(measurement_id)s, %(frequency)s, %(module)s, %(source_id)s,
                %(time)s, %(ra)s, %(dec)s, %(ra_uncertainty)s,
                %(dec_uncertainty)s, %(flux)s, %(flux_err)s, %(extra)s
            )
            ON CONFLICT (time, source_id, frequency, module) DO NOTHING
            RETURNING measurement_id
        """

        with self.tracer.start_as_current_span(
            "create_unassigned_flux_measurement"
        ) as span:
            span.set_attribute(
                "unassigned_flux.measurement_id", str(measurement.measurement_id)
            )

            params = measurement.model_dump()
            if params["extra"] is not None:
                params["extra"] = json.dumps(params["extra"])

            async with self.cursor() as cur:
                await cur.execute(query, params)
                row = await cur.fetchone()
                if row is None:
                    raise ValueError("INSERT RETURNING measurement_id returned no row")
                return row[0]
