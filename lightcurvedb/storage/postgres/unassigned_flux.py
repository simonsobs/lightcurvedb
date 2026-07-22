"""
PostgreSQL implementation of unassigned flux measurement storage.
"""

import json
from uuid import UUID

from psycopg.rows import class_row

from lightcurvedb.models import UnassignedFluxMeasurement
from lightcurvedb.models.exceptions import UnassignedFluxMeasurementNotFoundException
from lightcurvedb.storage.postgres.pooler import PostgresPoolUser
from lightcurvedb.storage.postgres.schema import UNASSIGNED_FLUX_MEASUREMENTS_TABLE
from lightcurvedb.storage.prototype.unassigned_flux import (
    ProvidesUnassignedFluxMeasurementStorage,
)


class PostgresUnassignedFluxMeasurementStorage(
    ProvidesUnassignedFluxMeasurementStorage, PostgresPoolUser
):
    """
    PostgreSQL storage for measurements awaiting cross-match review.
    """

    async def setup(self) -> None:
        async with self.cursor() as cur:
            await cur.execute(UNASSIGNED_FLUX_MEASUREMENTS_TABLE)

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

    async def get(self, measurement_id: UUID) -> UnassignedFluxMeasurement:
        """
        Retrieve an unassigned flux measurement by ID.
        """
        query = """
            SELECT measurement_id, frequency, module, source_id, time, ra, dec,
                   ra_uncertainty, dec_uncertainty, flux, flux_err, extra
            FROM unassigned_flux_measurements
            WHERE measurement_id = %(measurement_id)s
        """

        with self.tracer.start_as_current_span(
            "get_unassigned_flux_measurement"
        ) as span:
            span.set_attribute("unassigned_flux.measurement_id", str(measurement_id))

            async with self.cursor(
                row_factory=class_row(UnassignedFluxMeasurement)
            ) as cur:
                await cur.execute(query, {"measurement_id": measurement_id})
                row = await cur.fetchone()

            if row is None:
                raise UnassignedFluxMeasurementNotFoundException(
                    f"Unassigned flux measurement {measurement_id} not found"
                )
            return row

    async def get_for_source(self, source_id: UUID) -> list[UnassignedFluxMeasurement]:
        """
        Retrieve all unassigned flux measurements for a source.
        """
        query = """
            SELECT measurement_id, frequency, module, source_id, time, ra, dec,
                   ra_uncertainty, dec_uncertainty, flux, flux_err, extra
            FROM unassigned_flux_measurements
            WHERE source_id = %(source_id)s
            ORDER BY time, measurement_id
        """

        with self.tracer.start_as_current_span(
            "get_unassigned_flux_measurements_for_source"
        ) as span:
            span.set_attribute("unassigned_source.source_id", str(source_id))

            async with self.cursor(
                row_factory=class_row(UnassignedFluxMeasurement)
            ) as cur:
                await cur.execute(query, {"source_id": source_id})
                return await cur.fetchall()

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete an unassigned flux measurement by ID.
        """
        with self.tracer.start_as_current_span(
            "delete_unassigned_flux_measurement"
        ) as span:
            span.set_attribute("unassigned_flux.measurement_id", str(measurement_id))
            await self.get(measurement_id)

            async with self.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM unassigned_flux_measurements
                    WHERE measurement_id = %(measurement_id)s
                    """,
                    {"measurement_id": measurement_id},
                )
