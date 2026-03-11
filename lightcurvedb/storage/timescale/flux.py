"""
TimescaleDB implementation of FluxMeasurementStorage protocol.
"""

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
