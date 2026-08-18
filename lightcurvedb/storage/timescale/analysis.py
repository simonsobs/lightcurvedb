"""
TimescaleDB analysis provider.

Overrides get_median_flux_for_all_sources to read from the flux_median_monthly
continuous aggregate instead of scanning the full flux_measurements hypertable
on every call - see PostgresAnalysisProvider for the naive version this
replaces.
"""

from collections import defaultdict
from uuid import UUID

from lightcurvedb.storage.postgres.analysis import PostgresAnalysisProvider
from lightcurvedb.storage.timescale.schema import MEDIAN_CONTINUOUS_AGGREGATES


class TimescaleAnalysisProvider(PostgresAnalysisProvider):
    async def setup(self) -> None:
        """
        Create the flux_median_monthly continuous aggregate and its refresh
        policy.
        """
        async with self.flux_storage.cursor() as cur:
            for statement in MEDIAN_CONTINUOUS_AGGREGATES:
                await cur.execute(statement)

    async def get_median_flux_for_all_sources(self) -> dict[UUID, dict[int, float]]:
        """
        Get the median flux for every source, grouped by frequency, from the
        current month's bucket of the flux_median_monthly continuous
        aggregate rather than scanning all of flux_measurements.
        """
        query = """
            SELECT source_id, frequency, median_flux
            FROM flux_median_monthly
            WHERE bucket = time_bucket('30 days', now())
        """

        with self.tracer.start_as_current_span("get_median_flux_for_all_sources"):
            async with self.flux_storage.cursor() as cur:
                await cur.execute(query)
                rows = await cur.fetchall()

        result: dict[UUID, dict[int, float]] = defaultdict(dict)
        for source_id, frequency, median_flux in rows:
            result[source_id][frequency] = median_flux

        return dict(result)
