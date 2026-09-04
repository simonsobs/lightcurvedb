"""
TimescaleDB analysis provider.

Overrides get_median_flux_for_all_sources to read from the flux_monthly
continuous aggregate (created by TimescaleLightcurveProvider.setup()) instead
of scanning the full flux_measurements hypertable on every call - see
PostgresAnalysisProvider for the naive version this replaces.
"""

from collections import defaultdict
from uuid import UUID

from lightcurvedb.storage.postgres.analysis import PostgresAnalysisProvider


class TimescaleAnalysisProvider(PostgresAnalysisProvider):
    async def get_median_flux_for_all_sources(self) -> dict[UUID, dict[str, float]]:
        """
        Get the median flux for every source, grouped by module and frequency
        (keyed as f"{module}_{frequency}", matching
        PostgresAnalysisProvider's key format), from the current month's
        bucket of the flux_monthly continuous aggregate rather than scanning
        all of flux_measurements.
        """
        query = """
            SELECT source_id, module, frequency, median_flux
            FROM flux_monthly
            WHERE bucket = time_bucket('30 days', now())
        """

        with self.tracer.start_as_current_span("get_median_flux_for_all_sources"):
            async with self.flux_storage.cursor() as cur:
                await cur.execute(query)
                rows = await cur.fetchall()

        result: dict[UUID, dict[str, float]] = defaultdict(dict)
        for source_id, module, frequency, median_flux in rows:
            result[source_id][f"{module}_{frequency}"] = median_flux

        return dict(result)
