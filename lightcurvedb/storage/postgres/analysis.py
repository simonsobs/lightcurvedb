"""
Analysis of flux measurements and lightcurves.
"""

import asyncio
from datetime import datetime

from psycopg.rows import class_row

from lightcurvedb.models.statistics import SourceStatistics
from lightcurvedb.storage.postgres.flux import PostgresFluxMeasurementStorage
from lightcurvedb.storage.postgres.lightcurves import PostgresLightcurveProvider
from lightcurvedb.storage.prototype.analysis import ProvidesAnalysis


class PostgresAnalysisProvider(ProvidesAnalysis):
    def __init__(
        self,
        flux_storage: PostgresFluxMeasurementStorage,
        lightcurve_provider: PostgresLightcurveProvider,
    ):
        self.flux_storage = flux_storage
        self.lightcurve_provider = lightcurve_provider

    async def get_source_statistics_for_frequency_and_module(
        self,
        source_id: int,
        module: str,
        frequency: int,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Get statistics for a given source for a specific frequency and module.
        Supports "module = 'all'" to get statistics across all modules for the
        given frequency.
        """
        where_clauses = [
            "source_id = %(source_id)s",
            "frequency = %(frequency)s",
        ]
        params: dict[str, int | str | datetime] = {
            "source_id": source_id,
            "frequency": frequency,
        }

        if start_time is not None:
            where_clauses.append("time >= %(start_time)s")
            params["start_time"] = start_time
        if end_time is not None:
            where_clauses.append("time <= %(end_time)s")
            params["end_time"] = end_time
        if module != "all":
            where_clauses.append("module = %(module)s")
            params["module"] = module

        query = f"""
            SELECT
                %(source_id)s as source_id,
                {'%(module)s' if module != 'all' else "'all'"} as module,
                %(frequency)s as frequency,
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

        async with self.flux_storage.conn.cursor(
            row_factory=class_row(SourceStatistics)
        ) as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            return row

    async def get_source_statistics_for_frequency(
        self,
        source_id: int,
        frequency: int,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> SourceStatistics:
        """
        Get source statistics for a given frequency.
        """

        return await self.get_source_statistics_for_frequency_and_module(
            source_id=source_id,
            module="all",
            frequency=frequency,
            start_time=start_time,
            end_time=end_time,
        )

    async def get_source_statistics(
        self,
        source_id: int,
        collate_modules: bool = False,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> dict[str, SourceStatistics]:
        """
        Get source statistics across all frequencies and modules.
        """

        module_frequency_pairs = (
            await self.lightcurve_provider.get_module_frequency_pairs_for_source(
                source_id=source_id
            )
        )

        if collate_modules:
            unique_frequencies = set(pair[1] for pair in module_frequency_pairs)
            module_frequency_pairs = [("all", freq) for freq in unique_frequencies]

        statistics = await asyncio.gather(
            *[
                self.get_source_statistics_for_frequency_and_module(
                    source_id=source_id,
                    module=module,
                    frequency=frequency,
                    start_time=start_time,
                    end_time=end_time,
                )
                for module, frequency in module_frequency_pairs
            ]
        )

        if collate_modules:
            return {stats.frequency: stats for stats in statistics}

        else:
            return {f"{stats.module}_{stats.frequency}": stats for stats in statistics}
