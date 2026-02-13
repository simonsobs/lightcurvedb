"""
Provider for lightcurves from TimescaleDB data stores.

Extends the PostgreSQL provider by reading binned lightcurves from
TimescaleDB continuous aggregates rather than computing date_bin()
on the fly against raw flux_measurements rows.
"""

import datetime
from typing import Literal
from uuid import UUID

from psycopg.rows import class_row

from lightcurvedb.models.lightcurves import (
    BinnedFrequencyLightcurve,
    BinnedInstrumentLightcurve,
)
from lightcurvedb.storage.postgres.lightcurves import PostgresLightcurveProvider
from lightcurvedb.storage.timescale.flux import TimescaleFluxMeasurementStorage
from lightcurvedb.storage.timescale.schema import CONTINUOUS_AGGREGATES

# Maps the binning strategy literal used in the protocol to the continuous
# aggregate view name created in schema.py. Also removes the chance of
# SQL injection.
_BINNING_STRATEGY_TO_VIEW: dict[str, str] = {
    "1 day": "flux_daily",
    "7 days": "flux_weekly",
    "30 days": "flux_monthly",
}


class TimescaleLightcurveProvider(PostgresLightcurveProvider):
    """
    Provides lightcurves from a TimescaleDB data store.

    Unbinned queries are inherited from PostgresLightcurveProvider unchanged.
    Binned queries read from pre-computed continuous aggregates instead of
    computing aggregates on-the-fly.
    """

    def __init__(self, flux_storage: TimescaleFluxMeasurementStorage):
        super().__init__(flux_storage=flux_storage)

    async def setup(self) -> None:
        """
        Create the continuous aggregate materialized views and their
        refresh policies.
        """
        async with self.flux_storage.conn.cursor() as cur:
            for statement in CONTINUOUS_AGGREGATES:
                await cur.execute(statement)

    async def get_binned_instrument_lightcurve(
        self,
        source_id: UUID,
        module: str,
        frequency: int,
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        limit: int = 1000000,
    ) -> BinnedInstrumentLightcurve:
        """
        Get a binned lightcurve for a specific source, module, and frequency
        by reading from the appropriate continuous aggregate view.
        """

        view = _BINNING_STRATEGY_TO_VIEW[binning_strategy]

        query = """
            SELECT
                COALESCE(array_agg(bin_time), array[]::timestamptz[]) AS time,
                COALESCE(array_agg(bin_ra), array[]::real[]) AS ra,
                COALESCE(array_agg(bin_dec), array[]::real[]) AS dec,
                COALESCE(array_agg(bin_flux), array[]::real[]) AS flux,
                COALESCE(array_agg(bin_flux_err), array[]::real[]) AS flux_err,
                %(binning_strategy)s::text AS binning_strategy,
                %(source_id)s AS source_id,
                %(frequency)s AS frequency,
                %(module)s AS module,
                %(start_time)s AS start_time,
                %(end_time)s AS end_time
            FROM (
                SELECT
                    bucket + (%(binning_strategy)s::interval / 2) AS bin_time,
                    avg_ra AS bin_ra,
                    avg_dec AS bin_dec,
                    avg_flux AS bin_flux,
                    avg_flux_err AS bin_flux_err
                FROM {view}
                WHERE source_id = %(source_id)s
                  AND module = %(module)s
                  AND frequency = %(frequency)s
                  AND bucket >= %(start_time)s
                  AND bucket < %(end_time)s
                ORDER BY bucket
                LIMIT %(limit)s
            ) AS binned
        """.format(view=view)

        async with self.flux_storage.conn.cursor(
            row_factory=class_row(BinnedInstrumentLightcurve)
        ) as cur:
            await cur.execute(
                query,
                {
                    "source_id": source_id,
                    "module": module,
                    "frequency": frequency,
                    "binning_strategy": binning_strategy,
                    "start_time": start_time,
                    "end_time": end_time,
                    "limit": limit,
                },
            )
            return await cur.fetchone()

    async def get_binned_frequency_lightcurve(
        self,
        source_id: UUID,
        frequency: int,
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        limit: int = 1000000,
    ) -> BinnedFrequencyLightcurve:
        """
        Get a binned lightcurve for a specific source and frequency (all
        modules) by reading from the appropriate continuous aggregate view.
        """

        view = _BINNING_STRATEGY_TO_VIEW[binning_strategy]

        query = """
            SELECT
                COALESCE(array_agg(bin_time), array[]::timestamptz[]) AS time,
                COALESCE(array_agg(bin_module), array[]::text[]) AS module,
                COALESCE(array_agg(bin_ra), array[]::real[]) AS ra,
                COALESCE(array_agg(bin_dec), array[]::real[]) AS dec,
                COALESCE(array_agg(bin_flux), array[]::real[]) AS flux,
                COALESCE(array_agg(bin_flux_err), array[]::real[]) AS flux_err,
                %(binning_strategy)s::text AS binning_strategy,
                %(source_id)s AS source_id,
                %(frequency)s AS frequency,
                %(start_time)s AS start_time,
                %(end_time)s AS end_time
            FROM (
                SELECT
                    bucket + (%(binning_strategy)s::interval / 2) AS bin_time,
                    module AS bin_module,
                    avg_ra AS bin_ra,
                    avg_dec AS bin_dec,
                    avg_flux AS bin_flux,
                    avg_flux_err AS bin_flux_err
                FROM {view}
                WHERE source_id = %(source_id)s
                  AND frequency = %(frequency)s
                  AND bucket >= %(start_time)s
                  AND bucket < %(end_time)s
                ORDER BY bucket
                LIMIT %(limit)s
            ) AS binned
        """.format(view=view)

        async with self.flux_storage.conn.cursor(
            row_factory=class_row(BinnedFrequencyLightcurve)
        ) as cur:
            await cur.execute(
                query,
                {
                    "source_id": source_id,
                    "frequency": frequency,
                    "binning_strategy": binning_strategy,
                    "start_time": start_time,
                    "end_time": end_time,
                    "limit": limit,
                },
            )
            return await cur.fetchone()
