"""
Provider for lightcurves from postgres data stores.
"""

import asyncio
import datetime
from typing import Literal
from uuid import UUID

from psycopg.rows import class_row

from lightcurvedb.models.lightcurves import (
    BinnedFrequencyLightcurve,
    BinnedInstrumentLightcurve,
    FrequencyLightcurve,
    InstrumentLightcurve,
    SourceLightcurveBinnedFrequency,
    SourceLightcurveBinnedInstrument,
    SourceLightcurveFrequency,
    SourceLightcurveInstrument,
)
from lightcurvedb.storage.postgres.flux import PostgresFluxMeasurementStorage
from lightcurvedb.storage.prototype.lightcurves import ProvidesLightcurves


class PostgresLightcurveProvider(ProvidesLightcurves):
    """
    Provides lightcurves from a PostgreSQL data store.
    """

    def __init__(self, flux_storage: PostgresFluxMeasurementStorage):
        self.flux_storage = flux_storage

    async def get_instrument_lightcurve(
        self, source_id: UUID, module: str, frequency: int, limit: int = 1000000
    ) -> InstrumentLightcurve:
        """
        Get a lightcurve for a specific source, module, and frequency.
        """

        query = """
            SELECT
                %(source_id)s AS source_id,
                %(module)s AS module,
                %(frequency)s AS frequency,
                COALESCE(array_agg(measurement_id), array[]::uuid[]) AS measurement_id,
                COALESCE(array_agg(time), array[]::timestamptz[]) AS time,
                COALESCE(array_agg(ra), array[]::real[]) AS ra,
                COALESCE(array_agg(dec), array[]::real[]) AS dec,
                COALESCE(array_agg(flux), array[]::real[]) AS flux,
                COALESCE(array_agg(flux_err), array[]::real[]) AS flux_err,
                COALESCE(array_agg(extra), array[]::jsonb[]) AS extra
            FROM (
                SELECT * FROM flux_measurements
                WHERE source_id = %(source_id)s
                AND module = %(module)s
                AND frequency = %(frequency)s
                ORDER BY time
                LIMIT %(limit)s
            )
        """

        async with self.flux_storage.conn.cursor(
            row_factory=class_row(InstrumentLightcurve)
        ) as cur:
            await cur.execute(
                query,
                {
                    "source_id": source_id,
                    "module": module,
                    "frequency": frequency,
                    "limit": limit,
                },
            )
            return await cur.fetchone()

    async def get_frequency_lightcurve(
        self, source_id: UUID, frequency: int, limit: int = 1000000
    ) -> FrequencyLightcurve:
        """
        Get a lightcurve for a specific source andd frequency, for all modules.
        """

        query = """
            SELECT
                %(source_id)s AS source_id,
                %(frequency)s AS frequency,
                COALESCE(array_agg(measurement_id), array[]::uuid[]) AS measurement_id,
                COALESCE(array_agg(time), array[]::timestamptz[]) AS time,
                COALESCE(array_agg(module), array[]::text[]) AS module,
                COALESCE(array_agg(ra), array[]::real[]) AS ra,
                COALESCE(array_agg(dec), array[]::real[]) AS dec,
                COALESCE(array_agg(flux), array[]::real[]) AS flux,
                COALESCE(array_agg(flux_err), array[]::real[]) AS flux_err,
                COALESCE(array_agg(extra), array[]::jsonb[]) AS extra
            FROM (
                SELECT * FROM FLUX_MEASUREMENTS
                WHERE source_id = %(source_id)s
                AND frequency = %(frequency)s
                ORDER BY time
                LIMIT %(limit)s
            )
        """

        async with self.flux_storage.conn.cursor(
            row_factory=class_row(FrequencyLightcurve)
        ) as cur:
            await cur.execute(
                query, {"source_id": source_id, "frequency": frequency, "limit": limit}
            )
            return await cur.fetchone()

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
        Get a binned lightcurve for a specific source, module, and frequency.
        """
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
            %(start_time)s as start_time,
            %(end_time)s as end_time
            FROM (
            SELECT
                date_bin(%(binning_strategy)s::interval, time, %(start_time)s) + (%(binning_strategy)s::interval / 2) AS bin_time,
                avg(ra)::real AS bin_ra,
                avg(dec)::real AS bin_dec,
                avg(flux)::real AS bin_flux,
                CASE
                WHEN count(flux_err) FILTER (WHERE flux_err IS NOT NULL) > 0
                THEN (sqrt(sum(flux_err ^ 2) FILTER (WHERE flux_err IS NOT NULL)) / count(flux_err) FILTER (WHERE flux_err IS NOT NULL))::real
                ELSE NULL
                END AS bin_flux_err
            FROM FLUX_MEASUREMENTS
            WHERE source_id = %(source_id)s
            AND module = %(module)s
            AND frequency = %(frequency)s
            AND time >= %(start_time)s
            AND time < %(end_time)s
            GROUP BY date_bin(%(binning_strategy)s::interval, time, %(start_time)s)
            ORDER BY bin_time
            LIMIT %(limit)s
            ) AS binned
        """

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
        Get a binned lightcurve for a specific source and frequency, for all modules.
        """

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
            %(start_time)s as start_time,
            %(end_time)s as end_time
            FROM (
            SELECT
                date_bin(%(binning_strategy)s::interval, time, %(start_time)s) + (%(binning_strategy)s::interval / 2) AS bin_time,
                module AS bin_module,
                avg(ra)::real AS bin_ra,
                avg(dec)::real AS bin_dec,
                avg(flux)::real AS bin_flux,
                CASE
                WHEN count(flux_err) FILTER (WHERE flux_err IS NOT NULL) > 0
                THEN (sqrt(sum(flux_err ^ 2) FILTER (WHERE flux_err IS NOT NULL)) / count(flux_err) FILTER (WHERE flux_err IS NOT NULL))::real
                ELSE NULL
                END AS bin_flux_err
            FROM FLUX_MEASUREMENTS
            WHERE source_id = %(source_id)s
            AND frequency = %(frequency)s
            AND time >= %(start_time)s
            AND time < %(end_time)s
            GROUP BY date_bin(%(binning_strategy)s::interval, time, %(start_time)s), module
            ORDER BY bin_time
            LIMIT %(limit)s
            ) AS binned
        """

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

    async def get_frequencies_for_source(self, source_id: UUID) -> list[int]:
        """
        Get all frequencies for a given source.
        """
        query = """
            SELECT DISTINCT frequency
            FROM flux_measurements
            WHERE source_id = %(source_id)s
        """

        async with self.flux_storage.conn.cursor() as cur:
            await cur.execute(query, {"source_id": source_id})
            rows = await cur.fetchall()
            return [row[0] for row in rows]

    async def get_module_frequency_pairs_for_source(
        self, source_id: UUID
    ) -> list[tuple[str, int]]:
        """
        Get all modules for a given source.
        """
        query = """
            SELECT DISTINCT frequency, module
            FROM flux_measurements
            WHERE source_id = %(source_id)s
        """

        async with self.flux_storage.conn.cursor() as cur:
            await cur.execute(query, {"source_id": source_id})
            rows = await cur.fetchall()
            return [(row[1], row[0]) for row in rows]

    async def get_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency", "instrument"],
        limit: int = 1000000,
    ) -> SourceLightcurveFrequency | SourceLightcurveInstrument:
        """
        Get a lightcurve for a specific source, with the given strategy and binning.
        """

        if selection_strategy == "frequency":
            frequencies = await self.get_frequencies_for_source(source_id)
            lightcurves = await asyncio.gather(
                *[
                    self.get_frequency_lightcurve(source_id, frequency, limit=limit)
                    for frequency in frequencies
                ]
            )
            return SourceLightcurveFrequency(
                source_id=source_id,
                selection_strategy="frequency",
                binning_strategy="none",
                lightcurves={x.frequency: x for x in lightcurves},
            )
        elif selection_strategy == "instrument":
            module_frequency_pairs = await self.get_module_frequency_pairs_for_source(
                source_id
            )
            lightcurves = await asyncio.gather(
                *[
                    self.get_instrument_lightcurve(
                        source_id, module, frequency, limit=limit
                    )
                    for module, frequency in module_frequency_pairs
                ]
            )
            return SourceLightcurveInstrument(
                source_id=source_id,
                selection_strategy="instrument",
                binning_strategy="none",
                lightcurves={x.module: x for x in lightcurves},
            )
        else:
            raise ValueError(f"Invalid strategy: {selection_strategy}")

    async def get_binned_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency", "instrument"],
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        limit: int = 1000000,
    ) -> SourceLightcurveBinnedFrequency | SourceLightcurveBinnedInstrument:
        """
        Get a binned lightcurve for a specific source, with the given strategy and binning.
        """

        if selection_strategy == "frequency":
            frequencies = await self.get_frequencies_for_source(source_id)
            lightcurves = await asyncio.gather(
                *[
                    self.get_binned_frequency_lightcurve(
                        source_id,
                        frequency,
                        binning_strategy,
                        start_time,
                        end_time,
                        limit=limit,
                    )
                    for frequency in frequencies
                ]
            )
            return SourceLightcurveBinnedFrequency(
                source_id=source_id,
                selection_strategy="frequency",
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
                lightcurves={x.frequency: x for x in lightcurves},
            )
        elif selection_strategy == "instrument":
            module_frequency_pairs = await self.get_module_frequency_pairs_for_source(
                source_id
            )
            lightcurves = await asyncio.gather(
                *[
                    self.get_binned_instrument_lightcurve(
                        source_id,
                        module,
                        frequency,
                        binning_strategy,
                        start_time,
                        end_time,
                        limit=limit,
                    )
                    for module, frequency in module_frequency_pairs
                ]
            )
            return SourceLightcurveBinnedInstrument(
                source_id=source_id,
                selection_strategy="instrument",
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
                lightcurves={x.module: x for x in lightcurves},
            )
        else:
            raise ValueError(f"Invalid strategy: {selection_strategy}")
