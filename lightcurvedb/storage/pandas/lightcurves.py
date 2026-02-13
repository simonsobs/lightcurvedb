"""
Provider for lightcurves from pandas data stores.
"""

from __future__ import annotations

import asyncio
import datetime
from typing import Literal, overload
from uuid import UUID

import pandas as pd

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
from lightcurvedb.storage.pandas.flux import PandasFluxMeasurementStorage
from lightcurvedb.storage.prototype.lightcurves import ProvidesLightcurves


class PandasLightcurves(ProvidesLightcurves):
    """
    Provides lightcurves from a PostgreSQL data store.
    """

    def __init__(self, flux_storage: PandasFluxMeasurementStorage):
        self.flux_storage = flux_storage

    async def setup(self) -> None:
        """
        Set up the instrument storage system (e.g. create the tables).
        """
        return None

    async def _load_source_table(self, source_id: UUID) -> pd.DataFrame:
        table = await self.flux_storage._read_file(source_id)
        if table is None:
            return pd.DataFrame(
                columns=[
                    "measurement_id",
                    "time",
                    "module",
                    "frequency",
                    "ra",
                    "dec",
                    "flux",
                    "flux_err",
                    "extra",
                ]
            ).set_index("measurement_id")
        return table

    def _normalize_times(self, values: list) -> list[datetime.datetime]:
        times: list[datetime.datetime] = []
        for value in values:
            if isinstance(value, pd.Timestamp):
                dt = value.to_pydatetime()
            else:
                dt = value
            if isinstance(dt, datetime.datetime) and dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            times.append(dt)
        return times

    def _bin_freq(self, binning_strategy: Literal["1 day", "7 days", "30 days"]) -> str:
        return {
            "1 day": "1D",
            "7 days": "7D",
            "30 days": "30D",
        }[binning_strategy]

    def _empty_instrument_lightcurve(
        self, source_id: UUID, module: str, frequency: int
    ) -> InstrumentLightcurve:
        return InstrumentLightcurve(
            source_id=source_id,
            module=module,
            frequency=frequency,
            measurement_id=[],
            time=[],
            ra=[],
            dec=[],
            flux=[],
            flux_err=[],
            extra=[],
        )

    def _empty_frequency_lightcurve(
        self, source_id: UUID, frequency: int
    ) -> FrequencyLightcurve:
        return FrequencyLightcurve(
            source_id=source_id,
            frequency=frequency,
            measurement_id=[],
            time=[],
            module=[],
            ra=[],
            dec=[],
            flux=[],
            flux_err=[],
            extra=[],
        )

    async def get_instrument_lightcurve(
        self, source_id: UUID, module: str, frequency: int, limit: int = 1000000
    ) -> InstrumentLightcurve:
        """
        Get a lightcurve for a specific source, module, and frequency.
        """
        table = await self._load_source_table(source_id)
        if table.empty:
            return self._empty_instrument_lightcurve(source_id, module, frequency)

        filtered = table[
            (table["module"] == module) & (table["frequency"] == frequency)
        ].sort_values("time")

        if limit is not None:
            filtered = filtered.head(limit)

        if filtered.empty:
            return self._empty_instrument_lightcurve(source_id, module, frequency)

        return InstrumentLightcurve(
            source_id=source_id,
            module=module,
            frequency=frequency,
            measurement_id=[UUID(m) for m in filtered.index.astype(str)],
            time=self._normalize_times(filtered["time"].tolist()),
            ra=filtered["ra"].tolist(),
            dec=filtered["dec"].tolist(),
            flux=filtered["flux"].tolist(),
            flux_err=filtered["flux_err"].tolist(),
            extra=filtered["extra"].tolist(),
        )

    async def get_frequency_lightcurve(
        self, source_id: UUID, frequency: int, limit: int = 1000000
    ) -> FrequencyLightcurve:
        """
        Get a lightcurve for a specific source andd frequency, for all modules.
        """
        table = await self._load_source_table(source_id)
        if table.empty:
            return self._empty_frequency_lightcurve(source_id, frequency)

        filtered = table[table["frequency"] == frequency].sort_values("time")
        if limit is not None:
            filtered = filtered.head(limit)

        if filtered.empty:
            return self._empty_frequency_lightcurve(source_id, frequency)

        return FrequencyLightcurve(
            source_id=source_id,
            frequency=frequency,
            measurement_id=[UUID(m) for m in filtered.index.astype(str)],
            time=self._normalize_times(filtered["time"].tolist()),
            module=filtered["module"].tolist(),
            ra=filtered["ra"].tolist(),
            dec=filtered["dec"].tolist(),
            flux=filtered["flux"].tolist(),
            flux_err=filtered["flux_err"].tolist(),
            extra=filtered["extra"].tolist(),
        )

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
        table = await self._load_source_table(source_id)
        if table.empty:
            return BinnedInstrumentLightcurve(
                source_id=source_id,
                module=module,
                frequency=frequency,
                time=[],
                ra=[],
                dec=[],
                flux=[],
                flux_err=[],
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
            )

        subset = table[
            (table["module"] == module)
            & (table["frequency"] == frequency)
            & (table["time"] >= start_time)
            & (table["time"] < end_time)
        ]

        if subset.empty:
            return BinnedInstrumentLightcurve(
                source_id=source_id,
                module=module,
                frequency=frequency,
                time=[],
                ra=[],
                dec=[],
                flux=[],
                flux_err=[],
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
            )

        freq = self._bin_freq(binning_strategy)
        interval = pd.Timedelta(freq)

        grouped = subset.groupby(
            pd.Grouper(
                key="time", freq=freq, origin=start_time, closed="left", label="left"
            )
        )

        rows = []
        for bin_start, group in grouped:
            if group.empty:
                continue
            flux_err = group["flux_err"].dropna()
            if flux_err.empty:
                bin_flux_err = None
            else:
                bin_flux_err = float((flux_err.pow(2).sum() ** 0.5) / len(flux_err))

            rows.append(
                {
                    "time": bin_start + interval / 2,
                    "ra": float(group["ra"].mean()),
                    "dec": float(group["dec"].mean()),
                    "flux": float(group["flux"].mean()),
                    "flux_err": bin_flux_err,
                }
            )

        if not rows:
            return BinnedInstrumentLightcurve(
                source_id=source_id,
                module=module,
                frequency=frequency,
                time=[],
                ra=[],
                dec=[],
                flux=[],
                flux_err=[],
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
            )

        if limit is not None:
            rows = rows[:limit]

        return BinnedInstrumentLightcurve(
            source_id=source_id,
            module=module,
            frequency=frequency,
            time=self._normalize_times([row["time"] for row in rows]),
            ra=[row["ra"] for row in rows],
            dec=[row["dec"] for row in rows],
            flux=[row["flux"] for row in rows],
            flux_err=[row["flux_err"] for row in rows],
            binning_strategy=binning_strategy,
            start_time=start_time,
            end_time=end_time,
        )

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
        table = await self._load_source_table(source_id)
        if table.empty:
            return BinnedFrequencyLightcurve(
                source_id=source_id,
                frequency=frequency,
                time=[],
                ra=[],
                dec=[],
                flux=[],
                flux_err=[],
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
            )

        subset = table[
            (table["frequency"] == frequency)
            & (table["time"] >= start_time)
            & (table["time"] < end_time)
        ]

        if subset.empty:
            return BinnedFrequencyLightcurve(
                source_id=source_id,
                frequency=frequency,
                time=[],
                ra=[],
                dec=[],
                flux=[],
                flux_err=[],
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
            )

        freq = self._bin_freq(binning_strategy)
        interval = pd.Timedelta(freq)

        grouped = subset.groupby(
            pd.Grouper(
                key="time", freq=freq, origin=start_time, closed="left", label="left"
            )
        )

        rows = []
        for bin_start, group in grouped:
            if group.empty:
                continue
            flux_err = group["flux_err"].dropna()
            if flux_err.empty:
                bin_flux_err = None
            else:
                bin_flux_err = float((flux_err.pow(2).sum() ** 0.5) / len(flux_err))

            rows.append(
                {
                    "time": bin_start + interval / 2,
                    "ra": float(group["ra"].mean()),
                    "dec": float(group["dec"].mean()),
                    "flux": float(group["flux"].mean()),
                    "flux_err": bin_flux_err,
                }
            )

        if not rows:
            return BinnedFrequencyLightcurve(
                source_id=source_id,
                frequency=frequency,
                time=[],
                ra=[],
                dec=[],
                flux=[],
                flux_err=[],
                binning_strategy=binning_strategy,
                start_time=start_time,
                end_time=end_time,
            )

        if limit is not None:
            rows = rows[:limit]

        return BinnedFrequencyLightcurve(
            source_id=source_id,
            frequency=frequency,
            time=self._normalize_times([row["time"] for row in rows]),
            ra=[row["ra"] for row in rows],
            dec=[row["dec"] for row in rows],
            flux=[row["flux"] for row in rows],
            flux_err=[row["flux_err"] for row in rows],
            binning_strategy=binning_strategy,
            start_time=start_time,
            end_time=end_time,
        )

    async def get_frequencies_for_source(self, source_id: UUID) -> list[int]:
        """
        Get all frequencies for a given source.
        """
        table = await self._load_source_table(source_id)
        if table.empty:
            return []

        return sorted(table["frequency"].dropna().unique().tolist())

    async def get_module_frequency_pairs_for_source(
        self, source_id: UUID
    ) -> list[tuple[str, int]]:
        """
        Get all modules for a given source.
        """
        table = await self._load_source_table(source_id)
        if table.empty:
            return []

        pairs = (
            table[["module", "frequency"]]
            .dropna()
            .drop_duplicates()
            .itertuples(index=False, name=None)
        )
        return [(module, int(frequency)) for module, frequency in pairs]

    @overload
    async def get_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency"],
        limit: int = 1000000,
    ) -> SourceLightcurveFrequency: ...

    @overload
    async def get_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["instrument"],
        limit: int = 1000000,
    ) -> SourceLightcurveInstrument: ...

    async def get_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency", "instrument"],
        limit: int = 1000000,
    ) -> SourceLightcurveInstrument | SourceLightcurveFrequency:
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
        if selection_strategy == "instrument":
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

        raise ValueError(f"Invalid strategy: {selection_strategy}")

    @overload
    async def get_binned_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["frequency"],
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        limit: int = 1000000,
    ) -> SourceLightcurveBinnedFrequency: ...

    @overload
    async def get_binned_source_lightcurve(
        self,
        source_id: UUID,
        selection_strategy: Literal["instrument"],
        binning_strategy: Literal["1 day", "7 days", "30 days"],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        limit: int = 1000000,
    ) -> SourceLightcurveBinnedInstrument: ...

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
        if selection_strategy == "instrument":
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

        raise ValueError(f"Invalid strategy: {selection_strategy}")
