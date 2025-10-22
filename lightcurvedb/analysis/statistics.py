"""
Database-side analysis functions for lightcurve data.
"""


from datetime import datetime
from typing import Any, Dict, Iterable

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from lightcurvedb.analysis.aggregates import METRICS_REGISTRY
from lightcurvedb.models.analysis import BandStatistics, BandTimeSeries
from lightcurvedb.models.flux import FluxMeasurementTable


class DerivedStatisticsRegistry:
    """
    Registry describing derived statistics based on continuous aggregates.
    """

    def __init__(self) -> None:
        self.statistics: Dict[str, Dict[str, Any]] = {
            "weighted_mean_flux": {
                "method": self.weighted_mean_flux,
                "column": "weighted_mean_flux",
                "description": "Weighted mean of flux measurements",
                "mode": "aggregate",
            },
            "weighted_error_on_mean_flux": {
                "method": self.weighted_error_on_mean_flux,
                "column": "weighted_error_on_mean_flux",
                "description": "Uncertainty on the weighted mean flux",
                "mode": "aggregate",
            },
            "min_flux": {
                "method": self.min_flux,
                "column": "min_flux",
                "description": "Minimum flux value in the time period",
                "mode": "aggregate",
            },
            "max_flux": {
                "method": self.max_flux,
                "column": "max_flux",
                "description": "Maximum flux value in the time period",
                "mode": "aggregate",
            },
            "data_points": {
                "method": self.data_points,
                "column": "data_points",
                "description": "Number of data points contributing to the aggregate",
                "mode": "aggregate",
            },
            "mean_flux": {
                "method": self.mean_flux,
                "column": "mean_flux",
                "description": "Mean flux per bucket",
                "mode": "timeseries",
            },
            "variance_flux": {
                "method": self.variance_flux,
                "column": "variance_flux",
                "description": "Variance of flux across all measurements",
                "mode": "aggregate",
            },
        }

    def get_statistics_table(self, start_time: datetime | None = None, end_time: datetime | None = None):
        """
        Selects the appropriate table based on time range and configured thresholds.
        """
        from lightcurvedb.analysis.aggregates import AggregateConfigurations, select_aggregate_config

        if not start_time or not end_time:
            # Default to monthly when no time range specified
            config = next(c for c in AggregateConfigurations if c.time_resolution == "monthly")
        else:
            today = datetime.today()
            delta_days = (today - start_time).days
            config = select_aggregate_config(delta_days)

        table = METRICS_REGISTRY.get_continuous_aggregate_table(config.view_name)
        return table, config.time_resolution, config.display_date_correction

    def get_filter_columns(self, table: Any) -> tuple[Any, Any, Any]:
        """
        Get filter column references for queries.
        """
        columns = table.c
        return (columns.source_id, columns.band_name, columns.bucket)

    def get_statistic_expressions(self, table: Any, display_date_correction: str, mode: str = "aggregate") -> Dict[str, Any]:
        """
        Build SQLAlchemy expressions for statistics.
        """
        columns = table.c
        expressions = {
            name: meta["method"](columns).label(meta["column"])
            for name, meta in self.statistics.items()
            if meta["mode"] == mode
        }
        # Bucket range
        if mode == "aggregate":
            expressions["bucket_start"] = func.min(columns.bucket).label("bucket_start")
            expressions["bucket_end"] = self._calculate_bucket_end(columns, display_date_correction)
        elif mode == "timeseries":
            expressions["bucket"] = columns.bucket
        return expressions

    @staticmethod
    def _calculate_bucket_end(columns, display_date_correction: str):
        """
        Calculate the end of the bucket range based on time resolution.
        """
        max_bucket = func.max(columns.bucket)

        if display_date_correction:
            return (max_bucket + text(display_date_correction)).label("bucket_end")
        else:
            return max_bucket.label("bucket_end")

    @staticmethod
    def weighted_mean_flux(columns):
        numerator = func.sum(columns.sum_flux_over_uncertainty_squared)
        denominator = func.sum(columns.sum_inverse_uncertainty_squared)
        return numerator / denominator

    @staticmethod
    def weighted_error_on_mean_flux(columns):
        denominator = func.sum(columns.sum_inverse_uncertainty_squared)
        return 1 / func.sqrt(denominator)

    @staticmethod
    def min_flux(columns):
        return func.min(columns.min_flux)

    @staticmethod
    def max_flux(columns):
        return func.max(columns.max_flux)

    @staticmethod
    def data_points(columns):
        return func.sum(columns.data_points)

    @staticmethod
    def mean_flux(columns):
        return columns.sum_flux / columns.data_points

    @staticmethod
    def variance_flux(columns):
        total_sum = func.sum(columns.sum_flux)
        total_count = func.sum(columns.data_points)
        total_sum_squared = func.sum(columns.sum_flux_squared)

        mean = total_sum / total_count
        sum_of_squared_deviations = total_sum_squared - total_count * mean * mean

        return sum_of_squared_deviations / (total_count - 1)


class RawMeasurementStatisticsRegistry:
    """
    Registry describing statistics computed from raw flux measurements.
    """

    def __init__(self) -> None:
        self.statistics: Dict[str, Dict[str, Any]] = {
            "weighted_mean_flux": {
                "method": self.weighted_mean_flux,
                "column": "weighted_mean_flux",
                "description": "Weighted mean of flux measurements computed from raw data",
                "mode": "aggregate",
            },
            "weighted_error_on_mean_flux": {
                "method": self.weighted_error_on_mean_flux,
                "column": "weighted_error_on_mean_flux",
                "description": "Uncertainty on the weighted mean flux computed from raw data",
                "mode": "aggregate",
            },
            "min_flux": {
                "method": self.min_flux,
                "column": "min_flux",
                "description": "Minimum flux value in the raw measurements",
                "mode": "aggregate",
            },
            "max_flux": {
                "method": self.max_flux,
                "column": "max_flux",
                "description": "Maximum flux value in the raw measurements",
                "mode": "aggregate",
            },
            "data_points": {
                "method": self.data_points,
                "column": "data_points",
                "description": "Number of raw measurements contributing to the statistics",
                "mode": "aggregate",
            },
            "variance_flux": {
                "method": self.variance_flux,
                "column": "variance_flux",
                "description": "Variance of flux across all raw measurements",
                "mode": "aggregate",
            },
            "mean_flux": {
                "method": self.mean_flux,
                "column": "mean_flux",
                "description": "Mean flux per time bucket",
                "mode": "timeseries",
            },
        }

    def get_statistics_table(self, start_time: datetime | None = None, end_time: datetime | None = None):
        """
        Get raw measurements table.
        """
        return FluxMeasurementTable, "daily", ""

    def get_filter_columns(self, table: Any) -> tuple[Any, Any, Any]:
        """
        Get filter column references for queries.
        """
        return (table.source_id, table.band_name, table.time)

    def get_statistic_expressions(self, table: Any, display_date_correction: str, mode: str = "aggregate") -> Dict[str, Any]:
        """
        Build SQLAlchemy expressions for statistics.
        """
        expressions = {
            name: meta["method"](table).label(meta["column"])
            for name, meta in self.statistics.items()
            if meta["mode"] == mode
        }

        if mode == "aggregate":
            expressions["bucket_start"] = func.min(table.time).label("bucket_start")
            expressions["bucket_end"] = func.max(table.time).label("bucket_end")
        elif mode == "timeseries":
            expressions["bucket"] = table.time.label("bucket")

        return expressions

    @staticmethod
    def weighted_mean_flux(table_ref):
        numerator = METRICS_REGISTRY.sum_flux_over_uncertainty_squared(table_ref)
        denominator = METRICS_REGISTRY.sum_inverse_uncertainty_squared(table_ref)
        return numerator / denominator

    @staticmethod
    def weighted_error_on_mean_flux(table_ref):
        denominator = METRICS_REGISTRY.sum_inverse_uncertainty_squared(table_ref)
        return 1 / func.sqrt(denominator)

    @staticmethod
    def min_flux(table_ref):
        return METRICS_REGISTRY.min_flux(table_ref)

    @staticmethod
    def max_flux(table_ref):
        return METRICS_REGISTRY.max_flux(table_ref)

    @staticmethod
    def data_points(table_ref):
        return METRICS_REGISTRY.data_points_count(table_ref)

    @staticmethod
    def variance_flux(table_ref):
        total_sum = METRICS_REGISTRY.sum_flux(table_ref)
        total_count = METRICS_REGISTRY.data_points_count(table_ref)
        total_sum_squared = METRICS_REGISTRY.sum_flux_squared(table_ref)

        mean = total_sum / total_count
        sum_of_squared_deviations = total_sum_squared - total_count * mean * mean

        return sum_of_squared_deviations / (total_count - 1)

    @staticmethod
    def mean_flux(table_ref):
        return table_ref.i_flux


class BandStatisticsCalculator:
    """
    Calculator for band statistics.
    """

    def __init__(self, registry):
        """
        Initialize calculator with a statistics registry.
        """
        self.registry = registry

    async def get_statistics(
        self,
        source_id: int,
        band_name: str,
        conn: AsyncSession,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> tuple[BandStatistics, datetime | None, datetime | None, str]:
        """
        Calculate band statistics for given source and time range.
        """
        table, time_resolution, display_date_correction = self.registry.get_statistics_table(start_time, end_time)
        expressions = self.registry.get_statistic_expressions(table, display_date_correction, mode="aggregate")
        filters = self.registry.get_filter_columns(table)

        statistics, bucket_start, bucket_end = await _run_band_statistics_query(
            table=table,
            expressions=expressions,
            filter_columns=filters,
            source_id=source_id,
            band_name=band_name,
            conn=conn,
            start_time=start_time,
            end_time=end_time,
        )

        return statistics, bucket_start, bucket_end, time_resolution

    async def get_timeseries(
        self,
        source_id: int,
        band_name: str,
        conn: AsyncSession,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> tuple[BandTimeSeries, str]:
        """
        Get timeseries of mean flux values per bucket.
        """
        table, time_resolution, display_date_correction = self.registry.get_statistics_table(start_time, end_time)
        expressions = self.registry.get_statistic_expressions(table, display_date_correction, mode="timeseries")
        filters = self.registry.get_filter_columns(table)

        timeseries = await _run_band_timeseries_query(
            table=table,
            expressions=expressions,
            filter_columns=filters,
            source_id=source_id,
            band_name=band_name,
            conn=conn,
            start_time=start_time,
            end_time=end_time,
        )

        return timeseries, time_resolution


def _build_time_filters(column, start_time: datetime | None, end_time: datetime | None):
    """
    Build time filter conditions for queries.
    """
    filters = []
    if start_time:
        filters.append(column >= start_time)
    if end_time:
        filters.append(column <= end_time)
    return filters



async def _run_band_statistics_query(
    table,
    expressions: Dict[str, Any],
    filter_columns: Iterable[Any],
    source_id: int,
    band_name: str,
    conn: AsyncSession,
    start_time: datetime | None,
    end_time: datetime | None,
) -> tuple[BandStatistics, datetime | None, datetime | None]:
    """
    Execute the band statistics query and build a BandStatistics result.
    """

    source_column, band_column, time_column = filter_columns

    select_query = select(*expressions.values()).select_from(table).where(
        source_column == source_id,
        band_column == band_name,
    )

    time_filters = _build_time_filters(time_column, start_time, end_time)
    for filter_condition in time_filters:
        select_query = select_query.where(filter_condition)

    result = await conn.execute(select_query)
    row = result.first()

    statistics = BandStatistics(
        weighted_mean_flux=row.weighted_mean_flux,
        weighted_error_on_mean_flux=row.weighted_error_on_mean_flux,
        min_flux=row.min_flux,
        max_flux=row.max_flux,
        data_points=row.data_points,
        variance_flux=row.variance_flux,
    )

    bucket_start = getattr(row, 'bucket_start', None)
    bucket_end = getattr(row, 'bucket_end', None)

    return statistics, bucket_start, bucket_end


async def _run_band_timeseries_query(
    table,
    expressions: Dict[str, Any],
    filter_columns: Iterable[Any],
    source_id: int,
    band_name: str,
    conn: AsyncSession,
    start_time: datetime | None,
    end_time: datetime | None,
) -> BandTimeSeries:
    """
    Execute the band timeseries query and build a BandTimeSeries result.
    """

    source_column, band_column, time_column = filter_columns

    select_query = select(*expressions.values()).select_from(table).where(
        source_column == source_id,
        band_column == band_name,
    )

    time_filters = _build_time_filters(time_column, start_time, end_time)
    for filter_condition in time_filters:
        select_query = select_query.where(filter_condition)

    select_query = select_query.order_by(time_column)

    result = await conn.execute(select_query)
    rows = result.all()

    timestamps = [row.bucket for row in rows]
    mean_flux_values = [row.mean_flux for row in rows]

    return BandTimeSeries(timestamps=timestamps, mean_flux=mean_flux_values)


async def get_band_statistics(
    source_id: int,
    band_name: str,
    conn: AsyncSession,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> tuple[BandStatistics, datetime | None, datetime | None, str]:
    """
    Calculate band statistics for given source and time range.
    """
    calculator = BandStatisticsCalculator(RawMeasurementStatisticsRegistry())
    return await calculator.get_statistics(source_id, band_name, conn, start_time, end_time)


async def get_band_timeseries(
    source_id: int,
    band_name: str,
    conn: AsyncSession,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> tuple[BandTimeSeries, str]:
    """
    Get timeseries of mean flux values per bucket for given source and time range.
    """
    calculator = BandStatisticsCalculator(RawMeasurementStatisticsRegistry())
    return await calculator.get_timeseries(source_id, band_name, conn, start_time, end_time)
