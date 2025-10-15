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
        Selects the appropriate table that actually holds data for the requested range.
        """
        if not start_time or not end_time:
            view_name = "band_statistics_monthly"
            time_resolution = "monthly"
        else:
            today = datetime.today()
            delta_start = (today - start_time).days

            if delta_start <= 30:
                view_name = "band_statistics_daily"
                time_resolution = "daily"
            elif delta_start <= 180:
                view_name = "band_statistics_weekly"
                time_resolution = "weekly"
            else:
                view_name = "band_statistics_monthly"
                time_resolution = "monthly"

        table = METRICS_REGISTRY.get_continuous_aggregate_table(view_name)
        return table, time_resolution

    def get_statistic_expressions(self, columns: Any, time_resolution: str, mode: str = "aggregate") -> Dict[str, Any]:
        expressions = {
            name: meta["method"](columns).label(meta["column"])
            for name, meta in self.statistics.items()
            if meta["mode"] == mode
        }
        # Bucket range
        if mode == "aggregate":
            expressions["bucket_start"] = func.min(columns.bucket).label("bucket_start")
            expressions["bucket_end"] = self._calculate_bucket_end(columns, time_resolution)
        elif mode == "timeseries":
            expressions["bucket"] = columns.bucket
        return expressions

    @staticmethod
    def _calculate_bucket_end(columns, time_resolution: str):
        """
        Calculate the end of the bucket range based on time resolution.
        """
        max_bucket = func.max(columns.bucket)

        if time_resolution == "daily":
            return max_bucket.label("bucket_end")
        elif time_resolution == "weekly":
            return (max_bucket + text("INTERVAL '6 days'")).label("bucket_end")
        else:
            return (max_bucket + text("INTERVAL '1 month'") - text("INTERVAL '1 day'")).label("bucket_end")

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
            },
            "weighted_error_on_mean_flux": {
                "method": self.weighted_error_on_mean_flux,
                "column": "weighted_error_on_mean_flux",
                "description": "Uncertainty on the weighted mean flux computed from raw data",
            },
            "min_flux": {
                "method": self.min_flux,
                "column": "min_flux",
                "description": "Minimum flux value in the raw measurements",
            },
            "max_flux": {
                "method": self.max_flux,
                "column": "max_flux",
                "description": "Maximum flux value in the raw measurements",
            },
            "data_points": {
                "method": self.data_points,
                "column": "data_points",
                "description": "Number of raw measurements contributing to the statistics",
            },
            "variance_flux": {
                "method": self.variance_flux,
                "column": "variance_flux",
                "description": "Variance of flux across all raw measurements",
            },
        }

    def get_statistics_table(self):
        return FluxMeasurementTable

    def get_statistic_expressions(self, table_ref: Any) -> Dict[str, Any]:
        return {
            name: meta["method"](table_ref).label(meta["column"])
            for name, meta in self.statistics.items()
        }

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


DERIVED_STATISTICS_REGISTRY = DerivedStatisticsRegistry()
RAW_STATISTICS_REGISTRY = RawMeasurementStatisticsRegistry()


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
    use_continuous_aggregates: bool = False,
) -> tuple[BandStatistics, datetime | None, datetime | None, str]:
    """
    Calculate band statistics for given source and time range.
    use_continuous_aggregates: If True, use continuous aggregate tables.
                                   If False, use raw flux measurements.
    """

    if use_continuous_aggregates:
        table, time_resolution = DERIVED_STATISTICS_REGISTRY.get_statistics_table(start_time, end_time)
        columns = table.c
        expressions = DERIVED_STATISTICS_REGISTRY.get_statistic_expressions(columns, time_resolution, mode="aggregate")
        filters = (columns.source_id, columns.band_name, columns.bucket)
    else:
        table = RAW_STATISTICS_REGISTRY.get_statistics_table()
        expressions = RAW_STATISTICS_REGISTRY.get_statistic_expressions(table)
        expressions["bucket_start"] = func.min(table.time).label("bucket_start")
        expressions["bucket_end"] = func.max(table.time).label("bucket_end")
        filters = (table.source_id, table.band_name, table.time)
        time_resolution = "daily"

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


async def get_band_timeseries(
    source_id: int,
    band_name: str,
    conn: AsyncSession,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> BandTimeSeries:
    """
    Get timeseries of mean flux values per bucket for given source and time range.
    """

    table, time_resolution = DERIVED_STATISTICS_REGISTRY.get_statistics_table(start_time, end_time)
    columns = table.c
    expressions = DERIVED_STATISTICS_REGISTRY.get_statistic_expressions(columns, time_resolution, mode="timeseries")
    filters = (columns.source_id, columns.band_name, columns.bucket)

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

    return timeseries
