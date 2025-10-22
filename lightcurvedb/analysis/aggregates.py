"""
Functions for creating and managing TimescaleDB continuous aggregates.
"""


from dataclasses import dataclass
from typing import Any, Dict, List

from sqlalchemy import column, func, select, table, text
from sqlalchemy.orm import Session

from lightcurvedb.models import FluxMeasurementTable


@dataclass
class AggregateConfig:
    """
    Configuration for a continuous aggregate
    """
    view_name: str
    # Name of the materialized view table
    time_resolution: str
    # Time resolution label (daily, weekly, monthly)
    bucket_interval: str
    # Time window for grouping data
    drop_after_interval: str
    # How old data must be before deletion
    drop_schedule_interval: str
    # How often to check for and delete old data
    refresh_start_offset: str
    # How far back to look for new raw data
    refresh_end_offset: str
    # Exclude recent data to avoid incomplete buckets
    refresh_schedule_interval: str
    # How often to update the aggregate with new data
    evaluate_threshold_days: int
    # Maximum age in days for this aggregate to be selected
    display_date_correction: str
    # SQL interval expression for bucket_end display correction



AggregateConfigurations: List[AggregateConfig] = [
        AggregateConfig(
            view_name="band_statistics_daily",
            time_resolution="daily",
            bucket_interval="1 day",
            drop_after_interval="1 month",
            drop_schedule_interval="7 days",
            refresh_start_offset="7 days",
            refresh_end_offset="1 day",
            refresh_schedule_interval="1 day",
            evaluate_threshold_days=30,
            display_date_correction=""
        ),
        AggregateConfig(
            view_name="band_statistics_weekly",
            time_resolution="weekly",
            bucket_interval="1 week",
            drop_after_interval="6 months",
            drop_schedule_interval="1 month",
            refresh_start_offset="3 weeks",
            refresh_end_offset="1 week",
            refresh_schedule_interval="1 week",
            evaluate_threshold_days=180,
            display_date_correction="INTERVAL '6 days'"
        ),
        AggregateConfig(
            view_name="band_statistics_monthly",
            time_resolution="monthly",
            bucket_interval="1 month",
            drop_after_interval="3 years",
            drop_schedule_interval="4 months",
            refresh_start_offset="3 months",
            refresh_end_offset="1 month",
            refresh_schedule_interval="1 week",
            evaluate_threshold_days=3650,
            display_date_correction="INTERVAL '1 month' - INTERVAL '1 day'"
        )
    ]


def select_aggregate_config(delta_days: int) -> AggregateConfig:
    """
    Select appropriate aggregate configuration based on time delta in days.
    """
    for config in AggregateConfigurations:
        if delta_days <= config.evaluate_threshold_days:
            return config

class MetricsRegistry:
    """
    Registry for statistical metrics and their SQLAlchemy expressions.
    """

    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {
            "sum_flux_over_uncertainty_squared": {
                "method": self.sum_flux_over_uncertainty_squared,
                "aggregate_column": "sum_flux_over_uncertainty_squared",
                "description": "Sum of flux/uncertainty^2 for weighted calculations"
            },
            "sum_inverse_uncertainty_squared": {
                "method": self.sum_inverse_uncertainty_squared,
                "aggregate_column": "sum_inverse_uncertainty_squared",
                "description": "Sum of 1/uncertainty^2 for weighted calculations"
            },
            "min_flux": {
                "method": self.min_flux,
                "aggregate_column": "min_flux",
                "description": "Minimum flux value in the time period"
            },
            "max_flux": {
                "method": self.max_flux,
                "aggregate_column": "max_flux",
                "description": "Maximum flux value in the time period"
            },
            "data_points": {
                "method": self.data_points_count,
                "aggregate_column": "data_points",
                "description": "Number of data points in the time period"
            },
            "sum_flux": {
                "method": self.sum_flux,
                "aggregate_column": "sum_flux",
                "description": "Sum of flux values in the time period"
            },
            "sum_flux_squared": {
                "method": self.sum_flux_squared,
                "aggregate_column": "sum_flux_squared",
                "description": "Sum of squared flux values for variance calculation"
            },
        }
    @staticmethod
    def sum_flux_over_uncertainty_squared(table):
        return func.sum(table.i_flux / (table.i_uncertainty * table.i_uncertainty))

    @staticmethod
    def sum_inverse_uncertainty_squared(table):
        return func.sum(1 / (table.i_uncertainty * table.i_uncertainty))

    @staticmethod
    def min_flux(table):
        return func.min(table.i_flux)

    @staticmethod
    def max_flux(table):
        return func.max(table.i_flux)

    @staticmethod
    def data_points_count(table):
        return func.count()

    @staticmethod
    def sum_flux(table):
        return func.sum(table.i_flux)

    @staticmethod
    def sum_flux_squared(table):
        return func.sum(table.i_flux * table.i_flux)

    def get_continuous_aggregate_table(self, view_name: str):
        """
        Generate table reference for specified aggregate table.
        """
        base_columns = [
            column('bucket'),
            column('source_id'),
            column('band_name')
        ]
        metric_columns = [column(metric["aggregate_column"]) for metric in self.metrics.values()]

        return table(view_name, *(base_columns + metric_columns))

    def get_metric_expressions(self, table_ref):
        """
        Build SQLAlchemy expressions for all metrics.
        """
        return [
            metric["method"](table_ref).label(metric["aggregate_column"])
            for metric in self.metrics.values()
        ]


class ContinuousAggregateBuilder:
    """
    Builds TimescaleDB continuous aggregates.
    """

    def __init__(self, metrics_registry: MetricsRegistry, config: AggregateConfig):
        self.metrics_registry = metrics_registry
        self.config = config

    def build_aggregate_query(self, engine):
        ftable = FluxMeasurementTable
        bucket = func.time_bucket(text(f"INTERVAL '{self.config.bucket_interval}'"), ftable.time).label("bucket")
        group_cols = [bucket, ftable.source_id, ftable.band_name]

        metric_exprs = self.metrics_registry.get_metric_expressions(ftable)

        select_query = (
            select(
                bucket,
                ftable.source_id,
                ftable.band_name,
                *metric_exprs,
            )
            .select_from(ftable)
            .group_by(*group_cols)
        )

        return select_query.compile(engine, compile_kwargs={"literal_binds": True}).string

    def get_create_view_sql(self, select_query: str) -> str:
        """
        Build CREATE MATERIALIZED VIEW statement for the aggregate.
        """

        return f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {self.config.view_name}
        WITH (
            timescaledb.continuous,
            timescaledb.materialized_only = false
        ) AS
        {select_query}
        WITH DATA;
        """

    def get_refresh_policy_sql(self) -> str:
        """
        Build refresh policy SQL for the continuous aggregate.
        """

        return f"""
        SELECT add_continuous_aggregate_policy('{self.config.view_name}',
            start_offset => INTERVAL '{self.config.refresh_start_offset}',
            end_offset => INTERVAL '{self.config.refresh_end_offset}',
            schedule_interval => INTERVAL '{self.config.refresh_schedule_interval}'
        );
        """
    
    def get_retention_policy_sql(self) -> str:
        """
        Build retention policy SQL for continuous aggregate
        """

        return f"""
        SELECT add_retention_policy('{self.config.view_name}',
            drop_after => INTERVAL '{self.config.drop_after_interval}',
            schedule_interval => INTERVAL '{self.config.drop_schedule_interval}');
        """

    def create_aggregate(self, session: Session) -> None:
        engine = session.bind
        select_query = self.build_aggregate_query(engine)
        create_view_sql = self.get_create_view_sql(select_query)
        refresh_policy_sql = self.get_refresh_policy_sql()
        retention_policy_sql = self.get_retention_policy_sql()

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(create_view_sql))
            conn.execute(text(refresh_policy_sql))
            conn.execute(text(retention_policy_sql))



METRICS_REGISTRY = MetricsRegistry()


def create_continuous_aggregates(session: Session) -> None:
    """
    Create all continuous aggregates (daily, weekly, monthly).
    """
    for config in AggregateConfigurations:
        builder = ContinuousAggregateBuilder(METRICS_REGISTRY, config)
        builder.create_aggregate(session)


def refresh_continuous_aggregates(session: Session) -> None:
    """
    Manually refresh all continuous aggregates for historical data.
    """
    engine = session.get_bind()

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for config in AggregateConfigurations:
            conn.execute(text(f"CALL refresh_continuous_aggregate('{config.view_name}', NULL, NULL)"))
