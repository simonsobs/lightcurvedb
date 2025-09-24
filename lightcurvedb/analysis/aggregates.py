"""
Functions for creating and managing TimescaleDB continuous aggregates.
"""

from __future__ import annotations
from typing import Dict,Any,List
from dataclasses import dataclass
from sqlalchemy import func, select, text, table, column
from sqlalchemy.orm import Session
from lightcurvedb.models import FluxMeasurementTable

@dataclass
class AggregateConfig:
    """
    Configuration for a continuous aggregate.
    """
    view_name: str
    bucket_interval: str
    retention_drop_after: str
    retention_schedule_interval: str
    refresh_start_offset: str
    refresh_end_offset: str
    refresh_schedule_interval: str


class AggregateConfigurations:
    """
    Configurations for different time buckets.
    """
    
    CONFIGS: List[AggregateConfig] = [
        AggregateConfig(
            view_name="band_statistics_daily",
            bucket_interval="1 day",
            retention_drop_after="1 month",
            retention_schedule_interval="7 days",
            refresh_start_offset="7 days",
            refresh_end_offset="1 day", 
            refresh_schedule_interval="1 day"
        ),
        AggregateConfig(
            view_name="band_statistics_weekly", 
            bucket_interval="1 week",
            retention_drop_after="6 months",
            retention_schedule_interval="1 month",
            refresh_start_offset="3 weeks",
            refresh_end_offset="1 week",
            refresh_schedule_interval="1 week"
        ),
        AggregateConfig(
            view_name="band_statistics_monthly",
            bucket_interval="1 month", 
            retention_drop_after="3 years",
            retention_schedule_interval="4 months",
            refresh_start_offset="3 months",
            refresh_end_offset="1 month",
            refresh_schedule_interval="1 week"
        )
    ]


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

    def get_continuous_aggregate_table(self):
        """
        Generate table reference for band_statistics_monthly.
        """
        base_columns = [
            column('bucket'),
            column('source_id'),
            column('band_name')
        ]
        metric_columns = [column(metric["aggregate_column"]) for metric in self.metrics.values()]

        return table('band_statistics_monthly', *(base_columns + metric_columns))

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

    def __init__(self, metrics_registry: MetricsRegistry, config: ):
        self.metrics_registry = metrics_registry
        self.view_name = "band_statistics_monthly"
        self.bucket_interval = "1 month"

    def build_aggregate_query(self, engine):
        ftable = FluxMeasurementTable
        bucket = func.time_bucket(text(f"INTERVAL '{self.bucket_interval}'"), ftable.time).label("bucket")
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
        CREATE MATERIALIZED VIEW IF NOT EXISTS {self.view_name}
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
        SELECT add_continuous_aggregate_policy('{self.view_name}',
            start_offset => INTERVAL '3 months',
            end_offset => INTERVAL '1 month',
            schedule_interval => INTERVAL '1 week'
        );
        """
    
    def get_retention_policy_sql(self) -> str:
        """
        Build retention policy SQL for continuous aggregate
        """

        return f"""
        SELECT add_retention_policy('{self.view_name}'),
          drop_after => INTERVAL '3 years',
          schedule_interval => INTERVAL '4 months');
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
    for config in AggregateConfigurations.CONFIGS:
        builder = ContinuousAggregateBuilder(METRICS_REGISTRY, config)
        builder.create_aggregate(session)
