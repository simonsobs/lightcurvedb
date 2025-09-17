"""
Functions for creating and managing TimescaleDB continuous aggregates.
"""

from __future__ import annotations
from typing import Dict,Any
from sqlalchemy import func, select, text, table, column
from sqlalchemy.orm import Session
from lightcurvedb.models import FluxMeasurementTable


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

    def __init__(self, metrics_registry: MetricsRegistry):
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

    def create_aggregate(self, session: Session) -> None:
        engine = session.bind
        select_query = self.build_aggregate_query(engine)
        create_view_sql = self.get_create_view_sql(select_query)
        refresh_policy_sql = self.get_refresh_policy_sql()

        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(create_view_sql))
            conn.execute(text(refresh_policy_sql))



METRICS_REGISTRY = MetricsRegistry()


def create_continuous_aggregates(session: Session) -> None:
    """
    Create band_statistics_monthly continuous aggregate.
    """
    builder = ContinuousAggregateBuilder(METRICS_REGISTRY)
    builder.create_aggregate(session)
