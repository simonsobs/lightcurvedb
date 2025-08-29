"""
Functions for creating and managing continuous aggregates.
"""

from sqlalchemy import text
from sqlalchemy.orm import Session


def create_continuous_aggregates(session: Session):
    engine = session.bind
    
    # Create the continuous aggregate
    band_statistics_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS band_statistics_weekly
        WITH (
            timescaledb.continuous,
            timescaledb.materialized_only = false
        ) AS
        SELECT
            time_bucket('1 week', time) as bucket,
            source_id,
            band_name,
            SUM(i_flux) as sum_flux,
            MIN(i_flux) as min_flux,
            MAX(i_flux) as max_flux,
            COUNT(*) as data_points
        FROM flux_measurements
        GROUP BY bucket, source_id, band_name
        WITH DATA;
    """
    
    # Add refresh policy
    refresh_policy_sql = """
        SELECT add_continuous_aggregate_policy('band_statistics_weekly',
            start_offset => INTERVAL '3 weeks',
            end_offset => INTERVAL '1 week',
            schedule_interval => INTERVAL '4 days'
        );
    """
    
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(band_statistics_sql))
            conn.execute(text(refresh_policy_sql))