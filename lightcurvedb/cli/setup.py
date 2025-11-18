"""
Create the database tables if they do not exist.
"""

from sqlalchemy import text

# from lightcurvedb.analysis.aggregates import create_continuous_aggregates
from lightcurvedb.config import settings
from lightcurvedb.models import *  # noqa: F403


def main():
    manager = settings.sync_manager()
    manager.create_all()

    with manager.session() as session:
        session.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
        session.execute(text("""
                SELECT create_hypertable(
                    'flux_measurements',
                    'time',
                    chunk_time_interval => INTERVAL '7 days',
                    if_not_exists => TRUE
                );
                    """))
        # create_continuous_aggregates(session)
        session.commit()