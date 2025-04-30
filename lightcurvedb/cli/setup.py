"""
Create the database tables if they do not exist.
"""

from lightcurvedb.config import settings


def main():
    manager = settings.sync_manager()

    if not manager.engine.dialect.has_table("sources"):
        manager.create_all()
