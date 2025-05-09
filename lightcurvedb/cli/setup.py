"""
Create the database tables if they do not exist.
"""

from lightcurvedb.config import settings


def main():
    manager = settings.sync_manager()
    manager.create_all()
