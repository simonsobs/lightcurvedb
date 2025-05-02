"""
Create the database tables if they do not exist.
"""

from lightcurvedb.config import settings
from lightcurvedb.models import TABLES as tables


def main():
    manager = settings.sync_manager()
    manager.create_all()

