"""
Base schema definitions.

Only exports shared tables (sources, bands).
Backend-specific tables (flux_measurements) are defined in backend-specific schema modules.
"""

from lightcurvedb.storage.base.schema import (
    BANDS_TABLE,
    SOURCES_TABLE,
)

__all__ = [
    "SOURCES_TABLE",
    "BANDS_TABLE",
]
