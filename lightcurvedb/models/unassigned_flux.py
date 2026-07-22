"""
Individual flux measurements for unassigned sources.
"""

from uuid import UUID

from pydantic import Field as PydanticField

from .flux import FluxMeasurement, MeasurementMetadata


class UnassignedMeasurementMetadata(MeasurementMetadata):
    """
    Additional metadata about unassigned measurements stored as a JSONB column.
    """

    map_id: str | None = None
    simulation_scenario: str | None = None


class UnassignedFluxMeasurement(FluxMeasurement):
    """
    A flux measurement associated with an unassigned source.
    """

    source_id: UUID
    extra: UnassignedMeasurementMetadata | None = PydanticField(default=None)
