"""
Parquet implementation of unassigned flux measurement storage.
"""

from collections.abc import Iterable
from uuid import UUID

import pandas as pd

from lightcurvedb.models import UnassignedFluxMeasurement
from lightcurvedb.models.exceptions import UnassignedFluxMeasurementNotFoundException
from lightcurvedb.storage.parquet.flux import PandasFluxMeasurementStorage
from lightcurvedb.storage.prototype.unassigned_flux import (
    ProvidesUnassignedFluxMeasurementStorage,
)


class PandasUnassignedFluxMeasurementStorage(
    PandasFluxMeasurementStorage, ProvidesUnassignedFluxMeasurementStorage
):
    """
    Parquet storage for measurements awaiting cross-match review.
    """

    def _new_table(
        self, measurements: Iterable[UnassignedFluxMeasurement]
    ) -> pd.DataFrame:
        table = pd.DataFrame(
            [
                {
                    **measurement.model_dump(),
                    "measurement_id": str(measurement.measurement_id),
                    "source_id": str(measurement.source_id),
                }
                for measurement in measurements
            ]
        )

        table.set_index("measurement_id", inplace=True)

        return table

    async def get(self, measurement_id: UUID) -> UnassignedFluxMeasurement:
        """
        Retrieve an unassigned flux measurement by ID.
        """
        measurement_id_str = str(measurement_id)
        if not self.base_path.exists():
            raise UnassignedFluxMeasurementNotFoundException("Table not found")

        for path in self.base_path.glob("*.parquet"):
            source_id = UUID(path.stem)
            table = await self._read_file(source_id)
            if table is None or measurement_id_str not in table.index:
                continue

            data = table.loc[measurement_id_str].to_dict()
            data["measurement_id"] = measurement_id_str
            return UnassignedFluxMeasurement.model_validate(data)

        raise UnassignedFluxMeasurementNotFoundException(
            f"Unassigned flux measurement {measurement_id} not found"
        )

    async def get_for_source(self, source_id: UUID) -> list[UnassignedFluxMeasurement]:
        """
        Retrieve all unassigned flux measurements for a source.
        """
        if (table := await self._read_file(source_id)) is None:
            return []

        measurements = []
        for measurement_id, row in table.sort_values(["time"]).iterrows():
            data = row.to_dict()
            data["measurement_id"] = str(measurement_id)
            measurements.append(UnassignedFluxMeasurement.model_validate(data))
        return measurements
