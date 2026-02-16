from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from lightcurvedb.config import Settings
from lightcurvedb.storage.parquet.analysis import PandasAnalysis
from lightcurvedb.storage.parquet.cutout import PandasCutoutStorage
from lightcurvedb.storage.parquet.flux import PandasFluxMeasurementStorage
from lightcurvedb.storage.parquet.instrument import PandasInstrumentStorage
from lightcurvedb.storage.parquet.lightcurves import PandasLightcurves
from lightcurvedb.storage.parquet.source import PandasSourceStorage
from lightcurvedb.storage.prototype.backend import Backend


async def generate_pandas_backend(directory: Path) -> Backend:
    sources = PandasSourceStorage(directory / "sources.parquet")
    instruments = PandasInstrumentStorage(directory / "instruments.parquet")
    fluxes = PandasFluxMeasurementStorage(directory / "fluxes.parquet")
    cutouts = PandasCutoutStorage(directory / "cutouts.parquet")
    lightcurves = PandasLightcurves(flux_storage=fluxes)
    analysis = PandasAnalysis(flux_storage=fluxes)

    backend = Backend(
        sources=sources,
        instruments=instruments,
        fluxes=fluxes,
        cutouts=cutouts,
        lightcurves=lightcurves,
        analysis=analysis,
    )

    await backend.setup()

    return backend


@asynccontextmanager
async def pandas_backend(settings: Settings) -> AsyncIterator[Backend]:
    """
    Get a Pandas storage backend.
    """
    backend = await generate_pandas_backend(directory=settings.parquet_base_path)
    yield backend
