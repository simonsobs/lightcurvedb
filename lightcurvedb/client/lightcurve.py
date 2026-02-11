"""
A client for extracting complete light-curves.
"""

import asyncio

from lightcurvedb.models.responses import LightcurveBandResult, LightcurveResult
from lightcurvedb.storage.prototype.backend import Backend


async def lightcurve_read_band(
    id: int, band_name: str, backend: Backend
) -> LightcurveBandResult:
    source = await backend.sources.get(id)
    band = await backend.bands.get(band_name)
    band_data = await backend.fluxes.get_band_data(id, band_name)

    return LightcurveBandResult(source=source, band=band, **band_data.model_dump())


async def lightcurve_read_source(id: int, backend: Backend) -> LightcurveResult:
    source = await backend.sources.get(id)
    band_names = await backend.fluxes.get_bands_for_source(id)

    band_data_list = await asyncio.gather(
        *[backend.fluxes.get_band_data(id, band_name) for band_name in band_names]
    )

    return LightcurveResult(source=source, bands=band_data_list)
