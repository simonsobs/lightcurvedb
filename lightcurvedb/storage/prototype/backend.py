from lightcurvedb.storage.prototype.band import ProvidesBandStorage
from lightcurvedb.storage.prototype.cutout import ProvidesCutoutStorage
from lightcurvedb.storage.prototype.flux import ProvidesFluxMeasurementStorage
from lightcurvedb.storage.prototype.source import ProvidesSourceStorage


class Backend:
    sources: ProvidesSourceStorage
    bands: ProvidesBandStorage
    fluxes: ProvidesFluxMeasurementStorage

    def __init__(
        self,
        sources: ProvidesSourceStorage,
        bands: ProvidesBandStorage,
        fluxes: ProvidesFluxMeasurementStorage,
        cutouts: ProvidesCutoutStorage,
    ) -> None:
        self.sources = sources
        self.bands = bands
        self.fluxes = fluxes
        self.cutouts = cutouts

    async def setup(self) -> None:
        await self.bands.setup()
        await self.sources.setup()
        await self.fluxes.setup()
        await self.cutouts.setup()

        return
