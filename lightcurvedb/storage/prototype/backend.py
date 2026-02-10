from lightcurvedb.storage.prototype.instrument import ProvidesInstrumentStorage
from lightcurvedb.storage.prototype.cutout import ProvidesCutoutStorage
from lightcurvedb.storage.prototype.flux import ProvidesFluxMeasurementStorage
from lightcurvedb.storage.prototype.source import ProvidesSourceStorage


class Backend:
    sources: ProvidesSourceStorage
    instruments: ProvidesInstrumentStorage
    fluxes: ProvidesFluxMeasurementStorage

    def __init__(
        self,
        sources: ProvidesSourceStorage,
        instruments: ProvidesInstrumentStorage,
        fluxes: ProvidesFluxMeasurementStorage,
        cutouts: ProvidesCutoutStorage,
    ) -> None:
        self.sources = sources
        self.instruments = instruments
        self.fluxes = fluxes
        self.cutouts = cutouts

    async def setup(self) -> None:
        await self.instruments.setup()
        await self.sources.setup()
        await self.fluxes.setup()
        await self.cutouts.setup()

        return
