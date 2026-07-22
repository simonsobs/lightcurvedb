from lightcurvedb.storage.prototype.analysis import ProvidesAnalysis
from lightcurvedb.storage.prototype.cutout import ProvidesCutoutStorage
from lightcurvedb.storage.prototype.flux import ProvidesFluxMeasurementStorage
from lightcurvedb.storage.prototype.instrument import ProvidesInstrumentStorage
from lightcurvedb.storage.prototype.lightcurves import ProvidesLightcurves
from lightcurvedb.storage.prototype.source import ProvidesSourceStorage
from lightcurvedb.storage.prototype.unassigned_flux import (
    ProvidesUnassignedFluxMeasurementStorage,
)
from lightcurvedb.storage.prototype.unassigned_source import (
    ProvidesUnassignedSourceStorage,
)


class Backend:
    sources: ProvidesSourceStorage
    instruments: ProvidesInstrumentStorage
    fluxes: ProvidesFluxMeasurementStorage
    cutouts: ProvidesCutoutStorage
    lightcurves: ProvidesLightcurves
    analysis: ProvidesAnalysis
    unassigned_sources: ProvidesUnassignedSourceStorage
    unassigned_fluxes: ProvidesUnassignedFluxMeasurementStorage

    def __init__(
        self,
        sources: ProvidesSourceStorage,
        instruments: ProvidesInstrumentStorage,
        fluxes: ProvidesFluxMeasurementStorage,
        cutouts: ProvidesCutoutStorage,
        lightcurves: ProvidesLightcurves,
        analysis: ProvidesAnalysis,
        unassigned_sources: ProvidesUnassignedSourceStorage,
        unassigned_fluxes: ProvidesUnassignedFluxMeasurementStorage,
    ) -> None:
        self.sources = sources
        self.instruments = instruments
        self.fluxes = fluxes
        self.cutouts = cutouts
        self.analysis = analysis
        self.lightcurves = lightcurves
        self.unassigned_sources = unassigned_sources
        self.unassigned_fluxes = unassigned_fluxes

    async def setup(self) -> None:
        await self.instruments.setup()
        await self.sources.setup()
        await self.fluxes.setup()
        await self.cutouts.setup()
        await self.lightcurves.setup()
        await self.analysis.setup()
        await self.unassigned_sources.setup()
        await self.unassigned_fluxes.setup()

        return
