from .band import Band, BandTable
from .cutout import Cutout, CutoutTable
from .flux import FluxMeasurement, FluxMeasurementTable
from .source import Source, SourceTable

MODELS = [Cutout, FluxMeasurement, Source, Band]

TABLES = [CutoutTable, FluxMeasurementTable, SourceTable, BandTable]
