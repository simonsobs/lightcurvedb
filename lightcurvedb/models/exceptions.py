"""
Storage exceptions.
"""


class StorageException(Exception):
    pass


class SourceNotFoundException(StorageException):
    pass


class InstrumentNotFoundException(StorageException):
    pass


class CutoutNotFoundException(StorageException):
    pass


class FluxMeasurementNotFoundException(StorageException):
    pass


class UnassignedSourceNotFoundException(StorageException):
    pass


class UnassignedFluxMeasurementNotFoundException(StorageException):
    pass


class CandidateReviewConflictError(StorageException):
    """
    An incompatible or stale unassigned-source review transition was requested.
    """

    pass
