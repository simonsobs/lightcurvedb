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