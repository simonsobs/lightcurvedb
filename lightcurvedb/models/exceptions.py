"""
Storage exceptions.
"""


class StorageException(Exception):
    pass


class SourceNotFoundException(StorageException):
    pass


class BandNotFoundException(StorageException):
    pass


class CutoutNotFoundException(StorageException):
    pass