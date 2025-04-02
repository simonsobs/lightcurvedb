"""
Source generation.
"""

from random import random

from ..managers import SyncSessionManager
from ..models import SourceTable


def create_fixed_sources(number: int, manager: SyncSessionManager) -> list[int]:
    """
    Create a number of fixed sources in the database.

    Parameters
    ----------
    number : int
        The number of sources to create.

    Returns
    -------
    list[int]
        The IDs of the created sources.
    """

    sources = [
        SourceTable(
            ra=random() * 360.0 - 180.0,
            dec=random() * 180.0 - 90.0,
            variable=False,
        )
        for _ in range(number)
    ]

    with manager.session() as session:
        session.add_all(sources)
        session.commit()

        source_ids = [source.id for source in sources]

    return source_ids
