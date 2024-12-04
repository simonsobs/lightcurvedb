"""
Source generation.
"""

from random import random

from ..models import SourceTable
from ..sync import get_session


def create_fixed_sources(number: int) -> list[int]:
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
            ra=random() * 360.0,
            dec=random() * 180.0 - 90.0,
            variable=False,
        )
        for _ in range(number)
    ]

    with get_session() as session:
        session.add_all(sources)
        session.commit()

        source_ids = [source.id for source in sources]

    return source_ids
