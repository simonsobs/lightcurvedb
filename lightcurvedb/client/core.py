"""
Core client, including session management.
"""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)
from sqlmodel import SQLModel


class SessionManager:
    """
    A manager for asynchronous sessions. Expected usage of this class to interact
    with the LightcurveDB API is as follows:

    manager = SessionManager(conn_url)

    async with manager.session() as conn:
        res = await lightcurve_read_band(id=993, band_name="f220", conn=conn)

    Why do it this way, instead of having a global `engine` and `get_session` function?
    As lightcurvedb is primarily a library of the models stored in the lightcurve
    database, used by a number of pieces of software, having global variables
    that require database connections is highly undesirable.
    """

    connection_url: str
    engine: AsyncEngine
    session: async_sessionmaker

    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.engine = create_async_engine(self.connection_url)
        self.session = async_sessionmaker(self.engine)

    async def create_all(self):
        """
        Run the `SQLModel.metadata.create_all` migration tool. Required
        to set up the table schema.
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

    async def drop_all(self):
        """
        Run the `SQLModel.metadata.drop_all` deletion method. WARNING: this
        will delete all data in your database; you probably don't want to do this
        unless you are writing a test.
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.drop_all)
