"""
Creating database connections.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from lightcurvedb.config import ConnectionType, Settings


class ConnectionSettings:

    def __init__(self, settings: Settings):
        self.settings = settings
        self._session_manager = None

    @asynccontextmanager
    async def get_lightcurve_connection(self) -> AsyncIterator:
        from lightcurvedb.client.lightcurve import (
            PsycopgLightcurveConnection,
            SQLAlchemyLightcurveConnection,
        )

        if self.settings.connection_type == ConnectionType.PSYCOPG:
            import psycopg

            async with await psycopg.AsyncConnection.connect(
                self.settings.psycopg_uri
            ) as conn:
                yield PsycopgLightcurveConnection(conn)

        elif self.settings.connection_type == ConnectionType.SQLALCHEMY:
            if self._session_manager is None:
                from lightcurvedb.managers import AsyncSessionManager

                self._session_manager = AsyncSessionManager(
                    connection_url=self.settings.async_postgres_uri,
                    echo=self.settings.postgres_echo,
                )

            async with self._session_manager.session() as session:
                yield SQLAlchemyLightcurveConnection(session)
