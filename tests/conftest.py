"""
Sets up a testcontainer with a few lightcurves and cut-outs in it for
testing purposes.
"""

from testcontainers.postgres import PostgresContainer
from pytest import fixture as sync_fixture
from pytest_asyncio import fixture as async_fixture
from lightcurvedb.client.core import SessionManager

from lightcurvedb.client.source import source_add, source_delete
from lightcurvedb.models.source import Source

@sync_fixture(scope="session")
def base_server():
    """
    Sets up a server (completely empty).
    """

    with PostgresContainer() as container:
        conn_url = container.get_connection_url()

        yield conn_url.replace("psycopg2", "asyncpg")


@async_fixture(loop_scope="session", scope="session")
async def client(base_server):
    manager = SessionManager(base_server)

    await manager.create_all()

    yield manager

    await manager.drop_all()


@async_fixture(loop_scope="session", scope="session")
async def client_full(client):
    async with client.session() as conn:
        source_id = await source_add(
            Source(
                ra=44.4,
                dec=44.4,
                variable=False
            ),
            conn
        )

    yield client

    async with client.session() as conn:
        await source_delete(id=source_id, conn=conn)

    




