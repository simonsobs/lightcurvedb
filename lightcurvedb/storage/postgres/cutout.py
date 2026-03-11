"""
Store cutouts directly in a postgres array.
"""

from uuid import UUID

from psycopg.rows import class_row

from lightcurvedb.models import Cutout
from lightcurvedb.storage.postgres.pooler import PostgresPoolUser
from lightcurvedb.storage.postgres.schema import CUTOUT_INDEXES, CUTOUT_SCHEMA
from lightcurvedb.storage.prototype.cutout import ProvidesCutoutStorage


class PostgresCutoutStorage(ProvidesCutoutStorage, PostgresPoolUser):
    """
    PostgreSQL cutout storage with array aggregations.
    """

    async def setup(self) -> None:
        """
        Set up the cutout storage system (e.g. create the tables).
        """
        async with self.cursor() as cur:
            await cur.execute(CUTOUT_SCHEMA)
            await cur.execute(CUTOUT_INDEXES)

    async def create(self, cutout: Cutout) -> UUID:
        """
        Store a cutout for a given source and band.
        """
        query = """
            INSERT INTO cutouts (
                measurement_id,
                source_id,
                time,
                units,
                data,
                module,
                frequency
            ) VALUES (
                %(measurement_id)s,
                %(source_id)s,
                %(time)s,
                %(units)s,
                %(data)s,
                %(module)s,
                %(frequency)s
            )
        """
        params = cutout.model_dump()

        async with self.cursor() as cur:
            await cur.execute(query, params)

        if cutout.measurement_id is None:
            raise ValueError("Cutout measurement_id must not be None after creation")
        return cutout.measurement_id

    async def create_batch(self, cutouts: list[Cutout]) -> list[UUID]:
        """
        Store a cutout for a given source and band.
        """
        # Unnest _will not work_ but is also thankfully not necessary
        # because we don't have a unique primary key. It would represent
        # a performance improvement, but we're ok for now.

        query = """
            INSERT INTO cutouts (
                measurement_id,
                source_id,
                time,
                units,
                data,
                module,
                frequency
            ) VALUES (
                %(measurement_id)s,
                %(source_id)s,
                %(time)s,
                %(units)s,
                %(data)s,
                %(module)s,
                %(frequency)s
            )
        """

        params_list = [c.model_dump() for c in cutouts]

        async with self.cursor() as cur:
            await cur.executemany(query, params_list)

        measurement_ids: list[UUID] = []
        for c in cutouts:
            if c.measurement_id is None:
                raise ValueError("Cutout measurement_id must not be None after creation")
            measurement_ids.append(c.measurement_id)
        return measurement_ids

    async def retrieve_cutout(self, source_id: UUID, measurement_id: UUID) -> Cutout:
        """
        Retrieve a cutout for a given source and band.
        """
        query = """
            SELECT source_id, measurement_id, time, units, data, module, frequency
            FROM cutouts
            WHERE source_id = %(source_id)s AND measurement_id = %(measurement_id)s
        """

        async with self.cursor(row_factory=class_row(Cutout)) as cur:
            await cur.execute(
                query,
                {
                    "source_id": source_id,
                    "measurement_id": measurement_id,
                },
            )
            row = await cur.fetchone()

            if not row:
                from lightcurvedb.models.exceptions import CutoutNotFoundException

                raise CutoutNotFoundException(
                    f"Cutout {source_id}/{measurement_id} not found"
                )

            return row

    async def retrieve_cutouts_for_source(self, source_id: UUID) -> list[Cutout]:
        """
        Retrieve cutouts for a given source.
        """
        query = """
            SELECT source_id, measurement_id, time, units, data, module, frequency
            FROM cutouts
            WHERE source_id = %(source_id)s
        """

        async with self.cursor(row_factory=class_row(Cutout)) as cur:
            await cur.execute(
                query,
                {
                    "source_id": source_id,
                },
            )
            rows = await cur.fetchall()
            return rows

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete a cutout by ID.
        """
        query = """
            DELETE FROM cutouts
            WHERE measurement_id = %(measurement_id)s
        """

        async with self.cursor() as cur:
            await cur.execute(query, {"measurement_id": measurement_id})
