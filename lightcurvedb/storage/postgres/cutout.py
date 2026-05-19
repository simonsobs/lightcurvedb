"""
Store cutouts directly in a postgres array.
"""

import csv
from io import StringIO
from typing import Literal
from uuid import UUID

import pydantic
from psycopg.rows import class_row

from lightcurvedb.config import settings
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

        with self.tracer.start_as_current_span("create_cutout") as span:
            span.set_attribute("cutout.measurement_id", str(cutout.measurement_id))

            params = cutout.model_dump()

            async with self.cursor() as cur:
                await cur.execute(query, params)

            if cutout.measurement_id is None:
                raise ValueError(
                    "Cutout measurement_id must not be None after creation"
                )
            return cutout.measurement_id

    async def create_batch(
        self,
        cutouts: list[Cutout],
        bulk_insert_mode: Literal["text", "json", "csv", "rowwise"] | None = None,
    ) -> None:
        """
        Store a cutout for a given source and band.
        """

        bulk_insert_mode = bulk_insert_mode or settings.bulk_insert_mode_cutouts

        with self.tracer.start_as_current_span("create_batch_cutouts") as span:
            span.set_attribute("cutout.num_cutouts", len(cutouts))
            span.set_attribute("cutout.bulk_insert_mode", bulk_insert_mode)

            if bulk_insert_mode == "json":
                return await self._insert_batch_data_json(cutouts)
            elif bulk_insert_mode == "csv":
                return await self._insert_batch_data_copy_csv(cutouts)
            elif bulk_insert_mode == "text":
                return await self._insert_batch_data_text(cutouts)
            else:
                return await self._insert_batch_data_rowwise(cutouts)

    async def _insert_batch_data_copy_csv(self, cutouts: list[Cutout]) -> None:
        """
        Insert batch data using CSV.
        """
        async with self.cursor() as cur:
            async with cur.copy(f"""
                COPY cutouts (
                    {", ".join(cutouts[0].model_dump().keys())}
                ) FROM STDIN WITH (FORMAT csv)
            """) as copy:
                with self.tracer.start_as_current_span(
                    "prepare_batch_cutouts_for_csv_copy"
                ) as span:
                    span.set_attribute("cutout.num_cutouts", len(cutouts))
                    copy_buffer = StringIO()
                    writer = csv.writer(copy_buffer)
                    for row in cutouts:
                        writer.writerow(
                            row.model_dump(context={"target": "postgres"}).values()
                        )
                    span.set_attribute("cutout.payload_size_bytes", copy_buffer.tell())
                    copy_buffer.seek(0)

                await copy.write(copy_buffer.read())

    async def _insert_batch_data_text(self, cutouts: list[Cutout]) -> None:
        query = """
            COPY cutouts (
                measurement_id,
                data,
                time,
                units,
                frequency,
                module,
                source_id
            )
            FROM STDIN WITH (FORMAT text)
        """
        async with self.cursor() as cur:
            with self.tracer.start_as_current_span("copy_batch_cutouts") as span:
                span.set_attribute("cutout.num_cutouts", len(cutouts))

                async with cur.copy(query) as copy:
                    for c in cutouts:
                        await copy.write_row(
                            c.model_dump(context={"target": "postgres"}).values()
                        )

    async def _insert_batch_data_json(self, cutouts: list[Cutout]) -> None:
        """
        Insert batch data using JSON.
        """
        ta = pydantic.TypeAdapter(list[Cutout])

        query = """
            INSERT INTO cutouts (
                measurement_id,
                data,
                time,
                units,
                frequency,
                module,
                source_id
            )
            SELECT
                x.measurement_id,
                x.data,
                x.time,
                x.units,
                x.frequency,
                x.module,
                x.source_id
            FROM jsonb_to_recordset(%(cutouts)s) AS x(
                measurement_id uuid,
                data real[][],
                time timestamp,
                units text,
                frequency integer,
                module text,
                source_id uuid
            )
        """

        async with self.cursor() as cur:
            with self.tracer.start_as_current_span(
                "prepare_batch_cutouts_for_jsonb"
            ) as span:
                span.set_attribute("cutout.num_cutouts", len(cutouts))
                payload = ta.dump_json(cutouts, context={"target": "postgres"}).decode(
                    "utf-8"
                )
                span.set_attribute("cutout.payload_size_bytes", len(payload))

            await cur.execute(query, {"cutouts": payload})

        return

    async def _insert_batch_data_rowwise(self, cutouts: list[Cutout]) -> None:
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

        with self.tracer.start_as_current_span("create_batch_cutouts_rowwise") as span:
            span.set_attribute("cutout.num_cutouts", len(cutouts))

            params_list = [c.model_dump() for c in cutouts]

            async with self.cursor() as cur:
                await cur.executemany(query, params_list)

    async def retrieve_cutout(self, source_id: UUID, measurement_id: UUID) -> Cutout:
        """
        Retrieve a cutout for a given source and band.
        """
        query = """
            SELECT source_id, measurement_id, time, units, data, module, frequency
            FROM cutouts
            WHERE source_id = %(source_id)s AND measurement_id = %(measurement_id)s
        """

        with self.tracer.start_as_current_span("retrieve_cutout") as span:
            span.set_attribute("cutout.source_id", str(source_id))
            span.set_attribute("cutout.measurement_id", str(measurement_id))

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

        with self.tracer.start_as_current_span("retrieve_cutouts_for_source") as span:
            span.set_attribute("cutout.source_id", str(source_id))

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

        with self.tracer.start_as_current_span("delete_cutout") as span:
            span.set_attribute("cutout.measurement_id", str(measurement_id))

            async with self.cursor() as cur:
                await cur.execute(query, {"measurement_id": measurement_id})
