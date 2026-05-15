"""
PostgreSQL implementation of FluxMeasurementStorage protocol.
"""

import csv
import json
from collections import defaultdict
from io import BytesIO, StringIO
from typing import Literal
from uuid import UUID

import pydantic
from psycopg.rows import class_row
from uuid_extensions import uuid7

from lightcurvedb.config import settings
from lightcurvedb.models.flux import FluxMeasurement
from lightcurvedb.storage.postgres.pooler import PostgresPoolUser
from lightcurvedb.storage.postgres.schema import FLUX_INDEXES, FLUX_MEASUREMENTS_TABLE
from lightcurvedb.storage.prototype.flux import ProvidesFluxMeasurementStorage


class PostgresFluxMeasurementStorage(ProvidesFluxMeasurementStorage, PostgresPoolUser):
    """
    PostgreSQL flux measurement storage with array aggregations.
    """

    async def setup(self) -> None:
        async with self.cursor() as cur:
            await cur.execute(FLUX_MEASUREMENTS_TABLE)
            await cur.execute(FLUX_INDEXES)

    async def create(self, measurement: FluxMeasurement) -> UUID:
        """
        Insert single measurement.
        """
        query = """
            INSERT INTO flux_measurements (
                measurement_id, frequency, module, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty,
                flux, flux_err, extra
            )
            VALUES (
                %(measurement_id)s, %(frequency)s, %(module)s, %(source_id)s, %(time)s,
                %(ra)s, %(dec)s, %(ra_uncertainty)s, %(dec_uncertainty)s,
                %(flux)s, %(flux_err)s, %(extra)s
            )
            RETURNING measurement_id 
        """
        params = measurement.model_dump()

        if params["extra"] is not None:
            params["extra"] = json.dumps(params["extra"])

        async with self.cursor() as cur:
            await cur.execute(query, params)
            row = await cur.fetchone()
            if row is None:
                raise ValueError("INSERT RETURNING measurement_id returned no row")
            return row[0]

    async def create_batch(
        self,
        measurements: list[FluxMeasurement],
        bulk_insert_mode: Literal["unnest", "json", "csv"] | None = None,
    ) -> None:
        """
        Bulk insert. Allows for over-ride of the dumper otherwise reads
        from the settings.
        """
        bulk_insert_mode = bulk_insert_mode or settings.bulk_insert_mode

        if bulk_insert_mode == "json":
            return await self._insert_batch_data_copy_json(measurements)
        elif bulk_insert_mode == "csv":
            return await self._insert_batch_data_copy_csv(measurements)

        data = defaultdict(list)

        for measurement in measurements:
            measurement_dict = measurement.model_dump()
            if measurement_dict.get("measurement_id", None) is None:
                measurement_dict["measurement_id"] = uuid7()
            if measurement_dict["extra"] is not None:
                measurement_dict["extra"] = json.dumps(measurement_dict["extra"])
            for key, value in measurement_dict.items():
                data[key].append(value)

        if bulk_insert_mode == "csv":
            return await self._insert_batch_data_copy_csv(data)
        else:
            return await self._insert_batch_data(data)

    async def _insert_batch_data_copy_json(self, data: list[FluxMeasurement]) -> None:
        """
        Bulk insert using JSONB payload + jsonb_to_recordset.
        """
        ta = pydantic.TypeAdapter(list[FluxMeasurement])

        query = """
            INSERT INTO flux_measurements (
                measurement_id, frequency, module, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty, flux, flux_err, extra
            )
            SELECT
                x.measurement_id,
                x.frequency,
                x.module,
                x.source_id,
                x.time,
                x.ra,
                x.dec,
                x.ra_uncertainty,
                x.dec_uncertainty,
                x.flux,
                x.flux_err,
                x.extra
            FROM jsonb_to_recordset(%(payload)s::jsonb) AS x(
                measurement_id uuid,
                frequency integer,
                module text,
                source_id uuid,
                time timestamptz,
                ra real,
                dec real,
                ra_uncertainty real,
                dec_uncertainty real,
                flux real,
                flux_err real,
                extra jsonb
            )
            RETURNING measurement_id
        """

        async with self.cursor() as cur:
            await cur.execute(query, {"payload": ta.dump_json(data).decode("utf-8")})

        return []

    async def _insert_batch_data_copy_csv(self, data: list[FluxMeasurement]) -> None:
        """
        Bulk insert using CSV copy.
        """
        async with self.cursor() as cur:
            async with cur.copy(f"""
                COPY flux_measurements (
                    {", ".join(data[0].model_dump().keys())}
                )
                FROM STDIN WITH (FORMAT CSV)
                """) as copy:
                copy_buffer = StringIO()
                writer = csv.writer(copy_buffer)
                for row in data:
                    writer.writerow(row.model_dump_sub_as_json().values())
                copy_buffer.seek(0)

                await copy.write(copy_buffer.read())

    async def _insert_batch_data(self, data: dict[str, list]) -> None:
        query = """
            INSERT INTO flux_measurements (
                measurement_id, frequency, module, source_id, time, ra,
                dec, ra_uncertainty, dec_uncertainty, flux, flux_err, extra
            ) SELECT * FROM UNNEST(
                %(measurement_id)s::uuid[],
                %(frequency)s::integer[],
                %(module)s::text[],
                %(source_id)s::uuid[],
                %(time)s::timestamptz[],
                %(ra)s::real[],
                %(dec)s::real[],
                %(ra_uncertainty)s::real[],
                %(dec_uncertainty)s::real[],
                %(flux)s::real[],
                %(flux_err)s::real[],
                %(extra)s::jsonb[]
            )
        """

        async with self.cursor() as cur:
            await cur.execute(query, data)

    async def ingest_dataframe(
        self,
        parquet_bytes: BytesIO,
        parquet_ingest_mode: Literal["csv", "duckdb"] | None = None,
    ) -> None:
        """
        Insert a dataframe into flux measurements via parquet.
        """

        parquet_ingest_mode = parquet_ingest_mode or settings.parquet_ingest_mode

        if parquet_ingest_mode == "duckdb":
            return await self._ingest_dataframe_duckdb(parquet_bytes=parquet_bytes)
        else:
            return await self._ingest_dataframe_csv(parquet_bytes=parquet_bytes)

    async def _ingest_dataframe_csv(self, parquet_bytes: BytesIO) -> None:
        """
        Bulk insert from a DataFrame, usually a transferred Parquet file.
        """
        import pyarrow as pa
        import pyarrow.csv as pc
        import pyarrow.parquet as pq

        table = pq.read_table(parquet_bytes)

        for name in table.schema.names:
            if name in ("measurement_id", "source_id"):
                # This is temporary
                string_list = [str(uuid.as_py()) for uuid in table[name]]
                table = table.set_column(
                    table.schema.get_field_index(name), name, pa.array(string_list)
                )

        async with self.cursor() as cur:
            async with cur.copy(f"""
                COPY flux_measurements (
                    {", ".join(table.column_names)}
                )
                FROM STDIN WITH (FORMAT CSV)
                """) as copy:
                for batch in table.to_batches(max_chunksize=128000):
                    sink = BytesIO()
                    pc.write_csv(
                        batch, sink, write_options=pc.WriteOptions(include_header=False)
                    )
                    sink.seek(0)

                    # async write into COPY stream
                    await copy.write(sink.read())

    async def _ingest_dataframe_duckdb(self, parquet_bytes: BytesIO) -> None:
        import duckdb
        import pyarrow.parquet as pq

        table = pq.read_table(parquet_bytes)

        con = duckdb.connect()  # ONE connection

        # register arrow table
        con.register("temp_flux_measurements_source", table)

        # load postgres extension
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")

        # attach postgres
        con.execute(f"""
            ATTACH '{self.pool.conninfo}' AS pg (TYPE postgres)
        """)

        # insert
        con.execute(f"""
            INSERT INTO pg.flux_measurements (
                    {", ".join(table.column_names)}
            )
            SELECT * FROM temp_flux_measurements_source
        """)

    async def get(self, measurement_id: UUID) -> FluxMeasurement:
        """
        Get a flux measurement by ID.
        """
        query = """
            SELECT *
            FROM flux_measurements
            WHERE measurement_id = %(measurement_id)s
        """

        async with self.cursor(row_factory=class_row(FluxMeasurement)) as cur:
            await cur.execute(query, {"measurement_id": measurement_id})
            row = await cur.fetchone()
            if row is None:
                from lightcurvedb.models.exceptions import (
                    FluxMeasurementNotFoundException,
                )

                raise FluxMeasurementNotFoundException(
                    f"FluxMeasurement {measurement_id} not found"
                )
            return row

    async def delete(self, measurement_id: UUID) -> None:
        """
        Delete a flux measurement by ID.
        """
        query = (
            "DELETE FROM flux_measurements WHERE measurement_id = %(measurement_id)s"
        )

        async with self.cursor() as cur:
            await cur.execute(query, {"measurement_id": measurement_id})
