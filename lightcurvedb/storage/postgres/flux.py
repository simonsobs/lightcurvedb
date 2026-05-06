"""
PostgreSQL implementation of FluxMeasurementStorage protocol.
"""

import json
from collections import defaultdict
from io import BytesIO, StringIO
from uuid import UUID

import pandas as pd
from psycopg.rows import class_row

from lightcurvedb.models.flux import FluxMeasurement, FluxMeasurementCreate
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

    async def create(self, measurement: FluxMeasurementCreate) -> UUID:
        """
        Insert single measurement.
        """
        query = """
            INSERT INTO flux_measurements (
                frequency, module, source_id, time, ra, dec,
                ra_uncertainty, dec_uncertainty,
                flux, flux_err, extra
            )
            VALUES (
                %(frequency)s, %(module)s, %(source_id)s, %(time)s,
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
        self, measurements: list[FluxMeasurementCreate]
    ) -> list[UUID]:
        """
        Bulk insert
        """
        data = defaultdict(list)

        for measurement in measurements:
            measurement_dict = measurement.model_dump()
            if measurement_dict["extra"] is not None:
                measurement_dict["extra"] = json.dumps(measurement_dict["extra"])
            for key, value in measurement_dict.items():
                data[key].append(value)

        return await self._insert_batch_data(data)

    async def _insert_batch_data_copy(self, data: dict[str, list]) -> list[UUID]:
        """
        Alternative bulk insert using COPY. This is not currently used because COPY doesn't support RETURNING, so we can't get the inserted measurement IDs.
        """
        import csv

        async with self.cursor() as cur:
            async with cur.copy(
                """
                COPY flux_measurements (
                    measurement_id, frequency, module, source_id, time, ra, dec,
                    ra_uncertainty, dec_uncertainty, flux, flux_err, extra
                )
                FROM STDIN WITH (FORMAT CSV)
                """
            ) as copy:
                copy_buffer = StringIO()
                writer = csv.writer(copy_buffer)
                for row in zip(
                    data["frequency"],
                    data["module"],
                    data["source_id"],
                    data["time"],
                    data["ra"],
                    data["dec"],
                    data["ra_uncertainty"],
                    data["dec_uncertainty"],
                    data["flux"],
                    data["flux_err"],
                    data["extra"],
                ):
                    writer.writerow(row)
                copy_buffer.seek(0)

                await copy.write(copy_buffer.read())

        return []

    async def _insert_batch_data(self, data: dict[str, list]) -> list[UUID]:
        query = """
            INSERT INTO flux_measurements (measurement_id, frequency, module, source_id, time, ra,
            dec, ra_uncertainty, dec_uncertainty, flux, flux_err, extra)
            SELECT * 
            FROM UNNEST(
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

        return []

    def _parse_source_id(self, source_id: object) -> UUID:
        if isinstance(source_id, UUID):
            return source_id
        if isinstance(source_id, (bytes, bytearray, memoryview)):
            return UUID(bytes=bytes(source_id))
        return UUID(str(source_id))

    async def ingest_dataframe_csv(self, parquet_bytes: BytesIO) -> list[UUID]:
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
            async with cur.copy(
                """
                COPY flux_measurements (
                    measurement_id, frequency, module, source_id, time, ra, dec,
                    ra_uncertainty, dec_uncertainty, flux, flux_err
                )
                FROM STDIN WITH (FORMAT CSV)
                """
            ) as copy:
                for batch in table.to_batches(max_chunksize=128000):
                    sink = BytesIO()
                    pc.write_csv(
                        batch, sink, write_options=pc.WriteOptions(include_header=False)
                    )
                    sink.seek(0)

                    # async write into COPY stream
                    await copy.write(sink.read())

        return []

    async def ingest_parquet_duckdb(self, parquet_bytes: BytesIO) -> list[UUID]:
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
        con.execute("""
            INSERT INTO pg.flux_measurements (
                measurement_id, frequency, module, source_id, time, ra,
                dec, ra_uncertainty, dec_uncertainty, flux, flux_err
            )
            SELECT * FROM temp_flux_measurements_source
        """)

        return []

    async def ingest_dataframe(self, df: pd.DataFrame) -> list[UUID]:
        """
        Bulk insert from a DataFrame, usually a transferred Parquet file.
        """
        extra_series = (
            df["extra"]
            if "extra" in df.columns
            else pd.Series([None] * len(df), index=df.index)
        )

        data: dict[str, list] = {
            "frequency": df["frequency"].tolist(),
            "module": df["module"].tolist(),
            "source_id": [self._parse_source_id(value) for value in df["source_id"]],
            "time": df["time"].tolist(),
            "ra": df["ra"].tolist(),
            "dec": df["dec"].tolist(),
            "ra_uncertainty": [
                None if pd.isna(value) else value for value in df["ra_uncertainty"]
            ],
            "dec_uncertainty": [
                None if pd.isna(value) else value for value in df["dec_uncertainty"]
            ],
            "flux": df["flux"].tolist(),
            "flux_err": df["flux_err"].tolist(),
            "extra": [
                None if pd.isna(value) else json.dumps(value) for value in extra_series
            ],
        }

        return await self._insert_batch_data(data)

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
