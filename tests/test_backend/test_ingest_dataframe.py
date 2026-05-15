"""
Tests DataFrame ingestion for flux measurements.
"""

import datetime
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pytest_asyncio import fixture as async_fixture

from lightcurvedb.storage.prototype.backend import Backend


@async_fixture
async def sample_flux_dataframe(backend: Backend, setup_test_data):
    source_id = setup_test_data[0]

    module_frequency_pairs = (
        await backend.lightcurves.get_module_frequency_pairs_for_source(source_id)
    )
    module, frequency = module_frequency_pairs[0]

    base_time = datetime.datetime(2035, 1, 1, tzinfo=datetime.timezone.utc)

    rows = [
        {
            "frequency": frequency,
            "module": module,
            "source_id": source_id.bytes,
            "time": base_time + datetime.timedelta(minutes=index),
            "ra": 10.0 + index,
            "dec": -20.0 - index,
            "ra_uncertainty": 0.01 + (index * 0.001),
            "dec_uncertainty": 0.02 + (index * 0.001),
            "flux": 100.0 + index,
            "flux_err": 2.5 + index,
        }
        for index in range(3)
    ]

    return {
        "source_id": source_id,
        "module": module,
        "frequency": frequency,
        "rows": rows,
        "df": pd.DataFrame(rows),
    }


@pytest.mark.asyncio(loop_scope="session")
async def test_ingest_dataframe(backend: Backend, sample_flux_dataframe):
    payload = sample_flux_dataframe

    await backend.fluxes.ingest_dataframe(payload["df"])

    lightcurve = await backend.lightcurves.get_instrument_lightcurve(
        source_id=payload["source_id"],
        module=payload["module"],
        frequency=payload["frequency"],
    )

    expected_by_time = {row["time"]: row for row in payload["rows"]}
    inserted = [
        measurement
        for measurement in lightcurve
        if measurement.time in expected_by_time
    ]

    assert len(inserted) == len(payload["rows"])

    for measurement in inserted:
        expected = expected_by_time[measurement.time]

        assert measurement.source_id == payload["source_id"]
        assert measurement.module == payload["module"]
        assert measurement.frequency == payload["frequency"]
        assert measurement.ra == expected["ra"]
        assert measurement.dec == expected["dec"]
        assert measurement.flux == expected["flux"]
        assert measurement.flux_err == expected["flux_err"]


@pytest.mark.asyncio(loop_scope="session")
async def test_ingest_parquet(backend: Backend, sample_flux_dataframe):
    payload = sample_flux_dataframe

    parquet_buffer = BytesIO()
    table = pa.Table.from_pandas(payload["df"], preserve_index=False)
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)

    await backend.fluxes.ingest_parquet(parquet_buffer)

    lightcurve = await backend.lightcurves.get_instrument_lightcurve(
        source_id=payload["source_id"],
        module=payload["module"],
        frequency=payload["frequency"],
    )

    expected_by_time = {row["time"]: row for row in payload["rows"]}
    inserted = [
        measurement
        for measurement in lightcurve
        if measurement.time in expected_by_time
    ]

    assert len(inserted) == len(payload["rows"])

    for measurement in inserted:
        expected = expected_by_time[measurement.time]

        assert measurement.source_id == payload["source_id"]
        assert measurement.module == payload["module"]
        assert measurement.frequency == payload["frequency"]
        assert measurement.ra == expected["ra"]
        assert measurement.dec == expected["dec"]
        assert measurement.flux == expected["flux"]
        assert measurement.flux_err == expected["flux_err"]
