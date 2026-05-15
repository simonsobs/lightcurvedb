"""
Tests DataFrame ingestion for flux measurements.
"""

import datetime
from io import BytesIO

import pandas as pd
import pytest
from pytest_asyncio import fixture as async_fixture
from uuid_extensions import uuid7

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
            "measurement_id": uuid7(),
            "frequency": frequency,
            "module": module,
            "source_id": source_id,
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

    file = BytesIO()
    df = pd.DataFrame(rows)
    df.to_parquet(file, index=False)
    file.seek(0)

    return {
        "source_id": source_id,
        "module": module,
        "frequency": frequency,
        "rows": rows,
        "file": file,
    }


@pytest.mark.parametrize("parquet_ingest_mode", ["csv", "duckdb", None])
@pytest.mark.asyncio(loop_scope="session")
async def test_ingest_dataframe(
    backend: Backend, sample_flux_dataframe, parquet_ingest_mode
):
    payload = sample_flux_dataframe

    await backend.fluxes.ingest_dataframe(
        parquet_bytes=payload["file"], parquet_ingest_mode=parquet_ingest_mode
    )
    payload["file"].seek(0)

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

    for measurement in inserted:
        await backend.fluxes.delete(measurement_id=measurement.measurement_id)
