"""
Tests for instrument client interface.
"""

import pytest

from lightcurvedb.models.exceptions import InstrumentNotFoundException
from lightcurvedb.models.instrument import Instrument
from lightcurvedb.storage.prototype.backend import Backend


@pytest.mark.asyncio(loop_scope="session")
async def test_instrument_read_all(backend: Backend):
    instruments = await backend.instruments.get_all()

    assert len(instruments) > 0
    assert isinstance(instruments[0], Instrument)


@pytest.mark.asyncio(loop_scope="session")
async def test_instrument_creation_deletion_flow(backend: Backend):
    instrument = Instrument(
        frequency=105,
        module="uv0",
        telescope="hubble",
        instrument="wfc3",
        details={
            "comissioning_date": "2009-05-14",
        },
    )

    # Add instrument
    instrument_name = await backend.instruments.create(instrument=instrument)
    assert instrument_name == instrument.instrument_name

    # Read instrument back
    read_instrument = await backend.instruments.get(instrument.instrument_name)
    assert read_instrument.instrument_name == instrument.instrument_name
    assert read_instrument.telescope == instrument.telescope
    assert read_instrument.instrument == instrument.instrument
    assert read_instrument.frequency == instrument.frequency

    # Delete instrument
    await backend.instruments.delete(instrument.instrument_name)

    with pytest.raises(InstrumentNotFoundException):
        await backend.instruments.get(instrument.instrument_name)
