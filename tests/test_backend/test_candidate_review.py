"""Unassigned-source review workflow coverage for every backend."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from lightcurvedb.models import (
    CandidateReviewConflictError,
    UnassignedFluxMeasurement,
    UnassignedSource,
)
from lightcurvedb.models.review import CandidateDecisionCommand, CandidateMergeCommand


async def _create_candidate(backend, *, source_id, now, instrument, offset_minutes=0):
    source = UnassignedSource(
        source_id=source_id,
        ra=12.5,
        dec=-34.5,
        first_seen=now,
        last_seen=now,
    )
    await backend.unassigned_sources.create(source)
    measurement = UnassignedFluxMeasurement(
        measurement_id=uuid4(),
        source_id=source_id,
        frequency=instrument.frequency,
        module=instrument.module,
        time=now + timedelta(minutes=offset_minutes),
        ra=source.ra,
        dec=source.dec,
        ra_uncertainty=0.01,
        dec_uncertainty=0.01,
        flux=1.5,
        flux_err=0.2,
    )
    await backend.unassigned_fluxes.create(measurement)
    return source, measurement


@pytest.mark.asyncio(loop_scope="session")
async def test_merge_reparents_measurements_and_records_metadata(backend):
    now = datetime.now(UTC)
    instrument = (await backend.instruments.get_all())[0]
    source, measurement = await _create_candidate(
        backend,
        source_id=uuid4(),
        now=now,
        instrument=instrument,
    )
    target, target_measurement = await _create_candidate(
        backend,
        source_id=uuid4(),
        now=now + timedelta(days=1),
        instrument=instrument,
        offset_minutes=1,
    )
    command = CandidateMergeCommand(
        source_id=source.source_id,
        target_source_id=target.source_id,
        expected_version=1,
        reviewer="test-reviewer",
        reason="same object",
    )

    merged = await backend.unassigned_sources.merge(command)

    assert merged.source_id == source.source_id
    reviewed_source = await backend.unassigned_sources.get(source.source_id)
    assert reviewed_source.status == "merged"
    assert reviewed_source.reviewed_by == "test-reviewer"
    assert reviewed_source.review_metadata == {
        "target_source_id": str(target.source_id),
        "reason": "same object",
    }
    assert (await backend.unassigned_sources.get(target.source_id)).version == 2
    target_measurements = await backend.unassigned_fluxes.get_for_source(
        target.source_id
    )
    assert {item.measurement_id for item in target_measurements} == {
        measurement.measurement_id,
        target_measurement.measurement_id,
    }
    with pytest.raises(CandidateReviewConflictError):
        await backend.unassigned_sources.merge(command)


@pytest.mark.asyncio(loop_scope="session")
async def test_terminal_decisions_materialize_directly_and_retain_metadata(backend):
    now = datetime.now(UTC)
    instrument = (await backend.instruments.get_all())[0]
    noise_source, _ = await _create_candidate(
        backend,
        source_id=uuid4(),
        now=now,
        instrument=instrument,
    )
    noise = await backend.unassigned_sources.decide(
        CandidateDecisionCommand(
            source_id=noise_source.source_id,
            expected_version=1,
            outcome="noise",
            reviewer="test-reviewer",
        )
    )
    source, measurement = await _create_candidate(
        backend,
        source_id=uuid4(),
        now=now + timedelta(days=1),
        instrument=instrument,
    )
    accepted = await backend.unassigned_sources.decide(
        CandidateDecisionCommand(
            source_id=source.source_id,
            expected_version=1,
            outcome="novel",
            reviewer="test-reviewer",
            novel_name="CrossMatcher test source",
        )
    )

    assert noise.canonical_source_id is None
    assert (
        await backend.unassigned_sources.get(noise_source.source_id)
    ).status == "noise"
    reviewed_source = await backend.unassigned_sources.get(source.source_id)
    assert reviewed_source.status == "novel"
    assert reviewed_source.reviewed_by == "test-reviewer"
    assert reviewed_source.review_metadata["canonical_source_id"] == str(
        accepted.canonical_source_id
    )
    canonical_source = await backend.sources.get(accepted.canonical_source_id)
    assert canonical_source.name == "CrossMatcher test source"
    canonical_measurement = await backend.fluxes.get(measurement.measurement_id)
    assert canonical_measurement.source_id == canonical_source.source_id


@pytest.mark.asyncio(loop_scope="session")
async def test_terminal_decision_allows_an_empty_candidate(backend):
    now = datetime.now(UTC)
    source = UnassignedSource(
        source_id=uuid4(),
        ra=12.5,
        dec=-34.5,
        first_seen=now,
        last_seen=now,
    )
    await backend.unassigned_sources.create(source)

    decision = await backend.unassigned_sources.decide(
        CandidateDecisionCommand(
            source_id=source.source_id,
            expected_version=1,
            outcome="novel",
            reviewer="test-reviewer",
            novel_name="Empty candidate target",
        )
    )

    assert decision.canonical_source_id is not None
    assert (await backend.unassigned_sources.get(source.source_id)).status == "novel"
