"""
Deterministic fixtures for unassigned-source workflows.

The fixtures deliberately represent several outcomes that a cross-matching
application must distinguish.  Their reference names are fixture provenance,
not a result of a live catalogue query.
"""

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

from uuid_extensions import uuid7

from lightcurvedb.models import (
    Instrument,
    UnassignedFluxMeasurement,
    UnassignedMeasurementMetadata,
    UnassignedSource,
    UnassignedSourceMetadata,
)
from lightcurvedb.storage.prototype.backend import Backend


@dataclass(frozen=True)
class UnassignedSourceScenario:
    """Parameters for one reproducible unassigned-source fixture."""

    name: str
    ra: float
    dec: float
    base_flux: float
    spectral_index: float
    flags: list[str]
    reference_name: str | None = None
    ra_rate: float = 0.0
    dec_rate: float = 0.0


# RA values follow LightcurveDB's [-180, 180) convention.  Coordinates for
# 3C 273 and M31 are from SIMBAD's ICRS J2000 entries and are used only as
# stable fixture provenance.
_REQUIRED_SCENARIOS = (
    UnassignedSourceScenario(
        name="3c273-external-match",
        ra=-172.722084,
        dec=2.052388,
        base_flux=3.0,
        spectral_index=-0.7,
        flags=["simulation", "expected_external_match", "fixed"],
        reference_name="3C 273",
    ),
    UnassignedSourceScenario(
        name="m31-external-match",
        ra=10.685000,
        dec=41.269000,
        base_flux=1.8,
        spectral_index=-0.5,
        flags=["simulation", "expected_external_match", "fixed"],
        reference_name="M 31",
    ),
    UnassignedSourceScenario(
        name="moving-unmatched",
        ra=179.995000,
        dec=-12.500000,
        base_flux=0.7,
        spectral_index=0.2,
        flags=["simulation", "expected_no_match", "moving"],
        ra_rate=0.003,
        dec_rate=0.001,
    ),
    UnassignedSourceScenario(
        name="merge-primary",
        ra=74.120000,
        dec=-33.800000,
        base_flux=1.1,
        spectral_index=-0.3,
        flags=["simulation", "expected_merge", "fixed"],
    ),
    UnassignedSourceScenario(
        name="merge-secondary",
        ra=74.120900,
        dec=-33.799400,
        base_flux=1.0,
        spectral_index=-0.3,
        flags=["simulation", "expected_merge", "fixed"],
    ),
    UnassignedSourceScenario(
        name="fixed-unmatched",
        ra=-121.750000,
        dec=55.250000,
        base_flux=0.4,
        spectral_index=0.6,
        flags=["simulation", "expected_no_match", "fixed"],
    ),
)


def _normalize_ra(ra: float) -> float:
    """Normalize an angle to LightcurveDB's [-180, 180) RA convention."""
    return (ra + 180.0) % 360.0 - 180.0


def _additional_scenarios(number: int, seed: int) -> list[UnassignedSourceScenario]:
    """Create reproducible, deliberately unmatched fixtures beyond the core set."""
    rng = random.Random(seed)
    return [
        UnassignedSourceScenario(
            name=f"fixed-unmatched-{index:03d}",
            ra=rng.uniform(-179.0, 179.0),
            dec=rng.uniform(-75.0, 75.0),
            base_flux=rng.uniform(0.2, 1.5),
            spectral_index=rng.uniform(-1.2, 1.2),
            flags=["simulation", "expected_no_match", "fixed"],
        )
        for index in range(number)
    ]


def build_unassigned_source_fixtures(
    instruments: list[Instrument],
    start_time: datetime,
    cadence: timedelta,
    measurements_per_source: int,
    source_count: int = len(_REQUIRED_SCENARIOS),
    seed: int = 20260722,
) -> tuple[list[UnassignedSource], list[UnassignedFluxMeasurement]]:
    """Build reproducible unassigned-source and measurement fixtures.

    At least six sources are required so every review scenario is represented:
    likely catalogue matches, fixed no-match sources, a moving source, and a
    close pair intended for a merge review.
    """
    if source_count < len(_REQUIRED_SCENARIOS):
        raise ValueError(
            f"source_count must be at least {len(_REQUIRED_SCENARIOS)} to include "
            "all required scenarios"
        )
    if measurements_per_source < 1:
        raise ValueError("measurements_per_source must be positive")
    if cadence <= timedelta(0):
        raise ValueError("cadence must be positive")
    if not instruments:
        raise ValueError("at least one instrument is required")

    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    else:
        start_time = start_time.astimezone(timezone.utc)

    scenarios = [
        *_REQUIRED_SCENARIOS,
        *_additional_scenarios(source_count - len(_REQUIRED_SCENARIOS), seed),
    ]
    rng = random.Random(seed)
    sources: list[UnassignedSource] = []
    measurements: list[UnassignedFluxMeasurement] = []

    for scenario in scenarios:
        source_id = uuid7()
        last_seen = start_time + (measurements_per_source - 1) * cadence
        sources.append(
            UnassignedSource(
                source_id=source_id,
                ra=scenario.ra,
                dec=scenario.dec,
                first_seen=start_time,
                last_seen=last_seen,
                extra=UnassignedSourceMetadata(
                    flags=scenario.flags,
                    simulation_scenario=scenario.name,
                    reference_name=scenario.reference_name,
                    simulation_seed=seed,
                ),
            )
        )

        for measurement_index in range(measurements_per_source):
            time = start_time + measurement_index * cadence
            ra = _normalize_ra(scenario.ra + scenario.ra_rate * measurement_index)
            dec = max(
                -90.0,
                min(90.0, scenario.dec + scenario.dec_rate * measurement_index),
            )
            modulation = 1.0 + 0.08 * math.sin(measurement_index / 3.0)

            for instrument in instruments:
                spectral_flux = (
                    scenario.base_flux
                    * (instrument.frequency / instruments[0].frequency)
                    ** scenario.spectral_index
                )
                flux = max(0.01, spectral_flux * modulation + rng.gauss(0.0, 0.02))
                flux_err = max(0.01, flux * 0.05 + rng.uniform(0.005, 0.02))
                measurements.append(
                    UnassignedFluxMeasurement(
                        measurement_id=uuid7(),
                        source_id=source_id,
                        frequency=instrument.frequency,
                        module=instrument.module,
                        time=time,
                        ra=ra,
                        dec=dec,
                        ra_uncertainty=rng.uniform(0.0001, 0.002),
                        dec_uncertainty=rng.uniform(0.0001, 0.002),
                        flux=flux,
                        flux_err=flux_err,
                        extra=UnassignedMeasurementMetadata(
                            flags=scenario.flags,
                            map_id=f"sim-{scenario.name}-{measurement_index:04d}",
                            simulation_scenario=scenario.name,
                        ),
                    )
                )

    return sources, measurements


async def create_unassigned_source_fixtures(
    backend: Backend,
    instruments: list[Instrument],
    start_time: datetime,
    cadence: timedelta,
    measurements_per_source: int,
    source_count: int = len(_REQUIRED_SCENARIOS),
    seed: int = 20260722,
) -> list[UUID]:
    """Persist unassigned-source fixtures through the backend."""
    sources, measurements = build_unassigned_source_fixtures(
        instruments=instruments,
        start_time=start_time,
        cadence=cadence,
        measurements_per_source=measurements_per_source,
        source_count=source_count,
        seed=seed,
    )

    for source in sources:
        await backend.unassigned_sources.create(source)
    for measurement in measurements:
        await backend.unassigned_fluxes.create(measurement)

    return [source.source_id for source in sources]
