"""
Tools for creating simulated light-curves via flux depositions into the database.
"""

import random
from datetime import datetime, timedelta

import numpy as np

from lightcurvedb.models.instrument import Instrument
from lightcurvedb.models.flux import FluxMeasurementCreate, MeasurementMetadata
from lightcurvedb.models.source import Source
from lightcurvedb.storage.prototype.backend import Backend


def generate_fluxes_fixed_source_core(
    start_time: datetime,
    cadence: timedelta,
    number: int,
    bands: list[float],
    probability_of_flare: float = 0.1,
    peak_flux: float = 5.0,
    peak_flux_band_index: int = 0,
    flare_duration: timedelta = timedelta(days=10),
    noise_floor: float = 0.1,
    spectral_index_range: tuple[float, float] = (-2.0, 2.0),
):
    times = np.array([start_time + i * cadence for i in range(number)])
    flare_index = random.randint(0, int(number / probability_of_flare))
    flare_time = start_time + flare_index * cadence

    fluxes = [np.random.rand(number) * np.sqrt(noise_floor) + noise_floor] * len(bands)

    if flare_index < (number + flare_duration / cadence * 3):
        # We need to actually generate flare info.
        fluxes[peak_flux_band_index] += peak_flux * np.exp(
            -(((times - flare_time) / flare_duration) ** 2).astype(np.float32)
        )

        spectral_index = random.uniform(*spectral_index_range)
        for index, band in enumerate(bands):
            if index == peak_flux_band_index:
                continue

            fluxes[index] = (
                fluxes[peak_flux_band_index]
                * (band.frequency / bands[peak_flux_band_index].frequency)
                ** spectral_index
            )

    return times, fluxes


async def generate_fluxes_fixed_source(
    source: Source,
    bands: list[Instrument],
    backend: Backend,
    start_time: datetime,
    cadence: timedelta,
    number: int,
    probability_of_flare: float = 0.1,
    peak_flux: float = 5.0,
    peak_flux_band_index: int = 0,
    flare_duration: timedelta = timedelta(days=10),
    noise_floor: float = 0.1,
    spectral_index_range: tuple[float, float] = (-2.0, 2.0),
) -> list[int]:
    """
    Generate fluxes for a fixed source (i.e. we only need to care about time
    variability and the chance it produces a gaussian flare).

    Parameters
    ----------
    source : Source
        Source domain model (has id, ra, dec)
    bands : list[Band]
        List of band domain models
    backend : Backend
        Storage backend from factory
    start_time : datetime
        The start time of the light-curve.
    cadence : timedelta
        The cadence of the light-curve.
    number : int
        The number of fluxes to generate.
    probability_of_flare : float
        The probability of a flare occurring in the entire time range.
    peak_flux : float
        The peak flux of the flare.
    peak_flux_band_index : int
        The band that the peak flux should be deposited in.
    flare_duration : timedelta
        The duration of the flare.
    noise_floor : float
        The noise floor for the fluxes.
    spectral_index_range : tuple[float, float]
        The range of spectral indices to draw from.

    Returns
    -------
    list[int]
        The IDs of the created fluxes.
    """

    times, fluxes = generate_fluxes_fixed_source_core(
        start_time=start_time,
        cadence=cadence,
        number=number,
        bands=bands,
        probability_of_flare=probability_of_flare,
        peak_flux=peak_flux,
        peak_flux_band_index=peak_flux_band_index,
        flare_duration=flare_duration,
        noise_floor=noise_floor,
        spectral_index_range=spectral_index_range,
    )

    all_measurements = []

    for flux_values, band in zip(fluxes, bands):
        if random.random() < 0.1:
            metadata = MeasurementMetadata(flags=["test_flag"])
        else:
            metadata = None

        measurements = [
            FluxMeasurementCreate(
                frequency=band.frequency,
                module=band.module,
                source_id=source.source_id,
                time=times[i],
                ra=source.ra,
                dec=source.dec,
                ra_uncertainty=random.random(),
                dec_uncertainty=random.random(),
                i_flux=flux_values[i],
                i_uncertainty=noise_floor,
                extra=metadata,
            )
            for i in range(number)
        ]
        all_measurements.extend(measurements)

    return await backend.fluxes.create_batch(all_measurements)
