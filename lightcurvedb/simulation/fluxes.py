"""
Tools for creating simulated light-curves via flux depositions into the database.
"""

import math
import random
from datetime import datetime, timedelta

from ..models import BandTable, FluxMeasurementTable, SourceTable
from sqlmodel import Session


def generate_fluxes_fixed_source(
    source: SourceTable,
    bands: list[BandTable],
    start_time: datetime,
    cadence: timedelta,
    number: int,
    session: Session,
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
    source_id : int
        The ID of the source to generate fluxes for.
    bands : list[str]
        The bands to generate fluxes in.
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

    times = [start_time + i * cadence for i in range(number)]
    flare_index = random.randint(0, int(number / probability_of_flare))
    flare_time = start_time + flare_index * cadence

    fluxes = [[0.0] * number] * len(bands)

    if flare_index < (number + flare_duration / cadence * 3):
        # We need to actually generate flare info.
        fluxes[peak_flux_band_index] = [
            peak_flux * math.exp(-(((t - flare_time) / flare_duration) ** 2)) for t in times
        ]

        spectral_index = random.uniform(*spectral_index_range)
        for index, band in enumerate(bands):
            if index == peak_flux_band_index:
                continue

            fluxes[index] = [
                f
                * (band.frequency / bands[peak_flux_band_index].frequency)
                ** spectral_index
                for f in fluxes[peak_flux_band_index]
            ]

    # Now actually generate the objects:
    band_fluxes = []

    for index, band in enumerate(bands):
        band_fluxes += [
            FluxMeasurementTable(
                band=band,
                time=times[i],
                flux=max(
                    fluxes[index][i]
                    + noise_floor
                    + math.sqrt(noise_floor) * random.gauss(0.0, 1.0),
                    0.0,
                ),
                error=math.sqrt(noise_floor),
                source=source,
                source_id=source.id,
                band_name=band.name,
            )
            for i in range(number)
        ]

    session.add_all(band_fluxes)
    session.commit()

    flux_ids = [flux.id for flux in band_fluxes]

    return flux_ids