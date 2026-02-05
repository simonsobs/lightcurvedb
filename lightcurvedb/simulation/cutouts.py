"""
Simulates cut-outs around sources, based upon the flux measurements.
"""

import numpy as np

from ..models import Cutout, FluxMeasurement


def create_cutout_core(
    nside: int,
    flux: float,
    error: float,
) -> np.array:
    """
    Create a cut-out around a source.

    Parameters
    ----------
    nside : int
        The size of the cut-out.
    flux : float
        The flux of the source.
    error : float
        The error on the flux.

    Returns
    -------
    np.array
        The cut-out.
    """
    out = np.random.normal(error * error, np.sqrt(error), size=(nside, nside))
    # Use an extended source otherwise it's impossible to see.
    xs, ys = np.meshgrid(
        np.linspace(-3, 3, nside),
        np.linspace(-3, 3, nside),
    )
    out += flux * np.exp(-(np.sqrt(xs**2 + ys**2) ** 2))

    return out


def create_cutout(
    nside: int,
    flux: FluxMeasurement,
):
    cutout = create_cutout_core(nside, flux.i_flux, flux.i_uncertainty)

    if flux.id is None:
        raise ValueError("FluxMeasurement must have an ID to create a cutout.")

    return Cutout(
        data=cutout.tolist(),
        time=flux.time,
        units="mJy",
        source_id=flux.source_id,
        band_name=flux.band_name,
        flux_id=flux.id,
    )
