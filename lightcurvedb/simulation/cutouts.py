"""
Simulates cut-outs around sources, based upon the flux measurements.
"""

import numpy as np
from sqlmodel import Session

from ..models import CutoutTable, FluxMeasurementTable


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
    flux: FluxMeasurementTable,
    session: Session,
):
    cutout = create_cutout_core(nside, flux.i_flux, flux.i_uncertainty)

    cutout_table = CutoutTable(
        band=flux.band,
        band_name=flux.band.name,
        flux=flux,
        flux_id=flux.id,
        time=flux.time,
        data=cutout.tolist(),
        units="mJy",
    )

    session.add(cutout_table)
    session.commit()
