"""
Database-side analysis functions for lightcurve data.
"""


from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from lightcurvedb.models.analysis import BandStatistics


def _time_conditions(start_time: datetime | None, end_time: datetime | None) -> str:
    """
    Time filter conditions
    """
    conditions = []
    if start_time:
        conditions.append("bucket >= :start_time")
    if end_time:
        conditions.append("bucket <= :end_time")
    
    return " AND " + " AND ".join(conditions) if conditions else ""


async def get_band_statistics(
    source_id: int,
    band_name: str,
    conn: AsyncSession,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> BandStatistics:
    """
    Calculates statistical values for a given source and band over a specified time range.
    """
    time_filter = _time_conditions(start_time, end_time)
    
    query = text(f"""
        SELECT
            SUM(band_statistics_weekly.sum_flux) / SUM(band_statistics_weekly.data_points) as mean_flux,
            MIN(band_statistics_weekly.min_flux) as min_flux,
            MAX(band_statistics_weekly.max_flux) as max_flux,
            SUM(band_statistics_weekly.data_points) as data_points
        FROM band_statistics_weekly
        WHERE source_id = :source_id AND band_name = :band_name
        {time_filter}
    """)

    params = {
        "source_id": source_id,
        "band_name": band_name,
    }
    
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time

    result = await conn.execute(query, params)
    row = result.first()
    print(row)
    print(row.data_points)
    if not row or row.data_points == 0:
        return BandStatistics(
            mean_flux=None, min_flux=None, max_flux=None,
            std_flux=None, median_flux=None, data_points=0
        )

    return BandStatistics(**row._asdict())

async def get_band_statistics_wo_ca(
    source_id: int,
    band_name: str,
    conn: AsyncSession,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> BandStatistics:
    """
    Calculates statistical values for a given source and band over a specified time range.
    """
    time_filter = _time_conditions(start_time, end_time)
    
    query = text(f"""
        SELECT
            AVG(i_flux) as mean_flux,
            MIN(i_flux) as min_flux,
            MAX(i_flux) as max_flux,
            COUNT(*) as data_points
        FROM flux_measurements
        WHERE source_id = :source_id AND band_name = :band_name
        {time_filter}
    """)

    params = {
        "source_id": source_id,
        "band_name": band_name,
    }
    
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time

    result = await conn.execute(query, params)
    row = result.first()
    print(row)
    print(row.data_points)
    if not row or row.data_points == 0:
        return BandStatistics(
            mean_flux=None, min_flux=None, max_flux=None,
            std_flux=None, median_flux=None, data_points=0
        )

    return BandStatistics(**row._asdict())

