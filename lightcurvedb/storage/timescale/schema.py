FLUX_MEASUREMENTS_TABLE = """
CREATE TABLE flux_measurements (
    measurement_id UUID DEFAULT gen_random_uuid(),

    frequency INTEGER NOT NULL,
    module TEXT NOT NULL,

    source_id UUID REFERENCES sources(source_id),
    time TIMESTAMPTZ NOT NULL,

    ra REAL NOT NULL CHECK (ra >= -180 AND ra <= 180),
    dec REAL NOT NULL CHECK (dec >= -90 AND dec <= 90),
    ra_uncertainty REAL,
    dec_uncertainty REAL,
    
    flux REAL NOT NULL,
    flux_err REAL,
    extra JSONB,

    FOREIGN KEY (frequency, module) REFERENCES instruments(frequency, module),

    PRIMARY KEY (time, source_id, frequency, module)
) with (
  tsdb.hypertable,
  tsdb.segmentby = 'source_id',
  tsdb.orderby = 'time DESC'
);
"""

FLUX_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_flux_measurements_time_source_id
    ON flux_measurements (time DESC, source_id);

CREATE INDEX IF NOT EXISTS idx_flux_measurements_measurement_id
    ON flux_measurements (measurement_id);
"""

CUTOUT_SCHEMA = """
CREATE TABLE IF NOT EXISTS cutouts (
    measurement_id UUID,

    source_id UUID REFERENCES sources(source_id),

    frequency INTEGER NOT NULL,
    module TEXT NOT NULL,

    data real[][] NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    units TEXT NOT NULL,

    FOREIGN KEY (frequency, module) REFERENCES instruments(frequency, module),
    PRIMARY KEY (source_id, frequency, module, time)
)
"""

CUTOUT_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_cutouts_measurement_id
    ON cutouts (measurement_id);
"""


# ---------------------------------------------------------------------------
# Continuous aggregates for pre-computed binned lightcurves.
# Each view buckets flux_measurements by (source_id, frequency, module) and
# pre-computes the same statistics that PostgresLightcurveProvider calculates
# on-the-fly in get_binned_*() queries.
# ---------------------------------------------------------------------------

CONTINUOUS_AGGREGATE_DAILY = """
CREATE MATERIALIZED VIEW IF NOT EXISTS flux_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    source_id,
    frequency,
    module,
    avg(ra)::real AS avg_ra,
    avg(dec)::real AS avg_dec,
    avg(flux)::real AS avg_flux,
    CASE
        WHEN count(flux_err) FILTER (WHERE flux_err IS NOT NULL) > 0
        THEN (sqrt(sum(flux_err ^ 2) FILTER (WHERE flux_err IS NOT NULL))
              / count(flux_err) FILTER (WHERE flux_err IS NOT NULL))::real
        ELSE NULL
    END AS avg_flux_err
FROM flux_measurements
GROUP BY bucket, source_id, frequency, module
WITH NO DATA;
"""

CONTINUOUS_AGGREGATE_WEEKLY = """
CREATE MATERIALIZED VIEW IF NOT EXISTS flux_weekly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('7 days', time) AS bucket,
    source_id,
    frequency,
    module,
    avg(ra)::real AS avg_ra,
    avg(dec)::real AS avg_dec,
    avg(flux)::real AS avg_flux,
    CASE
        WHEN count(flux_err) FILTER (WHERE flux_err IS NOT NULL) > 0
        THEN (sqrt(sum(flux_err ^ 2) FILTER (WHERE flux_err IS NOT NULL))
              / count(flux_err) FILTER (WHERE flux_err IS NOT NULL))::real
        ELSE NULL
    END AS avg_flux_err
FROM flux_measurements
GROUP BY bucket, source_id, frequency, module
WITH NO DATA;
"""

CONTINUOUS_AGGREGATE_MONTHLY = """
CREATE MATERIALIZED VIEW IF NOT EXISTS flux_monthly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 days', time) AS bucket,
    source_id,
    frequency,
    module,
    avg(ra)::real AS avg_ra,
    avg(dec)::real AS avg_dec,
    avg(flux)::real AS avg_flux,
    CASE
        WHEN count(flux_err) FILTER (WHERE flux_err IS NOT NULL) > 0
        THEN (sqrt(sum(flux_err ^ 2) FILTER (WHERE flux_err IS NOT NULL))
              / count(flux_err) FILTER (WHERE flux_err IS NOT NULL))::real
        ELSE NULL
    END AS avg_flux_err
FROM flux_measurements
GROUP BY bucket, source_id, frequency, module
WITH NO DATA;
"""

CONTINUOUS_AGGREGATE_REFRESH_POLICIES = """
SELECT add_continuous_aggregate_policy('flux_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => true
);

SELECT add_continuous_aggregate_policy('flux_weekly',
    start_offset => INTERVAL '21 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => true
);

SELECT add_continuous_aggregate_policy('flux_monthly',
    start_offset => INTERVAL '90 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => true
);
"""

CONTINUOUS_AGGREGATES = [
    CONTINUOUS_AGGREGATE_DAILY,
    CONTINUOUS_AGGREGATE_WEEKLY,
    CONTINUOUS_AGGREGATE_MONTHLY,
    CONTINUOUS_AGGREGATE_REFRESH_POLICIES,
]
