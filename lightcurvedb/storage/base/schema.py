"""
Base SQL schema definitions.
"""



SOURCES_TABLE = """
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    ra DOUBLE PRECISION CHECK (ra IS NULL OR (ra >= -180 AND ra <= 180)),
    dec DOUBLE PRECISION CHECK (dec IS NULL OR (dec >= -90 AND dec <= 90)),
    variable BOOLEAN NOT NULL DEFAULT FALSE,
    extra JSONB
);

CREATE INDEX IF NOT EXISTS idx_sources_name ON sources (name);
CREATE INDEX IF NOT EXISTS idx_sources_position ON sources (ra, dec) WHERE ra IS NOT NULL AND dec IS NOT NULL;
"""

BANDS_TABLE = """
CREATE TABLE IF NOT EXISTS bands (
    name VARCHAR(50) PRIMARY KEY,
    telescope VARCHAR(100) NOT NULL,
    instrument VARCHAR(100) NOT NULL,
    frequency DOUBLE PRECISION NOT NULL
);
"""

FLUX_MEASUREMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS flux_measurements (
    id SERIAL PRIMARY KEY,
    band_name VARCHAR(50) NOT NULL REFERENCES bands(name),
    source_id INTEGER NOT NULL REFERENCES sources(id),
    time TIMESTAMPTZ NOT NULL,
    ra DOUBLE PRECISION NOT NULL CHECK (ra >= -180 AND ra <= 180),
    dec DOUBLE PRECISION NOT NULL CHECK (dec >= -90 AND dec <= 90),
    ra_uncertainty DOUBLE PRECISION,
    dec_uncertainty DOUBLE PRECISION,
    i_flux DOUBLE PRECISION NOT NULL,
    i_uncertainty DOUBLE PRECISION,
    extra JSONB
);

CREATE INDEX IF NOT EXISTS idx_flux_source_band_time
    ON flux_measurements (source_id, band_name, time DESC);

CREATE INDEX IF NOT EXISTS idx_flux_time
    ON flux_measurements (time DESC);
"""

# Create all tables
ALL_TABLES = f"""
{SOURCES_TABLE}
{BANDS_TABLE}
{FLUX_MEASUREMENTS_TABLE}
"""
