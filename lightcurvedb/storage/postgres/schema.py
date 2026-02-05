"""
PostgreSQL schema for Flux Measurement Table
"""

FLUX_MEASUREMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS flux_measurements (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    band_name TEXT REFERENCES bands(name),
    source_id INTEGER REFERENCES sources(id),
    time TIMESTAMPTZ NOT NULL,
    ra REAL NOT NULL CHECK (ra >= -180 AND ra <= 180),
    dec REAL NOT NULL CHECK (dec >= -90 AND dec <= 90),
    ra_uncertainty REAL,
    dec_uncertainty REAL,
    i_flux REAL NOT NULL,
    i_uncertainty REAL,
    extra JSONB
)
"""

FLUX_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_flux_source_band_time
    ON flux_measurements (source_id, band_name, time DESC);

CREATE INDEX IF NOT EXISTS idx_flux_time
    ON flux_measurements (time DESC);
"""

CUTOUT_SCHEMA = """
CREATE TABLE IF NOT EXISTS cutouts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id),
    flux_id BIGINT NOT NULL REFERENCES flux_measurements(id),
    band_name TEXT REFERENCES bands(name),
    cutout_data real[][] NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    units TEXT NOT NULL
)
"""

CUTOUT_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_cutouts_flux
    ON cutouts (flux_id);
"""
