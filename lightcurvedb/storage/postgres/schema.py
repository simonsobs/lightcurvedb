"""
PostgreSQL schema for Flux Measurement Table
"""

SOURCES_TABLE = """
CREATE TABLE IF NOT EXISTS sources (
    source_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    socat_id INTEGER UNIQUE,
    name TEXT,
    ra DOUBLE PRECISION CHECK (ra IS NULL OR (ra >= -180 AND ra <= 180)),
    dec DOUBLE PRECISION CHECK (dec IS NULL OR (dec >= -90 AND dec <= 90)),
    variable BOOLEAN NOT NULL DEFAULT FALSE,
    extra JSONB
);

CREATE INDEX IF NOT EXISTS idx_sources_name ON sources (name);
CREATE INDEX IF NOT EXISTS idx_sources_socat_id ON sources (socat_id);
CREATE INDEX IF NOT EXISTS idx_sources_position ON sources (ra, dec) WHERE ra IS NOT NULL AND dec IS NOT NULL;
"""

INSTRUMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS instruments (
    frequency INTEGER NOT NULL,
    module TEXT NOT NULL,
    telescope TEXT NOT NULL,
    instrument TEXT NOT NULL,
    details JSONB,
    PRIMARY KEY (frequency, module)
);
"""


FLUX_MEASUREMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS flux_measurements (
    measurement_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

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

    FOREIGN KEY (frequency, module) REFERENCES instruments(frequency, module)
)
"""

FLUX_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_flux_source_id
    ON flux_measurements (source_id);

CREATE INDEX IF NOT EXISTS idx_flux_time
    ON flux_measurements (time DESC);
"""

CUTOUT_SCHEMA = """
CREATE TABLE IF NOT EXISTS cutouts (
    measurement_id UUID PRIMARY KEY REFERENCES flux_measurements(measurement_id),

    source_id UUID REFERENCES sources(source_id),

    frequency INTEGER NOT NULL,
    module TEXT NOT NULL,

    data real[][] NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    units TEXT NOT NULL,

    FOREIGN KEY (frequency, module) REFERENCES instruments(frequency, module)
)
"""

CUTOUT_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_cutouts_measurement_id
    ON cutouts (measurement_id);
"""
