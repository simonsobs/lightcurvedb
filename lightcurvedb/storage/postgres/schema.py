"""
PostgreSQL schema for Flux Measurement Table
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
