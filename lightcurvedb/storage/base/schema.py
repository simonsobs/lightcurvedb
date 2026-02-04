"""
Shared SQL schema definitions.
"""

SOURCES_TABLE = """
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name TEXT,
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
    name TEXT PRIMARY KEY,
    telescope TEXT NOT NULL,
    instrument TEXT NOT NULL,
    frequency DOUBLE PRECISION NOT NULL
);
"""
