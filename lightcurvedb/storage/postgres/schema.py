"""
PostgreSQL schema for Flux Measurement Table
"""


def generate_flux_partitions(count: int) -> str:
    """
    Generate SQL for creating hash partitions.
    """
    statements = []
    for i in range(count):
        statements.append(f"""
CREATE TABLE flux_measurements_p{i} PARTITION OF flux_measurements
    FOR VALUES WITH (MODULUS {count}, REMAINDER {i});
""")

    return "\n".join(statements)


FLUX_MEASUREMENTS_TABLE = """
CREATE TABLE flux_measurements (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    band_name TEXT NOT NULL REFERENCES bands(name),
    source_id INTEGER NOT NULL REFERENCES sources(id),
    time TIMESTAMPTZ NOT NULL,
    ra DOUBLE PRECISION NOT NULL CHECK (ra >= -180 AND ra <= 180),
    dec DOUBLE PRECISION NOT NULL CHECK (dec >= -90 AND dec <= 90),
    ra_uncertainty DOUBLE PRECISION,
    dec_uncertainty DOUBLE PRECISION,
    i_flux DOUBLE PRECISION NOT NULL,
    i_uncertainty DOUBLE PRECISION,
    extra JSONB,
    PRIMARY KEY (source_id, id)
) PARTITION BY HASH (source_id);
"""

FLUX_INDEXES = """
CREATE INDEX idx_flux_source_band_time
    ON flux_measurements (source_id, band_name, time DESC);

CREATE INDEX idx_flux_time
    ON flux_measurements (time DESC);
"""