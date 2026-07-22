LightcurveDB
============

Python tools for managing data in the Lightcurve Database. This database stores
fluxes as a function of time for tracked objects. This package includes simulation
tools to fill the database with test data.

Installation - first download the git repository, then

```
uv pip install -e .
```

You can spin up an ephemeral instance of the database with the

```
lightcurvedb-ephemeral
```

tool. This uses `testcontainers`. It will print connection information.

The ephemeral command also creates deterministic fixtures for unassigned-source
cross-matching. The default set includes fixed catalogue-like and unmatched
sources, a moving source, and a nearby pair for merge review. The fixture count
must be at least six to retain every scenario; use `--unassigned-seed` to
reproduce the simulated source values. Fixture IDs use UUID7. For a fast,
unassigned-only Parquet fixture
database:

```
lightcurvedb-ephemeral --backend parquet --number 0 --unassigned-number 6 \
--unassigned-measurements 10 --unassigned-seed 20260722
```
