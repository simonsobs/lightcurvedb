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