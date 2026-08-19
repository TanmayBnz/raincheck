#!/usr/bin/env bash
# ERA5 download. Plain Python, not spark-submit: it is a few HTTP requests plus a
# ~76 MB netCDF reshape. Requires ~/.cdsapirc AND manual acceptance of the ERA5
# licence at https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
set -euo pipefail
cd "$(dirname "$0")/.."
# Pin the JVM system timezone. Spark SQL semantics already follow
# spark.sql.session.timeZone=UTC, but naive Python datetimes are read in the
# system zone (Asia/Kolkata here), which shifts them by 5.5 hours.
export TZ=UTC
export PYTHONPATH="$PWD/src:${PYTHONPATH:-}"
exec .venv/bin/python -m raincheck.jobs.fetch_era5 "$@"
