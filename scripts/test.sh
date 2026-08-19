#!/usr/bin/env bash
# Run the test suite. Spark comes from /opt/spark only - the venv deliberately
# has no pyspark installed, so there is never ambiguity about which Spark runs.
set -euo pipefail
cd "$(dirname "$0")/.."
# Pin the JVM system timezone. Spark SQL semantics already follow
# spark.sql.session.timeZone=UTC, but naive Python datetimes are read in the
# system zone (Asia/Kolkata here), which shifts them by 5.5 hours.
export TZ=UTC
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
PY4J=$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip | head -1)
export PYTHONPATH="$SPARK_HOME/python:$PY4J:${PYTHONPATH:-}"
exec .venv/bin/python -m pytest "$@"
