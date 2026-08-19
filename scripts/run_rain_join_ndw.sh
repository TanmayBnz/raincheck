#!/usr/bin/env bash
# L1 curation of harvested NDW measurements. Spark comes from /opt/spark; the
# venv supplies only the Python libraries, so one Spark version is in play.
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

# Ship the package so the job also works against a standalone master, not just
# local mode where PYTHONPATH alone would be enough.
mkdir -p build
rm -f build/raincheck.zip
( cd src && zip -qr ../build/raincheck.zip raincheck -x '*__pycache__*' )

export PYTHONPATH="$PWD/src:${PYTHONPATH:-}"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "${RAINCHECK_MASTER:-local[12]}" \
  --driver-memory "${RAINCHECK_DRIVER_MEM:-6g}" \
  --py-files build/raincheck.zip \
  src/raincheck/jobs/rain_join_ndw.py "$@"
