#!/usr/bin/env bash
# Harvest the NDW live feed into local Parquet partitions.
#
# Pure Python - no Spark. The feed updates once a minute and pyarrow writes the
# partitions directly, so spinning up a JVM per minute would be pointless
# overhead. Uploading the staged partitions to HDFS is a separate step.
#
#   ./scripts/harvest_ndw.sh              # run until Ctrl-C / SIGTERM
#   ./scripts/harvest_ndw.sh --once       # single fetch, smoke test
#   nohup ./scripts/harvest_ndw.sh >> harvest.log 2>&1 &
set -euo pipefail
cd "$(dirname "$0")/.."
# The feed stamps measurements in UTC; keep Python agreeing with it.
export TZ=UTC
export PYTHONPATH="$PWD/src"
exec .venv/bin/python -m raincheck.jobs.harvest_ndw "$@"
