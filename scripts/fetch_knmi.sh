#!/usr/bin/env bash
# Pure Python - no Spark needed. See src/raincheck/jobs/fetch_knmi.py.
set -euo pipefail
cd "$(dirname "$0")/.."
export TZ=UTC
export PYTHONPATH="$PWD/src"
exec .venv/bin/python -m raincheck.jobs.fetch_knmi "$@"
