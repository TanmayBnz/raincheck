#!/usr/bin/env bash
# Pure Python - no Spark needed. See src/raincheck/jobs/write_segments.py.
set -euo pipefail
cd "$(dirname "$0")/.."
export TZ=UTC
export PYTHONPATH="$PWD/src"
exec .venv/bin/python -m raincheck.jobs.write_segments "$@"
