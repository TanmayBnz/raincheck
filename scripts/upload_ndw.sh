#!/usr/bin/env bash
# Upload completed NDW hour partitions from the local stage into HDFS.
#
# Only *closed* hours are uploaded: the current hour is still being appended to
# by the harvester, so shipping it would produce a partial partition that later
# fetches cannot add to. Local files are kept unless --prune is passed, and
# --prune only ever deletes a partition after verifying it exists in HDFS.
#
#   ./scripts/upload_ndw.sh              # copy closed hours, keep local copies
#   ./scripts/upload_ndw.sh --prune      # copy, verify, then delete local
set -euo pipefail
cd "$(dirname "$0")/.."
export TZ=UTC
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk}
export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
PATH="$HADOOP_HOME/bin:$PATH"

STAGE=${RAINCHECK_STAGE:-/home/tanbnz/raincheck-data/stage}/ndw/measurements
DEST=${RAINCHECK_HDFS:-hdfs://localhost:9000}/raincheck/nl/raw/ndw/measurements
PRUNE=0
[[ ${1:-} == --prune ]] && PRUNE=1

if ! jps 2>/dev/null | grep -qi namenode; then
  echo "HDFS namenode is not running. Start it with: \$HADOOP_HOME/sbin/start-dfs.sh" >&2
  exit 1
fi

# Sites stamp measurements up to ~8 minutes behind wall clock (two cohorts, at
# ~2 and ~8 minutes), so an hour partition keeps receiving writes well after the
# hour rolls over. Treat a partition as closed only once it is this far past.
CLOSE_LAG_MIN=20
CUTOFF=$(date -u -d "-${CLOSE_LAG_MIN} minutes" +%Y-%m-%dT%H)
uploaded=0

for dir in "$STAGE"/date=*/hour=*; do
  [[ -d $dir ]] || continue
  partition=${dir#"$STAGE"/}
  # partition is date=YYYY-MM-DD/hour=HH -> comparable key YYYY-MM-DDTHH
  key=$(sed -E 's|date=([0-9-]+)/hour=([0-9]+)|\1T\2|' <<<"$partition")
  if [[ $key > $CUTOFF || $key == "$CUTOFF" ]]; then
    echo "skip  $partition (may still receive late writes)"
    continue
  fi

  hdfs dfs -mkdir -p "$DEST/$partition"
  hdfs dfs -put -f "$dir"/*.parquet "$DEST/$partition/"

  # Verify every local file landed before considering deletion.
  ok=1
  for file in "$dir"/*.parquet; do
    hdfs dfs -test -e "$DEST/$partition/$(basename "$file")" || ok=0
  done
  if (( ! ok )); then
    echo "FAIL  $partition did not verify; leaving local copy in place" >&2
    continue
  fi

  echo "ok    $partition"
  uploaded=$((uploaded + 1))
  if (( PRUNE )); then
    rm -f "$dir"/*.parquet
    rmdir --ignore-fail-on-non-empty "$dir"
  fi
done

echo "uploaded $uploaded partition(s) to $DEST"
