"""Independent cross-check of the Spark audit, using DuckDB against the RAW CSV.

The point is independence: if the Spark landing silently dropped, mistyped or
mis-joined rows, an audit computed from that same landed Parquet would repeat
the error confidently. DuckDB reads the original CSV and never touches the lake.

Run:  python -m raincheck.audit.crosscheck_duckdb
"""

from __future__ import annotations

import sys

import duckdb

from raincheck import config

TOLERANCE_PCT = 0.05  # percentage points


def main() -> int:
    con = duckdb.connect()

    raw = config.RAW_MEASUREMENTS.as_posix()
    print(f"scanning raw CSV: {raw}")

    # Note: read_csv with an explicit type for `error` -- DuckDB would otherwise
    # sniff it as BIGINT and collapse ''/NULL, hiding the per-city encoding
    # difference this project depends on.
    duck = con.execute(
        f"""
        SELECT city,
               count(*)                                   AS rows,
               count(DISTINCT detid)                      AS dets,
               count(DISTINCT day)                        AS days,
               min(day)                                   AS first_day,
               max(day)                                   AS last_day,
               100.0 * count(speed)  / count(*)           AS speed_pct,
               100.0 * count(occ)    / count(*)           AS occ_pct
        FROM read_csv('{raw}',
                      header=true,
                      columns={{'day':'DATE','interval':'INTEGER','detid':'VARCHAR',
                                'flow':'DOUBLE','occ':'DOUBLE','error':'VARCHAR',
                                'city':'VARCHAR','speed':'DOUBLE'}})
        GROUP BY city
        """
    ).fetchall()
    duck_map = {r[0]: r for r in duck}

    lake = config.LANDED_MEASUREMENTS.as_posix()
    spark_side = con.execute(
        f"""
        SELECT city,
               count(*)                          AS rows,
               count(DISTINCT detid)             AS dets,
               count(DISTINCT date)              AS days,
               min(date)                         AS first_day,
               max(date)                         AS last_day,
               100.0 * count(speed) / count(*)   AS speed_pct,
               100.0 * count(occ)   / count(*)   AS occ_pct
        FROM read_parquet('{lake}/**/*.parquet', hive_partitioning=true)
        GROUP BY city
        """
    ).fetchall()
    lake_map = {r[0]: r for r in spark_side}

    # The landing job repairs the losanageles->losangeles typo in the DETECTOR
    # file only; measurement city keys are already correct, so the two sides
    # should agree exactly on city names.
    only_raw = set(duck_map) - set(lake_map)
    only_lake = set(lake_map) - set(duck_map)

    failures = []
    if only_raw:
        failures.append(f"cities only in raw CSV: {sorted(only_raw)}")
    if only_lake:
        failures.append(f"cities only in lake: {sorted(only_lake)}")

    print(f"\n{'city':<14}{'rows(raw)':>13}{'rows(lake)':>13}  {'speed% raw':>10}{'speed% lake':>12}  status")
    for city in sorted(duck_map):
        if city not in lake_map:
            continue
        d, l = duck_map[city], lake_map[city]
        ok = True
        if d[1] != l[1]:
            failures.append(f"{city}: row count {d[1]:,} (raw) vs {l[1]:,} (lake)")
            ok = False
        if d[2] != l[2]:
            failures.append(f"{city}: detector count {d[2]} vs {l[2]}")
            ok = False
        if abs(d[6] - l[6]) > TOLERANCE_PCT:
            failures.append(f"{city}: speed% {d[6]:.2f} vs {l[6]:.2f}")
            ok = False
        if str(d[4]) != str(l[4]) or str(d[5]) != str(l[5]):
            failures.append(f"{city}: window {d[4]}..{d[5]} vs {l[4]}..{l[5]}")
            ok = False
        print(
            f"{city:<14}{d[1]:>13,}{l[1]:>13,}  {d[6]:>10.1f}{l[6]:>12.1f}  "
            f"{'ok' if ok else 'MISMATCH'}"
        )

    total_raw = sum(r[1] for r in duck_map.values())
    total_lake = sum(r[1] for r in lake_map.values())
    print(f"\ntotal rows: raw={total_raw:,}  lake={total_lake:,}")
    if total_raw != config.EXPECTED_MEASUREMENT_ROWS:
        failures.append(f"raw total {total_raw:,} != expected {config.EXPECTED_MEASUREMENT_ROWS:,}")

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("\nPASS: DuckDB(raw CSV) and Spark(landed Parquet) agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
