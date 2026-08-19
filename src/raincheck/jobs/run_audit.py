"""Phase-1 feasibility audit job.

Reads the raw UTD19 measurements from HDFS and reports, per city, whether it can
support the project at all: does it carry speed, over how many days, at what bin
width, with what error-flag retention, and in what units.

Run via scripts/run_audit.sh.
"""

import argparse
from pathlib import Path

from raincheck import paths
from raincheck.audit import audit_by_city, interval_resolution_by_city
from raincheck.gate import GERMAN_CITIES, classify_city
from raincheck.schemas import RAW_MEASUREMENTS
from raincheck.session import build_session

REPORT_COLUMNS = [
    "city",
    "german",
    "verdict",
    "rows",
    "n_detectors",
    "n_days",
    "first_day",
    "last_day",
    "span_days",
    "day_density_pct",
    "resolution_sec",
    "flow_pct",
    "occ_pct",
    "speed_pct",
    "error_ok_pct",
    "error_flagged_pct",
    "error_unassessed_pct",
    "occ_min",
    "occ_max",
    "speed_min",
    "speed_max",
    "speed_avg",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="reports", help="local report directory")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    spark = build_session("raincheck-l0-audit", master=args.master)
    raw = (
        spark.read.option("header", True)
        .schema(RAW_MEASUREMENTS)
        .csv(paths.RAW_MEASUREMENTS)
    )

    metrics = audit_by_city(raw)
    resolution = interval_resolution_by_city(raw)
    audit = metrics.join(resolution, on="city", how="left")

    audit.write.mode("overwrite").parquet(f"{paths.AUDIT}/city_audit")

    table = audit.toPandas()
    table["german"] = table["city"].isin(GERMAN_CITIES)
    table["verdict"] = [
        classify_city(speed_pct=s, n_days=d)
        for s, d in zip(table["speed_pct"], table["n_days"])
    ]
    table = table[REPORT_COLUMNS].sort_values(
        ["verdict", "german", "n_days"], ascending=[True, False, False]
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "phase1_city_audit.csv", index=False)
    (out_dir / "phase1_city_audit.md").write_text(
        "# Phase-1 city audit (UTD19)\n\n" + table.to_markdown(index=False) + "\n"
    )

    print(f"AUDIT_ROWS {len(table)}")
    print(f"AUDIT_PARQUET {paths.AUDIT}/city_audit")
    print(table.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
