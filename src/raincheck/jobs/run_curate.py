"""L1 curation job: raw UTD19 -> conformed, unit-normalised Parquet.

Scoped to the six study cities and the shared 2017-09-08..2017-11-18 window
selected by the Phase-1 audit. Run via scripts/run_curate.sh.
"""

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from raincheck import paths
from raincheck.audit import interval_resolution_by_city
from raincheck.cities import CITY_NAMES, WINDOW_END, WINDOW_START
from raincheck.curate import (
    add_timestamps,
    apply_error_policy,
    clean_speed,
    curation_quality,
    join_detectors,
    normalize_occupancy,
)
from raincheck.schemas import RAW_MEASUREMENTS
from raincheck.session import build_session

EXPECTED_RESOLUTION_SEC = 300


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    spark = build_session("raincheck-l1-curate", master=args.master)

    raw = (
        spark.read.option("header", True)
        .schema(RAW_MEASUREMENTS)
        .csv(paths.RAW_MEASUREMENTS)
    )
    detectors = spark.read.option("header", True).option("inferSchema", True).csv(
        paths.RAW_DETECTORS
    )

    scoped = raw.filter(
        F.col("city").isin(list(CITY_NAMES))
        & F.col("day").between(F.lit(WINDOW_START), F.lit(WINDOW_END))
    ).cache()

    # The 5-min re-binning step is a no-op for this city set. Assert it rather
    # than assume it: the audit found Paris at 3600s and Innsbruck at 240s.
    resolutions = {
        r["city"]: r["resolution_sec"]
        for r in interval_resolution_by_city(scoped).collect()
    }
    unexpected = {c: s for c, s in resolutions.items() if s != EXPECTED_RESOLUTION_SEC}
    if unexpected:
        raise SystemExit(
            f"re-binning required, cities not on {EXPECTED_RESOLUTION_SEC}s: {unexpected}"
        )
    print(f"RESOLUTION_OK all cities at {EXPECTED_RESOLUTION_SEC}s: {resolutions}")

    curated = clean_speed(normalize_occupancy(apply_error_policy(scoped)))
    quality = curation_quality(scoped, curated).cache()

    enriched = (
        join_detectors(add_timestamps(curated), detectors)
        .withColumn("year", F.year("day"))
        .withColumn("month", F.month("day"))
    )

    (
        enriched.write.mode("overwrite")
        .partitionBy("city", "year", "month", "day")
        .parquet(paths.CURATED_MEASUREMENTS)
    )
    quality.write.mode("overwrite").parquet(paths.CURATED_QUALITY)

    table = quality.toPandas().sort_values("rows_in", ascending=False)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "l1_curation_quality.csv", index=False)
    (out_dir / "l1_curation_quality.md").write_text(
        "# L1 curation quality\n\n" + table.to_markdown(index=False) + "\n"
    )

    print(f"CURATED_PARQUET {paths.CURATED_MEASUREMENTS}")
    print(table.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
