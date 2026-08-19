"""L2b: join radar rainfall to curated NDW bins and run the bias diagnostics.

    ./scripts/run_rain_join_ndw.sh

Joins on ``(segment_id, ts_utc)`` because both sides are already on the same
5-minute grid - the radar's native interval. That is the point of choosing radar:
at 28 km hourly, ERA5 forced a coarse spatial snap and an hour-containment join,
and Birmingham's whole detector set shared a single cell.

Then runs the two diagnostics that decide whether any dose-response estimate from
this corpus can be trusted:

* retention stratified by rain band - divergence between wet and dry means
  sample size is correlated with the treatment;
* the estimate at several minimum-sample-size thresholds - drift means the
  effect is an artefact of what the threshold deleted.
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from raincheck import paths
from raincheck.curate_ndw import retention_by_rain_band, threshold_sensitivity
from raincheck.rain import RADAR_BAND_EDGES, with_rain_band
from raincheck.session import build_session

CURATED = paths.LOCAL_STAGE.parent / "curated"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurements", default=str(CURATED / "ndw"))
    parser.add_argument("--rain", default=str(CURATED / "ndw_rain"))
    parser.add_argument("--segments", default=str(CURATED / "ndw_segments"))
    parser.add_argument("--output", default=str(CURATED / "ndw_features"))
    args = parser.parse_args(argv)

    spark = build_session("raincheck-rain-join-ndw")
    measurements = spark.read.parquet(args.measurements)
    rain = spark.read.parquet(args.rain)
    segments = spark.read.parquet(args.segments).select(
        "segment_id", "frc", "computation_method")

    # Left join on both keys: a bin with no radar frame keeps its row with null
    # rain, so coverage gaps are countable rather than silently vanished traffic.
    joined = (
        measurements.join(rain, on=["segment_id", "ts_utc"], how="left")
        .join(segments, on="segment_id", how="left")
    )
    featured = with_rain_band(
        joined, edges=RADAR_BAND_EDGES, top="extreme").cache()

    total = featured.count()
    matched = featured.filter(F.col("rain_mm_h").isNotNull()).count()
    print("\n=== L2b rain join (radar, 5 min / 1 km) ===")
    print(f"  bins                {total:>10,}")
    print(f"  with rainfall       {matched:>10,}  ({100 * matched / total:.2f}%)")
    print(f"  max intensity       {featured.agg(F.max('rain_mm_h')).first()[0]}")

    print("\n  band distribution:")
    for row in (featured.groupBy("rain_band").count()
                .orderBy(F.col("count").desc()).collect()):
        label = row.rain_band if row.rain_band is not None else "(no radar)"
        print(f"    {label:<12} {row['count']:>10,}"
              f"  ({100 * row['count'] / total:5.2f}%)")

    print("\n  === retention by rain band (the missingness diagnostic) ===")
    print("  divergence between wet and dry rows means the bias is live")
    report = retention_by_rain_band(
        featured.withColumnRenamed("quality_weight", "quality_weight"),
        min_weight=5.0)
    for row in report.orderBy("rain_band").collect():
        label = row.rain_band if row.rain_band is not None else "(no radar)"
        print(f"    {label:<12} n={row.n_obs:>9,}"
              f"  speed_null={row.speed_null_pct:>7}%"
              f"  mean_w={row.mean_quality_weight:>7}"
              f"  kept@w>=5={row.retained_pct:>7}%")

    print("\n  === threshold sensitivity (mean speed by band) ===")
    print("  an estimate that drifts monotonically is driven by missingness")
    rows = threshold_sensitivity(featured).orderBy("min_weight", "rain_band").collect()
    for row in rows:
        label = row.rain_band if row.rain_band is not None else "(no radar)"
        print(f"    w>={row.min_weight:<5} {label:<12}"
              f" mean_speed={row.mean_speed}  n={row.n_obs:,}")

    (featured.withColumn("date", F.to_date("ts_utc"))
     .write.mode("overwrite").partitionBy("date").parquet(args.output))
    print(f"\n  written to {args.output}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
