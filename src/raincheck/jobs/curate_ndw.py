"""L1: curate harvested NDW Parquet into the conformed measurement table.

    ./scripts/run_curate_ndw.sh                      # stage -> local curated
    ./scripts/run_curate_ndw.sh --output hdfs://...  # once HDFS is up

Reads the harvester's staged partitions, deduplicates, nulls implausible speeds
and re-bins to a fixed interval weighted by sample size. Writes a quality report
alongside, because retention here is a finding rather than an implementation
detail.
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from raincheck import paths
from raincheck.curate import clean_speed
from raincheck.curate_ndw import REBIN_MINUTES, dedupe, rebin
from raincheck.session import build_session


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(paths.LOCAL_STAGE_NDW))
    parser.add_argument("--output",
                        default=str(paths.LOCAL_STAGE.parent / "curated" / "ndw"))
    parser.add_argument("--minutes", type=int, default=REBIN_MINUTES)
    args = parser.parse_args(argv)

    spark = build_session("raincheck-curate-ndw")
    raw = spark.read.parquet(args.input)

    rows_in = raw.count()
    distinct_keys = raw.select("segment_id", "ts_utc").distinct().count()

    # clean_speed is reused unchanged from the UTD19 arm: it nulls rather than
    # clips, which matters because the p85 free-flow percentile reads the upper
    # tail. Live NDW speeds reach 173 km/h, above the 150 km/h cap.
    deduped = clean_speed(dedupe(raw))
    curated = rebin(deduped, minutes=args.minutes).cache()

    rows_out = curated.count()
    stats = curated.select(
        F.round(100.0 * F.avg(F.when(F.col("speed").isNull(), 1.0).otherwise(0.0)),
                3).alias("speed_null_pct"),
        F.round(F.avg("quality_weight"), 2).alias("mean_quality_weight"),
        F.round(F.avg("speed_weight"), 2).alias("mean_speed_weight"),
        F.round(F.avg("n_obs"), 2).alias("mean_obs_per_bin"),
        F.round(100.0 * F.sum("unknown_weight_obs") / F.sum("n_obs"), 2)
        .alias("unknown_weight_pct"),
        F.min("ts_utc").alias("first_bin"),
        F.max("ts_utc").alias("last_bin"),
    ).collect()[0]

    print("\n=== L1 NDW curation ===")
    print(f"  rows in                {rows_in:>12,}")
    print(f"  distinct (segment, ts) {distinct_keys:>12,}"
          f"   duplicates removed: {rows_in - distinct_keys:,}"
          f" ({100 * (rows_in - distinct_keys) / rows_in:.1f}%)")
    print(f"  {args.minutes}-minute bins out     {rows_out:>12,}")
    print(f"  speed null                 {stats.speed_null_pct:>8}%")
    print(f"  mean vehicles per bin      {stats.mean_quality_weight:>8}"
          f"   (behind speed: {stats.mean_speed_weight})")
    print(f"  mean observations per bin   {stats.mean_obs_per_bin:>8}")
    print(f"  observations with no stated sample size {stats.unknown_weight_pct:>5}%"
          f"   (counted as 1, so mean vehicles/bin is a floor)")
    print(f"  window                 {stats.first_bin} .. {stats.last_bin}")
    print("\n  Rain-stratified retention and threshold sensitivity require the")
    print("  L2b rain join; run them once rain_band exists.")

    (curated
     .withColumn("date", F.to_date("ts_utc"))
     .write.mode("overwrite").partitionBy("date").parquet(args.output))
    print(f"\n  written to {args.output} (partitioned by date)")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
