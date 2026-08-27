"""W1 -- land raw utd19_u.csv into partitioned Parquet.

This is the L0/L1 ingestion layer, not throwaway audit code. It performs the
work every downstream layer depends on: schema conformance, city-key repair,
quality-flag normalization, and a partitioned columnar landing.

Run:  python -m raincheck.ingest.land_utd19
"""

from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from raincheck import config

# Explicit schema -- never inferSchema. Two reasons:
#   1. Inference costs a second full pass over 7 GB.
#   2. It would type `error` as integer, coercing Manchester's empty flags to
#      null-vs-0 ambiguity and destroying the distinction the audit depends on.
# `error` stays a string so the quality rule can be revised without re-landing.
RAW_SCHEMA = StructType(
    [
        StructField("day", StringType(), True),       # yyyy-MM-dd, local date
        StructField("interval", IntegerType(), True),  # seconds since local midnight
        StructField("detid", StringType(), True),
        StructField("flow", DoubleType(), True),       # veh/h
        StructField("occ", DoubleType(), True),        # fraction, to be verified
        StructField("error", StringType(), True),      # NULL | "0" | "1", city-dependent
        StructField("city", StringType(), True),
        StructField("speed", DoubleType(), True),      # km/h, absent in 30 of 39 cities
    ]
)


def build(spark, conf: dict, limit: int | None = None):
    raw = (
        spark.read.option("header", True)
        # utd19_u.csv is clean: 134,380,371 rows, all exactly 8 fields, zero
        # malformed. No quote/multiline handling needed (unlike the detector
        # file), which keeps the CSV splittable across all 12 cores.
        .schema(RAW_SCHEMA)
        .csv(config.spark_path(config.RAW_MEASUREMENTS))
    )
    if limit is not None:
        raw = raw.limit(limit)

    # ---- city key repair -------------------------------------------------
    aliases = conf.get("aliases") or {}
    city = F.col("city")
    for wrong, right in aliases.items():
        city = F.when(city == wrong, F.lit(right)).otherwise(city)

    # ---- quality flag ----------------------------------------------------
    # See conf/cities.yml: `error = 0` would drop 100% of manchester/rotterdam/
    # torino, which use NULL-vs-"1" and contain no zeros at all.
    flagged = conf["quality"]["flagged_values"]
    quality_ok = ~F.col("error").isin(flagged) | F.col("error").isNull()

    landed = (
        raw.withColumn("city", city)
        .withColumn("quality_ok", quality_ok)
        .withColumn("date", F.to_date("day", "yyyy-MM-dd"))
        # Naive local timestamp. UTC conversion needs a per-city timezone table
        # and belongs to Phase 2; doing it here would silently shift readings.
        # Session timezone is pinned to UTC (see config), so midnight + interval
        # seconds round-trips literally rather than picking up a machine offset.
        .withColumn(
            "ts_local",
            F.timestamp_seconds(F.unix_timestamp("day", "yyyy-MM-dd") + F.col("interval")),
        )
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        # Spark dayofweek: 1=Sunday .. 7=Saturday
        .withColumn("dow", F.dayofweek("date"))
        .withColumn("hod", (F.col("interval") / 3600).cast("int"))
        .withColumn("bin5", (F.col("interval") / 300).cast("int"))
        .drop("day")
    )

    # Deliberately NO repartition("city","year","month").
    #
    # That would shuffle all 134M rows to co-locate each partition, and with
    # 7.6 GB of WSL memory it would spill heavily -- onto a C: drive with only
    # ~10 GB free.
    #
    # It is also unnecessary: utd19_u.csv is already grouped by city
    # (alphabetically, augsburg..zurich), so each ~128 MB input split covers
    # one or two cities. partitionBy therefore emits only a handful of files
    # per split, with no shuffle at all. The small-file problem the shuffle was
    # meant to solve does not arise.
    return landed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="smoke test on N rows into a scratch path; skips the row-conservation gate",
    )
    args = parser.parse_args()

    spark = config.get_spark("land_utd19")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()

    smoke = args.limit is not None
    out = config.spark_path(
        config.LANDED_MEASUREMENTS.with_name("measurements_smoke")
        if smoke
        else config.LANDED_MEASUREMENTS
    )
    print(f"landing {config.RAW_MEASUREMENTS} -> {out}" + (f" (limit {args.limit:,})" if smoke else ""))

    df = build(spark, conf, limit=args.limit)

    t0 = time.time()
    df.write.mode("overwrite").partitionBy("city", "year", "month").parquet(out)
    print(f"write completed in {(time.time() - t0) / 60:.1f} min")

    landed_rows = spark.read.parquet(out).count()
    print(f"landed rows : {landed_rows:,}")

    if smoke:
        spark.read.parquet(out).show(5, truncate=False)
        print("smoke run -- row-conservation gate skipped")
        spark.stop()
        return 0

    # ---- gate: row conservation -----------------------------------------
    expected = config.EXPECTED_MEASUREMENT_ROWS
    print(f"expected    : {expected:,}")
    if landed_rows != expected:
        print(f"FAIL: row count drift of {landed_rows - expected:+,} -- parser dropped rows")
        spark.stop()
        return 1

    print("PASS: row count conserved")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
