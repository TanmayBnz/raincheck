"""W1 -- land detectors_public.csv into Parquet.

Unlike utd19_u.csv, this file genuinely needs a quote-aware CSV parser: road
names contain commas (e.g. "Route de Balma"), so a naive split produces stray
rows like ` Balma"`.

Run:  python -m raincheck.ingest.land_detectors
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from raincheck import config

# `limit` and `long` are renamed: both are reserved/awkward in Spark SQL.
RAW_SCHEMA = StructType(
    [
        StructField("detid", StringType(), True),
        StructField("length", DoubleType(), True),      # link length, km
        StructField("pos", DoubleType(), True),         # position along link
        StructField("fclass", StringType(), True),      # OSM functional road class
        StructField("road", StringType(), True),        # may contain commas
        StructField("speed_limit", DoubleType(), True),
        StructField("citycode", StringType(), True),
        StructField("lanes", DoubleType(), True),
        StructField("linkid", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
    ]
)


def build(spark, conf: dict):
    raw = (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .schema(RAW_SCHEMA)
        .csv(config.spark_path(config.RAW_DETECTORS))
    )

    # detectors_public.csv misspells Los Angeles as "losanageles"; the
    # measurement table uses "losangeles". Unrepaired, 1,725 detectors join to
    # nothing at all.
    aliases = conf.get("aliases") or {}
    city = F.col("citycode")
    for wrong, right in aliases.items():
        city = F.when(city == wrong, F.lit(right)).otherwise(city)

    return raw.withColumn("city", city).drop("citycode")


def main() -> int:
    spark = config.get_spark("land_detectors")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()

    out = config.spark_path(config.LANDED_DETECTORS)
    df = build(spark, conf)
    df.write.mode("overwrite").parquet(out)

    n = spark.read.parquet(out).count()
    expected = config.EXPECTED_DETECTOR_ROWS
    print(f"landed detectors: {n:,} (expected {expected:,})")

    # A quote-parsing failure shows up as a bogus city key, not as a row-count
    # error, so check both.
    bogus = (
        spark.read.parquet(out)
        .filter(~F.col("city").rlike("^[a-z]+$"))
        .select("city")
        .distinct()
        .collect()
    )
    if bogus:
        print(f"FAIL: malformed city keys -> {[r.city for r in bogus][:10]}")
        spark.stop()
        return 1

    if n != expected:
        print(f"FAIL: row count drift of {n - expected:+,}")
        spark.stop()
        return 1

    print("PASS: detector count and city keys clean")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
