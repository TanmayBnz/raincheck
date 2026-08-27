"""Diagnose two defects the W2 audit surfaced in the candidate cities.

1. OCCUPANCY SCALE. UTD19 documents `occ` as a 0-1 fraction, but the audit
   found occ_max of 2094 (manchester), 1164 (bolton), 1020 (torino) -- values
   impossible on either a 0-1 or a 0-100 scale. Only essen is clean (max 0.80).
   This matters because L2a defines free-flow speed as a high percentile of
   speed CONDITIONED ON occupancy below critical -- which is meaningless if the
   occupancy scale is unknown or corrupt.

2. ZERO SPEEDS. speed_p01 = 0.0 in every candidate except essen. A zero-speed
   reading at zero flow means "no vehicles observed", not "traffic stopped".
   Including those in a median or 85th-percentile baseline biases it downward.

Run:  python -m raincheck.audit.diagnose_units
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

from raincheck import config


def main() -> int:
    spark = config.get_spark("diagnose_units")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    candidates = [c for g in conf["cohorts"].values() for c in g["cities"]]

    m = spark.read.parquet(config.spark_path(config.LANDED_MEASUREMENTS))

    print("\n=== OCCUPANCY SCALE ===")
    print(f"{'city':<12}{'rows':>10}{'p50':>9}{'p99':>9}{'p999':>10}{'max':>10}"
          f"{'>1':>8}{'>100':>8}{'dets>1':>8}{'dets':>6}")
    for city in candidates:
        c = m.filter((F.col("city") == city) & F.col("occ").isNotNull())
        n = c.count()
        if n == 0:
            continue
        q = c.approxQuantile("occ", [0.5, 0.99, 0.999], 0.0005)
        mx = c.agg(F.max("occ")).collect()[0][0]
        gt1 = c.filter(F.col("occ") > 1.0).count()
        gt100 = c.filter(F.col("occ") > 100.0).count()
        dets = c.select("detid").distinct().count()
        # Are the bad values confined to a few broken detectors?
        dets_gt1 = c.filter(F.col("occ") > 1.0).select("detid").distinct().count()
        print(f"{city:<12}{n:>10,}{q[0]:>9.3f}{q[1]:>9.2f}{q[2]:>10.2f}{mx:>10.1f}"
              f"{100.0*gt1/n:>7.2f}%{100.0*gt100/n:>7.2f}%{dets_gt1:>8}{dets:>6}")

    print("\n=== ZERO / NULL SPEED ===")
    print(f"{'city':<12}{'speed rows':>12}{'zero':>9}{'zero&flow0':>12}"
          f"{'zero&flow>0':>13}{'p01>0':>8}")
    for city in candidates:
        c = m.filter((F.col("city") == city) & F.col("speed").isNotNull())
        n = c.count()
        if n == 0:
            continue
        z = c.filter(F.col("speed") == 0)
        nz = z.count()
        z_noflow = z.filter(F.col("flow") == 0).count()
        z_flow = z.filter(F.col("flow") > 0).count()
        pos = c.filter(F.col("speed") > 0).approxQuantile("speed", [0.01], 0.0005)
        print(f"{city:<12}{n:>12,}{100.0*nz/n:>8.2f}%{100.0*z_noflow/n:>11.2f}%"
              f"{100.0*z_flow/n:>12.2f}%{pos[0]:>8.1f}")

    # If occ > 1 concentrates in a handful of detectors, they can simply be
    # excluded; if it is spread across all of them, the whole city's occupancy
    # channel is unusable and the two-channel decomposition dies with it.
    print("\n=== worst detectors by occ (manchester, torino) ===")
    for city in ["manchester", "torino"]:
        c = m.filter((F.col("city") == city) & F.col("occ").isNotNull())
        top = (
            c.groupBy("detid")
            .agg(F.max("occ").alias("max_occ"), F.count(F.lit(1)).alias("n"))
            .orderBy(F.desc("max_occ"))
            .limit(5)
            .collect()
        )
        share = c.groupBy("detid").agg(F.max("occ").alias("mx")).filter(F.col("mx") > 1).count()
        tot = c.select("detid").distinct().count()
        print(f"  {city}: {share}/{tot} detectors ever exceed 1.0")
        for r in top:
            print(f"    {r['detid']:<16} max_occ={r['max_occ']:>10.1f}  n={r['n']:,}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
