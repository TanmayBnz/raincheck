"""Phase 2 / L1 -- curate the landed UTD19 measurements for the study set.

Raw -> conformed. Every transform here is mandated by a defect the Phase-1
audit found; the rule numbers refer to reports/phase1_gate.md §6.

  1. quality flag   `error IS NULL OR error != '1'`, never `error = 0`
  2. occupancy      per-city rescale (percent -> fraction), Essen untouched
  3. occupancy      `inf` / out-of-range dropped to NULL, never rescaled
  4. speed          `speed = 0 AND flow = 0` -> NULL (absence, not standstill)
  5. speed          `speed = 0 AND flow > 0` kept but flagged for investigation
  6. resolution     baseline bin is hourly (Manchester/Torino), 30-min (Essen)

Plus the work L1 owes downstream regardless of any defect: local -> UTC
alignment, detector metadata join, and indexing onto the rainfall grids.

Nothing is silently discarded. Rows failing rules 3-5 are retained with the
offending column NULLed and a boolean flag set, so qa_curated.py can quantify
exactly what curation removed and the baseline layer can exclude on its own
terms. The one genuine filter is the quality flag (rule 1) and the study-set
restriction -- both counted and reported.

Run:  python -m raincheck.curate.curate_measurements
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from raincheck import config

# Detector metadata columns carried into the curated table. `road` is dropped:
# it is a free-text street name, useful for eyeballing but not for modelling,
# and it is the column whose embedded commas forced the quote-aware parser.
DETECTOR_COLS = ["fclass", "length", "lanes", "linkid", "speed_limit", "lat", "lon"]


def _case_by_city(mapping: dict, default=None):
    """Build a CASE expression keyed on `city` from a {city: value} dict."""
    expr = F.lit(default)
    for city, value in mapping.items():
        expr = F.when(F.col("city") == city, F.lit(value)).otherwise(expr)
    return expr


def local_to_utc(ts_col: str, tz_by_city: dict[str, str]):
    """Convert a naive local timestamp to UTC using each city's zone.

    `interval` is seconds since LOCAL midnight, so the landed ts_local is local
    wall-clock. to_utc_timestamp interprets it in the given zone. This cannot be
    done with a single session timezone -- the three study cities sit in two
    different zones (Europe/London vs Europe/Rome and Europe/Berlin, which share
    an offset but are distinct zones and best kept explicit).
    """
    expr = F.lit(None).cast("timestamp")
    for city, tz in tz_by_city.items():
        expr = F.when(F.col("city") == city, F.to_utc_timestamp(F.col(ts_col), tz)).otherwise(expr)
    return expr


def dst_ambiguous_day(tz_by_city: dict[str, str]):
    """True on local dates that are not 24 hours long.

    On a DST transition the mapping from "seconds since local midnight" to a
    wall-clock instant is either ambiguous (autumn: one local hour occurs twice)
    or undefined (spring: one local hour does not exist). Spark resolves both
    silently, so an hour of readings on those dates is attached to the wrong UTC
    instant -- and therefore to the wrong rainfall hour.

    Every study window contains at least one such date (UK 2017-10-29,
    DE 2017-03-26), so this is not hypothetical. The rows are kept and flagged
    rather than dropped; the baseline layer decides.
    """
    day_start = F.to_timestamp(F.col("date"))
    next_start = F.to_timestamp(F.date_add(F.col("date"), 1))
    expr = F.lit(None).cast("boolean")
    for city, tz in tz_by_city.items():
        length = F.unix_timestamp(F.to_utc_timestamp(next_start, tz)) - F.unix_timestamp(
            F.to_utc_timestamp(day_start, tz)
        )
        expr = F.when(F.col("city") == city, length != F.lit(86400)).otherwise(expr)
    return expr


def load_detectors(spark, cities: list[str]):
    """Detector metadata for the study set, asserted unique on (city, detid).

    A duplicate key would fan the measurement table out on the left join --
    silently inflating row counts and double-weighting whichever detectors are
    duplicated. Checked rather than assumed.
    """
    d = (
        spark.read.parquet(config.spark_path(config.LANDED_DETECTORS))
        .filter(F.col("city").isin(cities))
        .select("city", "detid", *DETECTOR_COLS)
    )
    total = d.count()
    distinct = d.select("city", "detid").distinct().count()
    return d, total, distinct


def curate(spark, conf: dict, m, detectors):
    study = conf["study"]
    cur = conf["curation"]
    grids = conf["spatial_index"]

    tz_by_city = {c: s["tz"] for c, s in study.items()}
    occ_max = _case_by_city(
        {c: float(cur["occ_max_raw"][s["occ_scale"]]) for c, s in study.items()}
    )
    occ_divisor = _case_by_city(
        {c: (100.0 if s["occ_scale"] == "percent" else 1.0) for c, s in study.items()}
    )
    res_min = _case_by_city({c: int(s["baseline_res_min"]) for c, s in study.items()})

    # ---- rule 1: quality flag -------------------------------------------
    # Computed at landing (see land_utd19.py) precisely so the rule could be
    # revised without re-landing 134M rows. This is the only row-dropping
    # filter in the job besides the study-set restriction.
    kept = m.filter(F.col("quality_ok"))

    # ---- rules 2 + 3: occupancy -----------------------------------------
    # Order matters: the range test is applied on the RAW value, against that
    # city's own ceiling, before any division. Rescaling first would turn
    # Torino's inf into inf and 2094 into a plausible-looking 20.94.
    occ_raw = F.col("occ")
    occ_bad = (
        occ_raw.isNotNull()
        & (F.isnan(occ_raw) | (occ_raw < 0) | (occ_raw > occ_max))
    )

    # ---- rules 4 + 5: speed ---------------------------------------------
    speed_raw = F.col("speed")
    zero_speed = speed_raw.isNotNull() & (speed_raw == 0)
    # Rule 4. `flow` is veh/h over the interval; 0 means nothing crossed the
    # loop, so there is no speed to record. Left as a literal zero it drags
    # every median and 85th percentile down -- and those are the DENOMINATOR
    # of the delay metric, so the bias flows straight into the headline result.
    speed_absent = zero_speed & F.col("flow").isNotNull() & (F.col("flow") == 0)
    # Rule 5. Vehicles counted but zero speed recorded. Kept, because at
    # Manchester's 0.4% this is plausibly genuine standstill; Rotterdam's 10.8%
    # was sensor fault, and Rotterdam is not in the study set.
    speed_zero_flowing = zero_speed & F.col("flow").isNotNull() & (F.col("flow") > 0)
    # Not a gate rule; see conf/cities.yml. An urban loop reporting >150 km/h is
    # faulty, and free-flow speed is an upper percentile, so these land squarely
    # in the statistic they would corrupt.
    cap = float(cur["speed_cap_kmh"])
    speed_implausible = speed_raw.isNotNull() & (F.isnan(speed_raw) | (speed_raw > cap))

    # ---- spatial index ---------------------------------------------------
    # Detectors are snapped to the rainfall grids themselves rather than to H3.
    # These are the actual join keys for L2b; an intermediate hex tiling would
    # add a second resampling step and a cell-size mismatch for nothing.
    era5_deg = float(grids["era5_deg"])
    hires_deg = float(grids["hires_deg"])

    def cell(name: str, size: float):
        lat = F.round(F.col("lat") / F.lit(size)) * F.lit(size)
        lon = F.round(F.col("lon") / F.lit(size)) * F.lit(size)
        return F.when(
            F.col("lat").isNull() | F.col("lon").isNull(), F.lit(None).cast("string")
        ).otherwise(
            F.concat_ws(
                "_", F.lit(name), F.round(lat, 4).cast("string"), F.round(lon, 4).cast("string")
            )
        )

    # Order is load-bearing. `occ` and `speed` are REPLACED in place, and a
    # withColumn resolves against the dataframe as it stands at that point --
    # so every flag must be materialized BEFORE its source column is
    # overwritten. Flagging afterwards silently yields all-false flags (the
    # cleaned column can no longer fail its own test), which looks exactly like
    # pristine data in the QA report. The normalized values are then derived
    # from the materialized flags rather than recomputing the predicates, so
    # the dependency is explicit and cannot be reordered by accident.
    curated = (
        kept.join(F.broadcast(detectors), ["city", "detid"], "left")
        .withColumn("occ_raw", occ_raw)
        .withColumn("speed_raw", speed_raw)
        .withColumn("occ_bad", occ_bad)
        .withColumn("speed_absent", speed_absent)
        .withColumn("speed_zero_flowing", speed_zero_flowing)
        .withColumn("speed_implausible", speed_implausible)
        .withColumn(
            "occ",
            F.when(
                F.col("occ_bad") | F.col("occ_raw").isNull(), F.lit(None).cast("double")
            ).otherwise(F.col("occ_raw") / occ_divisor),
        )
        .withColumn(
            "speed",
            F.when(
                F.col("speed_absent") | F.col("speed_implausible"),
                F.lit(None).cast("double"),
            ).otherwise(F.col("speed_raw")),
        )
        .withColumn("ts_utc", local_to_utc("ts_local", tz_by_city))
        .withColumn("dst_ambiguous", dst_ambiguous_day(tz_by_city))
        .withColumn("baseline_res_min", res_min)
        # Rule 6. The baseline cell is (detid, dow, tbin) at this city's
        # resolution. Note `bin5` survives untouched: measurements stay at their
        # native 5-min resolution and are compared AGAINST the coarser cell, so
        # rain-onset detection is unaffected -- only the denominator is hourly.
        .withColumn("tbin", (F.col("interval") / (res_min * 60)).cast("int"))
        .withColumn("era5_cell", cell("era5", era5_deg))
        .withColumn("hires_cell", cell("hires", hires_deg))
    )

    return curated.select(
        "city",
        "detid",
        "date",
        "interval",
        "ts_local",
        "ts_utc",
        "year",
        "month",
        "dow",
        "hod",
        "bin5",
        "tbin",
        "baseline_res_min",
        "dst_ambiguous",
        "flow",
        "occ",
        "occ_raw",
        "occ_bad",
        "speed",
        "speed_raw",
        "speed_absent",
        "speed_zero_flowing",
        "speed_implausible",
        *DETECTOR_COLS,
        "era5_cell",
        "hires_cell",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="smoke test on N landed rows into a scratch path",
    )
    args = parser.parse_args()

    spark = config.get_spark("curate_measurements")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    cities = list(conf["study"].keys())

    smoke = args.limit is not None
    out = config.spark_path(
        config.CURATED_MEASUREMENTS.with_name("measurements_smoke")
        if smoke
        else config.CURATED_MEASUREMENTS
    )

    # Partition pruning: the study set is 3 of 40 city= directories, so this
    # reads a small fraction of the 728 MB landed table rather than all of it.
    m = spark.read.parquet(config.spark_path(config.LANDED_MEASUREMENTS)).filter(
        F.col("city").isin(cities)
    )
    if smoke:
        m = m.limit(args.limit)

    print(f"study set   : {', '.join(cities)}")

    detectors, det_total, det_distinct = load_detectors(spark, cities)
    print(f"detectors   : {det_total:,} rows, {det_distinct:,} distinct (city, detid)")
    if det_total != det_distinct:
        print(
            f"FAIL: {det_total - det_distinct:,} duplicate detector keys would fan out the join"
        )
        spark.stop()
        return 1

    landed_rows = m.count()
    curated = curate(spark, conf, m, detectors)

    t0 = time.time()
    curated.write.mode("overwrite").partitionBy("city", "year", "month").parquet(out)
    print(f"write completed in {(time.time() - t0) / 60:.1f} min")

    written = spark.read.parquet(out)
    curated_rows = written.count()

    print(f"landed (study cities) : {landed_rows:,}")
    print(f"curated               : {curated_rows:,}")
    print(
        f"dropped by quality flag: {landed_rows - curated_rows:,} "
        f"({100.0 * (landed_rows - curated_rows) / landed_rows:.1f}%)"
    )

    if smoke:
        written.show(5, truncate=False)
        print("smoke run -- gates skipped")
        spark.stop()
        return 0

    # ---- gate: the join must not have fanned out or lost rows ------------
    # A left join on a key asserted unique can only preserve the row count.
    # If it did not, the uniqueness assumption above is wrong.
    expected = (
        spark.read.parquet(config.spark_path(config.LANDED_MEASUREMENTS))
        .filter(F.col("city").isin(cities))
        .filter(F.col("quality_ok"))
        .count()
    )
    if curated_rows != expected:
        print(f"FAIL: row drift of {curated_rows - expected:+,} across the detector join")
        spark.stop()
        return 1

    unmatched = written.filter(F.col("lat").isNull()).count()
    print(f"rows with no detector metadata: {unmatched:,}")

    manifest = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "cities": cities,
        "landed_study_rows": landed_rows,
        "curated_rows": curated_rows,
        "quality_dropped": landed_rows - curated_rows,
        "rows_without_detector_metadata": unmatched,
        "path": out,
    }
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    mpath = config.REPORTS_DIR / "phase2_curation_manifest.json"
    mpath.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"wrote {mpath}")

    print("PASS: curated table written, row count reconciles")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
