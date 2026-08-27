"""Phase 3 / L2a -- free-flow speed, typical speed profiles, delay metrics.

This is the layer that is genuinely novel relative to IUTF, which stopped at
flow. Three artefacts are produced:

  1. lake/utd19/baselines/freeflow  -- per detector: critical occupancy and
     free-flow speed (85th percentile of speed conditioned on occupancy below
     critical, dry intervals only).
  2. lake/utd19/baselines/profile   -- per (detector, dow, tbin): median dry
     speed and median dry flow, i.e. what this hour normally looks like at this
     detector. The flow half is what lets Phase 5 separate rain slowing traffic
     from rain suppressing demand.
  3. lake/utd19/curated/measurements_delay -- every curated interval with both
     baselines attached and three delay metrics computed.

**Dry-only is the point.** If rainy intervals enter the baseline it absorbs the
very effect the project is measuring, and the estimated rain impact shrinks
toward zero by roughly (1 - p) with p the wet fraction. The dry mask comes from
lake/era5/curated/rain_hourly and is buffered: for `dry_buffer_hours` after
rain stops the surface is still wet and the interval is still excluded.

Run:  python -m raincheck.baseline.build_baselines
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

from raincheck import config
from raincheck.weather.build_rain_mask import rain_ts_for


def attach_rain(m, rain):
    """Join the hourly city-level rain labels onto every measurement.

    Broadcast: the mask is 8,784 rows against 2.9 M measurements, so this is a
    map-side join with no shuffle.
    """
    keyed = m.withColumn("rain_ts", rain_ts_for("ts_utc"))
    cols = ["city", "rain_ts", "precip_mm", "is_wet", "band", "event_id",
            "hours_since_rain", "is_dry_clean"]
    return keyed.join(F.broadcast(rain.select(*cols)), ["city", "rain_ts"], "left")


def critical_occupancy(dry, bconf):
    """Per-detector critical occupancy, read off that detector's own FD.

    Critical occupancy is where flow peaks: below it the road is free-flowing,
    above it flow degrades as density rises. Estimated by binning occupancy and
    taking the bin with the highest MEDIAN flow -- median rather than mean
    because a single spurious flow reading would otherwise choose the bin.

    This matters because free-flow speed is defined as a percentile conditioned
    on occupancy below critical. Get the threshold wrong and "free-flow" silently
    becomes "off-peak", which is the very confusion CONTEXT.md §L2a sets out to
    avoid: an off-peak average is contaminated by whatever congestion happens to
    occur at night, and is not a physical property of the link.

    Detectors that cannot support the estimate, or whose peak lands outside a
    physically sensible band, fall back to their city's median estimate -- not
    to a global constant, since occupancy scale and detector siting differ by
    city.
    """
    width = float(bconf["occ_bin_width"])
    lo, hi = [float(x) for x in bconf["critical_occ_bounds"]]
    min_obs = int(bconf["min_obs_critical_occ"])

    binned = (
        dry.filter(F.col("occ").isNotNull() & F.col("flow").isNotNull())
        .withColumn("occ_bin", (F.col("occ") / F.lit(width)).cast("int") * F.lit(width))
        .groupBy("city", "detid", "occ_bin")
        .agg(
            F.percentile_approx("flow", 0.5).alias("med_flow"),
            F.count(F.lit(1)).alias("n"),
        )
        # A bin holding a handful of readings can win on noise alone.
        .filter(F.col("n") >= F.lit(20))
    )

    ranked = binned.withColumn(
        "rk",
        F.row_number().over(
            Window.partitionBy("city", "detid").orderBy(F.desc("med_flow"))
        ),
    ).filter(F.col("rk") == 1)

    totals = dry.groupBy("city", "detid").agg(F.count("occ").alias("n_occ"))

    raw = (
        totals.join(ranked.select("city", "detid", F.col("occ_bin").alias("occ_crit_raw")),
                    ["city", "detid"], "left")
        .withColumn(
            "occ_crit_ok",
            (F.col("n_occ") >= F.lit(min_obs))
            & F.col("occ_crit_raw").isNotNull()
            & (F.col("occ_crit_raw") >= F.lit(lo))
            & (F.col("occ_crit_raw") <= F.lit(hi)),
        )
    )

    city_median = (
        raw.filter(F.col("occ_crit_ok"))
        .groupBy("city")
        .agg(F.percentile_approx("occ_crit_raw", 0.5).alias("occ_crit_city"))
    )

    return (
        raw.join(city_median, "city", "left")
        .withColumn(
            "occ_crit",
            F.when(F.col("occ_crit_ok"), F.col("occ_crit_raw"))
            .when(F.col("occ_crit_city").isNotNull(), F.col("occ_crit_city"))
            .otherwise(F.lit(float(bconf["critical_occ_fallback"]))),
        )
        .withColumn(
            "occ_crit_source",
            F.when(F.col("occ_crit_ok"), F.lit("detector"))
            .when(F.col("occ_crit_city").isNotNull(), F.lit("city_median"))
            .otherwise(F.lit("fallback")),
        )
        .select("city", "detid", "n_occ", "occ_crit_raw", "occ_crit", "occ_crit_source")
    )


def free_flow(dry, crit, bconf):
    """Free-flow speed: high percentile of dry speed below critical occupancy."""
    pct = float(bconf["free_flow_pct"])
    min_obs = int(bconf["min_obs_free_flow"])

    joined = dry.join(F.broadcast(crit), ["city", "detid"], "left")
    uncongested = joined.filter(
        F.col("speed").isNotNull() & F.col("occ").isNotNull() & (F.col("occ") < F.col("occ_crit"))
    )

    ff = uncongested.groupBy("city", "detid").agg(
        F.percentile_approx("speed", pct).alias("free_flow_speed"),
        F.count(F.lit(1)).alias("n_free_flow"),
        F.percentile_approx("speed", 0.5).alias("uncongested_median_speed"),
    )

    return (
        crit.join(ff, ["city", "detid"], "left")
        .withColumn(
            "free_flow_ok",
            F.col("free_flow_speed").isNotNull() & (F.col("n_free_flow") >= F.lit(min_obs)),
        )
    )


def typical_profile(dry, bconf):
    """Median dry speed AND flow per (detector, dow, tbin) -- 'what this hour
    normally is'.

    tbin is already at the city's own baseline resolution (hourly for
    Manchester/Torino, 30-min for Essen), set during L1 curation.

    The flow half exists because rain does two things at once and they push the
    pooled speed average in opposite directions: it slows vehicles down, and it
    also removes them (fewer trips are taken, and on a signalised arterial
    fewer vehicles means HIGHER speeds). Without a flow baseline the two are
    indistinguishable, and a positive speed deviation under rain cannot be told
    apart from an emptier road. Phase 4 computed this on the fly inside its QA
    and discarded it; Phase 5 needs it in the specification, so it is persisted
    here alongside the speed profile it must be read against.

    Speed and flow get independent observation counts. A cell can hold plenty
    of flow readings while its speed readings were nulled by curation (speed
    absent, zero-with-no-flow, or above the plausibility cap), so a single
    `cell_ok` covering both would either discard usable flow or admit a speed
    baseline that was never estimable.
    """
    min_obs = int(bconf["min_obs_cell"])
    return (
        dry.groupBy("city", "detid", "dow", "tbin", "baseline_res_min")
        .agg(
            F.percentile_approx("speed", 0.5).alias("typical_speed"),
            F.count("speed").alias("n_obs"),
            F.percentile_approx("occ", 0.5).alias("typical_occ"),
            F.percentile_approx("flow", 0.5).alias("typical_flow"),
            F.count("flow").alias("n_obs_flow"),
        )
        .withColumn("cell_ok", F.col("n_obs") >= F.lit(min_obs))
        .withColumn("flow_cell_ok", F.col("n_obs_flow") >= F.lit(min_obs))
    )


def delay_metrics(labelled, ff, profile):
    """Attach both baselines to every interval and derive the two delay metrics.

    free_flow_delay_ratio -- congestion irrespective of cause, relative to the
        physical capability of the link.
    typical_speed_deviation -- anomaly against what this detector normally does
        in this hour of this weekday. THIS is the prediction target: it already
        nets out the recurring commute pattern, so what remains is the unusual
        part, which is where a rain effect would live.

    Both are NULL wherever the underlying baseline failed its observation
    threshold. Nulling is deliberate: a delay ratio computed against a baseline
    built from three observations is worse than no number at all, because it
    looks like a measurement.
    """
    out = (
        labelled.join(
            F.broadcast(
                ff.filter(F.col("free_flow_ok")).select(
                    "city", "detid", "free_flow_speed", "occ_crit"
                )
            ),
            ["city", "detid"],
            "left",
        )
        .join(
            profile.select(
                "city", "detid", "dow", "tbin",
                # Each baseline is gated by its own count, so a detector-hour
                # with usable flow but unusable speed still yields a flow
                # deviation instead of being dropped wholesale.
                F.when(F.col("cell_ok"), F.col("typical_speed")).alias("typical_speed"),
                F.when(F.col("flow_cell_ok"), F.col("typical_flow")).alias("typical_flow"),
            ),
            ["city", "detid", "dow", "tbin"],
            "left",
        )
        .withColumn(
            "free_flow_delay_ratio",
            F.when(
                F.col("speed").isNotNull()
                & F.col("free_flow_speed").isNotNull()
                & (F.col("free_flow_speed") > 0),
                F.lit(1.0) - F.col("speed") / F.col("free_flow_speed"),
            ),
        )
        .withColumn(
            "typical_speed_deviation",
            F.when(
                F.col("speed").isNotNull()
                & F.col("typical_speed").isNotNull()
                & (F.col("typical_speed") > 0),
                F.col("speed") / F.col("typical_speed") - F.lit(1.0),
            ),
        )
        # The demand channel. Read against typical_speed_deviation: flow falling
        # while speed rises means the road got emptier, not faster.
        .withColumn(
            "typical_flow_deviation",
            F.when(
                F.col("flow").isNotNull()
                & F.col("typical_flow").isNotNull()
                & (F.col("typical_flow") > 0),
                F.col("flow") / F.col("typical_flow") - F.lit(1.0),
            ),
        )
        # The two-channel decomposition (speed reduction on free-flowing roads
        # vs capacity reduction on congested ones) is estimated in Phase 5, but
        # it needs each interval's road state, and that is knowable here.
        .withColumn(
            "congested",
            F.when(
                F.col("occ").isNotNull() & F.col("occ_crit").isNotNull(),
                F.col("occ") >= F.col("occ_crit"),
            ),
        )
    )
    return out


def main() -> int:
    spark = config.get_spark("build_baselines")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    bconf = conf["baseline"]

    m = spark.read.parquet(config.spark_path(config.CURATED_MEASUREMENTS))
    rain = spark.read.parquet(config.spark_path(config.RAIN_HOURLY))

    labelled = attach_rain(m, rain)
    labelled.cache()

    total = labelled.count()
    unlabelled = labelled.filter(F.col("is_dry_clean").isNull()).count()
    print(f"measurements       : {total:,}")
    print(f"no rain label      : {unlabelled:,} ({100.0 * unlabelled / total:.2f}%)")
    if unlabelled:
        # An interval outside the ERA5 window cannot be called dry, so it must
        # not silently enter the baseline as if it were.
        print("  (these are excluded from the baseline -- absence of a label is not dryness)")

    dry = labelled.filter(F.col("is_dry_clean"))
    dry.cache()
    n_dry = dry.count()
    print(f"dry (buffered)     : {n_dry:,} ({100.0 * n_dry / total:.1f}%)")

    print("\n[1/4] critical occupancy")
    crit = critical_occupancy(dry, bconf)
    crit.cache()

    print("[2/4] free-flow speed")
    ff = free_flow(dry, crit, bconf)
    ff.cache()
    ff.write.mode("overwrite").partitionBy("city").parquet(
        config.spark_path(config.BASELINE_FREEFLOW)
    )

    print("[3/4] typical speed profile")
    profile = typical_profile(dry, bconf)
    profile.cache()
    profile.write.mode("overwrite").partitionBy("city").parquet(
        config.spark_path(config.BASELINE_PROFILE)
    )

    print("[4/4] delay metrics")
    enriched = delay_metrics(labelled, ff, profile)
    enriched.write.mode("overwrite").partitionBy("city", "year", "month").parquet(
        config.spark_path(config.MEASUREMENTS_DELAY)
    )

    written = spark.read.parquet(config.spark_path(config.MEASUREMENTS_DELAY))
    n_out = written.count()
    print(f"\nwrote {n_out:,} rows to {config.MEASUREMENTS_DELAY}")

    # ---- gates -----------------------------------------------------------
    if n_out != total:
        print(f"FAIL: row drift of {n_out - total:+,} across the baseline joins")
        spark.stop()
        return 1

    n_ff = ff.filter(F.col("free_flow_ok")).count()
    n_det = ff.count()
    print(f"detectors with usable free-flow speed: {n_ff:,} / {n_det:,}")
    if not n_ff:
        print("FAIL: no detector produced a free-flow speed")
        spark.stop()
        return 1

    # The dry-only rule is the project's central methodological claim. If the
    # mask let wet intervals through, everything downstream is quietly wrong,
    # so assert it rather than trusting the filter.
    leaked = dry.filter(F.col("is_wet")).count()
    if leaked:
        print(f"FAIL: {leaked:,} wet intervals reached the dry baseline")
        spark.stop()
        return 1
    print("PASS: baselines built from dry intervals only, row count conserved")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
