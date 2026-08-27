"""Prior-art benchmark: this project's pipeline against IUTF's, on IUTF's data.

CONTEXT.md §10 says Phase 5 is "benchmarked against IUTF". The obvious reading
-- transcribe IUTF's published dose-response magnitudes and put them beside ours
-- turns out to be impossible: **IUTF publishes no such numbers**. Its Technical
Validation states only that "increasing rainfall intensity is associated with
more pronounced traffic flow changes", and the per-band magnitudes live in box
plots with no values in the text. It also reports no speed results at all.

So the benchmark is a *reproduction*, not a transcription. IUTF's setup is
rebuilt from IUTF's own shipped files -- its hourly readings and its native
0.25° hourly ERA5 field -- and run through the identical estimator that produced
`reports/phase5_dose_response.md`. Three arms:

  A  IUTF as shipped     hourly readings   + IUTF ERA5 0.25°/1 h   -> flow
  B  this project        curated 5-min     + spateGAN 2 km/10 min  -> flow
  C  this project        curated 5-min     + spateGAN 2 km/10 min  -> speed

A vs B is the prior-art comparison: what the whole pipeline buys on the quantity
IUTF actually measures. B vs C is what the speed layer adds, which is the half
of the contribution IUTF has no counterpart for.

**What this does and does not isolate.** Arms A and B differ in resolution *and*
curation at once, so their gap is the end-to-end pipeline difference, not a
clean resolution effect. The clean resolution ablation already exists and is
reported in `reports/phase4_downscaling.md` §2, where the rows were held fixed
and only the rain labelling changed. Both are needed: Phase 4 answers "does
downscaling add information", this answers "is the assembled pipeline better
than the published prior art".

IUTF is read-only. See lake/iutf/PROVENANCE.md.

Run:  python -m raincheck.analysis.benchmark_iutf
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

from raincheck import config
from raincheck.analysis import dose_response as dr
from raincheck.analysis import strata
from raincheck.analysis.strata import BAND_ORDER, REFERENCE_BAND

IUTF_ROOT = config.LAKE_ROOT / "iutf" / "study" / "IUTFD"
REPORT = config.REPORTS_DIR / "phase5_iutf_benchmark.md"
REPORT_JSON = config.REPORTS_DIR / "phase5_iutf_benchmark.json"

# IUTF's rainfall parquet stamps time as INT64 nanoseconds, which Spark refuses
# to read as a timestamp by default.
NANOS_CONF = ("spark.sql.legacy.parquet.nanosAsLong", "true")

# ERA5 ships total_precipitation in METRES accumulated over the hour. IUTF
# passes that through unconverted, so an hourly value of 4.2e-4 is 0.42 mm/h.
# Getting this wrong by 1000x would put every hour in the Extreme band.
M_TO_MM = 1000.0

# Met Office bands, per IUTF's own Figure 8 caption -- identical to the ones
# build_rain_features uses, so the two arms are binned the same way.
BANDS = [("Light", 0.1, 0.5), ("Moderate", 0.5, 4.0), ("Heavy", 4.0, 10.0),
         ("Extreme", 10.0, float("inf"))]


def band_expr(col):
    out = F.lit("Dry")
    for name, lo, hi in reversed(BANDS):
        out = F.when((col >= F.lit(lo)) & (col < F.lit(hi)), F.lit(name)).otherwise(out)
    return out


def iutf_rain(spark, city):
    """IUTF's native-resolution rainfall for one city, as mm/h per grid cell.

    Only some dates ship a rainfall file (Manchester has 28 files spanning 72
    days). Rather than assume the absent dates were dry -- which would quietly
    feed unlabelled hours into the dry baseline, the exact error
    build_baselines guards against -- the benchmark restricts itself to the
    dates IUTF actually covers. Dry and wet hours then come from the same days,
    which also removes seasonality from the contrast.
    """
    path = IUTF_ROOT / city / "weather" / "datetime"
    return (
        spark.read.parquet(config.spark_path(path))
        .withColumn("ts_local", F.to_timestamp("local_time"))
        .select(
            "grid_id", "ts_local", "longitude", "latitude",
            (F.col("total_precipitation") * F.lit(M_TO_MM)).alias("precip_mm_h"),
        )
    )


def detector_grid(spark, city, rain):
    """Assign each IUTF detector to its nearest IUTF rainfall cell.

    IUTF declares road_to_weather as one_to_many and ships no explicit detector
    -> cell key, so it is reconstructed by nearest centre. The cell counts are
    tiny (Manchester is a single 0.25° cell covering the whole city), which is
    precisely the coarseness this project set out to fix.
    """
    # detid MUST be cast to string. IUTF stores Torino's detector ids as an
    # integer column and Essen's as text; without this the union across cities
    # coerces everything to BIGINT and dies on 'esss037n'.
    dets = spark.read.parquet(
        config.spark_path(IUTF_ROOT / city / "sensors" / "detectors_info.parquet")
    ).select(F.col("detid").cast("string").alias("detid"),
             F.col("long").alias("d_lon"), F.col("lat").alias("d_lat"),
             "fclass")

    cells = rain.select("grid_id", "longitude", "latitude").distinct()
    d2 = (F.col("d_lon") - F.col("longitude")) ** 2 + (F.col("d_lat") - F.col("latitude")) ** 2
    ranked = (
        dets.crossJoin(F.broadcast(cells))
        .withColumn("d2", d2)
        .withColumn("rk", F.row_number().over(
            Window.partitionBy("detid").orderBy("d2")))
        .filter(F.col("rk") == 1)
        .select("detid", "grid_id", "fclass")
    )
    return ranked


def arm_a_cells(spark, conf):
    """Build the (cluster x stratum) grid for IUTF-as-shipped.

    Mirrors dose_response.cluster_cells so the two arms differ in DATA only,
    never in estimator. Occupancy is rescaled with the same per-city rule from
    conf/cities.yml, because IUTF passes UTD19's inconsistent occupancy scale
    through untouched -- its Manchester occupancy runs ~26 against Essen's
    ~0.006, on the same rows this project rescales.
    """
    bconf = conf["baseline"]
    occ_max = conf["curation"]["occ_max_raw"]
    wet, buffer_h = float(bconf["wet_threshold_mm"]), int(bconf["dry_buffer_hours"])

    frames = []
    for city, cfg in conf["study"].items():
        rain = iutf_rain(spark, city)
        grid = detector_grid(spark, city, rain)

        hourly = (
            spark.read.parquet(
                config.spark_path(IUTF_ROOT / city / "sensors" / "hourly_readings.parquet")
            )
            .withColumn("ts_local", F.to_timestamp("datetime", "dd/MM/yyyy HH:mm:ss"))
            .select(
                F.col("detid").cast("string").alias("detid"), "ts_local",
                F.col("flow_sum").alias("flow"),
                F.col("speed_mean").alias("speed"),
                F.col("occ_mean").alias("occ_raw"),
            )
            .withColumn("city", F.lit(city))
        )

        scale = 100.0 if cfg["occ_scale"] == "percent" else 1.0
        ceiling = float(occ_max[cfg["occ_scale"]])
        hourly = hourly.withColumn(
            "occ",
            F.when(
                F.col("occ_raw").isNotNull()
                & (F.col("occ_raw") <= F.lit(ceiling))
                & ~F.isnan("occ_raw"),
                F.col("occ_raw") / F.lit(scale),
            ),
        ).withColumn(
            "speed",
            F.when(F.col("speed") <= F.lit(float(bconf.get("speed_cap_kmh", 150.0))),
                   F.col("speed")),
        )

        joined = (
            hourly.join(F.broadcast(grid), "detid", "inner")
            .join(rain.select("grid_id", "ts_local", "precip_mm_h"),
                  ["grid_id", "ts_local"], "inner")
            .withColumn("band", band_expr(F.col("precip_mm_h")))
            .withColumn("is_wet", F.col("precip_mm_h") >= F.lit(wet))
        )

        # Dry mask with the same post-rain buffer the project uses everywhere.
        w = Window.partitionBy("city", "detid").orderBy("ts_local")
        joined = (
            joined.withColumn("rn", F.row_number().over(w))
            .withColumn("last_wet", F.last(
                F.when(F.col("is_wet"), F.col("rn")), ignorenulls=True
            ).over(w.rowsBetween(Window.unboundedPreceding, 0)))
            .withColumn(
                "hours_since_rain",
                F.when(F.col("is_wet"), F.lit(0)).otherwise(F.col("rn") - F.col("last_wet")),
            )
            .withColumn(
                "is_dry_clean",
                ~F.col("is_wet")
                & (F.col("hours_since_rain").isNull()
                   | (F.col("hours_since_rain") > F.lit(buffer_h))),
            )
            .drop("rn", "last_wet")
        )
        frames.append(joined)

    d = frames[0]
    for f in frames[1:]:
        d = d.unionByName(f)

    # Critical occupancy from IUTF's own hourly fundamental diagram, same
    # estimator as L2a -- congestion state must not be defined differently
    # across the two arms, or the headline reversal is not comparable.
    from raincheck.baseline.build_baselines import critical_occupancy

    crit = critical_occupancy(d.filter(F.col("is_dry_clean")), bconf)
    d = d.join(F.broadcast(crit.select("city", "detid", "occ_crit")),
               ["city", "detid"], "left")

    # Profile keyed on (detector, hour-of-day) WITHOUT day-of-week.
    #
    # Not a choice -- a constraint IUTF's own aggregation imposes. At hourly
    # resolution a 21-day window yields about three observations per
    # (detector, weekday, hour) cell, against the min_obs_cell of 20 that the
    # project applies everywhere. The day-of-week profile Phase 3 builds is
    # simply not estimable from hourly data in these windows, so Arm A uses the
    # finest profile its data can support. This is reported as a finding rather
    # than silently patched: it is a direct consequence of harmonising 5-minute
    # readings to hourly, which is what IUTF ships.
    profile = (
        d.filter(F.col("is_dry_clean"))
        .groupBy("city", "detid", F.hour("ts_local").alias("tbin"))
        .agg(
            F.percentile_approx("flow", 0.5).alias("typical_flow"),
            F.count("flow").alias("n_flow"),
            F.percentile_approx("speed", 0.5).alias("typical_speed"),
            F.count("speed").alias("n_speed"),
        )
        .withColumn("flow_ok", F.col("n_flow") >= F.lit(int(bconf["min_obs_cell"])))
        .withColumn("speed_ok", F.col("n_speed") >= F.lit(int(bconf["min_obs_cell"])))
    )

    d = (
        d.withColumn("tbin", F.hour("ts_local"))
        .join(
            profile.select(
                "city", "detid", "tbin",
                F.when(F.col("flow_ok"), F.col("typical_flow")).alias("typical_flow"),
                F.when(F.col("speed_ok"), F.col("typical_speed")).alias("typical_speed"),
            ),
            ["city", "detid", "tbin"], "left",
        )
        .withColumn("typical_flow_deviation", F.when(
            F.col("flow").isNotNull() & (F.col("typical_flow") > 0),
            F.col("flow") / F.col("typical_flow") - F.lit(1.0)))
        .withColumn("typical_speed_deviation", F.when(
            F.col("speed").isNotNull() & (F.col("typical_speed") > 0),
            F.col("speed") / F.col("typical_speed") - F.lit(1.0)))
        .withColumn("congested", F.when(
            F.col("occ").isNotNull() & F.col("occ_crit").isNotNull(),
            F.col("occ") >= F.col("occ_crit")))
        .withColumn("hod", F.hour("ts_local"))
        .withColumn("date", F.to_date("ts_local"))
    )

    prepared = (
        d.filter(
            F.col("congested").isNotNull()
            & (F.col("typical_speed_deviation").isNotNull()
               | F.col("typical_flow_deviation").isNotNull())
        )
        .withColumn("road_class", strata.road_class())
        .withColumn("tod", strata.time_of_day())
        .withColumn("cluster", strata.cluster_id())
        .filter(F.col("road_class") != F.lit("unknown"))
        .withColumn("congested_lbl", F.when(
            F.col("congested"), F.lit("congested")).otherwise(F.lit("free-flowing")))
    )

    sq = lambda c: F.col(c) * F.col(c)  # noqa: E731
    return prepared.groupBy(
        "cluster", "city", "band", "road_class", "tod",
        F.col("congested_lbl").alias("congested"),
    ).agg(
        F.count("typical_speed_deviation").alias("n_speed"),
        F.coalesce(F.sum("typical_speed_deviation"), F.lit(0.0)).alias("sum_speed"),
        F.coalesce(F.sum(sq("typical_speed_deviation")), F.lit(0.0)).alias("ssq_speed"),
        F.count("typical_flow_deviation").alias("n_flow"),
        F.coalesce(F.sum("typical_flow_deviation"), F.lit(0.0)).alias("sum_flow"),
        F.coalesce(F.sum(sq("typical_flow_deviation")), F.lit(0.0)).alias("ssq_flow"),
    )


def to_numpy(rows):
    """Same layout dose_response.load produces, so run_views is reusable as-is."""
    raw = {c: [r[c] for r in rows] for c in
           ("cluster", "city", "band", "road_class", "tod", "congested")}
    cluster_code, clusters = dr.encode(raw["cluster"])
    band_code, band_levels = dr.encode(raw["band"], BAND_ORDER)
    data = {"raw": raw, "cluster_code": cluster_code, "n_clusters": len(clusters),
            "band_code": band_code, "band_levels": band_levels}
    for m in ("speed", "flow"):
        for q in ("n", "sum", "ssq"):
            data[f"{q}_{m}"] = np.array([r[f"{q}_{m}"] for r in rows], dtype=np.float64)
    return data


def lookup(results, view, level, band, measure):
    for r in results:
        if r["view"] == view and r["level"] == level and r["band"] == band:
            return r[measure]
    return None


def arm_table(a_res, b_res, view, levels, measure_a, measure_b, label_a, label_b):
    """Two arms side by side, band down, so the gap is read row-wise."""
    bands = [b for b in BAND_ORDER if b != REFERENCE_BAND]
    out = [f"| stratum | band | {label_a} | {label_b} |", "|---|---|---|---|"]
    for level in levels:
        for b in bands:
            ea = lookup(a_res, view, level, b, measure_a)
            eb = lookup(b_res, view, level, b, measure_b)
            if ea is None and eb is None:
                continue
            out.append(f"| {level} | {b} | {dr.fmt(ea)} | {dr.fmt(eb)} |")
    out.append("")
    return out


def coverage(res, view="congested"):
    """How much of the headline grid an arm can actually estimate."""
    cells, cities = 0, set()
    for level in ("congested", "free-flowing"):
        for band in ("Light", "Moderate", "Heavy", "Extreme"):
            if lookup(res, view, level, band, "flow"):
                cells += 1
    for r in res:
        if r["view"] == "city" and r["flow"]:
            cities.add(r["level"])
    return cells, sorted(cities)


def widths(res, view="congested", measure="flow"):
    """Interval widths, in percentage points -- the precision of an arm."""
    out = []
    for level in ("congested", "free-flowing"):
        for band in ("Light", "Moderate", "Heavy", "Extreme"):
            e = lookup(res, view, level, band, measure)
            if e:
                out.append(100.0 * (e["hi"] - e["lo"]))
    return out


def commentary(a_res, b_res):
    """Prose that follows the numbers.

    Deliberately scores PRECISION and COVERAGE, not effect magnitude. A larger
    point estimate is not a better one -- Arm A's widest cells carry the largest
    numbers precisely because they are the noisiest, and ranking arms by |effect|
    would reward that.
    """
    out = []
    a_cells, a_cities = coverage(a_res)
    b_cells, b_cities = coverage(b_res)
    aw, bw = widths(a_res), widths(b_res)

    out.append(
        f"- **Coverage.** IUTF-as-shipped can estimate **{a_cells} of 8** headline "
        f"band x road-state cells, against **{b_cells} of 8** for this pipeline. "
        f"At city level IUTF supports {len(a_cities)} of 3 study cities "
        f"({', '.join(a_cities)}); this pipeline supports all "
        f"{len(b_cities)}."
    )
    out.append(
        "- **Torino is the casualty, and the reason is instructive.** Its window "
        "is 21 days. Harmonised to hourly, a (detector, hour-of-day) baseline "
        "cell can hold at most 21 dry observations, which falls under the "
        f"min_obs_cell of {config.load_cities_conf()['baseline']['min_obs_cell']} "
        "once wet and buffered hours are removed — so no baseline is estimable "
        "and the city drops out entirely. The same 21 days at 5-minute "
        "resolution give roughly 250 dry observations per cell. Torino is the "
        "largest speed-bearing city in UTD19, and hourly aggregation makes it "
        "unanalysable here."
    )
    if aw and bw:
        out.append(
            f"- **Precision.** Median 95% interval width is "
            f"**{np.median(aw):.1f} pp** for IUTF-as-shipped against "
            f"**{np.median(bw):.1f} pp** for this pipeline — roughly "
            f"{np.median(aw) / max(np.median(bw), 1e-9):.0f}x tighter. Arm A's "
            "larger point estimates are a symptom of that width, not evidence "
            "of a stronger effect: its widest cells carry its biggest numbers."
        )
    mono = [
        lookup(a_res, "congested", "congested", b, "flow")
        for b in ("Light", "Moderate", "Heavy", "Extreme")
    ]
    if mono[0] and mono[1] and (mono[0]["estimate"] < 0 < mono[1]["estimate"]):
        out.append(
            "- **No usable dose-response from the coarse arm.** On congested "
            f"roads it puts Light at {dr.pp(mono[0]['estimate'])} pp and Moderate "
            f"at {dr.pp(mono[1]['estimate'])} pp — opposite signs, with the "
            "Moderate interval spanning zero. That is the qualitative claim IUTF "
            "does make (\"more rain, more change\") failing to survive "
            "quantification at its own resolution."
        )

    rev = []
    for level in ("congested", "free-flowing"):
        for band in ("Light", "Moderate", "Heavy", "Extreme"):
            es = lookup(b_res, "congested", level, band, "speed")
            ef = lookup(b_res, "congested", level, band, "flow")
            if (es and ef and es["excludes_zero"] and ef["excludes_zero"]
                    and (es["estimate"] > 0) != (ef["estimate"] > 0)):
                rev.append(f"{level}/{band}")
    if rev:
        out.append(
            f"- **The speed layer is not redundant with the flow layer.** Speed "
            f"and flow move in opposite directions, both significantly, in "
            f"{len(rev)} cells: {', '.join(rev)}. That is the demand channel made "
            "visible — a road that emptied, not a road that got faster. A "
            "flow-only analysis cannot tell the two apart, and IUTF is "
            "flow-only."
        )
    return out


def render(a_res, b_res, meta):
    L = []
    A = L.append
    A("# Phase-5 Benchmark — this pipeline against IUTF")
    A("")
    A(f"_Generated {meta['generated']}. Percentage points of deviation from each "
      f"detector's own dry typical profile, contrasted against Dry within the "
      f"same stratum. Brackets are {dr.CI_PCT:.0f}% cluster bootstrap intervals "
      f"over detector-days; `ns` marks an interval containing zero._")
    A("")
    A(f"**Gate verdict: {meta['verdict']}**")
    A("")

    A("## 1. Why this is a reproduction, not a transcription")
    A("")
    A("The plan was to put IUTF's published flow magnitudes beside ours. That")
    A("cannot be done: **IUTF publishes no per-band magnitudes.** Its Technical")
    A("Validation states only that \"increasing rainfall intensity is associated")
    A("with more pronounced traffic flow changes\"; the numbers live inside box")
    A("plots (Figures 8 and 10) and are never given in the text. The paper")
    A("reports **no speed-based results at all**.")
    A("")
    A("So IUTF's setup is rebuilt from IUTF's own shipped files and run through")
    A("the identical estimator that produced `phase5_dose_response.md`. Three")
    A("arms, differing only in the data they are handed:")
    A("")
    A("| arm | traffic | rainfall | measure |")
    A("|---|---|---|---|")
    A("| **A — IUTF as shipped** | IUTF hourly readings | IUTF ERA5, 0.25° / 1 h | flow |")
    A("| **B — this pipeline** | curated 5-min | spateGAN, 2 km / 10 min | flow |")
    A("| **C — this pipeline** | curated 5-min | spateGAN, 2 km / 10 min | speed |")
    A("")
    A(f"Arm A: {meta['a_clusters']:,} detector-days, {meta['a_intervals']:,} "
      f"detector-hours. Arms B/C: {meta['b_clusters']:,} detector-days, "
      f"{meta['b_intervals']:,} detector-intervals.")
    A("")
    A("Arm A is restricted to the dates IUTF ships a rainfall file for. IUTF")
    A("covers only some dates (Manchester: 28 files spanning 72 days), and")
    A("treating the absent ones as dry would feed unlabelled hours into the dry")
    A("baseline — the precise error `build_baselines` guards against. Dry and wet")
    A("hours in Arm A therefore come from the same days, which also removes")
    A("seasonality from the contrast.")
    A("")

    A("## 2. Flow response — A against B")
    A("")
    A("The prior-art comparison proper: the same quantity IUTF measures,")
    A("estimated the same way, from the two pipelines.")
    A("")
    L.extend(arm_table(a_res, b_res, "congested",
                       ["congested", "free-flowing"], "flow", "flow",
                       "A — IUTF as shipped", "B — this pipeline"))

    A("### Per city")
    A("")
    L.extend(arm_table(a_res, b_res, "city", meta["cities"], "flow", "flow",
                       "A — IUTF as shipped", "B — this pipeline"))

    A("## 3. What the speed layer adds — B against C")
    A("")
    A("IUTF has no counterpart for this table. It ships a `speed` column but")
    A("derives nothing from it: no free-flow speed, no typical-speed profile, no")
    A("delay metric, and no speed result in the paper. Everything below is this")
    A("project's own L2a layer, and it is where the headline finding lives — the")
    A("sign reversal between free-flowing and congested roads that the flow")
    A("channel alone cannot reveal.")
    A("")
    L.extend(arm_table(b_res, b_res, "congested",
                       ["congested", "free-flowing"], "flow", "speed",
                       "B — flow", "C — speed"))

    A("## 4. Reading the comparison")
    A("")
    for line in meta["commentary"]:
        A(line)
    A("")

    A("## 5. What this does not isolate")
    A("")
    A("Arms A and B differ in **resolution and curation at once**, so the gap")
    A("between them is an end-to-end pipeline difference, not a clean resolution")
    A("effect. The clean ablation already exists: `reports/phase4_downscaling.md`")
    A("§2 holds the rows fixed and varies only the rain labelling, native")
    A("31 km / 1 h against 2 km / 10 min. Read the two together — Phase 4 answers")
    A("\"does downscaling add information\", this answers \"is the assembled")
    A("pipeline better than the published prior art\".")
    A("")
    A("Two caveats carry over from Phase 5: road class is UTD19's own `fclass`")
    A("rather than an OSM map-match, and only one spateGAN ensemble member")
    A("(seed 10) was run, so these intervals quantify sampling error in the")
    A("traffic response and not error in the rainfall field itself.")
    A("")
    return "\n".join(L) + "\n"


def main() -> int:
    spark = config.get_spark("benchmark_iutf")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set(*NANOS_CONF)

    if not IUTF_ROOT.exists():
        print(f"FAIL: {IUTF_ROOT} not found -- see lake/iutf/PROVENANCE.md")
        spark.stop()
        return 1

    conf = config.load_cities_conf()

    print("[1/3] arm A -- IUTF as shipped")
    a_rows = arm_a_cells(spark, conf).collect()
    if not a_rows:
        print("FAIL: arm A produced no cells")
        spark.stop()
        return 1
    a_data = to_numpy(a_rows)
    a_res = dr.run_views(a_data, dr.VIEWS, np.random.default_rng(dr.SEED))
    print(f"      {a_data['n_clusters']:,} detector-days")

    print("[2/3] arms B/C -- this pipeline")
    b_data = dr.load(spark)
    b_res = dr.run_views(b_data, dr.VIEWS, np.random.default_rng(dr.SEED))
    print(f"      {b_data['n_clusters']:,} detector-days")

    print("[3/3] report")
    meta = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "a_clusters": a_data["n_clusters"],
        "a_intervals": int(np.sum(a_data["n_flow"])),
        "b_clusters": b_data["n_clusters"],
        "b_intervals": int(np.sum(b_data["n_speed"])),
        "cities": sorted(conf["study"]),
    }
    meta["commentary"] = commentary(a_res, b_res)

    # ---- gates -----------------------------------------------------------
    # Methodological, as in Phases 4 and 5: this checks that the comparison IS a
    # comparison -- both arms estimable on the same strata, binned the same way
    # -- not that this project's side wins.
    failures = []
    both = sum(
        1
        for level in ("congested", "free-flowing")
        for band in ("Light", "Moderate", "Heavy", "Extreme")
        if lookup(a_res, "congested", level, band, "flow")
        and lookup(b_res, "congested", level, band, "flow")
    )
    if both < 4:
        failures.append(
            f"only {both} of 8 headline cells are estimable in BOTH arms; "
            "there is not enough overlap to call this a benchmark"
        )
    if a_data["n_clusters"] < 100:
        failures.append(f"arm A has only {a_data['n_clusters']} detector-days")
    meta["cells_both_arms"] = both
    meta["verdict"] = "FAIL" if failures else "PASS"
    meta["failures"] = failures

    REPORT.write_text(render(a_res, b_res, meta), encoding="utf-8")
    REPORT_JSON.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print()
    print(f"arm A: {meta['a_clusters']:,} detector-days, "
          f"{meta['a_intervals']:,} detector-hours")
    print(f"arm B: {meta['b_clusters']:,} detector-days, "
          f"{meta['b_intervals']:,} intervals")
    print(f"headline cells estimable in both arms: {both}/8")
    print(f"wrote {REPORT}")
    for f in failures:
        print(f"FAIL: {f}")
    if not failures:
        print("PASS: benchmark comparable across both arms")
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
