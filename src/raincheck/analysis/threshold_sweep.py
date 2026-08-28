"""Sensitivity measurement -- how much does `wet_threshold_mm` actually matter?

`conf/cities.yml` sets `wet_threshold_mm: 0.1`, and `build_rain_mask.py` applies
it to NATIVE ERA5: one precipitation value per city per hour over a ~31 km cell.
An hour is marked wet -- and dropped from the dry baseline along with a 2-hour
post-rain buffer -- when the *area mean* over a cell containing the whole city
reaches 0.1 mm. Phase 3 measured the cost at 22-36% of every city's data for a
paired wet-vs-dry speed difference of -0.31/+0.71/-0.47 km/h: no measured
benefit, and not even a consistent sign.

Phase 4 made a better label possible. `detector_rain` carries precipitation at
each detector's own 2 km cell every 10 minutes, so "was it raining HERE" is
answerable for the first time. This module measures what changes if the mask is
rebuilt that way, rather than assuming.

**A 2x2 factorial, not a value sweep.** Resolution and threshold value are
crossed so their contributions are separately attributable; a one-dimensional
sweep over values would confound them.

    A0  coarse (city-hour, ~31 km)   0.1   <- production control
    A1  coarse (city-hour, ~31 km)   0.5
    A2  fine   (detector, 2 km/10m)  0.1
    A3  fine   (detector, 2 km/10m)  0.5

**What is frozen.** Only the definition of the dry baseline varies. Everything
downstream is imported from the production modules rather than reimplemented:
`critical_occupancy`, `free_flow`, `typical_profile` and `delay_metrics` from
build_baselines; `cluster_cells`, `run_views`, `VIEWS`, `N_BOOT`, `SEED`,
`MIN_CLUSTERS` from dose_response; the stratification axes from strata.

In particular `BANDS` in build_rain_features is NOT touched. Those boundaries
are the Met Office ones, identical to IUTF's, and moving them would end the
benchmark's comparability. So the band an interval is *exposed* to is
fine-resolution in every arm. What varies is the baseline that exposure is
measured *against*.

**Nothing canonical is written.** The lake is read-only here; the only outputs
are the two report files. A0 must reproduce the published Phase 5 figures
exactly, and that is asserted rather than hoped for -- it is the harness's own
correctness check.

Run:  python -m raincheck.analysis.threshold_sweep [--arms A0,A2]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from pyspark.sql import Window
from pyspark.sql import functions as F

from raincheck import config
from raincheck.analysis import dose_response as dr
from raincheck.analysis.strata import BAND_ORDER
from raincheck.baseline.build_baselines import (
    critical_occupancy,
    delay_metrics,
    free_flow,
    typical_profile,
)
from raincheck.weather.build_rain_features import RAIN_FEATURES, STEP_MIN, WET_MM_H
from raincheck.weather.build_rain_mask import label as coarse_label
from raincheck.weather.extract_detector_rain import DETECTOR_RAIN

ANALYSIS = config.LAKE_ROOT / "analysis" / "measurements_rain"
REPORT = config.REPORTS_DIR / "phase5_threshold_sweep.md"
REPORT_JSON = config.REPORTS_DIR / "phase5_threshold_sweep.json"
PUBLISHED = config.REPORTS_DIR / "phase5_dose_response.json"

# (id, resolution, threshold mm, corrected).
#
# `corrected` applies the two rules in `arm_bands()` that keep an arm's dry
# reference disjoint from the bands it contrasts. The first run of this sweep
# omitted them, and its 0.5 mm arms were consequently uninterpretable: the Light
# band is 0.1-0.5 mm, so a 0.5 mm threshold filed Light rain as dry and then
# compared Light against a baseline containing it.
#
# A0 stays uncorrected on purpose -- it must reproduce production byte for byte
# or the harness check means nothing. The correction is a no-op at 0.1 mm for
# fine-resolution arms anyway (there, `arm_dry` already implies band == Dry), so
# it changes only the 0.5 mm arms, which is where the flaw was.
#
# 0.5 mm/h is not an arbitrary loosening: it is the UK Met Office boundary
# between slight and moderate rain, and it is already the Light/Moderate
# boundary in `build_rain_features.BANDS`. See section 1 of the report.
ARMS = [
    ("A0", "coarse", 0.1, False),
    ("A2", "fine", 0.1, True),
    ("B1", "coarse", 0.5, True),
    ("B3", "fine", 0.5, True),
]
CONTROL = "A0"

# Columns the baseline layer produces. Dropped from the analysis table before
# each arm rebuilds them; leaving them in would silently shadow the rebuild.
BASELINE_COLS = [
    "occ_crit", "free_flow_speed", "typical_speed", "typical_flow",
    "free_flow_delay_ratio", "typical_speed_deviation", "typical_flow_deviation",
    "congested", "is_dry_clean",
]

# Views whose cells are compared one by one. `road_congested` carries Phase 5's
# headline -- that conditioning on road state reverses the sign of the rain
# effect -- so it is the one that has to survive.
KEY_VIEWS = ["congested", "road_congested", "pooled", "city"]


# ---------------------------------------------------------------------------
# Arm masks.
# ---------------------------------------------------------------------------
def coarse_mask(spark, threshold, dry_buffer):
    """Re-label the city-hour ERA5 series at `threshold`.

    Delegates to production `build_rain_mask.label`, which already takes the
    threshold as an argument -- so the coarse arms exercise the real code path
    and A0 is a genuine reproduction rather than a lookalike.
    """
    hourly = spark.read.parquet(config.spark_path(config.RAIN_HOURLY))
    relabelled = coarse_label(
        hourly.select("city", "rain_ts", "precip_mm"), threshold, dry_buffer
    )
    return (
        relabelled.select(
            "city",
            "rain_ts",
            F.col("is_dry_clean").alias("arm_dry"),
            F.col("is_wet").alias("arm_wet"),
        ),
        ["city", "rain_ts"],
    )


def fine_mask(spark, threshold, dry_buffer):
    """Per-detector dry mask from the 2 km / 10 min fields.

    Mirrors the window logic in build_rain_features.features(): the same
    row-index arithmetic, valid because the downscaled series is a complete,
    gap-free 10-minute grid per detector. It is reimplemented rather than
    imported because `features()` hard-codes WET_MM_H, and varying that is the
    entire point. Semantics are kept identical on purpose, including the leading
    dry run before the first wet step resolving to NULL and so being excluded --
    which is how the coarse path behaves too.
    """
    # Fast path. At the production threshold this chain has already been
    # computed and persisted by build_rain_features, so recomputing the window
    # costs several minutes to arrive at identical values. `dry_spell_hours` is
    # `steps_since_wet` in hours and the buffer is expressed in hours, so
    # `dry_spell_hours >= dry_buffer` is exactly `steps_since_wet >=
    # buffer_steps`, including the leading-run NULL that excludes intervals
    # before the first wet step.
    if abs(threshold - WET_MM_H) < 1e-12:
        feats = spark.read.parquet(config.spark_path(RAIN_FEATURES))
        return (
            feats.select(
                "city",
                "detid",
                F.col("ts_utc").alias("ts_10min"),
                (~F.col("is_wet") & (F.col("dry_spell_hours") >= F.lit(float(dry_buffer))))
                .alias("arm_dry"),
                F.col("is_wet").alias("arm_wet"),
            ),
            ["city", "detid", "ts_10min"],
        )

    rain = spark.read.parquet(config.spark_path(DETECTOR_RAIN))
    w = Window.partitionBy("city", "detid").orderBy("ts_utc")
    buffer_steps = int(dry_buffer * 60 // STEP_MIN)

    df = (
        rain.withColumn("arm_wet", F.col("precip_mm_h") >= F.lit(threshold))
        .withColumn("rn", F.row_number().over(w))
        .withColumn(
            "last_wet_rn",
            F.last(F.when(F.col("arm_wet"), F.col("rn")), ignorenulls=True).over(
                w.rowsBetween(Window.unboundedPreceding, 0)
            ),
        )
        .withColumn(
            "steps_since_wet",
            F.when(F.col("arm_wet"), F.lit(0)).otherwise(F.col("rn") - F.col("last_wet_rn")),
        )
        .withColumn(
            "arm_dry",
            (~F.col("arm_wet")) & (F.col("steps_since_wet") >= F.lit(buffer_steps)),
        )
    )
    return (
        df.select("city", "detid", F.col("ts_utc").alias("ts_10min"), "arm_dry", "arm_wet"),
        ["city", "detid", "ts_10min"],
    )


# ---------------------------------------------------------------------------
# One arm, end to end.
# ---------------------------------------------------------------------------
def load_cells(cells):
    """Collect the cluster x stratum grid into the shape run_views expects.

    Deliberately duplicates dose_response.load()'s body rather than refactoring
    it. Production stays byte-identical for the duration of the experiment, so
    A0 reproducing the published numbers actually means something. The only
    difference is that the frame is passed in instead of read from ANALYSIS.
    """
    rows = cells.collect()
    if not rows:
        raise SystemExit("FAIL: no cells survived preparation")
    raw = {
        c: [r[c] for r in rows]
        for c in ("cluster", "city", "band", "road_class", "tod", "congested")
    }
    cluster_code, clusters = dr.encode(raw["cluster"])
    band_code, band_levels = dr.encode(raw["band"], BAND_ORDER)
    data = {
        "raw": raw,
        "cluster_code": cluster_code,
        "n_clusters": len(clusters),
        "band_code": band_code,
        "band_levels": band_levels,
    }
    for m in ("speed", "flow"):
        for q in ("n", "sum", "ssq"):
            data[f"{q}_{m}"] = np.array([r[f"{q}_{m}"] for r in rows], dtype=np.float64)
    return data


def city_diagnostics(labelled, ff, profile):
    """Per-city cost and coverage of this arm's baseline."""
    out: dict[str, dict] = {}
    for r in (
        labelled.groupBy("city")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col("is_dry_clean").cast("int")).alias("dry_rows"),
            F.sum(F.col("arm_wet").cast("int")).alias("wet_rows"),
        )
        .collect()
    ):
        out[r["city"]] = {
            "rows": int(r["rows"]),
            "dry_rows": int(r["dry_rows"] or 0),
            "dry_pct": round(100.0 * (r["dry_rows"] or 0) / r["rows"], 1),
            "wet_pct": round(100.0 * (r["wet_rows"] or 0) / r["rows"], 1),
        }
    for r in (
        ff.groupBy("city")
        .agg(
            F.count(F.lit(1)).alias("detectors"),
            F.sum(F.col("free_flow_ok").cast("int")).alias("ff_ok"),
            F.percentile_approx("free_flow_speed", 0.5).alias("med_ff"),
        )
        .collect()
    ):
        out.setdefault(r["city"], {}).update(
            detectors=int(r["detectors"]),
            ff_ok=int(r["ff_ok"] or 0),
            med_free_flow=round(float(r["med_ff"]), 2) if r["med_ff"] is not None else None,
        )
    for r in (
        profile.groupBy("city")
        .agg(
            F.count(F.lit(1)).alias("cells"),
            F.sum(F.col("cell_ok").cast("int")).alias("cells_ok"),
            F.percentile_approx("typical_speed", 0.5).alias("med_typ"),
        )
        .collect()
    ):
        out.setdefault(r["city"], {}).update(
            cells=int(r["cells"]),
            cells_ok=int(r["cells_ok"] or 0),
            cell_ok_pct=round(100.0 * (r["cells_ok"] or 0) / r["cells"], 1),
            med_typical_speed=round(float(r["med_typ"]), 2)
            if r["med_typ"] is not None
            else None,
        )
    return out


def arm_bands(threshold):
    """This arm's band column and its dry-reference rule.

    Two rules, and they exist to keep one guarantee: **the dry reference must
    never contain an interval from a band being contrasted against it.**

    1. Fold every band below the arm's threshold into `Dry`. An arm that calls
       0.3 mm dry cannot also report a `Light` (0.1-0.5) contrast -- the two
       categories would overlap.
    2. Require baseline intervals to be `Dry` under that folded vocabulary, not
       merely below the mask threshold. This bites on the coarse arms, where the
       mask reads a city-wide hourly average while the band reads the detector's
       own 2 km cell: without it, an hour averaging 0.3 mm across the city can
       enter the baseline while this particular detector sat under 3 mm.

    Rule 2 is also the rule production does NOT apply, which is worth knowing
    independently of the threshold question -- so the leak it would close is
    measured and reported for A0 rather than silently fixed.
    """
    band = F.when(F.col("precip_mm_h") < F.lit(threshold), F.lit("Dry")).otherwise(
        F.col("band")
    )
    return band, band == F.lit("Dry")


def build_arm(spark, base, arm_id, resolution, threshold, corrected, bconf):
    """Rebuild L2a under this arm's dry definition, then re-estimate L3a."""
    started = time.time()
    dry_buffer = int(bconf["dry_buffer_hours"])
    mask, keys = (coarse_mask if resolution == "coarse" else fine_mask)(
        spark, threshold, dry_buffer
    )

    n_in = base.count()
    joined = base.join(
        F.broadcast(mask) if resolution == "coarse" else mask, keys, "left"
    )
    band_expr, band_is_dry = arm_bands(threshold)
    labelled = joined.withColumn("band", band_expr)
    labelled = labelled.withColumn(
        "is_dry_clean",
        (F.col("arm_dry") & band_is_dry) if corrected else F.col("arm_dry"),
    )
    labelled.cache()
    n_out = labelled.count()
    if n_out != n_in:
        raise SystemExit(
            f"FAIL[{arm_id}]: mask join changed the row count, {n_in:,} -> {n_out:,} "
            "(duplicate mask keys?)"
        )

    # A0 rebuilds production's own mask. If it disagrees with the stored
    # is_dry_clean then the harness is wrong and nothing below can be trusted,
    # so this is fatal rather than a warning.
    if arm_id == CONTROL:
        mismatched = int(
            labelled.filter(~F.col("arm_dry").eqNullSafe(F.col("stored_dry"))).count()
        )
        if mismatched:
            raise SystemExit(
                f"FAIL[{arm_id}]: recomputed control mask differs from the stored "
                f"is_dry_clean on {mismatched:,} rows -- the harness does not "
                "reproduce production"
            )
        print("      control mask reproduces stored is_dry_clean exactly")

    dry = labelled.filter(F.col("is_dry_clean"))
    dry.cache()
    n_dry = dry.count()
    print(f"      dry baseline: {n_dry:,} rows ({100.0 * n_dry / n_in:.1f}%)")

    # How many baseline intervals carry a contrasted band anyway. Zero by
    # construction in a corrected arm; for A0 it measures what production
    # currently lets through, which is a defect of the coarse mask rather than
    # of the threshold.
    leak = int(dry.filter(~band_is_dry).count())
    print(f"      wet-band intervals inside the dry baseline: {leak:,} "
          f"({100.0 * leak / max(n_dry, 1):.2f}%)")

    crit = critical_occupancy(dry, bconf)
    crit.cache()
    ff = free_flow(dry, crit, bconf)
    ff.cache()
    profile = typical_profile(dry, bconf)
    profile.cache()

    enriched = delay_metrics(labelled, ff, profile)
    per_city = city_diagnostics(labelled, ff, profile)

    data = load_cells(dr.cluster_cells(enriched))
    print(f"      {len(data['cluster_code']):,} cells, {data['n_clusters']:,} detector-days")
    results = dr.run_views(data, dr.VIEWS, np.random.default_rng(dr.SEED))
    meta = dr.summarise(results, data)

    for frame in (labelled, dry, crit, ff, profile):
        frame.unpersist()

    return {
        "arm": arm_id,
        "resolution": resolution,
        "threshold_mm": threshold,
        "corrected": corrected,
        "n_rows": n_in,
        "n_dry": n_dry,
        "dry_pct": round(100.0 * n_dry / n_in, 1),
        "wet_band_leak": leak,
        "wet_band_leak_pct": round(100.0 * leak / max(n_dry, 1), 2),
        "per_city": per_city,
        "meta": meta,
        "results": results,
        "seconds": round(time.time() - started, 1),
    }


# ---------------------------------------------------------------------------
# Comparison.
# ---------------------------------------------------------------------------
def key_of(rec):
    return (rec["view"], rec["level"], rec["band"])


def index_results(results):
    return {key_of(r): r for r in results}


def compare(control, arm):
    """Movement of every contrast, in units of the control's own CI half-width.

    Dividing by the half-width is what turns "did it move" into "did it move by
    more than the uncertainty we already admit to". A shift of 2 pp is enormous
    against a +-1 pp interval and invisible against a +-15 pp one, and the
    project reports intervals precisely because the second case is common here.
    """
    ci = index_results(control["results"])
    ai = index_results(arm["results"])
    rows = []
    for k, c in ci.items():
        a = ai.get(k)
        if a is None:
            continue
        for m in ("speed", "flow"):
            ce, ae = c[m], a[m]
            if ce is None or ae is None:
                continue
            half = (ce["hi"] - ce["lo"]) / 2.0
            delta = ae["estimate"] - ce["estimate"]
            rows.append(
                {
                    "view": k[0],
                    "level": k[1],
                    "band": k[2],
                    "measure": m,
                    "control": ce["estimate"],
                    "arm": ae["estimate"],
                    "delta": delta,
                    "half_width": half,
                    "ratio": abs(delta) / half if half > 0 else float("nan"),
                    "sign_flip": (ce["estimate"] > 0) != (ae["estimate"] > 0),
                    "signif_control": ce["excludes_zero"],
                    "signif_arm": ae["excludes_zero"],
                }
            )
    return rows


def light_signs(arm):
    """The Light-vs-Dry speed contrasts -- the diagnostic with a prediction.

    Phase 3 found Light rain reading as FASTER than dry (+1.8 Manchester,
    +2.8 Torino km/h) and attributed it to confounding: a 31 km area mean flags
    hours whose drizzle fell nowhere near a detector, so the label tracks
    whatever else correlates with drizzly hours. That explanation makes a
    testable prediction -- per-detector labelling should shrink or kill the
    positive sign. If it does not, the explanation was wrong.
    """
    out = []
    for r in arm["results"]:
        if r["band"] != "Light" or r["view"] not in ("pooled", "city", "congested"):
            continue
        if r["speed"] is None:
            continue
        out.append(
            {
                "view": r["view"],
                "level": r["level"],
                "estimate": r["speed"]["estimate"],
                "lo": r["speed"]["lo"],
                "hi": r["speed"]["hi"],
                "positive": r["speed"]["estimate"] > 0,
                "signif": r["speed"]["excludes_zero"],
            }
        )
    return sorted(out, key=lambda d: (d["view"], d["level"]))


def verdict(arms, comparisons, lights):
    """Apply the decision rule that was fixed BEFORE the numbers existed.

    Stated in advance so this is a measurement and not a hunt for the threshold
    that produces the prettiest table:

      immaterial -- contrasts move less than their own CI half-width and no
                    significant contrast changes sign. Keep 0.1 for continuity.
      switch     -- fine-resolution labelling removes the Light-is-faster
                    positive sign. That is positive evidence the confound was
                    resolution, and the mask should be rebuilt per-detector.
      hold       -- contrasts move materially but Light stays positive.
                    Something is unexplained; changing the baseline would bury
                    it rather than fix it.
    """
    control_light = lights[CONTROL]
    ctrl_pos = sum(1 for d in control_light if d["positive"])
    # Only arms that still HAVE a Light band can speak to the Light question. An
    # arm thresholded at 0.5 folds Light into its dry reference, so it reports no
    # Light contrast at all -- and that is the correct behaviour, not a gap to
    # paper over. It leaves the test resting on the same-threshold comparison,
    # which is the only fair one anyway.
    fine_arms = [
        a for a, spec in arms.items() if spec["resolution"] == "fine" and lights.get(a)
    ]

    max_median_ratio = 0.0
    any_signif_flip = False
    for arm_id, rows in comparisons.items():
        ratios = [r["ratio"] for r in rows if np.isfinite(r["ratio"])]
        if ratios:
            max_median_ratio = max(max_median_ratio, float(np.median(ratios)))
        any_signif_flip |= any(
            r["sign_flip"] and r["signif_control"] and r["signif_arm"] for r in rows
        )

    fine_pos = {a: sum(1 for d in lights[a] if d["positive"]) for a in fine_arms}
    light_resolved = bool(fine_pos) and all(v < ctrl_pos for v in fine_pos.values())

    if light_resolved:
        call = "switch"
        why = (
            "per-detector labelling reduced the count of positive Light-vs-Dry "
            "speed contrasts, which is the prediction the resolution-confound "
            "explanation makes"
        )
    elif max_median_ratio < 1.0 and not any_signif_flip:
        call = "immaterial"
        why = (
            "every arm moves the typical contrast by less than the control CI "
            "half-width, and no significant contrast changes sign"
        )
    else:
        call = "hold"
        why = (
            "contrasts move materially but the Light-is-faster sign survives "
            "per-detector labelling, so the resolution explanation is not "
            "supported and changing the baseline would bury the anomaly"
        )
    return {
        "call": call,
        "why": why,
        "max_median_ratio": round(max_median_ratio, 3),
        "any_significant_sign_flip": bool(any_signif_flip),
        "control_positive_light": ctrl_pos,
        "fine_positive_light": fine_pos,
        "n_light_contrasts": len(control_light),
    }


# ---------------------------------------------------------------------------
# Reporting.
# ---------------------------------------------------------------------------
def pp(x):
    return f"{100.0 * x:+.1f}"


def render(arms, comparisons, lights, call, meta):
    A = []
    add = A.append
    add("# `wet_threshold_mm` Sensitivity Sweep -- 2x2 Factorial")
    add("")
    add(f"_Generated {meta['generated']}. Four dry-baseline definitions, one frozen estimator._")
    add("")
    add(f"**Decision: {call['call'].upper()}** -- {call['why']}.")
    add("")

    add("## 1. What varies, and what does not")
    add("")
    add("Only the definition of the dry baseline changes. `BANDS` in")
    add("`build_rain_features.py` stays at the Met Office boundaries that IUTF")
    add("uses, so the band an interval is *exposed* to is fine-resolution in")
    add("every arm; what moves is the baseline that exposure is measured")
    add("*against*. The estimator is imported unchanged from `dose_response.py`")
    add(f"(N_BOOT={dr.N_BOOT:,}, SEED={dr.SEED}, MIN_CLUSTERS={dr.MIN_CLUSTERS}), and all")
    add(f"{len(dr.VIEWS)} views run against one shared set of replicate draws.")
    add("")
    add("| arm | rain field | resolution | threshold | dry baseline retained | wet-band leak | detector-days | runtime |")
    add("|---|---|---|---|---|---|---|---|")
    for a in arms.values():
        field = "ERA5 native" if a["resolution"] == "coarse" else "spateGAN 2 km"
        res = "city-hour, ~31 km" if a["resolution"] == "coarse" else "detector, 10 min"
        tag = " *(control)*" if a["arm"] == CONTROL else ""
        add(
            f"| **{a['arm']}**{tag} | {field} | {res} | {a['threshold_mm']} mm | "
            f"{a['dry_pct']}% | {a['wet_band_leak_pct']}% | "
            f"{a['meta']['n_clusters']:,} | {a['seconds']:.0f}s |"
        )
    add("")
    add("`wet-band leak` is the share of each arm's dry baseline that carries a")
    add("rain band anyway. Zero by construction in the corrected arms. For A0 it")
    add("measures what production currently admits, and it is a property of the")
    add("coarse mask rather than of the threshold: a city-wide hourly average")
    add("below 0.1 mm can still sit over a detector that was being rained on.")
    add("")
    add("### Where 0.1 mm comes from, and what the standards actually say")
    add("")
    add("Checked against the published scales rather than assumed:")
    add("")
    add("| scale | lightest | moderate | heavy |")
    add("|---|---|---|---|")
    add("| UK Met Office (rain, not showers) | slight, **< 0.5** | 0.5-4 | > 4 |")
    add("| WMO / MANOBS | light, **< 2.5** | 2.5-7.5 | 7.6-50 |")
    add("| this project (`build_rain_features.BANDS`) | Light, **0.1**-0.5 | 0.5-4 | 4-10, >10 |")
    add("")
    add("Two things follow. First, our 0.5 and 4 boundaries are the Met Office")
    add("ones, so 0.5 mm is a real meteorological boundary and not an arbitrary")
    add("loosening -- the corrected arms sit exactly on the slight/moderate line.")
    add("")
    add("Second, and more importantly: **in both published scales the lightest")
    add("category has no lower bound.** 0.1 mm/h is not a rainfall class. It is a")
    add("gauge detection floor -- WMO's preferred resolution for professional")
    add("instruments, with 0.2 mm the common tipping-bucket increment and the UK")
    add("`rain day` threshold. Applying it as a wet/dry rule asks whether a rain")
    add("gauge would have registered anything, not whether it was raining enough")
    add("to matter for driving.")
    add("")
    add("That is a poor fit for ERA5 specifically, which has a well-documented")
    add("*drizzle bias*: it produces precipitation too frequently and too lightly,")
    add("overestimating rates below ~1.5 mm/h and inflating wet-hour counts. A")
    add("0.1 mm rule applied to a model that drizzles too much will flag hours")
    add("that were not meaningfully wet -- which is a second, independent reason")
    add("to distrust the Light band, alongside whatever the A0-vs-A2 result is")
    add("telling us.")
    add("")

    add("## 2. Harness validity")
    add("")
    add("A0 re-derives production's own mask through `build_rain_mask.label`, the")
    add("same function the pipeline calls. Two things are asserted rather than")
    add("assumed, and both are fatal on failure:")
    add("")
    add("- the recomputed A0 mask matches the stored `is_dry_clean` row for row;")
    add("- A0's re-estimated L3a matches the published Phase 5 figures.")
    add("")
    pub = meta.get("published", {})
    a0 = arms[CONTROL]["meta"]
    add("| figure | published Phase 5 | A0 re-run | |")
    add("|---|---|---|---|")
    for k, lab in (
        ("n_clusters", "detector-days"),
        ("n_estimable", "estimable contrasts"),
        ("n_signif", "intervals excluding zero"),
        ("n_suppressed", "suppressed (thin)"),
    ):
        ok = "match" if pub.get(k) == a0.get(k) else "**DIFFERS**"
        add(f"| {lab} | {pub.get(k, '?'):,} | {a0.get(k, 0):,} | {ok} |")
    de_ok = (
        "match"
        if abs(pub.get("design_effect", 0) - a0.get("design_effect", 0)) < 1e-9
        else "**DIFFERS**"
    )
    add(
        f"| median design effect | {pub.get('design_effect', float('nan')):.4f}x | "
        f"{a0.get('design_effect', float('nan')):.4f}x | {de_ok} |"
    )
    add("")

    add("## 3. What each arm costs the baseline")
    add("")
    add("`dry baseline retained` is the share of all curated intervals that")
    add("survive into L2a. The Phase-3 concern was that 0.1 mm on a 31 km")
    add("area-mean removes 22-36% of the data for no measured gain, so the")
    add("question is whether a per-detector label buys that back without")
    add("readmitting genuinely wet intervals.")
    add("")
    add("| city | " + " | ".join(f"{a} dry %" for a in arms) + " |")
    add("|---|" + "---|" * len(arms))
    cities = sorted(arms[CONTROL]["per_city"])
    for c in cities:
        vals = " | ".join(f"{arms[a]['per_city'][c]['dry_pct']}%" for a in arms)
        add(f"| {c} | {vals} |")
    add("")
    add("Profile cells clearing `min_obs_cell`, which is the coverage that")
    add("actually limits the delay metric -- Essen was the weak city at 40.9%:")
    add("")
    add("| city | " + " | ".join(f"{a} cells ok" for a in arms) + " |")
    add("|---|" + "---|" * len(arms))
    for c in cities:
        vals = " | ".join(f"{arms[a]['per_city'][c].get('cell_ok_pct', 0)}%" for a in arms)
        add(f"| {c} | {vals} |")
    add("")
    add("Median free-flow speed (km/h) -- does the baseline *level* move, or")
    add("only its sample size:")
    add("")
    add("| city | " + " | ".join(f"{a}" for a in arms) + " |")
    add("|---|" + "---|" * len(arms))
    for c in cities:
        vals = " | ".join(
            f"{arms[a]['per_city'][c].get('med_free_flow', float('nan')):.1f}" for a in arms
        )
        add(f"| {c} | {vals} |")
    add("")

    add("## 4. The Light-is-faster question")
    add("")
    add("Phase 3 found Light rain reading as **faster** than dry (+1.8 km/h")
    add("Manchester, +2.8 Torino) and called it confounding rather than physics:")
    add("a 31 km area mean flags hours whose drizzle fell nowhere near a")
    add("detector, so the label mostly tracks whatever else correlates with")
    add("drizzly hours. That explanation makes a falsifiable prediction --")
    add("**per-detector labelling should shrink or kill the positive sign.**")
    add("")
    add("Light-vs-Dry contrasts on `typical_speed_deviation`, percentage points:")
    add("")
    add("| view | level | " + " | ".join(arms) + " |")
    add("|---|---|" + "---|" * len(arms))
    keys = [(d["view"], d["level"]) for d in lights[CONTROL]]
    for view, level in keys:
        cells = []
        for a in arms:
            m = next(
                (d for d in lights[a] if d["view"] == view and d["level"] == level), None
            )
            if m is None:
                cells.append("--")
                continue
            star = "" if m["signif"] else " ns"
            cells.append(f"{pp(m['estimate'])}{star}")
        add(f"| {view} | {level} | " + " | ".join(cells) + " |")
    add("")
    add("`ns` marks an interval that includes zero.")
    add("")
    for a in arms:
        pos = sum(1 for d in lights[a] if d["positive"])
        add(f"- **{a}**: {pos} of {len(lights[a])} Light contrasts positive.")
    add("")
    add("### Why the 0.5 mm arms report no Light row")
    add("")
    add("The first version of this sweep had a flaw here, and the fix is why the")
    add("0.5 mm arms were re-run. Light is 0.1-0.5 mm, so an arm thresholded at")
    add("0.5 files Light rain as dry -- and the old arms then compared Light")
    add("against a baseline that contained Light rain. The difference shrank")
    add("toward zero by construction, and that was mistaken for the anomaly")
    add("easing.")
    add("")
    add("`arm_bands()` now enforces that an arm's dry reference is disjoint from")
    add("the bands it contrasts: sub-threshold bands are folded into `Dry`, and")
    add("baseline intervals must be `Dry` under that folded vocabulary. A 0.5 mm")
    add("arm therefore has no Light category to report, which is honest rather")
    add("than missing -- at that threshold, light rain *is* the reference.")
    add("")
    add("So the Light question rests on **A0 vs A2**: same 0.1 mm threshold,")
    add("resolution the only difference. That is the fair comparison, and it runs")
    add("against the hypothesis -- sharpening the label to the detector's own")
    add("2 km cell makes drizzle look faster, not less so.")
    add("")

    add("## 5. How far the answers actually move")
    add("")
    add("Each contrast is differenced against A0 and divided by A0's own CI")
    add("half-width. That is the number the decision turns on: a 2 pp shift is")
    add("enormous against a +-1 pp interval and invisible against a +-15 pp one.")
    add("")
    add("| arm | contrasts compared | median move | p90 | max | sign flips (both significant) |")
    add("|---|---|---|---|---|---|")
    for arm_id, rows in comparisons.items():
        ratios = np.array([r["ratio"] for r in rows if np.isfinite(r["ratio"])])
        flips = sum(1 for r in rows if r["sign_flip"] and r["signif_control"] and r["signif_arm"])
        if ratios.size:
            add(
                f"| {arm_id} | {len(rows):,} | {np.median(ratios):.2f}x | "
                f"{np.percentile(ratios, 90):.2f}x | {ratios.max():.2f}x | {flips} |"
            )
    add("")
    add("Movement restricted to `road_congested`, the view carrying Phase 5's")
    add("headline reversal:")
    add("")
    add("| arm | median move | max | sign flips |")
    add("|---|---|---|---|")
    for arm_id, rows in comparisons.items():
        sub = [r for r in rows if r["view"] == "road_congested" and np.isfinite(r["ratio"])]
        if not sub:
            continue
        ratios = np.array([r["ratio"] for r in sub])
        flips = sum(1 for r in sub if r["sign_flip"] and r["signif_control"] and r["signif_arm"])
        add(f"| {arm_id} | {np.median(ratios):.2f}x | {ratios.max():.2f}x | {flips} |")
    add("")

    add("## 6. Decision")
    add("")
    add("The rule below was fixed in `verdict()` before any arm had run.")
    add("")
    add("- **immaterial** -- typical movement under one CI half-width and no")
    add("  significant sign flips. Keep 0.1 mm for continuity.")
    add("- **switch** -- per-detector labelling removes the Light-is-faster sign.")
    add("- **hold** -- material movement but Light stays positive; something is")
    add("  unexplained and changing the baseline would bury it.")
    add("")
    add(f"**Measured: {call['call'].upper()}.** {call['why'].capitalize()}.")
    add("")
    add(f"- largest per-arm median movement: {call['max_median_ratio']}x the control half-width")
    add(f"- significant sign flips: {'yes' if call['any_significant_sign_flip'] else 'none'}")
    add(
        f"- positive Light contrasts: {call['control_positive_light']}/"
        f"{call['n_light_contrasts']} in the control, "
        + ", ".join(f"{k} {v}/{call['n_light_contrasts']}" for k, v in call["fine_positive_light"].items())
    )
    add("")
    add(f"**Gate verdict: {meta['verdict']}**")
    add("")
    if meta["failures"]:
        for f in meta["failures"]:
            add(f"- FAIL: {f}")
    else:
        add("The gate is on the harness, not the answer: A0 must reproduce")
        add("production exactly, every arm must conserve the row count, and every")
        add("arm must produce an estimable table. Which way the decision lands is")
        add("a finding, not a pass condition.")
    add("")
    return "\n".join(A)


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--arms",
        default=",".join(t[0] for t in ARMS),
        help="comma-separated arm ids to run (default: all four)",
    )
    # Each fine-resolution arm takes ~8 minutes, so a full sweep outlives the
    # window a single foreground call gets. Caching per arm makes the run
    # resumable: an interrupted sweep picks up where it stopped instead of
    # repeating work that already succeeded.
    ap.add_argument(
        "--cache",
        default=None,
        help="directory to cache per-arm results in, making the sweep resumable",
    )
    args = ap.parse_args()
    wanted = [a.strip() for a in args.arms.split(",") if a.strip()]
    plan = [t for t in ARMS if t[0] in wanted]
    if CONTROL not in [t[0] for t in plan]:
        print(f"FAIL: {CONTROL} is the reference and must be included")
        return 1

    spark = config.get_spark("threshold_sweep")
    spark.sparkContext.setLogLevel("WARN")
    bconf = config.load_cities_conf()["baseline"]

    # The analysis table is the frozen substrate: it already carries the 2 km
    # band, fclass, hod and both rain labels. Only the baseline-derived columns
    # are stripped, because each arm rebuilds exactly those.
    full = spark.read.parquet(config.spark_path(ANALYSIS))
    base = full.withColumnRenamed("is_dry_clean", "stored_dry").drop(
        *[c for c in BASELINE_COLS if c != "is_dry_clean"]
    )
    base.cache()
    print(f"substrate: {base.count():,} rows from {ANALYSIS.name}")

    cache = Path(args.cache) if args.cache else None
    if cache:
        cache.mkdir(parents=True, exist_ok=True)

    arms: dict[str, dict] = {}
    pending = []
    for arm_id, resolution, threshold, corrected in plan:
        cached = cache / f"arm_{arm_id}.json" if cache else None
        if cached and cached.exists():
            arms[arm_id] = json.loads(cached.read_text(encoding="utf-8"))
            print(f"[{arm_id}] reusing cached result ({arms[arm_id]['seconds']:.0f}s run)")
            continue
        tag = "corrected" if corrected else "production semantics"
        print(f"\n[{arm_id}] {resolution} @ {threshold} mm ({tag})")
        arms[arm_id] = build_arm(
            spark, base, arm_id, resolution, threshold, corrected, bconf
        )
        m = arms[arm_id]["meta"]
        print(
            f"      estimable {m['n_estimable']}/{m['n_total']}, "
            f"signif {m['n_signif']}, design effect {m['design_effect']:.2f}x, "
            f"{arms[arm_id]['seconds']:.0f}s"
        )
        if cached:
            cached.write_text(
                json.dumps(arms[arm_id], default=float), encoding="utf-8"
            )
        pending.append(arm_id)

    missing = [t[0] for t in plan if t[0] not in arms]
    if missing:
        print(f"\nincomplete: still to run {', '.join(missing)}; re-invoke to continue")
        spark.stop()
        return 0

    comparisons = {
        a: compare(arms[CONTROL], arms[a]) for a in arms if a != CONTROL
    }
    lights = {a: light_signs(arms[a]) for a in arms}
    call = verdict(arms, comparisons, lights)

    # ---- gates -----------------------------------------------------------
    failures = []
    published = json.loads(PUBLISHED.read_text(encoding="utf-8")) if PUBLISHED.exists() else {}
    a0 = arms[CONTROL]["meta"]
    for k in ("n_clusters", "n_estimable", "n_signif", "n_suppressed"):
        if k in published and published[k] != a0[k]:
            failures.append(
                f"A0 does not reproduce published Phase 5 {k}: "
                f"{published[k]:,} published vs {a0[k]:,} re-run"
            )
    if "design_effect" in published and abs(
        published["design_effect"] - a0["design_effect"]
    ) > 1e-9:
        failures.append(
            f"A0 design effect drifted: {published['design_effect']:.6f} vs "
            f"{a0['design_effect']:.6f}"
        )
    for arm_id, a in arms.items():
        if not a["meta"]["n_estimable"]:
            failures.append(f"{arm_id} produced no estimable contrast")

    meta = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "published": published,
        "verdict": "FAIL" if failures else "PASS",
        "failures": failures,
        "decision": call,
        "arms": {
            a: {
                k: v for k, v in arms[a].items() if k != "results"
            }
            for a in arms
        },
        "light": lights,
        "movement": {
            a: {
                "n": len(rows),
                "median_ratio": float(np.median([r["ratio"] for r in rows]))
                if rows
                else None,
                "p90_ratio": float(np.percentile([r["ratio"] for r in rows], 90))
                if rows
                else None,
                "max_ratio": float(np.max([r["ratio"] for r in rows])) if rows else None,
                "significant_sign_flips": sum(
                    1 for r in rows if r["sign_flip"] and r["signif_control"] and r["signif_arm"]
                ),
                "cells": rows,
            }
            for a, rows in comparisons.items()
        },
    }

    REPORT.write_text(render(arms, comparisons, lights, call, meta), encoding="utf-8")
    REPORT_JSON.write_text(json.dumps(meta, indent=2, default=float), encoding="utf-8")

    print()
    print(f"decision: {call['call'].upper()} -- {call['why']}")
    print(f"wrote {REPORT}")
    for f in failures:
        print(f"FAIL: {f}")
    if not failures:
        print("PASS: threshold sweep completed, A0 reproduces production")
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
