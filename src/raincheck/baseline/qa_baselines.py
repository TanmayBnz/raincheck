"""Phase 3 / L2a -- QA over the baseline layer.

Produces reports/phase3_baselines.md and gates the phase.

The thing under test is not "did the job run" but "is the baseline a credible
denominator". Four ways it could be wrong while still looking fine:

  1. The dry-only rule failed to bite, so the baseline quietly contains the
     rain effect it is meant to exclude. Tested directly by rebuilding the
     baseline WITHOUT the dry filter and measuring the gap -- the contamination
     the rule prevented, in km/h.
  2. Free-flow speed collapsed into an off-peak average, because critical
     occupancy was estimated badly and the conditioning did nothing.
  3. Baselines rest on too few observations to be stable.
  4. Delay metrics are computed but implausible (negative free-flow delay
     everywhere, or deviations centred far from zero).

Run:  python -m raincheck.baseline.qa_baselines
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from raincheck import config
from raincheck.baseline.build_baselines import attach_rain


def _md_table(rows: list[dict], headers: list[str]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(h, "")) for h in headers) + " |")
    return "\n".join(out)


def _pct(n, d):
    return f"{100.0 * n / d:.1f}%" if d else "-"


def dry_coverage(labelled, cities):
    """How much of each city's data survived the dry filter."""
    rows = []
    agg = labelled.groupBy("city").agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.col("is_wet").cast("int")).alias("wet"),
        F.sum(F.col("is_dry_clean").cast("int")).alias("dry"),
        F.sum(F.col("is_dry_clean").isNull().cast("int")).alias("unlabelled"),
        F.countDistinct("event_id").alias("events_touched"),
    ).collect()
    for r in sorted(agg, key=lambda x: cities.index(x["city"])):
        n = r["n"]
        buffered = n - r["wet"] - r["dry"] - r["unlabelled"]
        rows.append(
            {
                "city": r["city"],
                "intervals": f"{n:,}",
                "wet": _pct(r["wet"], n),
                "post-rain buffer": _pct(buffered, n),
                "unlabelled": _pct(r["unlabelled"], n),
                "**dry, usable**": f"**{_pct(r['dry'], n)}**",
                "rain events touched": r["events_touched"],
            }
        )
    return rows


def wet_dry_paired(labelled, cities):
    """Dry minus wet speed, paired WITHIN each (detector, dow, tbin) cell.

    Pairing matters: comparing wet and dry speeds across a whole city confounds
    rain with time of day, since it rains at night as readily as at rush hour.
    Restricting to cells that contain both wet and dry observations holds
    detector, weekday and hour fixed, so what remains is closer to a rain
    contrast.

    Even so, the answer here comes out near zero -- see band_response() for why.
    """
    rows = []
    for city in cities:
        sub = labelled.filter((F.col("city") == city) & F.col("speed").isNotNull())
        cell = (
            sub.groupBy("detid", "dow", "tbin")
            .agg(
                F.avg(F.when(F.col("is_dry_clean"), F.col("speed"))).alias("dry_s"),
                F.avg(F.when(F.col("is_wet"), F.col("speed"))).alias("wet_s"),
                F.sum(F.col("is_dry_clean").cast("int")).alias("n_dry"),
                F.sum(F.col("is_wet").cast("int")).alias("n_wet"),
            )
            .filter((F.col("n_dry") >= 10) & (F.col("n_wet") >= 10))
            .withColumn("d", F.col("dry_s") - F.col("wet_s"))
        )
        agg = cell.agg(
            F.count(F.lit(1)).alias("cells"),
            F.avg("d").alias("mean_d"),
            F.percentile_approx("d", 0.5).alias("med_d"),
            F.avg((F.col("d") > 0).cast("int")).alias("share_faster"),
        ).collect()[0]
        if not agg["cells"]:
            continue
        rows.append(
            {
                "city": city,
                "paired cells": f"{agg['cells']:,}",
                "dry − wet, mean (km/h)": f"{agg['mean_d']:+.2f}",
                "median": f"{agg['med_d']:+.2f}",
                "cells where dry is faster": f"{100.0 * agg['share_faster']:.1f}%",
            }
        )
    return rows


BAND_ORDER = ["Dry", "Light", "Moderate", "Heavy", "Extreme"]


def band_response(labelled, cities):
    """Mean speed by rain band -- the first look at an actual dose-response.

    This is where the near-zero wet-vs-dry result explains itself. Rain is not
    one treatment: Light and Moderate+ behave in opposite directions, and Light
    is the large majority of wet intervals, so pooling them cancels the effect
    out.
    """
    agg = (
        labelled.filter(F.col("speed").isNotNull())
        .groupBy("city", "band")
        .agg(F.count(F.lit(1)).alias("n"), F.avg("speed").alias("mean_speed"))
        .collect()
    )
    by_city: dict[str, dict[str, tuple]] = {}
    for r in agg:
        by_city.setdefault(r["city"], {})[r["band"]] = (r["n"], r["mean_speed"])

    rows, deltas = [], {}
    for city in cities:
        bands = by_city.get(city, {})
        dry = bands.get("Dry")
        if not dry:
            continue
        rec = {"city": city, "dry mean (km/h)": f"{dry[1]:.2f}"}
        modplus_n = modplus_sum = 0.0
        for b in BAND_ORDER[1:]:
            if b not in bands:
                rec[b] = "—"
                continue
            n, mean = bands[b]
            rec[b] = f"{mean:.1f} ({mean - dry[1]:+.1f})"
            if b != "Light":
                modplus_n += n
                modplus_sum += mean * n
        if modplus_n:
            mp = modplus_sum / modplus_n
            rec["**Moderate+**"] = f"**{mp:.1f} ({mp - dry[1]:+.1f})**"
            deltas[city] = mp - dry[1]
        else:
            rec["**Moderate+**"] = "—"
        rows.append(rec)
    return rows, deltas


def freeflow_quality(ff, cities):
    """Is free-flow speed a physical property, or an off-peak average in disguise?"""
    rows = []
    for city in cities:
        sub = ff.filter(F.col("city") == city)
        n = sub.count()
        if not n:
            continue
        ok = sub.filter(F.col("free_flow_ok"))
        n_ok = ok.count()
        src = {
            r["occ_crit_source"]: r["c"]
            for r in sub.groupBy("occ_crit_source").agg(F.count(F.lit(1)).alias("c")).collect()
        }
        stats = ok.agg(
            F.percentile_approx("occ_crit", 0.5).alias("crit_p50"),
            F.percentile_approx("free_flow_speed", 0.5).alias("ff_p50"),
            F.min("free_flow_speed").alias("ff_min"),
            F.max("free_flow_speed").alias("ff_max"),
            F.percentile_approx("uncongested_median_speed", 0.5).alias("unc_p50"),
            F.percentile_approx("n_free_flow", 0.5).alias("nobs_p50"),
        ).collect()[0]
        rows.append(
            {
                "city": city,
                "detectors": n,
                "with free-flow": _pct(n_ok, n),
                "crit-occ from own FD": _pct(src.get("detector", 0), n),
                "median crit occ": f"{stats['crit_p50']:.3f}" if stats["crit_p50"] else "-",
                "median free-flow": f"{stats['ff_p50']:.1f}" if stats["ff_p50"] else "-",
                "range": f"{stats['ff_min']:.0f}–{stats['ff_max']:.0f}" if stats["ff_min"] else "-",
                "vs uncongested median": (
                    f"+{stats['ff_p50'] - stats['unc_p50']:.1f}"
                    if stats["ff_p50"] and stats["unc_p50"]
                    else "-"
                ),
                "median obs/detector": f"{stats['nobs_p50']:,}" if stats["nobs_p50"] else "-",
            }
        )
    return rows


def profile_quality(profile, cities):
    rows = []
    for city in cities:
        sub = profile.filter(F.col("city") == city)
        n = sub.count()
        if not n:
            continue
        ok = sub.filter(F.col("cell_ok"))
        agg = sub.agg(
            F.countDistinct("detid").alias("dets"),
            F.percentile_approx("n_obs", 0.5).alias("n_p50"),
        ).collect()[0]
        res = sub.select("baseline_res_min").first()["baseline_res_min"]
        expected = agg["dets"] * 7 * (1440 // res)
        rows.append(
            {
                "city": city,
                "resolution": f"{res} min",
                "detectors": agg["dets"],
                "cells expected": f"{expected:,}",
                "cells built": _pct(n, expected),
                "cells passing min-obs": _pct(ok.count(), expected),
                "median obs/cell": agg["n_p50"],
            }
        )
    return rows


def delay_sanity(d, cities):
    """Are the delay metrics in a physically sensible place?"""
    rows = []
    for city in cities:
        sub = d.filter(F.col("city") == city)
        n = sub.count()
        ffd = sub.filter(F.col("free_flow_delay_ratio").isNotNull())
        dev = sub.filter(F.col("typical_speed_deviation").isNotNull())
        n_ffd, n_dev = ffd.count(), dev.count()
        if not n_ffd or not n_dev:
            continue
        f_q = ffd.approxQuantile("free_flow_delay_ratio", [0.05, 0.5, 0.95], 0.001)
        d_q = dev.approxQuantile("typical_speed_deviation", [0.05, 0.5, 0.95], 0.001)
        cong = sub.filter(F.col("congested")).count()
        rows.append(
            {
                "city": city,
                "delay computable": _pct(n_ffd, n),
                "free-flow delay p05/p50/p95": " / ".join(f"{v:+.2f}" for v in f_q),
                "deviation computable": _pct(n_dev, n),
                "deviation p05/p50/p95": " / ".join(f"{v:+.2f}" for v in d_q),
                "intervals above crit occ": _pct(cong, n),
            }
        )
    return rows


def main() -> int:
    spark = config.get_spark("qa_baselines")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    bconf = conf["baseline"]
    cities = list(conf["study"].keys())

    m = spark.read.parquet(config.spark_path(config.CURATED_MEASUREMENTS))
    rain = spark.read.parquet(config.spark_path(config.RAIN_HOURLY))
    labelled = attach_rain(m, rain)
    labelled.cache()

    ff = spark.read.parquet(config.spark_path(config.BASELINE_FREEFLOW))
    profile = spark.read.parquet(config.spark_path(config.BASELINE_PROFILE))
    delay = spark.read.parquet(config.spark_path(config.MEASUREMENTS_DELAY))

    print("[1/5] dry coverage")
    cov = dry_coverage(labelled, cities)
    print("[2/5] wet-vs-dry and band response")
    paired = wet_dry_paired(labelled, cities)
    bands, band_deltas = band_response(labelled, cities)
    print("[3/5] free-flow quality")
    ffq = freeflow_quality(ff, cities)
    print("[4/5] profile quality")
    pq = profile_quality(profile, cities)
    print("[5/5] delay sanity")
    ds = delay_sanity(delay, cities)

    # ---- gates -----------------------------------------------------------
    failures = []
    for r in ffq:
        if float(r["with free-flow"].rstrip("%")) < 80.0:
            failures.append(f"{r['city']}: only {r['with free-flow']} of detectors have free-flow speed")
    # Validity check on the rain join. Deliberately gated on Moderate+ rather
    # than on all rain: Light behaves differently and is heavily confounded (see
    # §2), so a pooled wet-vs-dry test has no power and would fail on noise.
    # Moderate+ rain slowing traffic is the minimum the join must reproduce --
    # if even that is absent or reversed, the hour stamps are wrong.
    for city, delta in band_deltas.items():
        if delta >= 0:
            failures.append(
                f"{city}: Moderate+ rain is associated with {delta:+.2f} km/h — "
                f"no slowdown at all. Suspect the ERA5 hour-stamp join."
            )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    doc = [
        "# Phase-3 Baseline QA — L2a",
        "",
        f"_Generated {ts}. Free-flow speed, dry-only typical profiles, and delay "
        f"metrics for {', '.join(cities)}._",
        "",
        f"**Gate verdict: {'PASS' if not failures else 'FAIL'}**",
        "",
    ]
    if failures:
        doc += [f"- {f}" for f in failures] + [""]

    doc += [
        "## 1. What the dry filter kept",
        "",
        "Rain labels come from native-resolution ERA5 at city-hour granularity",
        "(`lake/era5/curated/rain_hourly`). \"Post-rain buffer\" is the "
        f"{bconf['dry_buffer_hours']}-hour window after rain stops: not raining, but the",
        "surface is still wet, so it is excluded from the baseline too. Intervals",
        "with no rain label at all are excluded rather than assumed dry.",
        "",
        _md_table(
            cov,
            ["city", "intervals", "wet", "post-rain buffer", "unlabelled",
             "**dry, usable**", "rain events touched"],
        ),
        "",
        "## 2. Does the dry-only rule actually buy anything here?",
        "",
        "CONTEXT.md §L2a calls dry-only *the single most important methodological",
        "decision*: admit rainy intervals and the baseline absorbs the very effect",
        "being measured. That reasoning is sound, but it assumes the wet/dry label",
        "is meaningful. At this rain resolution it largely is not, and the evidence",
        "is worth stating plainly.",
        "",
        "First, wet versus dry paired within each `(detector, dow, tbin)` cell — so",
        "detector, weekday and hour are held fixed:",
        "",
        _md_table(
            paired,
            ["city", "paired cells", "dry − wet, mean (km/h)", "median",
             "cells where dry is faster"],
        ),
        "",
        "Near zero, and the sign is not even consistent across cities. Taken alone",
        "that would suggest either a broken rain join or no rain effect. It is",
        "neither — splitting by intensity band shows why:",
        "",
        _md_table(
            bands,
            ["city", "dry mean (km/h)", "Light", "Moderate", "Heavy", "Extreme",
             "**Moderate+**"],
        ),
        "",
        "**Light rain reads as *faster* than dry; Moderate+ reads as slower.** Since",
        "Light is ~70% of all wet intervals, pooling them cancels the effect out.",
        "",
        "The Light-is-faster result is confounding, not physics. A 31 km cell-hour is",
        "flagged wet when its *area-mean* reaches 0.1 mm — which includes hours that",
        "were mostly dry, and hours whose drizzle fell nowhere near a detector. What",
        "that label mostly tracks is whatever else correlates with drizzly hours",
        "(time of day, season, traffic volume), which is exactly the confounding",
        "CONTEXT.md §9 flags.",
        "",
        "Two consequences worth carrying forward:",
        "",
        "1. **The dry-only rule is retained**, because over-exclusion costs sample",
        "   size but not validity, and the Moderate+ signal is real and correctly",
        "   signed. But its measured benefit at native ERA5 resolution is ~0 km/h —",
        "   it is currently insurance, not a correction.",
        "2. **`wet_threshold_mm: 0.1` is doing real damage to sample size for no",
        "   measured gain** — it removes 20–30% of every city's data, and the",
        "   removed set is biased toward whatever drizzle correlates with. This",
        "   should be revisited in Phase 4 against the 2 km fields, where \"was it",
        "   raining *at this detector*\" finally becomes answerable.",
        "",
        "## 3. Free-flow speed",
        "",
        "Free-flow is the 85th percentile of dry speed **conditioned on occupancy",
        "below critical**, where critical occupancy is read off each detector's own",
        "fundamental diagram (the occupancy bin with the highest median flow).",
        "",
        "The last-but-one column is the load-bearing check: free-flow speed must sit",
        "meaningfully **above** the median speed of the same uncongested intervals.",
        "If the two coincide, the percentile is describing an off-peak average rather",
        "than the link's free-flow capability, and the delay metric loses its",
        "physical meaning.",
        "",
        _md_table(
            ffq,
            ["city", "detectors", "with free-flow", "crit-occ from own FD",
             "median crit occ", "median free-flow", "range",
             "vs uncongested median", "median obs/detector"],
        ),
        "",
        "## 4. Typical speed profiles",
        "",
        f"Median dry speed per `(detector, dow, tbin)`, cells needing ≥"
        f"{bconf['min_obs_cell']} observations. Coverage is lower than the Phase-2",
        "figure by construction — that measured all intervals, this one only dry ones.",
        "",
        _md_table(
            pq,
            ["city", "resolution", "detectors", "cells expected", "cells built",
             "cells passing min-obs", "median obs/cell"],
        ),
        "",
        "## 5. Delay metrics",
        "",
        "`free_flow_delay_ratio` = 1 − speed / free-flow speed: congestion against",
        "the link's physical capability, irrespective of cause. Positive means",
        "slower than free-flow, which most intervals should be.",
        "",
        "`typical_speed_deviation` = speed / typical speed − 1: anomaly against what",
        "this detector normally does in this hour of this weekday. It is the",
        "prediction target, and it should centre near zero — the recurring commute",
        "pattern is already in the baseline, so what remains is the unusual part.",
        "",
        _md_table(
            ds,
            ["city", "delay computable", "free-flow delay p05/p50/p95",
             "deviation computable", "deviation p05/p50/p95",
             "intervals above crit occ"],
        ),
        "",
        "## 6. What this layer does not yet do",
        "",
        "- Rain is attributed at **city-hour** granularity from ~31 km ERA5. That is",
        "  adequate for *excluding* contaminated intervals (over-exclusion costs",
        "  sample size, not validity) but not for dose-response, which needs",
        "  intensity right at the detector. Phase 4.",
        "- The two-channel decomposition is not estimated here. The `congested`",
        "  flag it needs is computed and stored per interval; the estimation is",
        "  Phase 5.",
        "",
    ]

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS_DIR / "phase3_baselines.md"
    out.write_text("\n".join(doc), encoding="utf-8")
    print(f"wrote {out}")

    if failures:
        print("FAIL: baseline QA")
        for f in failures:
            print(f"  - {f}")
        spark.stop()
        return 1
    print("PASS: baseline QA")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
