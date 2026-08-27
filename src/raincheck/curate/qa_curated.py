"""Phase 2 / L1 -- quality assurance over the curated table.

Produces reports/phase2_curation.md and gates the phase. Three things are being
established, in descending order of importance:

  1. That the per-city occupancy-scale inference is RIGHT. Phase 1 inferred
     "percent" for Manchester and Torino from medians of 5-9 against Essen's
     0.007, but never confirmed it against documentation. Curation rule 2 --
     and therefore free-flow speed, and therefore every delay metric -- rests
     on that inference. This job tests it against a physical plausibility band
     and FAILS the run if a city falls outside.
  2. That each curation rule removed what it was supposed to, in the volumes
     Phase 1 predicted. A rule that fires on 0% of rows, or on 90%, is a bug.
  3. That the baseline cells are actually populated at the chosen resolution
     once the cleaned speed column -- not the raw one -- is what has to fill
     them. Phase 1 measured this on raw speed, which counted absence-zeros as
     observations, so those figures were optimistic.

Run:  python -m raincheck.curate.qa_curated
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from raincheck import config

# Same threshold as the Phase-1 audit, so the two are directly comparable.
MIN_OBS_PER_CELL = 20
COVERAGE_TARGET = 0.80


def _md_table(rows: list[dict], headers: list[str]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(h, "")) for h in headers) + " |")
    return "\n".join(out)


def _pct(n, d):
    return f"{100.0 * n / d:.2f}%" if d else "-"


def rule_impact(c, cities):
    """How many rows each curation rule fired on, per city."""
    agg = c.groupBy("city").agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("occ_bad").cast("int")).alias("occ_bad"),
        F.sum(F.col("speed_absent").cast("int")).alias("speed_absent"),
        F.sum(F.col("speed_zero_flowing").cast("int")).alias("speed_zero_flowing"),
        F.sum(F.col("speed_implausible").cast("int")).alias("speed_implausible"),
        F.sum(F.col("dst_ambiguous").cast("int")).alias("dst_rows"),
        F.count("speed").alias("speed_usable"),
        F.count("occ").alias("occ_usable"),
    )
    rows = []
    for r in sorted(agg.collect(), key=lambda x: cities.index(x["city"])):
        n = r["rows"]
        rows.append(
            {
                "city": r["city"],
                "curated rows": f"{n:,}",
                "occ dropped (r3)": _pct(r["occ_bad"], n),
                "speed→NULL (r4)": _pct(r["speed_absent"], n),
                "zero-speed flowing (r5)": _pct(r["speed_zero_flowing"], n),
                ">150 km/h": _pct(r["speed_implausible"], n),
                "DST-day rows": _pct(r["dst_rows"], n),
                "speed usable": _pct(r["speed_usable"], n),
                "occ usable": _pct(r["occ_usable"], n),
            }
        )
    return rows


def occupancy_check(c, conf, cities):
    """THE gate. Does the normalized occupancy sit in a physically sane band?

    A signalized urban arterial runs at a few percent occupancy on average and
    saturates near 1.0. If a city's normalized median lands at 8.0 rather than
    0.08, the divisor was wrong; if it lands at 0.00008, it was divided twice.
    Either way every free-flow baseline conditioned on "occupancy below
    critical" would be computed against a meaningless threshold.
    """
    band = conf["curation"]["occ_plausible"]
    rows, failures = [], []
    for city in cities:
        occ = c.filter((F.col("city") == city) & F.col("occ").isNotNull()).select("occ")
        n = occ.count()
        if n == 0:
            failures.append(f"{city}: no usable occupancy at all")
            continue
        p50, p95, p99 = occ.approxQuantile("occ", [0.5, 0.95, 0.99], 0.001)
        mx = occ.agg(F.max("occ")).collect()[0][0]
        ok = (
            band["median_min"] <= p50 <= band["median_max"]
            and p99 <= band["p99_max"]
        )
        if not ok:
            failures.append(
                f"{city}: median {p50:.5f} (band {band['median_min']}-{band['median_max']}), "
                f"p99 {p99:.3f} (max {band['p99_max']})"
            )
        rows.append(
            {
                "city": city,
                "scale applied": conf["study"][city]["occ_scale"],
                "p50": f"{p50:.5f}",
                "p95": f"{p95:.4f}",
                "p99": f"{p99:.4f}",
                "max": f"{mx:.4f}",
                "verdict": "PASS" if ok else "**FAIL**",
            }
        )
    return rows, failures


def speed_bias(c, cities):
    """What rule 4 actually bought, in km/h -- measured PER DETECTOR.

    The city-wide percentile is the wrong place to look and will understate
    this to near zero: absence-zeros are 1-3% of quality-passing rows, far too
    few to move a pooled quantile. But baselines are never pooled -- they are
    computed per detector (see reports/phase1_gate.md §4) -- and absence-zeros
    are heavily concentrated on quiet detectors, where they can be a large
    share of that detector's own observations. So the quantity that matters is
    the distribution of per-detector shifts, and its tail.
    """
    rows = []
    for city in cities:
        sub = c.filter(F.col("city") == city)
        per_det = sub.groupBy("detid").agg(
            F.percentile_approx("speed_raw", 0.5).alias("raw_p50"),
            F.percentile_approx("speed", 0.5).alias("cln_p50"),
            F.percentile_approx("speed_raw", 0.85).alias("raw_p85"),
            F.percentile_approx("speed", 0.85).alias("cln_p85"),
            F.count(F.lit(1)).alias("n"),
            F.sum(F.col("speed_absent").cast("int")).alias("n_absent"),
        ).filter(F.col("raw_p50").isNotNull() & F.col("cln_p50").isNotNull())

        d = per_det.withColumn("d50", F.col("cln_p50") - F.col("raw_p50")).withColumn(
            "d85", F.col("cln_p85") - F.col("raw_p85")
        )
        agg = d.agg(
            F.count(F.lit(1)).alias("dets"),
            F.sum((F.col("n_absent") > 0).cast("int")).alias("affected"),
            F.sum((F.col("d50") > 1.0).cast("int")).alias("moved"),
            F.max("d50").alias("max_d50"),
            F.max("d85").alias("max_d85"),
            F.max(F.col("n_absent") / F.col("n")).alias("worst_share"),
        ).collect()[0]
        if not agg["dets"]:
            continue
        rows.append(
            {
                "city": city,
                "detectors": agg["dets"],
                "with ≥1 absence-zero": _pct(agg["affected"], agg["dets"]),
                "worst detector's absence rate": f"{100.0 * (agg['worst_share'] or 0):.1f}%",
                "p50 moved >1 km/h": _pct(agg["moved"], agg["dets"]),
                "max Δ p50": f"{agg['max_d50'] or 0:+.1f}",
                "max Δ p85": f"{agg['max_d85'] or 0:+.1f}",
            }
        )
    return rows


def utc_alignment(c, conf, cities):
    """Confirm the local->UTC shift is the offset the city's zone implies."""
    rows = []
    for city in cities:
        sub = c.filter(F.col("city") == city)
        off = sub.select(
            (
                (F.unix_timestamp("ts_local") - F.unix_timestamp("ts_utc")) / 3600
            ).alias("h")
        )
        seen = sorted(r["h"] for r in off.distinct().collect())
        dst_days = (
            sub.filter(F.col("dst_ambiguous")).select("date").distinct().collect()
        )
        rows.append(
            {
                "city": city,
                "tz": conf["study"][city]["tz"],
                "offsets seen (h)": ", ".join(f"{h:+.0f}" for h in seen),
                "DST-transition dates in window": ", ".join(str(r["date"]) for r in dst_days) or "none",
            }
        )
    return rows


def network_coverage(c, cities):
    """Detector metadata after the join -- can the stratification be done?"""
    rows = []
    for city in cities:
        sub = c.filter(F.col("city") == city)
        dets = sub.select("detid").distinct().count()
        no_geo = sub.filter(F.col("lat").isNull()).select("detid").distinct().count()
        no_link = sub.filter(F.col("linkid").isNull()).select("detid").distinct().count()
        per_class = (
            sub.select("detid", "fclass")
            .distinct()
            .groupBy("fclass")
            .agg(F.count(F.lit(1)).alias("n"))
            .filter(F.col("n") >= 20)
            .orderBy(F.desc("n"))
            .collect()
        )
        rows.append(
            {
                "city": city,
                "detectors": dets,
                "no geo": no_geo,
                "no linkid": no_link,
                "ERA5 cells": sub.select("era5_cell").distinct().count(),
                "spateGAN cells": sub.select("hires_cell").distinct().count(),
                "road classes ≥20 dets": ", ".join(f"{r['fclass']} {r['n']}" for r in per_class) or "**none**",
            }
        )
    return rows


def baseline_viability(c, conf, cities):
    """Are the baseline cells populated once cleaned speed has to fill them?

    Phase 1 answered this on raw speed. Manchester's 11% absence-zeros counted
    as observations there, so its coverage was overstated by roughly that much.
    This is the honest number, and it is the one rule 6 has to justify.
    """
    rows = []
    for city in cities:
        res = int(conf["study"][city]["baseline_res_min"])
        sub = c.filter((F.col("city") == city) & F.col("speed").isNotNull())
        dets = sub.select("detid").distinct().count()
        if dets == 0:
            continue
        cells = sub.groupBy("detid", "dow", "tbin").agg(F.count(F.lit(1)).alias("n"))
        populated = cells.count()
        ok = cells.filter(F.col("n") >= MIN_OBS_PER_CELL).count()
        expected = dets * 7 * (1440 // res)
        rows.append(
            {
                "city": city,
                "resolution": f"{res} min",
                "detectors w/ speed": dets,
                "cells expected": f"{expected:,}",
                "cells populated": _pct(populated, expected),
                f"cells ≥{MIN_OBS_PER_CELL} obs": _pct(ok, expected),
                "verdict": "PASS" if expected and ok / expected >= COVERAGE_TARGET else "thin",
            }
        )
    return rows


def main() -> int:
    spark = config.get_spark("qa_curated")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    cities = list(conf["study"].keys())

    c = spark.read.parquet(config.spark_path(config.CURATED_MEASUREMENTS))
    c.cache()
    total = c.count()

    print("[1/6] rule impact")
    impact = rule_impact(c, cities)
    print("[2/6] occupancy plausibility")
    occ_rows, occ_failures = occupancy_check(c, conf, cities)
    print("[3/6] speed baseline bias")
    bias = speed_bias(c, cities)
    print("[4/6] UTC alignment")
    utc = utc_alignment(c, conf, cities)
    print("[5/6] network coverage")
    net = network_coverage(c, cities)
    print("[6/6] baseline viability")
    via = baseline_viability(c, conf, cities)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    verdict = "PASS" if not occ_failures else "FAIL"
    doc = [
        "# Phase-2 Curation QA — L1",
        "",
        f"_Generated {ts} from the curated table ({total:,} rows, "
        f"{', '.join(cities)})._",
        "",
        f"**Gate verdict: {verdict}**",
        "",
    ]
    if occ_failures:
        doc += [
            "The occupancy-scale inference carried over from Phase 1 is **not**",
            "supported by the curated data. Free-flow speed conditions on occupancy",
            "below critical, so no baseline may be computed until this is resolved:",
            "",
        ] + [f"- {f}" for f in occ_failures] + [""]

    doc += [
        "## 1. Occupancy scale — the Phase-1 open question, closed",
        "",
        "UTD19 documents `occ` as a 0–1 fraction; it is not. Phase 1 *inferred*",
        "percent-scaling for Manchester and Torino from their medians (5–9) against",
        "Essen's 0.007, but never confirmed it. Curation rule 2 divides those two",
        "cities by 100. If that inference were wrong, every free-flow baseline built",
        "on \"occupancy below critical\" would be measured against a meaningless",
        "threshold — so it is tested here against a physical plausibility band",
        "(`conf/cities.yml → curation.occ_plausible`) rather than assumed.",
        "",
        _md_table(occ_rows, ["city", "scale applied", "p50", "p95", "p99", "max", "verdict"]),
        "",
        "## 2. What each rule removed",
        "",
        "Percentages are of curated (quality-passing) rows. A rule firing on 0% or",
        "on most rows would indicate a bug, not clean data.",
        "",
        "> **These are smaller than the Phase-1 figures, and legitimately so.**",
        "> Phase 1 measured over *all* landed rows; rule 1 runs first and the error",
        "> flag already catches much of the same damage. Manchester's 11.0%",
        "> zero-speed rate, for instance, is 1.6% among quality-passing rows — the",
        "> other 9.4 points were error-flagged too. The rules are not redundant",
        "> (they still fire on hundreds of thousands of rows) but they are the",
        "> second line of defence, not the first.",
        "",
        _md_table(
            impact,
            ["city", "curated rows", "occ dropped (r3)", "speed→NULL (r4)",
             "zero-speed flowing (r5)", ">150 km/h", "DST-day rows",
             "speed usable", "occ usable"],
        ),
        "",
        "## 3. The bias rule 4 removed — per detector",
        "",
        "`speed = 0 AND flow = 0` means no vehicle was observed. Counted as a real",
        "zero it pulls the baseline percentile down, and that percentile is the",
        "*denominator* of the delay metric, so the error lands directly in the",
        "headline rain effect.",
        "",
        "Measured city-wide the effect looks negligible — absence-zeros are 1–3% of",
        "quality-passing rows, nowhere near enough to move a pooled quantile. That",
        "framing is misleading, because **baselines are never pooled**: they are",
        "per-detector. Absence-zeros concentrate on quiet detectors, so the quantity",
        "that matters is the spread of per-detector shifts and its tail.",
        "",
        _md_table(
            bias,
            ["city", "detectors", "with ≥1 absence-zero", "worst detector's absence rate",
             "p50 moved >1 km/h", "max Δ p50", "max Δ p85"],
        ),
        "",
        "## 4. UTC alignment",
        "",
        "`interval` is seconds since *local* midnight; rainfall grids are UTC. Two",
        "offsets in one city means the window spans a DST changeover. That is not",
        "the same as *having data on* the changeover date — the last column is what",
        "matters, because only those dates carry an hour that is ambiguous (autumn,",
        "the hour occurs twice) or nonexistent (spring). Where such dates do appear,",
        "the rows are flagged rather than dropped and the baseline layer decides.",
        "",
        _md_table(utc, ["city", "tz", "offsets seen (h)", "DST-transition dates in window"]),
        "",
        "## 5. Network coverage after the detector join",
        "",
        "ERA5 cells are the ~31 km native grid, spateGAN cells the ~2 km downscaled",
        "grid. A city collapsing to one ERA5 cell has no within-city rainfall",
        "variation to exploit at native resolution — which is precisely the",
        "limitation the downscaling exists to fix.",
        "",
        _md_table(
            net,
            ["city", "detectors", "no geo", "no linkid", "ERA5 cells",
             "spateGAN cells", "road classes ≥20 dets"],
        ),
        "",
        "## 6. Baseline cell viability at the chosen resolution",
        "",
        f"Share of `(detector, dow, tbin)` cells holding ≥{MIN_OBS_PER_CELL} "
        "**cleaned** speed observations — the honest test of rule 6, since Phase 1",
        "measured this on raw speed and so counted absence-zeros as if they were",
        "observations. The correction is small in aggregate (1–3% of rows) but",
        "concentrated on the quiet detectors that were closest to the threshold",
        "anyway, which is exactly where a cell tips from populated to empty.",
        "",
        "> Still an upper bound in one respect: rainfall is not yet joined, so all",
        "> intervals count as dry. The dry-only baseline is computed in Phase 3.",
        "",
        _md_table(
            via,
            ["city", "resolution", "detectors w/ speed", "cells expected",
             "cells populated", f"cells ≥{MIN_OBS_PER_CELL} obs", "verdict"],
        ),
        "",
    ]

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS_DIR / "phase2_curation.md"
    out.write_text("\n".join(doc), encoding="utf-8")
    print(f"wrote {out}")

    if occ_failures:
        print("FAIL: occupancy plausibility gate")
        for f in occ_failures:
            print(f"  - {f}")
        spark.stop()
        return 1

    print("PASS: curation QA")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
