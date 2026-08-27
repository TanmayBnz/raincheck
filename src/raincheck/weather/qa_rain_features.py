"""Phase 4 / L2b -- QA, and the test of whether downscaling earned its place.

CONTEXT.md stakes the project's contribution on replacing IUTF's native ~31 km
ERA5 with 2 km / 10 min spateGAN fields. Phase 3 produced an uncomfortable
result: at native resolution the wet/dry label barely related to speed at all,
and Light rain even read as *faster* than dry. Either rain does not measurably
affect these cities, or the label was too coarse to see it.

Phase 4 can now distinguish those. Both labels sit on the same rows, so the
comparison is paired and the only thing that changed is resolution.

Four things are tested:
  1. Spatial resolution actually gained -- does rain now vary WITHIN a city?
     At 31 km, Manchester and Essen were a single cell, so every detector was
     forced to share one label. If the downscaled fields do not disagree across
     detectors, nothing was gained and the rest is moot.
  2. Dose-response, coarse vs downscaled, on identical intervals.
  3. Onset -- the first minutes of rain, which a 1-hour label cannot express.
  4. Dry-spell antecedent -- the oil-film effect.

Run:  python -m raincheck.weather.qa_rain_features
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from raincheck import config
from raincheck.weather.build_rain_features import ANALYSIS

BAND_ORDER = ["Dry", "Light", "Moderate", "Heavy", "Extreme"]
# Deviation is a ratio (speed / typical - 1), so effects are reported in
# percentage points of typical speed.
DEV = "typical_speed_deviation"


def _md_table(rows: list[dict], headers: list[str]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(h, "")) for h in headers) + " |")
    return "\n".join(out)


def spatial_gain(d, cities):
    """Does rain vary across detectors within one city at the same instant?

    This is the claim downscaling has to make good on. At native resolution the
    answer is no by construction -- Manchester and Essen resolved to a single
    ERA5 cell (reports/phase2_curation.md §5), so every detector in the city
    received an identical value.
    """
    rows = []
    for city in cities:
        sub = d.filter((F.col("city") == city) & F.col("precip_mm_h").isNotNull())
        per_ts = sub.groupBy("ts_10min").agg(
            F.countDistinct("band").alias("n_bands"),
            (F.max("precip_mm_h") - F.min("precip_mm_h")).alias("spread"),
            F.max("precip_mm_h").alias("mx"),
        )
        wet_ts = per_ts.filter(F.col("mx") >= F.lit(0.1))
        n_wet = wet_ts.count()
        if not n_wet:
            continue
        agg = wet_ts.agg(
            F.avg((F.col("n_bands") > 1).cast("int")).alias("disagree"),
            F.avg("spread").alias("mean_spread"),
            F.max("spread").alias("max_spread"),
        ).collect()[0]
        rows.append(
            {
                "city": city,
                "wet timestamps": f"{n_wet:,}",
                "detectors disagree on band": f"{100.0 * agg['disagree']:.1f}%",
                "mean within-city spread (mm/h)": f"{agg['mean_spread']:.2f}",
                "max spread": f"{agg['max_spread']:.1f}",
            }
        )
    return rows


def dose_response(d, cities, band_col: str, label: str):
    """Mean speed deviation by band, for one labelling."""
    sub = d.filter(F.col(DEV).isNotNull() & F.col(band_col).isNotNull())
    agg = (
        sub.groupBy("city", band_col)
        .agg(F.count(F.lit(1)).alias("n"), F.avg(DEV).alias("dev"))
        .collect()
    )
    by_city: dict[str, dict[str, tuple]] = {}
    for r in agg:
        by_city.setdefault(r["city"], {})[r[band_col]] = (r["n"], r["dev"])

    rows, contrasts = [], {}
    for city in cities:
        bands = by_city.get(city, {})
        dry = bands.get("Dry")
        if not dry:
            continue
        rec = {"city": city, "label": label}
        mp_n = mp_sum = 0.0
        for b in BAND_ORDER[1:]:
            if b in bands:
                n, dev = bands[b]
                # Reported relative to Dry, in percentage points.
                rec[b] = f"{100.0 * (dev - dry[1]):+.1f}"
                if b != "Light":
                    mp_n += n
                    mp_sum += dev * n
            else:
                rec[b] = "—"
        if mp_n:
            mp = mp_sum / mp_n
            contrasts[city] = 100.0 * (mp - dry[1])
            rec["**Moderate+**"] = f"**{contrasts[city]:+.1f}**"
        else:
            rec["**Moderate+**"] = "—"
        rows.append(rec)
    return rows, contrasts


def two_channel(d, cities):
    """Split the rain effect by road state -- the decomposition §L2a promises.

    Rain acts through two channels that behave oppositely, and pooling them
    hides both. On uncongested roads it cuts free-flow speed. On congested ones
    it cuts capacity, which bites hardest where demand already exceeds it.

    There is also a confound this table is meant to expose rather than hide:
    rain suppresses travel demand. Fewer vehicles on a signalised arterial means
    HIGHER speeds, which works against the direct effect and can flip the pooled
    sign. Conditioning on whether the detector was above its own critical
    occupancy separates "the road was slower" from "the road was emptier".
    """
    # Flow is compared the SAME way speed is -- against this detector's own dry
    # median for this weekday and time bin. A raw mean flow comparison would only
    # report that rain happened to fall during busier hours, and would sit in the
    # table looking like a demand effect.
    #
    # That baseline used to be recomputed here on the fly and thrown away. It now
    # lives in the L2a profile layer as `typical_flow_deviation`, because Phase 5
    # needs it in the model specification and two independently derived flow
    # baselines would eventually disagree.
    sub = d.filter(
        F.col(DEV).isNotNull() & F.col("congested").isNotNull() & F.col("band").isNotNull()
    ).withColumn("flow_dev", F.col("typical_flow_deviation"))
    agg = (
        sub.groupBy("city", "congested", "band")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.avg(DEV).alias("dev"),
            F.avg("flow_dev").alias("flow"),
        )
        .collect()
    )
    by: dict[tuple, dict] = {}
    for r in agg:
        by.setdefault((r["city"], r["congested"]), {})[r["band"]] = (r["n"], r["dev"], r["flow"])

    rows = []
    for city in cities:
        for cong in (False, True):
            bands = by.get((city, cong), {})
            dry = bands.get("Dry")
            if not dry:
                continue
            rec = {
                "city": city,
                "road state": "congested" if cong else "free-flowing",
                "dry n": f"{dry[0]:,}",
            }
            mp_n = mp_sum = mp_flow = 0.0
            for b in BAND_ORDER[1:]:
                if b in bands and b != "Light":
                    n, dev, flow = bands[b]
                    mp_n += n
                    mp_sum += dev * n
                    mp_flow += flow * n
            if mp_n:
                rec["Moderate+ Δspeed (pp)"] = f"{100.0 * (mp_sum / mp_n - dry[1]):+.1f}"
                # Both columns are now deviations from the same dry baseline, so
                # they can be read against each other: flow falling while speed
                # rises means the road emptied rather than sped up.
                rec["Moderate+ Δflow (pp)"] = f"{100.0 * (mp_flow / mp_n - dry[2]):+.1f}"
                rec["Moderate+ n"] = f"{int(mp_n):,}"
            else:
                rec["Moderate+ Δspeed (pp)"] = rec["Moderate+ Δflow (pp)"] = rec["Moderate+ n"] = "—"
            rows.append(rec)
    return rows


def onset_effect(d, cities):
    """Speed deviation by minutes since rain began.

    Only expressible because the fields are 10-minute. An hourly label cannot
    distinguish the first ten minutes of a downpour from its fifth hour.
    """
    bucket = (
        F.when(F.col("minutes_since_onset") < 10, "0-10 min")
        .when(F.col("minutes_since_onset") < 30, "10-30 min")
        .when(F.col("minutes_since_onset") < 60, "30-60 min")
        .otherwise("60+ min")
    )
    sub = (
        d.filter(F.col(DEV).isNotNull() & F.col("is_wet") & F.col("minutes_since_onset").isNotNull())
        .withColumn("bucket", bucket)
    )
    agg = sub.groupBy("city", "bucket").agg(
        F.count(F.lit(1)).alias("n"), F.avg(DEV).alias("dev")
    ).collect()

    dry = {
        r["city"]: r["dev"]
        for r in d.filter(F.col(DEV).isNotNull() & ~F.col("is_wet"))
        .groupBy("city").agg(F.avg(DEV).alias("dev")).collect()
    }

    by_city: dict[str, dict[str, tuple]] = {}
    for r in agg:
        by_city.setdefault(r["city"], {})[r["bucket"]] = (r["n"], r["dev"])

    rows = []
    for city in cities:
        b = by_city.get(city, {})
        if not b or city not in dry:
            continue
        rec = {"city": city}
        for k in ("0-10 min", "10-30 min", "30-60 min", "60+ min"):
            if k in b:
                n, dev = b[k]
                rec[k] = f"{100.0 * (dev - dry[city]):+.1f} ({n:,})"
            else:
                rec[k] = "—"
        rows.append(rec)
    return rows


def dry_spell_effect(d, cities):
    """The 'first rain after a dry spell' oil-film effect.

    Measured on onset intervals only, bucketed by how long it had been dry
    before the rain arrived.
    """
    bucket = (
        F.when(F.col("dry_spell_before") < 6, "< 6 h")
        .when(F.col("dry_spell_before") < 24, "6-24 h")
        .when(F.col("dry_spell_before") < 72, "1-3 d")
        .otherwise("3+ d")
    )
    from pyspark.sql import Window

    w = Window.partitionBy("city", "detid").orderBy("ts_10min")
    sub = (
        d.withColumn("dry_spell_before", F.lag("dry_spell_hours", 1).over(w))
        .filter(F.col(DEV).isNotNull() & F.col("is_onset") & F.col("dry_spell_before").isNotNull())
        .withColumn("bucket", bucket)
    )
    agg = sub.groupBy("city", "bucket").agg(
        F.count(F.lit(1)).alias("n"), F.avg(DEV).alias("dev")
    ).collect()
    by_city: dict[str, dict[str, tuple]] = {}
    for r in agg:
        by_city.setdefault(r["city"], {})[r["bucket"]] = (r["n"], r["dev"])

    rows = []
    for city in cities:
        b = by_city.get(city, {})
        if not b:
            continue
        rec = {"city": city}
        for k in ("< 6 h", "6-24 h", "1-3 d", "3+ d"):
            rec[k] = f"{100.0 * b[k][1]:+.1f} ({b[k][0]:,})" if k in b else "—"
        rows.append(rec)
    return rows


def main() -> int:
    spark = config.get_spark("qa_rain_features")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()
    cities = list(conf["study"].keys())

    d = spark.read.parquet(config.spark_path(ANALYSIS))
    d.cache()
    total = d.count()

    print("[1/5] spatial gain")
    spatial = spatial_gain(d, cities)
    print("[2/5] dose-response (native ERA5)")
    coarse_rows, coarse_c = dose_response(d, cities, "era5_band", "31 km / 1 h")
    print("[3/5] dose-response (downscaled)")
    fine_rows, fine_c = dose_response(d, cities, "band", "2 km / 10 min")
    print("[4/6] two-channel decomposition")
    channels = two_channel(d, cities)
    print("[5/6] onset")
    onset = onset_effect(d, cities)
    print("[6/6] dry-spell antecedent")
    spell = dry_spell_effect(d, cities)

    # ---- gates -----------------------------------------------------------
    failures = []
    for r in spatial:
        if float(r["detectors disagree on band"].rstrip("%")) < 1.0:
            failures.append(
                f"{r['city']}: detectors never disagree on rain band — the downscaled "
                f"field carries no within-city variation, so nothing was gained"
            )
    # NOT gated on "rain must slow traffic". That looks like the obvious check
    # and it is the wrong one: rain also suppresses demand, and on a signalised
    # arterial fewer vehicles means higher speeds. The pooled sign is therefore
    # genuinely ambiguous, and failing the phase on it would be asserting a
    # result rather than measuring one. §4 reports the decomposition instead.
    #
    # What IS gated is the thing this phase was built to deliver: spatial
    # information that did not exist at 31 km.

    comparison = []
    for city in cities:
        if city in coarse_c and city in fine_c:
            comparison.append(
                {
                    "city": city,
                    "31 km / 1 h": f"{coarse_c[city]:+.1f}",
                    "2 km / 10 min": f"{fine_c[city]:+.1f}",
                    "change": f"{fine_c[city] - coarse_c[city]:+.1f}",
                    "sharper?": "yes" if fine_c[city] < coarse_c[city] else "no",
                }
            )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    doc = [
        "# Phase-4 Downscaling QA — L2b",
        "",
        f"_Generated {ts}. {total:,} intervals, each carrying both the native ~31 km "
        f"hourly ERA5 label and the spateGAN 2 km / 10 min label._",
        "",
        f"**Gate verdict: {'PASS' if not failures else 'FAIL'}**",
        "",
    ]
    if failures:
        doc += [f"- {f}" for f in failures] + [""]

    doc += [
        "## 1. Did downscaling actually add spatial information?",
        "",
        "The prerequisite for everything else. At native resolution Manchester and",
        "Essen each collapsed to a **single ERA5 cell**, so every detector in the",
        "city was assigned identical rainfall — there was no within-city variation",
        "to exploit at all. If the 2 km fields do not disagree across detectors,",
        "the downscaling is decoration.",
        "",
        _md_table(
            spatial,
            ["city", "wet timestamps", "detectors disagree on band",
             "mean within-city spread (mm/h)", "max spread"],
        ),
        "",
        "## 2. Dose-response: coarse vs downscaled, same intervals",
        "",
        "Mean `typical_speed_deviation` by band, in **percentage points relative to",
        "Dry**. Negative means slower than this detector normally is in this hour of",
        "this weekday. Both labellings sit on the same rows, so this is a paired",
        "comparison in which the only thing that changed is resolution.",
        "",
        "**Native ~31 km / hourly:**",
        "",
        _md_table(coarse_rows, ["city", "Light", "Moderate", "Heavy", "Extreme", "**Moderate+**"]),
        "",
        "**Downscaled 2 km / 10 min:**",
        "",
        _md_table(fine_rows, ["city", "Light", "Moderate", "Heavy", "Extreme", "**Moderate+**"]),
        "",
        "**Moderate+ contrast, side by side** (more negative = rain effect resolved",
        "more sharply):",
        "",
        _md_table(comparison, ["city", "31 km / 1 h", "2 km / 10 min", "change", "sharper?"]),
        "",
        "## 3. Two-channel decomposition — and the demand confound",
        "",
        "The pooled numbers above are hard to read because rain does not do one",
        "thing. It slows vehicles, and it also **removes** them: fewer trips are",
        "taken, and on a signalised arterial fewer vehicles means higher speeds.",
        "Those two effects work in opposite directions on the pooled average, which",
        "is why a positive Moderate+ figure is not evidence that rain speeds traffic",
        "up.",
        "",
        "Splitting by whether the detector was above its own critical occupancy",
        "separates them, and carrying Δflow alongside Δspeed makes the demand",
        "channel visible directly: if flow falls while speed rises, the road got",
        "emptier, not faster.",
        "",
        _md_table(
            channels,
            ["city", "road state", "dry n", "Moderate+ n",
             "Moderate+ Δspeed (pp)", "Moderate+ Δflow (pp)"],
        ),
        "",
        "## 4. Rain onset",
        "",
        "Speed deviation by minutes since rain began, in percentage points relative",
        "to dry intervals (counts in brackets). Driver adaptation theory predicts the",
        "first minutes are disproportionately disruptive.",
        "",
        "This table cannot be produced at native resolution at all: an hourly label",
        "cannot separate the first ten minutes of rain from its fifth hour. It is the",
        "clearest single justification for the 10-minute fields.",
        "",
        _md_table(onset, ["city", "0-10 min", "10-30 min", "30-60 min", "60+ min"]),
        "",
        "## 5. Dry-spell antecedent",
        "",
        "Deviation at rain onset, bucketed by how long it had been dry beforehand.",
        "The documented oil-film effect predicts a larger slowdown after a long dry",
        "spell.",
        "",
        _md_table(spell, ["city", "< 6 h", "6-24 h", "1-3 d", "3+ d"]),
        "",
        "## 6. Caveats",
        "",
        "- **One ensemble member.** spateGAN is probabilistic and CONTEXT.md §L2b",
        "  specifies ensemble spread as an uncertainty covariate. Only seed 10 was",
        "  run; `run_downscaling --seed N` produces further members, and the feature",
        "  pipeline is member-agnostic. Until then there is no spread column.",
        "- **The downscaler is not observation.** These are plausible high-resolution",
        "  realisations conditioned on ERA5, not measurements. No radar ground truth",
        "  exists for these cities, so the honest framing (CONTEXT.md §9) is a",
        "  realisation, and the ablation in this report is the evidence it helps.",
        "- **Germany is in-domain, the UK and Italy are not.** spateGAN was trained on",
        "  German radar, so Essen is the in-domain anchor and Manchester and Torino",
        "  are out-of-domain generalisation tests.",
        "",
    ]

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS_DIR / "phase4_downscaling.md"
    out.write_text("\n".join(doc), encoding="utf-8")
    print(f"wrote {out}")

    if failures:
        print("FAIL: downscaling QA")
        for f in failures:
            print(f"  - {f}")
        spark.stop()
        return 1
    print("PASS: downscaling QA")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
