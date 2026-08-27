"""W2 -- Phase-1 feasibility audit over the landed Parquet.

Produces reports/phase1_audit.md: the document the Phase-1 gate is decided on.

Run:  python -m raincheck.audit.audit_utd19
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from raincheck import config

# Minimum observations for a (detector, dow, time-bin) cell to yield a usable
# median. Below this the baseline is noise -- and the baseline is the
# denominator of the delay metric.
MIN_OBS_PER_CELL = 20
COVERAGE_TARGET = 0.80

# Candidate profile resolutions, finest first.
RESOLUTIONS_MIN = [5, 15, 30, 60]

# Urban loop detectors reporting above this are almost certainly faulty.
SPEED_PLAUSIBILITY_CAP = 150.0


def _md_table(rows: list[dict], headers: list[str]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(h, "")) for h in headers) + " |")
    return "\n".join(out)


def city_availability(m):
    """Per-city variable availability, window, and quality retention."""
    agg = (
        m.groupBy("city")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("detid").alias("dets"),
            F.countDistinct("date").alias("days"),
            F.min("date").alias("first_day"),
            F.max("date").alias("last_day"),
            F.countDistinct("interval").alias("n_intervals"),
            (100.0 * F.count("speed") / F.count(F.lit(1))).alias("speed_pct"),
            (100.0 * F.count("flow") / F.count(F.lit(1))).alias("flow_pct"),
            (100.0 * F.count("occ") / F.count(F.lit(1))).alias("occ_pct"),
            (100.0 * F.sum(F.col("quality_ok").cast("int")) / F.count(F.lit(1))).alias("qok_pct"),
        )
        .orderBy(F.desc("speed_pct"), F.desc("rows"))
    )

    rows = []
    for r in agg.collect():
        step = int(86400 / r["n_intervals"]) if r["n_intervals"] else 0
        span = (r["last_day"] - r["first_day"]).days + 1
        # Fraction of the theoretically possible observations actually present.
        possible = r["dets"] * r["days"] * (86400 / step) if step else 0
        density = 100.0 * r["rows"] / possible if possible else 0.0
        rows.append(
            {
                "city": r["city"],
                "rows": f"{r['rows']:,}",
                "dets": r["dets"],
                "days": r["days"],
                "span_d": span,
                "step_s": step,
                "density%": f"{density:.0f}",
                "speed%": f"{r['speed_pct']:.1f}",
                "flow%": f"{r['flow_pct']:.1f}",
                "occ%": f"{r['occ_pct']:.1f}",
                "qual_ok%": f"{r['qok_pct']:.1f}",
                "window": f"{r['first_day']}..{r['last_day']}",
            }
        )
    return rows


def error_distribution(m):
    """The `error` encoding differs per city -- show it rather than assuming it."""
    agg = m.groupBy("city", "error").count().collect()
    per_city: dict[str, dict] = {}
    for r in agg:
        per_city.setdefault(r["city"], {})[r["error"] if r["error"] is not None else "NULL"] = r["count"]
    rows = []
    for city, dist in sorted(per_city.items()):
        total = sum(dist.values())
        parts = ", ".join(f"`{k}`={v / total * 100:.1f}%" for k, v in sorted(dist.items()))
        rows.append({"city": city, "distinct_values": len(dist), "distribution": parts})
    return rows


def profile_resolution(m, cities: list[str]):
    """Finest time binning at which the dry-baseline cells are populated enough.

    NOTE: rainfall is not yet joined, so every interval is treated as dry. These
    figures are therefore an UPPER BOUND -- the real numbers drop once rainy
    intervals are excluded. Recompute after the ERA5 pre-check (W3).
    """
    rows = []
    for city in cities:
        c = m.filter(F.col("city") == city).filter(F.col("quality_ok"))
        dets = c.select("detid").distinct().count()
        if dets == 0:
            continue
        rec = {"city": city, "dets": dets}
        for res in RESOLUTIONS_MIN:
            bins_per_day = 1440 // res
            cells = (
                c.withColumn("tbin", (F.col("interval") / (res * 60)).cast("int"))
                .groupBy("detid", "dow", "tbin")
                .agg(F.count(F.lit(1)).alias("n"))
            )
            ok = cells.filter(F.col("n") >= MIN_OBS_PER_CELL).count()
            expected = dets * 7 * bins_per_day
            rec[f"{res}min"] = f"{100.0 * ok / expected:.0f}%" if expected else "-"
        # Finest resolution clearing the target.
        best = "none"
        for res in RESOLUTIONS_MIN:
            if float(rec[f"{res}min"].rstrip("%")) >= COVERAGE_TARGET * 100:
                best = f"{res} min"
                break
        rec["viable"] = best
        rows.append(rec)
    return rows


def sanity(m, cities: list[str]):
    """Speed and occupancy distributions -- units and outliers."""
    rows = []
    for city in cities:
        c = m.filter(F.col("city") == city).filter(F.col("quality_ok"))
        q = c.select("speed").filter(F.col("speed").isNotNull())
        n_speed = q.count()
        if n_speed == 0:
            continue
        p = q.approxQuantile("speed", [0.01, 0.5, 0.95, 0.99], 0.001)
        over = q.filter(F.col("speed") > SPEED_PLAUSIBILITY_CAP).count()
        occ = c.select("occ").filter(F.col("occ").isNotNull())
        occ_max = occ.agg(F.max("occ")).collect()[0][0] if occ.limit(1).count() else None
        occ_p99 = occ.approxQuantile("occ", [0.99], 0.001)[0] if occ.limit(1).count() else None
        rows.append(
            {
                "city": city,
                "speed_p01": f"{p[0]:.1f}",
                "speed_p50": f"{p[1]:.1f}",
                "speed_p95": f"{p[2]:.1f}",
                "speed_p99": f"{p[3]:.1f}",
                f">{SPEED_PLAUSIBILITY_CAP:.0f}km/h": f"{100.0 * over / n_speed:.3f}%",
                "occ_p99": f"{occ_p99:.3f}" if occ_p99 is not None else "-",
                "occ_max": f"{occ_max:.3f}" if occ_max is not None else "-",
                "occ_scale": ("0-1" if (occ_max or 0) <= 1.5 else "0-100 (!)"),
            }
        )
    return rows


def join_integrity(spark, m):
    """Detector metadata vs measurements -- silent join loss is the risk."""
    d = spark.read.parquet(config.spark_path(config.LANDED_DETECTORS))
    meas_keys = m.select("city", "detid").distinct()
    det_keys = d.select("city", "detid").distinct()

    only_meas = meas_keys.join(det_keys, ["city", "detid"], "left_anti").count()
    only_det = det_keys.join(meas_keys, ["city", "detid"], "left_anti").count()

    det_city = d.groupBy("city").agg(
        F.count(F.lit(1)).alias("dets"),
        F.count("linkid").alias("with_linkid"),
        F.count("lat").alias("with_geo"),
        F.countDistinct("fclass").alias("n_fclass"),
    )
    return only_meas, only_det, det_city.orderBy("city").collect()


def main() -> int:
    spark = config.get_spark("audit_utd19")
    spark.sparkContext.setLogLevel("WARN")
    conf = config.load_cities_conf()

    candidates = [c for g in conf["cohorts"].values() for c in g["cities"]]

    m = spark.read.parquet(config.spark_path(config.LANDED_MEASUREMENTS))

    print("[1/5] city availability")
    avail = city_availability(m)
    print("[2/5] error distribution")
    errs = error_distribution(m)
    print("[3/5] profile resolution (candidates)")
    prof = profile_resolution(m, candidates)
    print("[4/5] sanity distributions (candidates)")
    san = sanity(m, candidates)
    print("[5/5] join integrity")
    only_meas, only_det, det_city = join_integrity(spark, m)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    doc = [
        "# Phase-1 Feasibility Audit — UTD19",
        "",
        f"_Generated {ts} from landed Parquet ({config.EXPECTED_MEASUREMENT_ROWS:,} rows)._",
        "",
        "## 1. Per-city availability",
        "",
        "`density%` = observed rows as a share of `detectors x days x bins/day`.",
        "A long window with low density is mostly holes.",
        "",
        _md_table(
            avail,
            ["city", "rows", "dets", "days", "span_d", "step_s", "density%",
             "speed%", "flow%", "occ%", "qual_ok%", "window"],
        ),
        "",
        "## 2. Quality-flag encoding",
        "",
        "UTD19 encodes `error` inconsistently across cities. A `WHERE error = 0`",
        "filter would discard every row in the cities that use NULL-vs-`1`.",
        "",
        _md_table(errs, ["city", "distinct_values", "distribution"]),
        "",
        "## 3. Baseline profile resolution (candidate cities)",
        "",
        f"Share of `(detector, dow, time-bin)` cells holding at least "
        f"{MIN_OBS_PER_CELL} observations, by bin width. `viable` is the finest "
        f"binning reaching {COVERAGE_TARGET:.0%}.",
        "",
        "> **Upper bound.** Rainfall is not yet joined, so all intervals count as",
        "> dry. Recompute after the ERA5 pre-check (W3).",
        "",
        _md_table(prof, ["city", "dets"] + [f"{r}min" for r in RESOLUTIONS_MIN] + ["viable"]),
        "",
        "## 4. Sanity distributions (candidate cities)",
        "",
        _md_table(
            san,
            ["city", "speed_p01", "speed_p50", "speed_p95", "speed_p99",
             f">{SPEED_PLAUSIBILITY_CAP:.0f}km/h", "occ_p99", "occ_max", "occ_scale"],
        ),
        "",
        "## 5. Join integrity",
        "",
        f"- Measurement `(city, detid)` keys absent from detector metadata: **{only_meas:,}**",
        f"- Detector metadata keys absent from measurements: **{only_det:,}**",
        "",
        _md_table(
            [
                {
                    "city": r["city"],
                    "dets": r["dets"],
                    "with_linkid": r["with_linkid"],
                    "with_geo": r["with_geo"],
                    "n_fclass": r["n_fclass"],
                }
                for r in det_city
            ],
            ["city", "dets", "with_linkid", "with_geo", "n_fclass"],
        ),
        "",
    ]

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS_DIR / "phase1_audit.md"
    out.write_text("\n".join(doc), encoding="utf-8")
    print(f"wrote {out}")
    # Machine-readable windows for the ERA5 pre-check (W3), so the rain pull
    # uses each city's true window rather than a hardcoded guess.
    by_city = {r["city"]: r for r in avail}
    windows = {
        c: {
            "first_day": by_city[c]["window"].split("..")[0],
            "last_day": by_city[c]["window"].split("..")[1],
            "dets": by_city[c]["dets"],
            "bbox": conf["bboxes"].get(c),
        }
        for c in candidates
        if c in by_city
    }
    wout = config.REPORTS_DIR / "phase1_windows.json"
    wout.write_text(json.dumps(windows, indent=2), encoding="utf-8")
    print(f"wrote {wout}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
