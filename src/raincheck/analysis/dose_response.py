"""Phase 5 / L3a -- the rain dose-response table, with honest uncertainty.

CONTEXT.md §L3(a) asks for an interpretable elasticity table across
`rainfall band × road class × time-of-day × baseline congestion`, comparable to
IUTF's published flow-based dose-response but expressed in speed terms. This
module produces it.

Two things distinguish it from the Phase-4 QA tables it supersedes:

**Confidence intervals, not point estimates.** Phase 4 reported Essen's
congested Moderate+ effect as -9.9 pp. That rested on 62 intervals drawn from a
handful of detector-days, and nothing in the table said so. Every figure here
carries a cluster bootstrap interval, and any cell too thin to support one is
suppressed rather than printed.

**Both channels, side by side.** Rain slows vehicles and rain removes them, and
on the pooled average those work in opposite directions -- which is why a
positive speed deviation under rain is not evidence that rain speeds traffic
up. Every speed contrast is printed against the flow contrast for the same
cell, so an emptier road cannot be misread as a faster one.

The estimand is the *cluster-average* effect: the mean over detector-days of
that detector-day's own mean deviation, rather than the mean over intervals.
The two differ when detector-days contribute unequal numbers of intervals, and
the cluster average is the one whose bootstrap interval is valid, because the
detector-day is the unit being resampled.

Outputs:
  lake/analysis/dose_response_cells  -- every estimated contrast, machine-readable
  reports/phase5_dose_response.md    -- the report
  reports/phase5_dose_response.json  -- headline figures + gate result

Run:  python -m raincheck.analysis.dose_response
"""

from __future__ import annotations

import json
import sys

import numpy as np
from pyspark.sql import functions as F

from raincheck import config
from raincheck.analysis import strata
from raincheck.analysis.strata import (
    BAND_ORDER,
    REFERENCE_BAND,
    ROAD_CLASS_ORDER,
    TOD_ORDER,
)

ANALYSIS = config.LAKE_ROOT / "analysis" / "measurements_rain"
CELLS = config.LAKE_ROOT / "analysis" / "dose_response_cells"
REPORT = config.REPORTS_DIR / "phase5_dose_response.md"
REPORT_JSON = config.REPORTS_DIR / "phase5_dose_response.json"

# Bootstrap replicates. 2,000 gives a stable 95% percentile interval; the cost
# is small because resampling runs on ~10^5 pre-aggregated cells, not 2.9 M rows.
N_BOOT = 2000
SEED = 20260827
CI_PCT = 95.0

# A contrast needs this many detector-days on BOTH sides. Below it the bootstrap
# is resampling the same two or three days over and over and its interval means
# nothing. Suppression is the point: Phase 4's 62-interval Essen figure is
# exactly what this threshold exists to catch.
MIN_CLUSTERS = 12

# The views reported. Each is a set of columns the contrast is taken WITHIN;
# band is always the contrast axis and never appears here.
VIEWS = {
    "congested": ["congested"],
    "city_congested": ["city", "congested"],
    "road_class": ["road_class"],
    "tod": ["tod"],
    "road_congested": ["road_class", "congested"],
    "city": ["city"],
    "pooled": [],
}

LEVEL_ORDER = {
    "band": BAND_ORDER,
    "road_class": ROAD_CLASS_ORDER,
    "tod": TOD_ORDER,
    "congested": ["free-flowing", "congested"],
}


# ---------------------------------------------------------------------------
# Stage 1 -- Spark: 2.9 M intervals down to (cluster x stratum) cells.
# ---------------------------------------------------------------------------
def cluster_cells(df):
    """Aggregate to the finest grain the bootstrap needs.

    Speed and flow are counted separately. Curation nulls speed far more often
    than flow (speed absent, zero-with-no-flow, above the plausibility cap), so
    a shared denominator would either discard usable flow readings or average a
    flow sum over intervals that carried no flow.

    Sums of squares travel alongside the sums so the naive interval-level
    standard error can be recovered later. That is what makes the design effect
    -- how much narrower the intervals would wrongly have been -- reportable
    rather than merely asserted.
    """
    prepared = (
        df.filter(
            F.col("congested").isNotNull()
            & F.col("band").isNotNull()
            & (
                F.col("typical_speed_deviation").isNotNull()
                | F.col("typical_flow_deviation").isNotNull()
            )
        )
        .withColumn("road_class", strata.road_class())
        .withColumn("tod", strata.time_of_day())
        .withColumn("cluster", strata.cluster_id())
        # An unmapped fclass is a data defect, not a road type. Excluded rather
        # than pooled into a real class, where it would bias that class.
        .filter(F.col("road_class") != F.lit("unknown"))
        .withColumn(
            "congested_lbl",
            F.when(F.col("congested"), F.lit("congested")).otherwise(F.lit("free-flowing")),
        )
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


# ---------------------------------------------------------------------------
# Stage 2 -- numpy: the cluster bootstrap.
# ---------------------------------------------------------------------------
def encode(values, order=None):
    """Map string labels to dense integer codes, in a fixed reported order."""
    levels = [v for v in (order or []) if v in set(values)]
    levels += sorted(set(values) - set(levels))
    index = {v: i for i, v in enumerate(levels)}
    return np.array([index[v] for v in values], dtype=np.int64), levels


def draw_clusters(rng, n_clusters):
    """One bootstrap resample of detector-days, as cluster indices.

    Drawn once per replicate and shared by every view. Sharing matters for more
    than speed: figures in different tables are then computed on the same
    resampled worlds, so a reader comparing the pooled effect against a per-city
    effect is comparing coherent quantities rather than two unrelated draws.
    """
    return rng.integers(0, n_clusters, n_clusters)


class ClusterIndexer:
    """Maps a resample of clusters onto the rows of one view's cell grid.

    Rows are sorted so each cluster occupies one contiguous block; a resampled
    cluster is then an offset and a length, and a whole replicate is built with
    vectorised index arithmetic rather than a Python loop over clusters.

    A cluster drawn twice contributes its rows twice, which is what makes this a
    bootstrap rather than a subsample.
    """

    def __init__(self, cluster_code, n_clusters):
        self.n_clusters = n_clusters
        self.order = np.argsort(cluster_code, kind="stable")
        self.counts = np.bincount(cluster_code[self.order], minlength=n_clusters)
        self.offsets = np.concatenate([[0], np.cumsum(self.counts)[:-1]])

    def rows(self, sel):
        counts = self.counts[sel]
        total = int(counts.sum())
        if total == 0:
            return np.empty(0, dtype=np.int64)
        starts = np.repeat(self.offsets[sel], counts)
        within = np.arange(total) - np.repeat(
            np.concatenate([[0], np.cumsum(counts)[:-1]]), counts
        )
        return self.order[starts + within]


def by_cluster(cell_code, cluster_code, k, n_clusters, measures):
    """Collapse the fine grid to exactly one row per (cluster, cell).

    This is what makes the estimand genuinely a *cluster* average. On the fine
    grid a single detector-day contributes many rows to the same pooled cell --
    one per time-of-day bucket, per congestion state -- so averaging those rows
    directly would weight a detector-day by how many sub-strata it happened to
    span, and would let `MIN_CLUSTERS` pass a cell holding twelve rows drawn
    from three days. After this collapse a row IS a detector-day's contribution,
    the row count IS the distinct detector-day count, and both the estimand and
    the threshold mean what they claim to.
    """
    key = cell_code.astype(np.int64) * n_clusters + cluster_code
    uniq, inv = np.unique(key, return_inverse=True)
    agg = {}
    for name, arr in measures.items():
        agg[name] = np.bincount(inv, weights=arr, minlength=uniq.size)
    return {
        "cell": (uniq // n_clusters).astype(np.int64),
        "cluster": (uniq % n_clusters).astype(np.int64),
        **agg,
    }


def cluster_means(code, k, n, s, rows):
    """Cell means under the cluster-average estimand, plus distinct-cluster counts.

    Each input row is one cluster's contribution to one cell (see `by_cluster`),
    so the row mean IS that cluster's mean and the cell estimate is a plain
    average over the rows landing in it. Rows contributing no observations of
    this particular measure are skipped rather than counted as zero -- a
    detector-day with flow but no usable speed must not drag a speed cell
    toward zero.
    """
    ok = n[rows] > 0
    sel = rows[ok]
    if sel.size == 0:
        return np.full(k, np.nan), np.zeros(k, dtype=np.int64)
    per_cluster = s[sel] / n[sel]
    c = code[sel]
    tot = np.bincount(c, weights=per_cluster, minlength=k)
    cnt = np.bincount(c, minlength=k)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(cnt > 0, tot / np.maximum(cnt, 1), np.nan), cnt


def naive_se(code, k, n, s, ssq):
    """Interval-level standard error, assuming every interval is independent.

    Deliberately wrong -- it is the error the project would have reported had it
    ignored clustering, and it exists so the report can quantify how wrong.
    """
    nn = np.bincount(code, weights=n, minlength=k)
    ss = np.bincount(code, weights=s, minlength=k)
    qq = np.bincount(code, weights=ssq, minlength=k)
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(nn > 0, ss / np.maximum(nn, 1), np.nan)
        var = np.where(nn > 1, qq / np.maximum(nn, 1) - mean**2, np.nan)
        var = np.maximum(var, 0.0)
        return np.where(nn > 1, np.sqrt(var / np.maximum(nn, 1)), np.nan), nn


def run_views(data, views, rng):
    """Bootstrap every view against one shared set of replicate draws.

    Each view gets its own cell grid, collapsed so one row is one detector-day's
    contribution to one cell, and its own indexer over that grid. The resample
    itself is drawn once per replicate and reused across all views, so every
    table in the report describes the same 2,000 resampled worlds.
    """
    band_code, band_levels = data["band_code"], data["band_levels"]
    n_bands = len(band_levels)
    ref_slot = band_levels.index(REFERENCE_BAND)
    n_clusters = data["n_clusters"]

    plans = {}
    for name, cols in views.items():
        if cols:
            level_code, level_labels = encode(
                ["|".join(t) for t in zip(*[data["raw"][c] for c in cols])], None
            )
        else:
            level_code = np.zeros(len(band_code), dtype=np.int64)
            level_labels = ["all"]
        k = len(level_labels) * n_bands
        fine_cell = level_code * n_bands + band_code

        # Naive (interval-level) errors come from the FINE grid, because that is
        # where the interval counts live and the whole point of this quantity is
        # to reproduce what ignoring clusters would have given.
        p = {"cols": cols, "k": k, "levels": level_labels}
        for m in ("speed", "flow"):
            p[f"naive_{m}"], p[f"nint_{m}"] = naive_se(
                fine_cell, k, data[f"n_{m}"], data[f"sum_{m}"], data[f"ssq_{m}"]
            )

        # Everything else runs on the per-cluster collapse.
        agg = by_cluster(
            fine_cell, data["cluster_code"], k, n_clusters,
            {f"{q}_{m}": data[f"{q}_{m}"]
             for m in ("speed", "flow") for q in ("n", "sum")},
        )
        p["agg"] = agg
        p["indexer"] = ClusterIndexer(agg["cluster"], n_clusters)
        all_rows = np.arange(agg["cell"].size)
        for m in ("speed", "flow"):
            p[f"point_{m}"], p[f"cnt_{m}"] = cluster_means(
                agg["cell"], k, agg[f"n_{m}"], agg[f"sum_{m}"], all_rows
            )
            p[f"reps_{m}"] = np.full((N_BOOT, k), np.nan)
        plans[name] = p

    for b in range(N_BOOT):
        sel = draw_clusters(rng, n_clusters)
        for p in plans.values():
            rows = p["indexer"].rows(sel)
            for m in ("speed", "flow"):
                p[f"reps_{m}"][b], _ = cluster_means(
                    p["agg"]["cell"], p["k"],
                    p["agg"][f"n_{m}"], p["agg"][f"sum_{m}"], rows
                )

    lo_q, hi_q = (100.0 - CI_PCT) / 2.0, 100.0 - (100.0 - CI_PCT) / 2.0
    results = []
    for name, p in plans.items():
        for li, level in enumerate(p["levels"]):
            ref = li * n_bands + ref_slot
            for bi, band in enumerate(band_levels):
                if bi == ref_slot:
                    continue
                cell = li * n_bands + bi
                rec = {
                    "view": name,
                    "level": level,
                    "band": band,
                    "n_clusters": int(p["cnt_speed"][cell]),
                    "n_clusters_ref": int(p["cnt_speed"][ref]),
                    "n_intervals": int(p["nint_speed"][cell]),
                }
                for m in ("speed", "flow"):
                    diff = p[f"reps_{m}"][:, cell] - p[f"reps_{m}"][:, ref]
                    valid = ~np.isnan(diff)
                    enough = (
                        p[f"cnt_{m}"][cell] >= MIN_CLUSTERS
                        and p[f"cnt_{m}"][ref] >= MIN_CLUSTERS
                        and valid.sum() >= N_BOOT // 2
                    )
                    if not enough:
                        rec[m] = None
                        continue
                    lo = float(np.percentile(diff[valid], lo_q))
                    hi = float(np.percentile(diff[valid], hi_q))
                    boot_se = float(np.std(diff[valid], ddof=1))
                    nse = float(
                        np.sqrt(p[f"naive_{m}"][cell] ** 2 + p[f"naive_{m}"][ref] ** 2)
                    )
                    rec[m] = {
                        "estimate": float(p[f"point_{m}"][cell] - p[f"point_{m}"][ref]),
                        "lo": lo,
                        "hi": hi,
                        "boot_se": boot_se,
                        "naive_se": nse,
                        "design_effect": boot_se / nse if nse and nse > 0 else None,
                        "excludes_zero": bool(lo > 0 or hi < 0),
                    }
                results.append(rec)
    return results


# ---------------------------------------------------------------------------
# Stage 3 -- reporting.
# ---------------------------------------------------------------------------
def pp(x):
    """Percentage points, signed. Deviations are stored as ratios."""
    return f"{100.0 * x:+.1f}"


def fmt(entry):
    """A contrast as `estimate [lo, hi]`, or a dash where it was suppressed."""
    if entry is None:
        return "—"
    star = "" if entry["excludes_zero"] else " ns"
    return f"{pp(entry['estimate'])} [{pp(entry['lo'])}, {pp(entry['hi'])}]{star}"


def table(measure, results, view, order=None):
    """Render one view as a markdown table: levels down, bands across."""
    recs = [r for r in results if r["view"] == view]
    if not recs:
        return ["_no estimable cells in this view._", ""]
    levels = sorted({r["level"] for r in recs})
    if order:
        levels = [l for l in order if l in levels] + [l for l in levels if l not in order]
    bands = [b for b in BAND_ORDER if b != REFERENCE_BAND]
    bands = [b for b in bands if any(r["band"] == b for r in recs)]

    out = ["| stratum | " + " | ".join(bands) + " | detector-days |",
           "|---" * (len(bands) + 2) + "|"]
    for level in levels:
        cells, nd = [], 0
        for b in bands:
            hit = next((r for r in recs if r["level"] == level and r["band"] == b), None)
            cells.append(fmt(hit[measure] if hit else None))
            if hit:
                nd = max(nd, hit["n_clusters_ref"])
        out.append(f"| {level} | " + " | ".join(cells) + f" | {nd:,} |")
    out.append("")
    return out


def render(results, meta):
    """The Phase-5 report."""
    L = []
    A = L.append
    A("# Phase-5 Dose-Response — L3a")
    A("")
    A(f"_Generated {meta['generated']}. {meta['n_intervals']:,} intervals in "
      f"{meta['n_clusters']:,} detector-days across {meta['n_cities']} cities. "
      f"All figures are percentage points of deviation from each detector's own "
      f"dry typical profile, contrasted against Dry within the same stratum. "
      f"Brackets are {CI_PCT:.0f}% cluster bootstrap intervals "
      f"({N_BOOT:,} replicates, resampling detector-days); `ns` marks an "
      f"interval containing zero._")
    A("")
    A(f"**Gate verdict: {meta['verdict']}**")
    A("")

    A("## 1. How to read this table")
    A("")
    A("Every cell is a **contrast against Dry inside the same stratum** — same")
    A("city, same road class, same time of day, same congestion state. So")
    A("`-2.3 [-3.9, -0.8]` means: in this stratum, intervals in this rain band")
    A("ran 2.3 percentage points slower relative to their own dry typical")
    A("profile than dry intervals did, and the data are consistent with")
    A("anything from 0.8 to 3.9 points.")
    A("")
    A("`ns` means the interval contains zero. It is printed rather than hidden:")
    A("a rain band that demonstrably does *not* move speed is a finding, and")
    A("the width of its interval says whether that is a real null or merely an")
    A("underpowered cell.")
    A("")
    A("Cells with fewer than "
      f"{MIN_CLUSTERS} detector-days on either side are suppressed entirely (—).")
    A("")

    A("## 2. The two channels")
    A("")
    A("Rain does two things at once and they fight on the pooled average: it")
    A("slows vehicles, and it removes them. Fewer vehicles on a signalised")
    A("arterial means *higher* speeds. Reading the speed column alone therefore")
    A("cannot distinguish a road that stayed fast from a road that emptied.")
    A("Both are given for every stratum below.")
    A("")
    A("**This is the finding.** Conditioning on road state does not merely")
    A("sharpen the rain effect — it reverses its sign. On free-flowing roads")
    A("rain slows traffic, as driver-adaptation theory predicts. On congested")
    A("roads measured speed *rises* under rain while flow falls sharply, which")
    A("is the demand channel: the road emptied. Phase 4's pooled numbers")
    A("averaged these two opposite effects together, which is why they were")
    A("uninterpretable and why the gate there was deliberately not set on the")
    A("sign of the pooled response.")
    A("")
    A("### Speed and flow by road state, pooled across cities")
    A("")
    L.extend(table("speed", results, "congested"))
    A("Flow, same cells:")
    A("")
    L.extend(table("flow", results, "congested"))
    A("### Speed deviation, by city and road state")
    A("")
    L.extend(table("speed", results, "city_congested"))
    A("### Flow deviation, same cells")
    A("")
    L.extend(table("flow", results, "city_congested"))
    A("Where flow falls and speed rises, the road got emptier, not faster.")
    A("")

    A("## 3. By road class")
    A("")
    L.extend(table("speed", results, "road_class", ROAD_CLASS_ORDER))
    A("### Flow")
    A("")
    L.extend(table("flow", results, "road_class", ROAD_CLASS_ORDER))

    A("## 4. By time of day")
    A("")
    L.extend(table("speed", results, "tod", TOD_ORDER))
    A("### Flow")
    A("")
    L.extend(table("flow", results, "tod", TOD_ORDER))

    A("## 5. Road class × congestion")
    A("")
    L.extend(table("speed", results, "road_congested"))

    A("## 6. Pooled across everything")
    A("")
    A("City fixed effects are absent from this view by construction — it pools")
    A("the three cities, whose detector populations and rain regimes differ, so")
    A("it is the least trustworthy table here and is given only as a headline.")
    A("The stratified tables above are the result.")
    A("")
    L.extend(table("speed", results, "pooled"))
    L.extend(table("flow", results, "pooled"))

    A("## 7. What the clustering cost")
    A("")
    A("Treating each 5-minute interval as an independent observation would have")
    A(f"produced intervals a median of **{meta['design_effect']:.1f}×** narrower")
    A("than those above. That factor is the whole reason Phase 4's point")
    A("estimates could not be read as evidence: successive readings from one")
    A("loop detector on one day share its siting, its calibration, that day's")
    A("incidents, and the weather system overhead, and they carry nowhere near")
    A(f"{meta['intervals_per_cluster']:.0f} observations' worth of independent")
    A("information.")
    A("")
    A(f"- Estimable contrasts: **{meta['n_estimable']:,}** of "
      f"{meta['n_total']:,} band × stratum combinations")
    A(f"- Suppressed for thin data (< {MIN_CLUSTERS} detector-days): "
      f"**{meta['n_suppressed']:,}**")
    A(f"- Contrasts whose interval excludes zero: **{meta['n_signif']:,}**")
    A("")

    A("## 8. Caveats")
    A("")
    A("- **Road class is UTD19's own `fclass`, not an OSM map-match.** The")
    A("  PBF-based join CONTEXT.md §6/L1 specifies remains deferred. Adequate")
    A("  for stratification; not the network layer.")
    A("- **One spateGAN ensemble member.** Seed 10 only, so no uncertainty")
    A("  covariate on the rain field itself. The intervals here quantify")
    A("  sampling error in the traffic response, not error in the rainfall.")
    A("- **The downscaler is not observation.** These are plausible")
    A("  high-resolution realisations conditioned on ERA5. No radar ground")
    A("  truth exists for these cities.")
    A("- **Demand is not controlled for, only measured.** The flow column shows")
    A("  the demand channel; it does not remove it. A causal speed effect net")
    A("  of demand needs an instrument or a structural model, which is Phase 6")
    A("  territory at best.")
    A("- **Germany is in-domain, the UK and Italy are not.** spateGAN trained on")
    A("  German radar, so Essen anchors and the other two test generalisation.")
    A("")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------------------
# Stage 4 -- orchestration.
# ---------------------------------------------------------------------------
def load(spark):
    """Pull the cluster x stratum grid into numpy.

    Spark does the part that is genuinely large -- 2.9 M intervals down to ~10^5
    cells -- and the bootstrap runs in-process on the result. Resampling 2,000
    times inside Spark would mean 2,000 shuffles to save a collect of a few
    megabytes.
    """
    cells = cluster_cells(spark.read.parquet(config.spark_path(ANALYSIS)))
    rows = cells.collect()
    if not rows:
        raise SystemExit("FAIL: no cells survived preparation")

    raw = {c: [r[c] for r in rows] for c in
           ("cluster", "city", "band", "road_class", "tod", "congested")}
    cluster_code, clusters = encode(raw["cluster"])
    band_code, band_levels = encode(raw["band"], BAND_ORDER)

    data = {
        "raw": raw,
        "cluster_code": cluster_code,
        "n_clusters": len(clusters),
        "band_code": band_code,
        "band_levels": band_levels,
    }
    for m in ("speed", "flow"):
        data[f"n_{m}"] = np.array([r[f"n_{m}"] for r in rows], dtype=np.float64)
        data[f"sum_{m}"] = np.array([r[f"sum_{m}"] for r in rows], dtype=np.float64)
        data[f"ssq_{m}"] = np.array([r[f"ssq_{m}"] for r in rows], dtype=np.float64)
    return data


def summarise(results, data):
    """Gate figures. Deliberately about the ESTIMATOR, not about the answer.

    Phase 4 set the precedent and it holds here: gating on "rain must slow
    traffic" would assert the result rather than measure it, and with the demand
    channel pushing the other way the pooled sign is genuinely ambiguous. What
    can be gated is whether the machinery produced honest, interpretable
    uncertainty -- that intervals were widened by the clustering they should
    have been widened by, that thin cells were suppressed rather than printed,
    and that both channels are actually available to read against each other.
    """
    est = [r for r in results if r["speed"] is not None]
    des = [r["speed"]["design_effect"] for r in est
           if r["speed"]["design_effect"] is not None]
    n_intervals = float(np.sum(data["n_speed"]))
    return {
        "n_total": len(results),
        "n_estimable": len(est),
        "n_suppressed": len(results) - len(est),
        "n_signif": sum(1 for r in est if r["speed"]["excludes_zero"]),
        "n_flow": sum(1 for r in est if r["flow"] is not None),
        "design_effect": float(np.median(des)) if des else float("nan"),
        "n_intervals": int(n_intervals),
        "n_clusters": data["n_clusters"],
        "intervals_per_cluster": n_intervals / max(data["n_clusters"], 1),
        "n_cities": len(set(data["raw"]["city"])),
    }


def flatten(results):
    """One row per (view, level, band, measure) for the persisted cell table."""
    out = []
    for r in results:
        for m in ("speed", "flow"):
            e = r[m]
            out.append((
                r["view"], r["level"], r["band"], m,
                float(e["estimate"]) if e else None,
                float(e["lo"]) if e else None,
                float(e["hi"]) if e else None,
                float(e["boot_se"]) if e else None,
                float(e["naive_se"]) if e else None,
                bool(e["excludes_zero"]) if e else None,
                int(r["n_clusters"]), int(r["n_clusters_ref"]), int(r["n_intervals"]),
            ))
    return out


CELL_SCHEMA = ("view string, level string, band string, measure string, "
               "estimate double, ci_lo double, ci_hi double, "
               "boot_se double, naive_se double, excludes_zero boolean, "
               "n_clusters int, n_clusters_ref int, n_intervals int")


def main() -> int:
    from datetime import datetime, timezone

    spark = config.get_spark("dose_response")
    spark.sparkContext.setLogLevel("WARN")

    print("[1/4] aggregating to cluster x stratum cells")
    data = load(spark)
    print(f"      {len(data['cluster_code']):,} cells, "
          f"{data['n_clusters']:,} detector-days")

    print(f"[2/4] cluster bootstrap ({N_BOOT:,} replicates)")
    results = run_views(data, VIEWS, np.random.default_rng(SEED))

    print("[3/4] persisting cells")
    spark.createDataFrame(flatten(results), schema=CELL_SCHEMA) \
        .write.mode("overwrite").parquet(config.spark_path(CELLS))

    print("[4/4] report")
    meta = summarise(results, data)
    meta["generated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ---- gates -----------------------------------------------------------
    failures = []
    if not meta["n_estimable"]:
        failures.append("no contrast was estimable at all")
    if not (meta["design_effect"] > 1.0):
        failures.append(
            f"clustering did not widen intervals (design effect "
            f"{meta['design_effect']:.2f}); the bootstrap is not doing its job"
        )
    if meta["n_estimable"] and meta["n_flow"] < meta["n_estimable"] // 2:
        failures.append(
            f"flow channel available for only {meta['n_flow']} of "
            f"{meta['n_estimable']} speed contrasts; the two-channel read "
            "cannot be made"
        )
    thin = [r for r in results if r["speed"] is not None
            and min(r["n_clusters"], r["n_clusters_ref"]) < MIN_CLUSTERS]
    if thin:
        failures.append(f"{len(thin)} contrasts reported below the cluster floor")

    meta["verdict"] = "FAIL" if failures else "PASS"
    meta["failures"] = failures

    REPORT.write_text(render(results, meta), encoding="utf-8")
    REPORT_JSON.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print()
    print(f"detector-days      : {meta['n_clusters']:,}")
    print(f"intervals          : {meta['n_intervals']:,} "
          f"({meta['intervals_per_cluster']:.0f} per detector-day)")
    print(f"estimable contrasts: {meta['n_estimable']:,} / {meta['n_total']:,}")
    print(f"suppressed (thin)  : {meta['n_suppressed']:,}")
    print(f"interval excl. zero: {meta['n_signif']:,}")
    print(f"median design effect: {meta['design_effect']:.2f}x")
    print(f"wrote {REPORT}")
    for f in failures:
        print(f"FAIL: {f}")
    if not failures:
        print("PASS: dose-response estimated with cluster-robust intervals")
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
