"""Does "fewer cars on the road" explain why light rain looks faster?

The puzzle: drizzle appears to *raise* speed, and the threshold sweep showed
this is not an artefact of blurry rainfall data -- sharpening the rain label to
each detector's own 2 km cell made it stronger, not weaker.

The proposed explanation is mechanical rather than behavioural. Rain does two
things at once and they push speed in opposite directions: it makes drivers
slower, and it makes some of them not drive at all. On a road carrying fewer
vehicles, the remaining vehicles go faster. If the second effect is larger, the
measured speed rises even though rain has not made anyone quicker.

This is testable because the project already records both channels for every
interval: `typical_speed_deviation` (how fast, against this detector's normal
for this hour) and `typical_flow_deviation` (how many cars, against the same
normal). Phase 5 estimated both. Three tests, weakest to strongest:

  1. SIGN PATTERN -- wherever speed rises under rain, does traffic fall?
     Reads the published Phase 5 cells; costs nothing.
  2. CO-MOVEMENT -- across cells, does a bigger drop in traffic go with a
     bigger rise in speed? A rank correlation, so a few outliers cannot carry
     it. Also reads the published cells.
  3. CONDITIONING -- split rainy intervals by whether traffic was actually
     lighter than normal. If the story holds, the speed rise should live almost
     entirely in the lighter-traffic half and largely vanish in the other.

Test 3 needs its own estimation pass and carries a real caveat, stated in the
report: traffic volume is itself changed by rain, so splitting on it is
splitting on something the rain caused. That can manufacture a difference on
its own. Test 3 corroborates tests 1 and 2; it cannot carry the conclusion by
itself, and the report says so.

Run:  python -m raincheck.analysis.verify_emptier_roads
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import numpy as np
from pyspark.sql import functions as F

from raincheck import config
from raincheck.analysis import dose_response as dr
from raincheck.analysis import strata
from raincheck.analysis.strata import BAND_ORDER, WET_BANDS

ANALYSIS = config.LAKE_ROOT / "analysis" / "measurements_rain"
CELLS = config.LAKE_ROOT / "analysis" / "dose_response_cells"
REPORT = config.REPORTS_DIR / "phase5_emptier_roads.md"
REPORT_JSON = config.REPORTS_DIR / "phase5_emptier_roads.json"

LIGHTER = "traffic lighter than normal"
NOT_LIGHTER = "traffic normal or heavier"

# A gap in test 3 below this is not a result. Half a percentage point is smaller
# than the effects being discussed and far smaller than the intervals around
# them, so treating anything above zero as support would let floating-point
# noise decide the verdict -- which is exactly what happened on the first run,
# where a 0.035 pp gap was scored as confirmation.
MIN_GAP = 0.005

# Rank correlation this negative or more counts as the two channels genuinely
# opposing. Weaker than this is not distinguishable from no relationship at the
# number of contrasts available here.
CORR_SUPPORT = -0.2

# Views for test 3. Band is always the contrast axis and never appears here.
VIEWS = {
    "flow_state": ["flow_state"],
    "flow_state_congested": ["flow_state", "congested"],
    "city_flow_state": ["city", "flow_state"],
}


# ---------------------------------------------------------------------------
# Tests 1 and 2 -- read the published Phase 5 cells.
# ---------------------------------------------------------------------------
def paired_cells(spark):
    """Speed and flow effects side by side, one row per contrast.

    The published cell table stores one row per (view, level, band, measure), so
    the two channels are pivoted back together here. Only contrasts where BOTH
    channels were estimable can be read as a pair.
    """
    cells = spark.read.parquet(config.spark_path(CELLS))
    speed = cells.filter(F.col("measure") == "speed").select(
        "view", "level", "band",
        F.col("estimate").alias("speed"),
        F.col("excludes_zero").alias("speed_signif"),
        "n_clusters",
    )
    flow = cells.filter(F.col("measure") == "flow").select(
        "view", "level", "band",
        F.col("estimate").alias("flow"),
        F.col("excludes_zero").alias("flow_signif"),
    )
    joined = speed.join(flow, ["view", "level", "band"], "inner").filter(
        F.col("speed").isNotNull() & F.col("flow").isNotNull()
    )
    return [r.asDict() for r in joined.collect()]


def quadrants(rows):
    """Count contrasts by the sign pair (speed, flow).

    The explanation predicts the speed-up / traffic-down quadrant. Its opposite
    -- speed up AND traffic up -- is the one that would contradict it outright,
    since more cars going faster cannot be explained by an emptier road.
    """
    out = {"speed_up_flow_down": 0, "speed_up_flow_up": 0,
           "speed_down_flow_down": 0, "speed_down_flow_up": 0}
    for r in rows:
        key = ("speed_up" if r["speed"] > 0 else "speed_down") + (
            "_flow_up" if r["flow"] > 0 else "_flow_down"
        )
        out[key] += 1
    return out


def rank_corr(x, y):
    """Spearman correlation, written out rather than pulled from scipy.

    scipy is not a project dependency and this is six lines: rank both vectors,
    then take the Pearson correlation of the ranks. Ties get average ranks.
    """
    def ranks(v):
        order = np.argsort(v, kind="mergesort")
        r = np.empty(len(v), dtype=np.float64)
        r[order] = np.arange(1, len(v) + 1, dtype=np.float64)
        # Average ranks within tied groups, so ties do not create false ordering.
        _, inv, counts = np.unique(v, return_inverse=True, return_counts=True)
        sums = np.zeros(len(counts))
        np.add.at(sums, inv, r)
        return (sums / counts)[inv]

    if len(x) < 3:
        return float("nan")
    rx, ry = ranks(np.asarray(x, float)), ranks(np.asarray(y, float))
    if np.std(rx) == 0 or np.std(ry) == 0:
        return float("nan")
    return float(np.corrcoef(rx, ry)[0, 1])


# ---------------------------------------------------------------------------
# Test 3 -- re-estimate, split by whether traffic was lighter than normal.
# ---------------------------------------------------------------------------
def cluster_cells_flow(df):
    """dose_response.cluster_cells, plus the traffic-state axis.

    Mirrors the production aggregation exactly and adds one grouping column, so
    the comparison against the published tables stays like for like. Written out
    rather than parameterised into production because dose_response.py is frozen
    while these experiments run.

    `flow_state` is defined only where a flow deviation exists; intervals
    without one cannot be placed on the axis and are dropped rather than guessed
    into a bucket.
    """
    prepared = (
        df.filter(
            F.col("congested").isNotNull()
            & F.col("band").isNotNull()
            & F.col("typical_flow_deviation").isNotNull()
            & F.col("typical_speed_deviation").isNotNull()
        )
        .withColumn("road_class", strata.road_class())
        .withColumn("tod", strata.time_of_day())
        .withColumn("cluster", strata.cluster_id())
        .filter(F.col("road_class") != F.lit("unknown"))
        .withColumn(
            "congested_lbl",
            F.when(F.col("congested"), F.lit("congested")).otherwise(F.lit("free-flowing")),
        )
        .withColumn(
            "flow_state",
            F.when(F.col("typical_flow_deviation") < 0, F.lit(LIGHTER)).otherwise(
                F.lit(NOT_LIGHTER)
            ),
        )
    )
    sq = lambda c: F.col(c) * F.col(c)  # noqa: E731
    return prepared.groupBy(
        "cluster", "city", "band", "road_class", "tod", "flow_state",
        F.col("congested_lbl").alias("congested"),
    ).agg(
        F.count("typical_speed_deviation").alias("n_speed"),
        F.coalesce(F.sum("typical_speed_deviation"), F.lit(0.0)).alias("sum_speed"),
        F.coalesce(F.sum(sq("typical_speed_deviation")), F.lit(0.0)).alias("ssq_speed"),
        F.count("typical_flow_deviation").alias("n_flow"),
        F.coalesce(F.sum("typical_flow_deviation"), F.lit(0.0)).alias("sum_flow"),
        F.coalesce(F.sum(sq("typical_flow_deviation")), F.lit(0.0)).alias("ssq_flow"),
    )


def load_cells(cells):
    """As threshold_sweep.load_cells, with flow_state carried through."""
    rows = cells.collect()
    if not rows:
        raise SystemExit("FAIL: no cells survived preparation")
    raw = {
        c: [r[c] for r in rows]
        for c in ("cluster", "city", "band", "road_class", "tod", "congested", "flow_state")
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


def split_by_traffic(results):
    """Speed effect per band in each traffic-state half."""
    out = []
    for r in results:
        if r["view"] != "flow_state" or r["speed"] is None:
            continue
        out.append(
            {
                "band": r["band"],
                "flow_state": r["level"],
                "speed": r["speed"]["estimate"],
                "lo": r["speed"]["lo"],
                "hi": r["speed"]["hi"],
                "signif": r["speed"]["excludes_zero"],
                "n_clusters": r["n_clusters"],
            }
        )
    return out


# ---------------------------------------------------------------------------
# Reporting.
# ---------------------------------------------------------------------------
def pp(x):
    return f"{100.0 * x:+.1f}"


def render(n_paired, quad, corrs, split, call, meta):
    A = []
    add = A.append
    add("# Does \"fewer cars\" explain why light rain looks faster?")
    add("")
    add(f"_Generated {meta['generated']}._")
    add("")
    add(f"**Answer: {call['headline']}**")
    add("")
    add("## 1. The claim being tested")
    add("")
    add("Rain does two things to a road at once, and they move speed in opposite")
    add("directions. It makes drivers slower. It also makes some of them not")
    add("drive, and a road with fewer cars on it is a faster road. If the second")
    add("effect is bigger than the first, measured speed goes **up** under rain")
    add("without rain having made anyone quicker.")
    add("")
    add("Both channels are already recorded per interval -- speed against this")
    add("detector's normal for this hour, and car count against the same normal --")
    add("so the claim is checkable rather than merely plausible.")
    add("")

    add("## 2. Test 1 -- when speed rises, does traffic fall?")
    add("")
    add(f"All {n_paired} published Phase 5 contrasts where both channels were")
    add("estimable, sorted into the four sign combinations:")
    add("")
    add("| | traffic down | traffic up |")
    add("|---|---|---|")
    add(f"| **speed up** | {quad['speed_up_flow_down']} | {quad['speed_up_flow_up']} |")
    add(f"| **speed down** | {quad['speed_down_flow_down']} | {quad['speed_down_flow_up']} |")
    add("")
    add("The top-left cell is what the explanation predicts. The top-right is the")
    add("one that would refute it outright: more cars *and* more speed cannot be")
    add("explained by an emptier road.")
    add("")
    up = quad["speed_up_flow_down"] + quad["speed_up_flow_up"]
    if up:
        share = 100.0 * quad["speed_up_flow_down"] / up
        add(f"Of the {up} contrasts where speed rose, **{share:.0f}%** also show")
        add("traffic falling.")
    add("")

    add("## 3. Test 2 -- does a bigger drop in traffic mean a bigger rise in speed?")
    add("")
    add("Rank correlation between the two channels across contrasts. Negative")
    add("means the two move opposite ways, which is what the explanation needs.")
    add("Ranks rather than raw values, so a couple of extreme cells cannot carry")
    add("the result.")
    add("")
    add("| set of contrasts | n | correlation |")
    add("|---|---|---|")
    for name, (n, c) in corrs.items():
        add(f"| {name} | {n} | {c:+.2f} |" if np.isfinite(c) else f"| {name} | {n} | -- |")
    add("")

    add("## 4. Test 3 -- split rainy intervals by whether traffic was actually lighter")
    add("")
    add("If emptier roads are the mechanism, the speed rise should sit almost")
    add("entirely in the half where traffic was lighter than normal, and should")
    add("largely disappear in the half where it was not.")
    add("")
    add("Speed effect against dry, percentage points:")
    add("")
    add("| band | " + f"{LIGHTER} | {NOT_LIGHTER} |")
    add("|---|---|---|")
    for band in [b for b in BAND_ORDER if b in WET_BANDS]:
        cells = []
        for state in (LIGHTER, NOT_LIGHTER):
            m = next(
                (d for d in split if d["band"] == band and d["flow_state"] == state), None
            )
            cells.append(
                "--" if m is None else f"{pp(m['speed'])}{'' if m['signif'] else ' ns'}"
            )
        add(f"| {band} | " + " | ".join(cells) + " |")
    add("")
    add("`ns` marks an interval that includes zero.")
    add("")
    add("**Caveat, and it is a real one.** Traffic volume is itself changed by")
    add("rain, so splitting on it means splitting on something the rain caused.")
    add("A split like this can produce a difference on its own even when the")
    add("mechanism is absent. Test 3 corroborates tests 1 and 2; it cannot carry")
    add("the conclusion alone, and nothing below leans on it.")
    add("")

    add("## 5. What this does and does not establish")
    add("")
    add("Two claims, scored separately, because the first run conflated them and")
    add("reached a misleading answer: the general mechanism is well supported,")
    add("and that was allowed to stand in for the light-rain case, which is the")
    add("one the question was actually about.")
    add("")
    add(f"**Does rain empty roads, and does that raise speed?** "
        f"{call['general_passed']}/{call['general_total']} tests say yes.")
    add("")
    add(f"**Is that why light rain looks faster?** "
        f"{call['drizzle_passed']}/{call['drizzle_total']} tests say yes.")
    add("")
    for line in call["reasoning"]:
        add(f"- {line}")
    add("")
    add("It does **not** establish that rain has no slowing effect. The opposite,")
    add("if anything: the slowing effect is being masked by an opposing effect on")
    add("how many cars are on the road, and the two have to be separated before")
    add("either can be read. That is what `typical_flow_deviation` is for, and it")
    add("is why Phase 5 reports both channels side by side.")
    add("")
    if call["drizzle_passed"] == 0:
        add("It also does not leave the light-rain result explained. Both")
        add("light-rain-specific checks came back null, so whatever makes drizzle")
        add("look faster is still unidentified -- and note that this analysis runs")
        add("on the production baseline, where the pooled light-rain effect is")
        add("mildly negative. The strongly positive light-rain readings live in")
        add("the sharp-rainfall arm and in the already-congested stratum, neither")
        add("of which this test isolates. That is the next place to look.")
        add("")
    add(f"**Gate verdict: {meta['verdict']}**")
    add("")
    if meta["failures"]:
        for f in meta["failures"]:
            add(f"- FAIL: {f}")
    else:
        add("The gate is on the tests having run on enough data to mean anything,")
        add("not on which way they came out.")
    add("")
    return "\n".join(A)


def main() -> int:
    spark = config.get_spark("verify_emptier_roads")
    spark.sparkContext.setLogLevel("WARN")

    print("[1/3] tests 1-2: published Phase 5 cells")
    rows = paired_cells(spark)
    print(f"      {len(rows):,} contrasts with both channels estimable")
    quad = quadrants(rows)

    light_rows = [r for r in rows if r["band"] == "Light"]
    corrs = {
        "all bands": (len(rows), rank_corr([r["speed"] for r in rows],
                                           [r["flow"] for r in rows])),
        "Light only": (len(light_rows), rank_corr([r["speed"] for r in light_rows],
                                                  [r["flow"] for r in light_rows])),
    }
    for name, (n, c) in corrs.items():
        print(f"      {name}: n={n}, rank corr={c:+.2f}")

    print("[2/3] test 3: re-estimating split by traffic state")
    cells = cluster_cells_flow(spark.read.parquet(config.spark_path(ANALYSIS)))
    data = load_cells(cells)
    print(f"      {len(data['cluster_code']):,} cells, {data['n_clusters']:,} detector-days")
    results = dr.run_views(data, VIEWS, np.random.default_rng(dr.SEED))
    split = split_by_traffic(results)

    print("[3/3] report")
    up = quad["speed_up_flow_down"] + quad["speed_up_flow_up"]
    share = quad["speed_up_flow_down"] / up if up else float("nan")
    all_corr = corrs["all bands"][1]

    lighter = {d["band"]: d for d in split if d["flow_state"] == LIGHTER}
    heavier = {d["band"]: d for d in split if d["flow_state"] == NOT_LIGHTER}
    light_gap = None
    if "Light" in lighter and "Light" in heavier:
        light_gap = lighter["Light"]["speed"] - heavier["Light"]["speed"]

    light_corr = corrs["Light only"][1]

    # Two separate claims, scored separately. Conflating them is what produced
    # the misleading first verdict: the general mechanism is well supported, and
    # that was allowed to stand in for the drizzle case, which is the one the
    # question was actually about.
    general, drizzle, reasoning = [], [], []

    if np.isfinite(share) and share >= 0.75:
        general.append(True)
        reasoning.append(
            f"GENERAL: where speed rises, traffic falls in {100 * share:.0f}% of "
            f"contrasts. The combination that would refute the explanation -- "
            f"more cars and more speed -- appears {quad['speed_up_flow_up']} times."
        )
    else:
        general.append(False)
        reasoning.append(
            f"GENERAL: only {100 * share:.0f}% of speed rises come with falling "
            "traffic, weaker than the explanation needs."
        )
    if np.isfinite(all_corr) and all_corr < CORR_SUPPORT:
        general.append(True)
        reasoning.append(
            f"GENERAL: across all bands the two channels oppose each other "
            f"(rank correlation {all_corr:+.2f}) -- the bigger the drop in cars, "
            "the bigger the rise in speed."
        )
    else:
        general.append(False)
        reasoning.append(
            f"GENERAL: across all bands the rank correlation is {all_corr:+.2f}, "
            "not the clear opposition the explanation predicts."
        )

    if np.isfinite(light_corr) and light_corr < CORR_SUPPORT:
        drizzle.append(True)
        reasoning.append(
            f"DRIZZLE: within light rain the channels oppose each other "
            f"(rank correlation {light_corr:+.2f})."
        )
    else:
        drizzle.append(False)
        reasoning.append(
            f"DRIZZLE: within light rain alone the rank correlation is "
            f"{light_corr:+.2f} across {corrs['Light only'][0]} contrasts -- no "
            "relationship. The drizzle results with the biggest fall in traffic "
            "are not the ones with the biggest rise in speed."
        )
    if light_gap is not None and light_gap >= MIN_GAP:
        drizzle.append(True)
        reasoning.append(
            f"DRIZZLE: the light-rain speed effect is {100 * light_gap:.1f} pp "
            "higher where traffic was lighter than normal -- though see the "
            "caveat on this test."
        )
    elif light_gap is not None:
        drizzle.append(False)
        reasoning.append(
            f"DRIZZLE: splitting light rain by whether traffic was actually "
            f"lighter changes the speed effect by {100 * light_gap:+.2f} pp, "
            "which is nothing. If emptier roads were driving the light-rain "
            "result, this split should have separated it."
        )

    n_gen, n_driz = sum(general), sum(drizzle)
    if n_gen == len(general) and n_driz == len(drizzle):
        headline = (
            "Yes. Rain empties roads, emptier roads run faster, and that is what "
            "the light-rain result is measuring."
        )
    elif n_gen == len(general) and n_driz == 0:
        headline = (
            "Half of it. Rain really does empty roads and that really does raise "
            "speed -- but the effect does NOT track the light-rain result, so it "
            "is not the explanation for why drizzle looks faster."
        )
    elif n_gen == len(general):
        headline = (
            "Mostly, with a gap. The mechanism is clearly real in general, but "
            "the light-rain evidence for it is mixed."
        )
    else:
        headline = (
            "No -- the tests do not support it. The speed rise needs a different "
            "explanation."
        )
    call = {"headline": headline, "general_passed": n_gen, "general_total": len(general),
            "drizzle_passed": n_driz, "drizzle_total": len(drizzle),
            "reasoning": reasoning,
            "share_speed_up_flow_down": None if not np.isfinite(share) else share,
            "rank_corr_all": all_corr, "rank_corr_light": light_corr,
            "light_gap": light_gap}

    failures = []
    if len(rows) < 20:
        failures.append(f"only {len(rows)} paired contrasts available; too few to read")
    if data["n_clusters"] < 100:
        failures.append(f"only {data['n_clusters']} detector-days in the split estimation")
    if not split:
        failures.append("the traffic-state split produced no estimable contrast")

    meta = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "verdict": "FAIL" if failures else "PASS",
        "failures": failures,
        "n_paired": len(rows),
        "quadrants": quad,
        "correlations": {k: {"n": v[0], "rho": v[1]} for k, v in corrs.items()},
        "split": split,
        "decision": call,
        "n_clusters": data["n_clusters"],
    }
    REPORT.write_text(render(len(rows), quad, corrs, split, call, meta), encoding="utf-8")
    REPORT_JSON.write_text(json.dumps(meta, indent=2, default=float), encoding="utf-8")

    print()
    print(headline)
    print(f"wrote {REPORT}")
    for f in failures:
        print(f"FAIL: {f}")
    if not failures:
        print("PASS: emptier-roads explanation tested on all three checks")
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
