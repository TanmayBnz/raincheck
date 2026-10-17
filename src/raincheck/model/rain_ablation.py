"""Phase 6a / D5 -- rain ablation in Spark MLlib.

Does rainfall data help predict unusual slowdowns, and does the 2 km / 10 min
downscaled rain help more than raw 31 km / hourly ERA5?

Three gradient-boosted tree models, identical in rows, split, hyperparameters
and seed, differing only in which rain features they see:

  none    -- city, road class, hour, weekday, road geometry, the detector's
             normal dry speed. Everything knowable without a weather feed.
  coarse  -- none + native ERA5 (city-level, hourly).
  fine    -- none + spateGAN 2 km / 10 min rain at the detector, with
             accumulations, time since onset and preceding dry spell.

Target is `typical_speed_deviation` (speed vs this detector's dry median for
this weekday and hour), so the recurring commute pattern is already netted out
and what is left to explain is the unusual part -- where rain would live.

No feature measured in the same interval as the target (flow, occupancy) is
used: that would predict traffic from traffic and say nothing about rain.

Evaluation is 5-fold cross-validation grouped by city-day: every day is
predicted exactly once, by models that never saw any reading from it, so no
test reading shares a rain event with its training data. A single 20% day
holdout was tried first and rejected -- with 75 city-days it held out 8, of
which only 1,306 readings were wet. Error differences carry cluster-bootstrap
intervals over detector-days, the same unit as Phase 5.

"Time since" counters are capped (dry spell 24 h, onset 180 min). Uncapped,
the dry-spell counter was the fine model's top feature: within a three-week
window it is close to a calendar index, so trees used it to memorise
individual days and the model got WORSE on unseen days.

Outputs:
  reports/phase6a_rain_ablation.md / .json

Run:  python -m raincheck.model.rain_ablation
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import functions as F

from raincheck import config
from raincheck.analysis import strata

ANALYSIS = config.LAKE_ROOT / "analysis" / "measurements_rain"
REPORT = config.REPORTS_DIR / "phase6a_rain_ablation.md"
REPORT_JSON = config.REPORTS_DIR / "phase6a_rain_ablation.json"

SEED = 20261017
N_FOLDS = 5
DRY_SPELL_CAP_H = 24.0
ONSET_CAP_MIN = 180.0
N_BOOT = 2000

# Sentinel for "not applicable" (minutes since onset when dry, dry spell when
# wet). Trees split on it cleanly; a NULL would drop the row.
NA = -1.0

BASE = ["city_idx", "road_idx", "hod", "dow", "lanes", "speed_limit", "length",
        "typical_speed", "free_flow_speed"]
COARSE = ["era5_precip_mm", "era5_wet", "era5_hours_since_rain"]
FINE = ["precip_mm_h", "fine_wet", "acc_30min_mm", "acc_60min_mm",
        "minutes_since_onset", "dry_spell_hours"]

MODELS = {"none": BASE, "coarse": BASE + COARSE, "fine": BASE + FINE}


def prepare(df):
    """Rows with a target and complete base features, rain columns made dense."""
    out = (
        df.filter(F.col("typical_speed_deviation").isNotNull())
        .withColumn("road_class", strata.road_class())
        .withColumn("era5_wet", F.col("era5_is_wet").cast("double"))
        .withColumn("fine_wet", F.col("is_wet").cast("double"))
        .withColumn("cluster", strata.cluster_id())
        # Day-grouped folds, computed once so all three models share them.
        .withColumn(
            "fold",
            F.abs(F.hash(F.col("city"), F.col("date").cast("string"), F.lit(SEED)))
            % N_FOLDS,
        )
        .withColumn("dry_spell_hours", F.least("dry_spell_hours", F.lit(DRY_SPELL_CAP_H)))
        .withColumn("era5_hours_since_rain",
                    F.least("era5_hours_since_rain", F.lit(DRY_SPELL_CAP_H)))
        .withColumn("minutes_since_onset",
                    F.least("minutes_since_onset", F.lit(ONSET_CAP_MIN)))
    )
    out = out.fillna(NA, subset=["minutes_since_onset", "dry_spell_hours",
                                 "era5_hours_since_rain"])
    for c in BASE[2:] + COARSE + FINE:
        out = out.withColumn(c, F.col(c).cast("double"))
    needed = [c for c in BASE[2:] + COARSE + FINE]
    return out.dropna(subset=needed)


def fit_fold(train, test):
    """Fit the three models on one fold; return its test rows with all predictions."""
    importances = {}
    for name, feats in MODELS.items():
        vec = f"f_{name}"
        assembler = VectorAssembler(inputCols=feats, outputCol=vec)
        gbt = GBTRegressor(
            featuresCol=vec, labelCol="typical_speed_deviation",
            predictionCol=f"pred_{name}", maxIter=40, maxDepth=5,
            stepSize=0.1, subsamplingRate=0.8, seed=SEED,
        )
        model = gbt.fit(assembler.transform(train))
        test = model.transform(assembler.transform(test)).drop(vec)
        importances[name] = model.featureImportances.toArray()
    return test.select("cluster", "is_wet", "typical_speed_deviation",
                       *[f"pred_{n}" for n in MODELS]), importances


def cross_validate(df):
    """Every city-day predicted once, out of fold. Importances averaged over folds."""
    scored, imps = None, {n: np.zeros(len(f)) for n, f in MODELS.items()}
    for k in range(N_FOLDS):
        part, imp = fit_fold(df.filter(F.col("fold") != k), df.filter(F.col("fold") == k))
        part = part.cache()
        print(f"      fold {k + 1}/{N_FOLDS}: {part.count():,} test readings")
        scored = part if scored is None else scored.unionByName(part)
        for n in MODELS:
            imps[n] += imp[n] / N_FOLDS
    return scored, {n: dict(zip(MODELS[n], imps[n].tolist())) for n in MODELS}


def per_cluster(test):
    """Squared and absolute error sums per (detector-day, wet) for the bootstrap."""
    y = F.col("typical_speed_deviation")
    aggs = [F.count(F.lit(1)).alias("n"), F.sum(y * y).alias("sse_zero")]
    for name in MODELS:
        e = y - F.col(f"pred_{name}")
        aggs += [F.sum(e * e).alias(f"sse_{name}")]
    return test.groupBy("cluster", F.col("is_wet").alias("wet")).agg(*aggs).collect()


def bootstrap(rows, rng):
    """RMSE per model and the RMSE differences, all rows and wet rows only."""
    out = {}
    for subset in ("all", "wet"):
        sel = [r for r in rows if subset == "all" or r["wet"]]
        # Collapse to one row per cluster so a cluster is resampled as a unit.
        keys = sorted({r["cluster"] for r in sel})
        idx = {k: i for i, k in enumerate(keys)}
        m = len(keys)
        n = np.zeros(m)
        sse = {k: np.zeros(m) for k in ("zero", *MODELS)}
        for r in sel:
            i = idx[r["cluster"]]
            n[i] += r["n"]
            for k in sse:
                sse[k][i] += r[f"sse_{k}"]

        def rmse(k, w):
            return np.sqrt((w * sse[k]).sum() / (w * n).sum())

        point = {k: float(rmse(k, np.ones(m))) for k in sse}
        reps = {k: [] for k in ("coarse_vs_none", "fine_vs_none", "fine_vs_coarse")}
        for _ in range(N_BOOT):
            w = np.bincount(rng.integers(0, m, m), minlength=m).astype(float)
            r_ = {k: rmse(k, w) for k in ("none", "coarse", "fine")}
            # Positive = the second-named model has lower error (is better).
            reps["coarse_vs_none"].append(r_["none"] - r_["coarse"])
            reps["fine_vs_none"].append(r_["none"] - r_["fine"])
            reps["fine_vs_coarse"].append(r_["coarse"] - r_["fine"])
        diffs = {}
        for k, v in reps.items():
            v = np.array(v)
            a, b = k.split("_vs_")
            est = point[b] - point[a]
            diffs[k] = {
                "estimate": float(est),
                "lo": float(np.percentile(v, 2.5)),
                "hi": float(np.percentile(v, 97.5)),
                "pct_of_baseline": float(100.0 * est / point[b]),
            }
        out[subset] = {"n_rows": int(n.sum()), "n_clusters": m,
                       "rmse": point, "improvement": diffs}
    return out


def pp(x):
    return f"{100.0 * x:.2f}"


def render(res, imp, meta):
    L = []
    A = L.append
    A("# Phase-6a Rain Ablation — Spark MLlib")
    A("")
    A(f"_Generated {meta['generated']}. {meta['n_rows']:,} readings over "
      f"{meta['all_days']} city-days, {N_FOLDS}-fold cross-validation grouped by "
      f"day: every reading is predicted by models that never saw its day. "
      f"Gradient-boosted trees, 40 rounds, depth 5, identical for all three "
      f"models._")
    A("")
    A("Error is RMSE of speed deviation from the detector's normal dry speed, in")
    A("percentage points. `zero` is the no-model baseline: predict \"normal speed\".")
    A("Improvement is how much lower one model's error is than another's, with a")
    A(f"95% cluster-bootstrap interval over detector-days ({N_BOOT:,} replicates).")
    A("An interval that excludes zero is a real difference.")
    A("")
    for subset, title in (("all", "All test readings"), ("wet", "Wet test readings only (2 km label)")):
        r = res[subset]
        A(f"## {title}")
        A("")
        A(f"{r['n_rows']:,} readings, {r['n_clusters']:,} detector-days.")
        A("")
        A("| model | RMSE (pp) |")
        A("|---|---|")
        for k in ("zero", "none", "coarse", "fine"):
            A(f"| {k} | {pp(r['rmse'][k])} |")
        A("")
        A("| comparison | error reduction (pp) | 95% interval | % of baseline error |")
        A("|---|---|---|---|")
        for k, label in (("coarse_vs_none", "coarse rain vs no rain"),
                         ("fine_vs_none", "2 km rain vs no rain"),
                         ("fine_vs_coarse", "2 km rain vs coarse rain")):
            d = r["improvement"][k]
            sig = "" if (d["lo"] > 0 or d["hi"] < 0) else " ns"
            A(f"| {label} | {pp(d['estimate'])} | [{pp(d['lo'])}, {pp(d['hi'])}]{sig} "
              f"| {d['pct_of_baseline']:.2f}% |")
        A("")
    A("## What the 2 km model leans on")
    A("")
    A("Feature importance (share of total split gain) in the `fine` model.")
    A("")
    A("| feature | importance |")
    A("|---|---|")
    for f, v in sorted(imp["fine"].items(), key=lambda kv: -kv[1]):
        A(f"| {f} | {v:.3f} |")
    A("")
    A("## Caveats")
    A("")
    A("- Day-grouped cross-validation only, not the four holdout designs")
    A("  CONTEXT.md §6/L3b originally specified (spatial, event, city).")
    A("- Hyperparameters are fixed, not tuned; all three models share them, so the")
    A("  comparison is fair, but absolute errors are not the best achievable.")
    A("- One spateGAN ensemble member; the 2 km rain is a model output, not an")
    A("  observation.")
    A("")
    return "\n".join(L) + "\n"


def main() -> int:
    spark = config.get_spark("rain_ablation")
    spark.sparkContext.setLogLevel("WARN")

    print("[1/4] preparing rows")
    df = prepare(spark.read.parquet(config.spark_path(ANALYSIS)))
    # Indexing once over all rows is not leakage: it only maps labels to codes.
    df = Pipeline(stages=[
        StringIndexer(inputCol="city", outputCol="city_idx", handleInvalid="keep"),
        StringIndexer(inputCol="road_class", outputCol="road_idx", handleInvalid="keep"),
    ]).fit(df).transform(df).cache()
    n_rows = df.count()
    all_days = df.select("city", "date").distinct().count()
    print(f"      {n_rows:,} readings, {all_days} city-days")

    print(f"[2/4] {N_FOLDS}-fold cross-validation, three models per fold")
    scored, imp = cross_validate(df)

    print("[3/4] scoring and bootstrap")
    res = bootstrap(per_cluster(scored), np.random.default_rng(SEED))

    print("[4/4] report")
    meta = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "n_rows": n_rows, "all_days": all_days,
    }
    failures = []
    if res["all"]["n_rows"] != n_rows:
        failures.append(f"scored {res['all']['n_rows']:,} of {n_rows:,} rows; folds lost data")
    if res["wet"]["n_rows"] == 0:
        failures.append("no wet rows scored")
    if not res["all"]["rmse"]["none"] < res["all"]["rmse"]["zero"]:
        failures.append("the no-rain model does not beat predicting normal speed")
    meta["verdict"] = "FAIL" if failures else "PASS"
    meta["failures"] = failures

    REPORT.write_text(render(res, imp, meta), encoding="utf-8")
    REPORT_JSON.write_text(json.dumps({**meta, "results": res, "importance": imp},
                                      indent=2), encoding="utf-8")
    for s in ("all", "wet"):
        r = res[s]
        print(f"{s:4s} RMSE  zero {pp(r['rmse']['zero'])}  none {pp(r['rmse']['none'])}  "
              f"coarse {pp(r['rmse']['coarse'])}  fine {pp(r['rmse']['fine'])}")
    print(f"wrote {REPORT}")
    for f in failures:
        print(f"FAIL: {f}")
    if not failures:
        print("PASS: rain ablation estimated")
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
