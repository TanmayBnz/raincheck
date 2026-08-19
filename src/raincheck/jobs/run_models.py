"""L3(b) job: GBT delay model, its rain ablation, and reference baselines.

Run via scripts/run_models.sh, after run_dose_response.sh.
"""

import argparse
from pathlib import Path

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, Window, functions as F

from raincheck import paths
from raincheck.model import TARGET, add_splits, feature_columns
from raincheck.session import build_session

INDEXED = (("fclass", "fclass_idx"), ("city", "city_idx"), ("rain_band", "rain_band_idx"))

# Trees tolerate a sentinel better than an imputed mean, and -1 is out of range
# for every feature here, so it reads as "absent" rather than as a real value.
MISSING = -1.0


def prepare(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("is_weekend_num", F.col("is_weekend").cast("double"))
        # Null when dry, which is information rather than absence.
        .withColumn("hours_since_onset_f", F.coalesce(F.col("hours_since_onset").cast("double"), F.lit(MISSING)))
        .withColumn("antecedent_dry_hours_f", F.coalesce(F.col("antecedent_dry_hours").cast("double"), F.lit(MISSING)))
    )
    for column in ("lanes", "limit", "length", "free_flow_speed", "typical_speed",
                   "rain_mm_h", "rain_1h", "rain_3h", "rain_6h"):
        df = df.withColumn(column, F.coalesce(F.col(column).cast("double"), F.lit(MISSING)))

    indexers = [
        StringIndexer(inputCol=src, outputCol=dst, handleInvalid="keep")
        for src, dst in INDEXED
    ]
    return Pipeline(stages=indexers).fit(df).transform(df)


def _persistence(df: DataFrame) -> DataFrame:
    """Previous interval's deviation at the same detector."""
    order = Window.partitionBy("city", "detid").orderBy("ts_utc")
    return df.withColumn("persistence_pred", F.lag(TARGET).over(order))


def _score(predictions: DataFrame, column: str) -> dict:
    scored = predictions.filter(F.col(column).isNotNull())
    out = {}
    for metric in ("rmse", "mae"):
        out[metric] = RegressionEvaluator(
            labelCol=TARGET, predictionCol=column, metricName=metric
        ).evaluate(scored)
    out["n"] = scored.count()
    return out


def _fit_and_score(train: DataFrame, test: DataFrame, include_rain: bool) -> dict:
    features = feature_columns(include_rain)
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    gbt = GBTRegressor(featuresCol="features", labelCol=TARGET, maxIter=20, maxDepth=5, seed=42)
    model = Pipeline(stages=[assembler, gbt]).fit(train)
    return _score(model.transform(test).withColumnRenamed("prediction", "pred"), "pred")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    spark = build_session("raincheck-l3b-models", master=args.master)

    training = spark.read.parquet(paths.TRAINING).filter(F.col(TARGET).isNotNull())
    df = add_splits(_persistence(prepare(training))).cache()
    print(f"MODEL_ROWS {df.count()}")

    splits = {
        "temporal_holdout": F.col("temporal_test"),
        "spatial_holdout": F.col("spatial_test"),
        "cross_city_uk_to_mainland": F.col("mainland_test"),
    }

    rows = []
    for name, flag in splits.items():
        train, test = df.filter(~flag), df.filter(flag)
        n_train, n_test = train.count(), test.count()
        if n_train == 0 or n_test == 0:
            print(f"SKIP {name}: train={n_train} test={n_test}")
            continue
        print(f"=== {name}: train={n_train} test={n_test} ===")

        with_rain = _fit_and_score(train, test, include_rain=True)
        no_rain = _fit_and_score(train, test, include_rain=False)

        train_mean = train.agg(F.avg(TARGET)).collect()[0][0]
        mean_scored = _score(test.withColumn("mean_pred", F.lit(train_mean)), "mean_pred")
        persist_scored = _score(test, "persistence_pred")

        for model_name, scores in (
            ("gbt_with_rain", with_rain),
            ("gbt_rain_ablated", no_rain),
            ("historical_mean", mean_scored),
            ("naive_persistence", persist_scored),
        ):
            rows.append({"split": name, "model": model_name, **scores})

        gain = 100.0 * (no_rain["rmse"] - with_rain["rmse"]) / no_rain["rmse"]
        print(f"ABLATION {name}: rmse {no_rain['rmse']:.5f} -> {with_rain['rmse']:.5f} "
              f"({gain:+.2f}% from rain features)")
        rows.append({"split": name, "model": "__rain_gain_pct__", "rmse": gain, "mae": None, "n": n_test})

        # Event-based: the same temporal model judged on rain intervals only, so
        # the dry majority cannot dilute the comparison.
        if name == "temporal_holdout":
            wet = test.filter(F.col("is_wet"))
            if wet.count() > 0:
                wr = _fit_and_score(train, wet, include_rain=True)
                nr = _fit_and_score(train, wet, include_rain=False)
                rows.append({"split": "event_based_wet_only", "model": "gbt_with_rain", **wr})
                rows.append({"split": "event_based_wet_only", "model": "gbt_rain_ablated", **nr})
                g = 100.0 * (nr["rmse"] - wr["rmse"]) / nr["rmse"]
                print(f"ABLATION event_based_wet_only: rmse {nr['rmse']:.5f} -> {wr['rmse']:.5f} ({g:+.2f}%)")
                rows.append({"split": "event_based_wet_only", "model": "__rain_gain_pct__",
                             "rmse": g, "mae": None, "n": wet.count()})

    table = pd.DataFrame(rows)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / "model_performance.csv", index=False)
    (out_dir / "model_performance.md").write_text(
        "# L3(b) delay model performance\n\n"
        f"Target: `{TARGET}` (speed reduction relative to the dry typical speed).\n"
        "Features exclude contemporaneous occupancy, flow and speed, so the model "
        "reflects the operational case: forecast rain in, expected delay out.\n\n"
        "`__rain_gain_pct__` is the RMSE improvement attributable to the rain "
        "features - the number that justifies the weather pipeline.\n\n"
        + table.to_markdown(index=False)
        + "\n"
    )
    print(table.to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
