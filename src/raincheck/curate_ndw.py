"""L1 curation for the NDW canonical schema.

Each transform is a standalone DataFrame->DataFrame step, so it can be tested on
a handful of rows rather than only observed at billion-row scale.

Three properties of the source drive this module, all measured in
``reports/phase1_nl_audit.md``:

* Publications overlap and the harvester's dedup window is per-process, so
  duplicates reach L1 and **must** be removed here.
* 86% of non-null 1-minute speeds rest on fewer than five vehicles, so
  re-binning weighted by sample size is mandatory rather than cosmetic.
* Thin counts correlate with rain, so filtering on sample size biases the
  estimate toward zero. Retention has to be *reported* per rain band, not
  silently applied.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F

KEY = ("segment_id", "ts_utc")


def dedupe(df: DataFrame) -> DataFrame:
    """One row per (segment_id, ts_utc), keeping the best-supported observation.

    Mandatory, not defensive. The harvester's dedup window is per-process and
    in-memory, so any restart re-ingests up to 30 minutes of overlap - two
    concurrent runs were observed producing 29,528 rows for a timestamp whose
    ceiling is 20,519.

    ``dropDuplicates`` would keep an arbitrary row. The copies need not agree: a
    later publication can revise a value once more samples have arrived, so the
    row with the largest ``quality_weight`` is the better estimate and is the one
    retained.
    """
    ranked = Window.partitionBy(*KEY).orderBy(
        F.col("quality_weight").desc_nulls_last())
    return (
        df.withColumn("_rank", F.row_number().over(ranked))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


REBIN_MINUTES = 5


def rebin(df: DataFrame, minutes: int = REBIN_MINUTES) -> DataFrame:
    """Aggregate 1-minute observations onto a fixed bin, weighted by sample size.

    Mandatory rather than cosmetic: 86% of non-null 1-minute speeds rest on fewer
    than five vehicles, so a raw 1-minute mean speed carries a standard error of
    several km/h against a rain effect of order 1-2%.

    Speed is a **vehicle-weighted** mean, so a bin's estimate is dominated by the
    minutes that actually saw traffic. Two weights come out, because they answer
    different questions:

    * ``quality_weight`` - every vehicle in the bin, the bin's own support;
    * ``speed_weight``  - only vehicles behind a non-null speed, which is the
      correct weight for a model whose target derives from speed.

    Aggregating to a fixed bin is also the favourable trade on the missingness
    problem: it converts a *selection* problem (dropping thin observations, which
    preferentially deletes rain) into a *variance* problem.
    """
    seconds = 60 * minutes
    binned = df.withColumn(
        "ts_utc",
        F.timestamp_seconds(
            F.floor(F.unix_timestamp(F.col("ts_utc")) / seconds) * seconds),
    )

    speed, weight = F.col("speed"), F.col("quality_weight")
    has_speed = speed.isNotNull()

    # numberOfInputValuesUsed is absent on 59% of live rows, and 313,791 of
    # 630,250 harvested rows carry a valid speed with no weight. Weighting by the
    # raw column drops those terms, so a bin whose speeds all lack a weight
    # collapses to sum(null)/sum(null) = null - which inflated speed-null from
    # 14% at 1-minute grain to 70% after re-binning. An unknown sample size means
    # unknown support, not zero support, so it counts once.
    effective = F.coalesce(weight, F.lit(1.0))
    speed_support = F.when(has_speed, effective)

    return binned.groupBy("segment_id", "ts_utc").agg(
        (F.sum(F.when(has_speed, speed * effective)) / F.sum(speed_support))
        .alias("speed"),
        F.avg("flow").alias("flow"),
        F.sum(effective).alias("quality_weight"),
        F.sum(F.coalesce(speed_support, F.lit(0.0))).alias("speed_weight"),
        F.count(F.lit(1)).alias("n_obs"),
        # Surfaced, not hidden: a bin resting entirely on unweighted speeds has a
        # far weaker claim to its value than the weight column alone suggests.
        F.sum(F.when(weight.isNull(), 1).otherwise(0)).alias("unknown_weight_obs"),
    )


def retention_by_rain_band(df: DataFrame, min_weight: float) -> DataFrame:
    """Sample support and speed availability, stratified by rain band.

    The single most valuable diagnostic in L1, and the reason it exists rather
    than a filter being applied silently.

    Rain reduces traffic volume, so wet observations are systematically the thin
    ones. Sample size is therefore correlated with the treatment: any minimum-`n`
    filter deletes rain preferentially and biases the estimated effect **toward
    zero** - the direction that manufactures a null result. Weighting by sample
    size is variance-optimal but bias-pessimal for the same reason.

    Divergence between the wet and dry rows of this report is direct evidence
    that the bias is live. Read it before trusting any dose-response estimate.
    """
    total = F.count(F.lit(1))
    return df.groupBy("rain_band").agg(
        total.alias("n_obs"),
        F.round(100.0 * F.sum(F.when(F.col("speed").isNull(), 1).otherwise(0))
                / total, 4).alias("speed_null_pct"),
        F.round(F.avg("quality_weight"), 4).alias("mean_quality_weight"),
        F.round(100.0 * F.sum(
            F.when(F.col("quality_weight") >= F.lit(min_weight), 1).otherwise(0))
            / total, 4).alias("retained_pct"),
    )


SENSITIVITY_THRESHOLDS = (1.0, 5.0, 20.0)


def threshold_sensitivity(df: DataFrame,
                          thresholds=SENSITIVITY_THRESHOLDS) -> DataFrame:
    """Mean speed per rain band, recomputed at each minimum-sample-size threshold.

    A minimum-`n` threshold cannot be chosen once and defended, because the
    observations it removes are not missing at random - thin observations are
    disproportionately rainy. Reporting the estimate across several thresholds
    turns that into something checkable: an effect that is stable across
    thresholds is an effect, while one that drifts monotonically with the
    threshold is an artefact of what the threshold deleted.

    Long-format on purpose, so the result can be pivoted or plotted directly.
    """
    frames = []
    for threshold in thresholds:
        frames.append(
            df.filter(F.col("quality_weight") >= F.lit(threshold))
            .groupBy("rain_band")
            .agg(
                F.round(F.avg("speed"), 4).alias("mean_speed"),
                F.count(F.lit(1)).alias("n_obs"),
            )
            .withColumn("min_weight", F.lit(float(threshold)))
        )

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined.select("min_weight", "rain_band", "mean_speed", "n_obs")
