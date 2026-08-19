from raincheck.model import (
    BASE_FEATURES,
    RAIN_FEATURES,
    add_splits,
    feature_columns,
)


def test_ablation_actually_removes_every_rain_feature():
    # The ablation is the headline result, so it must be impossible for a rain
    # column to survive into the "no rain" model by accident.
    with_rain = feature_columns(include_rain=True)
    without = feature_columns(include_rain=False)

    assert set(RAIN_FEATURES).isdisjoint(without)
    assert set(RAIN_FEATURES).issubset(with_rain)
    assert set(without) == set(BASE_FEATURES)
    assert len(with_rain) > len(without)


def test_contemporaneous_traffic_is_never_a_feature():
    # The model must predict from forecastable inputs. Occupancy, flow and speed
    # are measured at the same instant as the target, so including them would
    # make the model an interpolator and would swamp the rain signal it exists
    # to test.
    leaky = {"occ", "flow", "speed", "typical_deviation", "ff_delay_ratio"}

    assert leaky.isdisjoint(feature_columns(include_rain=True))


def test_spatial_split_keeps_a_detector_wholly_on_one_side(spark):
    # If one detector's rows land in both train and test, the "unseen detector"
    # holdout measures nothing.
    rows = [("manchester", f"d{d}", f"2017-09-{8 + i:02d}") for d in range(40) for i in range(3)]
    df = spark.createDataFrame(rows, ["city", "detid", "day"])

    split = add_splits(df)
    train = {r["detid"] for r in split.filter(~split.spatial_test).collect()}
    test = {r["detid"] for r in split.filter(split.spatial_test).collect()}

    assert train and test                      # both sides populated
    assert train.isdisjoint(test)


def test_temporal_split_holds_out_the_latest_days_per_city(spark):
    rows = [("manchester", "d1", f"2017-09-{d:02d}") for d in range(8, 28)]
    df = spark.createDataFrame(rows, ["city", "detid", "day"])

    split = add_splits(df, temporal_holdout_days=5)
    test_days = sorted({r["day"] for r in split.filter(split.temporal_test).collect()})
    train_days = sorted({r["day"] for r in split.filter(~split.temporal_test).collect()})

    assert test_days == ["2017-09-23", "2017-09-24", "2017-09-25", "2017-09-26", "2017-09-27"]
    assert max(train_days) < min(test_days)
