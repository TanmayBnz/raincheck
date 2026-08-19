from raincheck.gate import classify_city


def test_city_without_speed_fails_the_gate():
    assert classify_city(speed_pct=0.0, n_days=400) == "NO_SPEED"


def test_city_with_speed_on_a_minority_of_rows_is_not_yet_usable():
    assert classify_city(speed_pct=12.0, n_days=400) == "SPARSE_SPEED"


def test_city_with_speed_but_too_few_days_fails_on_coverage():
    assert classify_city(speed_pct=99.0, n_days=5) == "SHORT_COVERAGE"


def test_city_with_speed_and_coverage_passes():
    assert classify_city(speed_pct=99.0, n_days=400) == "VIABLE"
