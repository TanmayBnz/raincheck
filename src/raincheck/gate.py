"""The Phase-1 gate.

CLAUDE.md treats city selection as a hard gate: the largest risk to the project
is building the pipeline and only then discovering the chosen cities lack speed
or span too few days. These thresholds encode that gate so it is explicit and
reviewable rather than a judgement made once in someone's head.
"""

# A city reporting speed on fewer than this share of rows cannot support a
# per-detector free-flow percentile, however many rows it has in total.
MIN_SPEED_PCT = 50.0

# Dose-response needs enough distinct rain events; distinct days is the cheap
# proxy available before ERA5 has been joined.
MIN_DAYS = 30

# Speed present on under this share of rows is treated as absent, not sparse:
# it is stray values, not a reporting city.
SPEED_ABSENT_PCT = 1.0


def classify_city(
    speed_pct: float,
    n_days: int,
    min_speed_pct: float = MIN_SPEED_PCT,
    min_days: int = MIN_DAYS,
) -> str:
    """Gate verdict for one city, from its audited speed coverage and day count."""
    if speed_pct < SPEED_ABSENT_PCT:
        return "NO_SPEED"
    if speed_pct < min_speed_pct:
        return "SPARSE_SPEED"
    if n_days < min_days:
        return "SHORT_COVERAGE"
    return "VIABLE"


# UTD19's twelve German cities, as spelled in the dataset's `city` column.
# These are in-domain for spateGAN-ERA5, which was trained on German
# gauge-adjusted radar, so a German study city keeps the downscaler defensible.
GERMAN_CITIES = frozenset(
    {
        "augsburg",
        "bremen",
        "constance",
        "darmstadt",
        "essen",
        "frankfurt",
        "hamburg",
        "kassel",
        "munich",
        "speyer",
        "stuttgart",
        "wolfsburg",
    }
)
