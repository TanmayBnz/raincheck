"""Phase 5 / L3a -- the stratification axes of the dose-response table.

CONTEXT.md §L3(a) specifies interactions across
`rainfall band × road class × time-of-day × baseline congestion`. Three of
those four need defining before anything can be estimated, and each definition
is a judgement that changes the answer, so they live here rather than inline in
the estimator.

Kept separate from dose_response.py because Phase 6's predictive model must
stratify its spatial and event-based holdouts on exactly these axes. Two
definitions of "AM peak" across the two phases would make the explanatory table
and the predictive evaluation quietly incomparable.
"""

from __future__ import annotations

from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Road class.
#
# UTD19 carries OSM's fclass, which is finer than the effect can support: with
# Extreme rain totalling 2,130 intervals across all three cities, an 8-way
# split produces cells of single digits. These groupings collapse OSM classes
# that share a driving regime -- free-flowing grade-separated, high-capacity
# signalised, local distributor -- so the surviving cells are estimable.
#
# `_link` suffixes are slip roads. They are folded into their parent class:
# they carry that road's traffic, and separating them buys nothing here.
#
# CAVEAT: this is UTD19's own fclass, NOT the PBF-based OSM map-match that
# CONTEXT.md §6/L1 specifies -- that remains deferred. Adequate for
# stratification; not a substitute for the real network join.
# ---------------------------------------------------------------------------
ROAD_CLASS_MAP = {
    "motorway": "motorway/trunk",
    "motorway_link": "motorway/trunk",
    "trunk": "motorway/trunk",
    "trunk_link": "motorway/trunk",
    "primary": "primary",
    "primary_link": "primary",
    "secondary": "secondary",
    "secondary_link": "secondary",
    "tertiary": "tertiary",
    "residential": "residential/other",
    "other": "residential/other",
}

ROAD_CLASS_ORDER = [
    "motorway/trunk", "primary", "secondary", "tertiary", "residential/other",
]

# ---------------------------------------------------------------------------
# Time of day, on LOCAL clock hours.
#
# `hod` is derived from ts_local, which is the right clock: the commute peak
# follows local time, not UTC, and Torino and Manchester sit in different
# zones. Using UTC here would smear Torino's peak by an hour.
#
# Boundaries follow the ordinary European urban commute rather than any single
# city's measured peak, so the same bucket means the same thing in all three
# cities -- a prerequisite for pooling with city fixed effects.
# ---------------------------------------------------------------------------
TOD_ORDER = ["night", "am_peak", "midday", "pm_peak", "evening"]

# ---------------------------------------------------------------------------
# Rain bands: Met Office thresholds, identical to IUTF's, set in
# build_rain_features. Repeated here only to fix the display order --
# dose-response is meaningless if the bands are not read in intensity order.
# ---------------------------------------------------------------------------
BAND_ORDER = ["Dry", "Light", "Moderate", "Heavy", "Extreme"]
WET_BANDS = ["Light", "Moderate", "Heavy", "Extreme"]

# The reference level every contrast is taken against.
REFERENCE_BAND = "Dry"


def road_class(col=None):
    """Collapse OSM fclass onto the five estimable driving regimes.

    `col` resolves at call time, not as a default argument: F.col() needs a live
    SparkContext, and a module-level default would evaluate at import.
    """
    col = F.col("fclass") if col is None else col
    out = F.lit("residential/other")
    for raw, grouped in ROAD_CLASS_MAP.items():
        out = F.when(col == F.lit(raw), F.lit(grouped)).otherwise(out)
    # An unmapped or NULL fclass must not silently become residential/other:
    # that would put unknown roads into a real class and bias it.
    return F.when(col.isNull(), F.lit("unknown")).otherwise(out)


def time_of_day(col=None):
    """Bucket local clock hour into five commute regimes."""
    col = F.col("hod") if col is None else col
    return (
        F.when((col >= 6) & (col < 10), F.lit("am_peak"))
        .when((col >= 10) & (col < 16), F.lit("midday"))
        .when((col >= 16) & (col < 19), F.lit("pm_peak"))
        .when((col >= 19) & (col < 23), F.lit("evening"))
        .otherwise(F.lit("night"))
    )


def cluster_id():
    """The unit of independent information: the detector-day.

    Successive 5-minute readings from one loop are emphatically not independent
    -- they share the detector's siting, its calibration, that day's incident
    and roadworks history, and the weather system passing overhead. Treating
    2.9 M intervals as 2.9 M independent observations would produce confidence
    intervals roughly sqrt(n_per_cluster) times too narrow, which is how a
    62-interval result comes to look publishable.

    The detector-day is the coarsest unit that still leaves enough clusters to
    resample (~520 detectors x ~21 days), and it absorbs both the within-day
    autocorrelation and the fact that a rain event covers a whole city at once.
    """
    return F.concat_ws("|", F.col("city"), F.col("detid"), F.col("date").cast("string"))
