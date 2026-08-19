"""Study-city configuration for the RainCheck slice.

Chosen by the Phase-1 audit (reports/phase1_city_audit.md). These six are the
only UTD19 cities that both report observed speed and share a common window:
2017-09-08 to 2017-11-18. Between them they cover 749 detectors in three
countries using two ERA5 domains.

The German-anchor plan in CLAUDE.md 4.3 did not survive the audit: every large
German city (Hamburg, Darmstadt, Bremen, Augsburg, Munich...) reports flow and
occupancy but no speed. Essen is retained as the sole in-domain German city.
"""

import datetime as dt
from dataclasses import dataclass

WINDOW_START = dt.date(2017, 9, 8)
WINDOW_END = dt.date(2017, 11, 18)

# Above any plausible urban free-flow speed, so this removes sensor artefacts
# (observed maxima reach 243 km/h) without discarding real observations.
SPEED_CAP_KMH = 150.0


@dataclass(frozen=True)
class City:
    name: str
    timezone: str
    # Divisor mapping raw `occ` onto a 0-1 fraction. Four of the six cities
    # report occupancy as a percentage; Essen reports it as a fraction already.
    occ_scale: float
    # Birmingham ships no occupancy at all, so it cannot support the
    # occupancy-conditioned free-flow speed definition and is excluded from L2a
    # baselines. It still supports the typical-speed-deviation target.
    occ_available: bool


CITIES = (
    City("manchester", "Europe/London", 100.0, True),
    City("bolton", "Europe/London", 100.0, True),
    City("birmingham", "Europe/London", 1.0, False),
    City("rotterdam", "Europe/Amsterdam", 100.0, True),
    City("groningen", "Europe/Amsterdam", 100.0, True),
    City("essen", "Europe/Berlin", 1.0, True),
)

CITY_NAMES = tuple(c.name for c in CITIES)
TIMEZONES = {c.name: c.timezone for c in CITIES}
OCC_SCALES = {c.name: c.occ_scale for c in CITIES}
