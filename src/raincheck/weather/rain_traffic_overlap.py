"""W3b -- rain events restricted to days that actually have traffic data.

The raw ERA5 pre-check counts rain across each city's calendar span. That
overstates the usable total, because several cities sample scattered days:
Manchester has traffic on 28 of 72 calendar days, Essen on 35 of 188. Rain
falling on a day with no detector readings contributes nothing to the analysis.

This intersects the two and reports the number the gate should actually use.

Run:  python -m raincheck.weather.rain_traffic_overlap
"""

from __future__ import annotations

import json
import sys

import duckdb

from raincheck import config
from raincheck.weather.era5_precheck import (
    BANDS,
    ERA5_RAW,
    EVENT_GAP_HOURS,
    PEAK_HOURS,
    WET_THRESHOLD_MM,
)


def traffic_days(city: str) -> set[str]:
    """Distinct dates with at least one quality-passing reading."""
    lake = config.LANDED_MEASUREMENTS.as_posix()
    rows = duckdb.connect().execute(
        f"""
        SELECT DISTINCT CAST(date AS VARCHAR) AS d
        FROM read_parquet('{lake}/**/*.parquet', hive_partitioning=true)
        WHERE city = ? AND quality_ok
        """,
        [city],
    ).fetchall()
    return {r[0] for r in rows}


def series_for(city: str, spec: dict):
    import xarray as xr

    ds = xr.open_dataset(ERA5_RAW / f"{city}.nc")
    var = "tp" if "tp" in ds else list(ds.data_vars)[0]
    da = ds[var]
    tname = next(d for d in ("valid_time", "time") if d in da.dims)
    space = [d for d in da.dims if d != tname]
    s = (da.mean(dim=space) if space else da) * 1000.0
    s = s.sel({tname: slice(spec["first_day"], spec["last_day"])})
    return s[tname].values, s.values


def count(times, mm, keep_days: set[str] | None):
    """Rain statistics, optionally restricted to a set of dates."""
    idx = [i for i in range(len(mm)) if keep_days is None or str(times[i])[:10] in keep_days]
    vals = [mm[i] for i in idx]
    if not vals:
        return {"hours": 0, "wet_hours": 0, "events": 0, "moderate_plus": 0, "peak_wet_hours": 0}

    wet = [v >= WET_THRESHOLD_MM for v in vals]
    bands = {n: sum(1 for v in vals if lo <= v < hi) for n, lo, hi in BANDS}

    events, dry_run, in_event, peak = 0, 0, False, 0
    prev_day = None
    for j, i in enumerate(idx):
        day = str(times[i])[:10]
        # A gap between sampled days breaks any run in progress -- otherwise
        # rain either side of a 40-day hole would merge into one "event".
        if prev_day is not None and day != prev_day and _gap(prev_day, day):
            in_event, dry_run = False, EVENT_GAP_HOURS
        prev_day = day
        if wet[j]:
            if int(str(times[i])[11:13]) in PEAK_HOURS:
                peak += 1
            if not in_event:
                events += 1
                in_event = True
            dry_run = 0
        else:
            dry_run += 1
            if dry_run >= EVENT_GAP_HOURS:
                in_event = False

    # Cast through int(): the values come off numpy arrays as int64/float64,
    # which json cannot serialize.
    return {
        "hours": int(len(vals)),
        "wet_hours": int(sum(wet)),
        "wet_pct": round(100.0 * sum(wet) / len(vals), 1),
        "events": int(events),
        "moderate_plus": int(bands["Moderate"] + bands["Heavy"] + bands["Extreme"]),
        "peak_wet_hours": int(peak),
        "bands": {k: int(v) for k, v in bands.items()},
    }


def _gap(d1: str, d2: str) -> bool:
    from datetime import date

    a = date(int(d1[:4]), int(d1[5:7]), int(d1[8:10]))
    b = date(int(d2[:4]), int(d2[5:7]), int(d2[8:10]))
    return (b - a).days > 1


def main() -> int:
    windows = json.loads(
        (config.REPORTS_DIR / "phase1_windows.json").read_text(encoding="utf-8")
    )

    out = {}
    print(f"{'city':<12}{'cal days':>9}{'data days':>10}{'events(cal)':>12}"
          f"{'events(data)':>13}{'Mod+(data)':>11}{'peak(data)':>11}")
    for city, spec in windows.items():
        path = ERA5_RAW / f"{city}.nc"
        if not path.exists():
            continue
        times, mm = series_for(city, spec)
        days = traffic_days(city)
        cal = count(times, mm, None)
        obs = count(times, mm, days)
        out[city] = {"calendar": cal, "with_traffic": obs, "data_days": len(days)}
        print(f"{city:<12}{cal['hours']//24:>9}{len(days):>10}{cal['events']:>12}"
              f"{obs['events']:>13}{obs['moderate_plus']:>11}{obs['peak_wet_hours']:>11}")

    (config.REPORTS_DIR / "phase1_rain_overlap.json").write_text(
        json.dumps(out, indent=2), encoding="utf-8"
    )
    print(f"\nwrote {config.REPORTS_DIR / 'phase1_rain_overlap.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
