"""W3 -- ERA5 rain-event pre-check for the candidate cities.

CONTEXT.md's Phase-1 gate requires "an adequate count of distinct rain events".
Speed availability alone cannot close the gate: a city can pass every traffic
criterion and still contain almost no rain.

This uses ERA5 at its NATIVE ~31 km resolution. spateGAN downscaling is Phase 4
work -- its 672x672 km extent and 16-hour sequence constraints are irrelevant to
counting events, and skipping it keeps the pull cheap.

Requires CDS API credentials in ~/.cdsapirc. Register at
https://cds.climate.copernicus.eu/ then accept the ERA5 licence.

Run:  python -m raincheck.weather.era5_precheck
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from raincheck import config

# Met Office bands, in mm/h -- matched to IUTF's published thresholds so the
# results stay directly comparable with the prior-art benchmark.
BANDS = [
    ("Light", 0.1, 0.5),
    ("Moderate", 0.5, 4.0),
    ("Heavy", 4.0, 10.0),
    ("Extreme", 10.0, float("inf")),
]

WET_THRESHOLD_MM = 0.1
# Two consecutive dry hours separate one rain event from the next.
EVENT_GAP_HOURS = 2

# Local-time peak periods; rain landing here matters most operationally.
PEAK_HOURS = set(range(7, 10)) | set(range(16, 19))

ERA5_RAW = config.LAKE_ROOT / "era5" / "raw"


def fetch_city(client, city: str, spec: dict) -> Path:
    """Download hourly total precipitation for one city window. Cached."""
    ERA5_RAW.mkdir(parents=True, exist_ok=True)
    target = ERA5_RAW / f"{city}.nc"
    if target.exists():
        print(f"  {city}: cached -> {target.name}")
        return target

    north, west, south, east = spec["bbox"]
    first, last = spec["first_day"], spec["last_day"]
    y0, m0 = int(first[:4]), int(first[5:7])
    y1, m1 = int(last[:4]), int(last[5:7])

    years = sorted({str(y) for y in range(y0, y1 + 1)})
    months = sorted({f"{m:02d}" for m in _months_in_range(y0, m0, y1, m1)})

    print(f"  {city}: requesting {first}..{last} bbox={spec['bbox']}")
    client.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable": "total_precipitation",
            "year": years,
            "month": months,
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": [north, west, south, east],
            "format": "netcdf",
        },
        str(target),
    )
    return target


def _months_in_range(y0: int, m0: int, y1: int, m1: int):
    """Month numbers touched by the window (union across years -- ERA5 requests
    are a cross-product, so a multi-year window over-requests slightly)."""
    out = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.append(m)
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def analyse(path: Path, spec: dict) -> dict:
    import xarray as xr

    ds = xr.open_dataset(path)
    var = "tp" if "tp" in ds else list(ds.data_vars)[0]
    da = ds[var]

    # The current CDS names the time axis `valid_time`; older files use `time`.
    # Getting this wrong silently averages the time axis away and leaves a
    # scalar, so resolve it explicitly rather than by exclusion.
    tname = next((d for d in ("valid_time", "time") if d in da.dims), None)
    if tname is None:
        raise ValueError(f"no time dimension in {path.name}; dims={da.dims}")

    # Average over space only (a small bbox may be a single ~31 km cell).
    space_dims = [d for d in da.dims if d != tname]
    # ERA5 total_precipitation accumulates metres over the preceding hour.
    series = (da.mean(dim=space_dims) if space_dims else da) * 1000.0
    series = series.sel({tname: slice(spec["first_day"], spec["last_day"])})

    times = series[tname].values
    mm = series.values

    wet = mm >= WET_THRESHOLD_MM
    n_hours = len(mm)
    n_wet = int(wet.sum())

    band_counts = {
        name: int(((mm >= lo) & (mm < hi)).sum()) for name, lo, hi in BANDS
    }

    # Distinct events: wet runs separated by >= EVENT_GAP_HOURS dry hours.
    events, dry_run, in_event = 0, 0, False
    peak_wet = 0
    for i, is_wet in enumerate(wet):
        hour = int(str(times[i])[11:13])
        if is_wet:
            if hour in PEAK_HOURS:
                peak_wet += 1
            if not in_event:
                events += 1
                in_event = True
            dry_run = 0
        else:
            dry_run += 1
            if dry_run >= EVENT_GAP_HOURS:
                in_event = False

    return {
        "hours": n_hours,
        "wet_hours": n_wet,
        "wet_pct": round(100.0 * n_wet / n_hours, 1) if n_hours else 0.0,
        "events": events,
        "peak_wet_hours": peak_wet,
        "bands": band_counts,
        "moderate_plus": band_counts["Moderate"] + band_counts["Heavy"] + band_counts["Extreme"],
    }


def main() -> int:
    windows_path = config.REPORTS_DIR / "phase1_windows.json"
    if not windows_path.exists():
        print(f"missing {windows_path} -- run the W2 audit first")
        return 1
    windows = json.loads(windows_path.read_text(encoding="utf-8"))

    try:
        import cdsapi
    except ImportError:
        print("cdsapi not installed -- pip install cdsapi xarray netCDF4")
        return 1

    # Credentials come from the project .env (gitignored) when present, else
    # fall back to a conventional ~/.cdsapirc.
    env = config.load_env()
    url, key = env.get("CDS_API_URL"), env.get("CDS_API_KEY")

    if key and not key.startswith("your-"):
        if ":" in key:
            print(
                "CDS_API_KEY looks like the LEGACY 'UID:KEY' format, which the\n"
                "current API rejects. Copy the bare Personal Access Token instead."
            )
            return 1
        client = cdsapi.Client(url=url or "https://cds.climate.copernicus.eu/api", key=key)
        print("using credentials from .env")
    elif (Path.home() / ".cdsapirc").exists():
        client = cdsapi.Client()
        print("using credentials from ~/.cdsapirc")
    else:
        print(
            "No CDS credentials found.\n"
            "  Put CDS_API_URL and CDS_API_KEY in .env (see .env.example), or\n"
            "  write ~/.cdsapirc.\n"
            "  Token: https://cds.climate.copernicus.eu/ -> your name -> "
            "Personal Access Token\n"
            "  You must also accept the ERA5 licence on the dataset Download tab."
        )
        return 1
    results = {}
    for city, spec in windows.items():
        if not spec.get("bbox"):
            print(f"  {city}: no bbox in conf/cities.yml -- skipped")
            continue
        try:
            path = fetch_city(client, city, spec)
            results[city] = analyse(path, spec)
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"  {city}: FAILED -- {exc}")

    conf = config.load_cities_conf()
    lines = [
        "# Phase-1 ERA5 Rain-Event Pre-check",
        "",
        "Native-resolution (~31 km) ERA5, area-mean over each city bbox, across",
        "that city's actual UTD19 window. Bands follow Met Office / IUTF thresholds.",
        "",
        "| city | hours | wet hrs | wet% | events | peak wet hrs | Light | Moderate | Heavy | Extreme | Mod+ |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for city, r in results.items():
        b = r["bands"]
        lines.append(
            f"| {city} | {r['hours']} | {r['wet_hours']} | {r['wet_pct']} | {r['events']} | "
            f"{r['peak_wet_hours']} | {b['Light']} | {b['Moderate']} | {b['Heavy']} | "
            f"{b['Extreme']} | {r['moderate_plus']} |"
        )

    lines += ["", "## Pooled cohorts", "", "| cohort | cities | events | Mod+ |", "|---|---|---|---|"]
    for name, grp in conf["cohorts"].items():
        cs = [c for c in grp["cities"] if c in results]
        if not cs:
            continue
        lines.append(
            f"| {name} | {', '.join(cs)} | {sum(results[c]['events'] for c in cs)} | "
            f"{sum(results[c]['moderate_plus'] for c in cs)} |"
        )

    out = config.REPORTS_DIR / "phase1_rain.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (config.REPORTS_DIR / "phase1_rain.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
