"""Phase 7 -- data for the RainCheck monitor page.

Turns finished pipeline outputs into small JSON files the page draws from.
Nothing here computes a new result; every number traces to a report.

  export/dashboard/replay_{city}.json  -- one storm day at 10-minute frames:
      2 km rain cropped to the map, the 31 km value, and every sensor's speed
      against its normal for that hour.
  export/dashboard/summary.json        -- headline figures from reports/.

Run:  python -m raincheck.dashboard.export_dashboard
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import xarray as xr

from raincheck import config

EXPORT = config.PROJECT_ROOT / "export" / "dashboard"
ANALYSIS = config.LAKE_ROOT / "analysis" / "measurements_rain"
DOWNSCALED = config.LAKE_ROOT / "era5" / "downscaled"
CELLS_EXPECTED = config.LAKE_ROOT / "analysis" / "dose_response_cells_expected"

# The wettest weekday daytime with traffic data in each city (chosen from the
# analysis table: sum of the city-mean 2 km rain between 06:00 and 22:00 local).
STORMS = {
    "manchester": {"date": "2017-09-13", "tz": "Europe/London",
                   "label": "Storm Aileen, Wednesday 13 September 2017"},
    "torino": {"date": "2016-10-14", "tz": "Europe/Rome",
               "label": "Friday 14 October 2016"},
    # Essen's normal-speed profile covers at most ~57 of 108 frames on any day;
    # this is the wettest of the best-covered days.
    "essen": {"date": "2017-09-18", "tz": "Europe/Berlin",
              "label": "Monday 18 September 2017"},
}
START_H, END_H = 5, 23   # local replay window
STEP = timedelta(minutes=10)
PAD_DEG = 0.03           # rain crop beyond the road network's extent


def rnd(x, n=1):
    return None if x is None or (isinstance(x, float) and np.isnan(x)) else round(float(x), n)


def frames(spec):
    tz = ZoneInfo(spec["tz"])
    d = datetime.fromisoformat(spec["date"])
    t, end = d.replace(hour=START_H, tzinfo=tz), d.replace(hour=END_H, tzinfo=tz)
    out = []
    while t < end:
        out.append((t, t.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)))
        t += STEP
    return out


def rain_grid(city, graph, utc_times):
    """2 km fields at each frame, cropped to the map. Values in mm/h, 0.1 precision."""
    lons = [p[0] for e in graph["edges"] for p in e["c"]]
    lats = [p[1] for e in graph["edges"] for p in e["c"]]
    s, n = min(lats) - PAD_DEG, max(lats) + PAD_DEG
    w, e = min(lons) - PAD_DEG, max(lons) + PAD_DEG
    t0, t1 = np.datetime64(utc_times[0]), np.datetime64(utc_times[-1])
    parts = []
    for f in sorted((DOWNSCALED / city).glob("spateGAN_*.nc")):
        with xr.open_dataset(f) as ds:
            da = ds["precipitation"].sel(time=slice(t0, t1), lat=slice(s, n), lon=slice(w, e))
            if da.sizes["time"]:
                parts.append(da.load())
    da = xr.concat(parts, "time")
    # Overlapping runs repeat timesteps; keep the first of each.
    da = da.isel(time=~da.indexes["time"].duplicated()).sortby("time")
    da = da.sel(time=utc_times, method="nearest")
    lat, lon = da["lat"].values, da["lon"].values
    vals = np.round(np.clip(da.values, 0, None), 1)
    # A frame the model did not cover is null (NaN is not valid JSON).
    fields = [None if np.isnan(v).all() else np.nan_to_num(v).ravel().tolist() for v in vals]
    return {
        "lat0": float(lat[0]), "dlat": float(lat[1] - lat[0]), "nlat": len(lat),
        "lon0": float(lon[0]), "dlon": float(lon[1] - lon[0]), "nlon": len(lon),
    }, fields


def sensor_series(city, spec, frame_index):
    """Per sensor per frame: speed, normal speed, deviation, flow deviation, rain."""
    q = f"""
      select detid,
             time_bucket(interval 10 minute, ts_local) as f,
             avg(speed) speed, avg(typical_speed) normal,
             avg(typical_speed_deviation) dev, avg(typical_flow_deviation) fdev,
             avg(precip_mm_h) rain, avg(era5_precip_mm) era5
      from read_parquet('{config.spark_path(ANALYSIS)}/city={city}/**/*.parquet',
                        hive_partitioning=true)
      where cast(ts_local as date) = date '{spec["date"]}'
      group by all"""
    rows = duckdb.sql(q).fetchall()
    n = len(frame_index)
    series, coarse = {}, [[] for _ in range(n)]
    for det, f, speed, normal, dev, fdev, rain, era5 in rows:
        i = frame_index.get(f.strftime("%H:%M"))
        if i is None:
            continue
        s = series.setdefault(det, {k: [None] * n for k in ("speed", "normal", "dev", "fdev", "rain")})
        s["speed"][i], s["normal"][i] = rnd(speed), rnd(normal)
        s["dev"][i], s["fdev"][i], s["rain"][i] = rnd(dev, 3), rnd(fdev, 3), rnd(rain)
        if era5 is not None:
            coarse[i].append(era5)
    return series, [rnd(np.mean(c)) if c else None for c in coarse]


def replay(city):
    spec = STORMS[city]
    graph = json.loads((EXPORT / f"graph_{city}.json").read_text())
    fr = frames(spec)
    index = {loc.strftime("%H:%M"): i for i, (loc, _) in enumerate(fr)}
    grid, fine = rain_grid(city, graph, [u for _, u in fr])
    series, coarse = sensor_series(city, spec, index)
    on_map = [s for s in graph["sensors"] if s["edge"] is not None and s["id"] in series]
    out = {
        "city": city, "date": spec["date"], "label": spec["label"], "tz": spec["tz"],
        "times": list(index), "grid": grid, "fine": fine, "coarse": coarse,
        "sensors": [{"id": s["id"], **series[s["id"]]} for s in on_map],
    }
    (EXPORT / f"replay_{city}.json").write_text(
        json.dumps(out, separators=(",", ":"), allow_nan=False))
    wet = sum(1 for v in coarse if v and v >= 0.1)
    print(f"  {city}: {len(fr)} frames, grid {grid['nlat']}x{grid['nlon']}, "
          f"{len(out['sensors'])} sensors, {wet} wet frames (31 km)")
    return {"date": spec["date"], "label": spec["label"], "frames": len(fr),
            "sensors": len(out["sensors"])}


def summary(storms):
    rep = config.REPORTS_DIR
    ablation = json.loads((rep / "phase6a_rain_ablation.json").read_text())
    graph = json.loads((rep / "phase6b_road_graph.json").read_text())["results"]
    cells = duckdb.sql(f"""
        select view, level, band, measure, estimate, ci_lo, ci_hi, n_clusters
        from read_parquet('{config.spark_path(CELLS_EXPECTED)}/*.parquet')
        where view in ('congested', 'road_class', 'pooled') and estimate is not null
        """).fetchall()
    effects = [{"view": v, "level": lv, "band": b, "measure": m,
                "est": rnd(100 * e, 2), "lo": rnd(100 * lo, 2), "hi": rnd(100 * hi, 2),
                "days": n} for v, lv, b, m, e, lo, hi, n in cells]
    manifest = json.loads((rep / "phase2_curation_manifest.json").read_text())
    return {
        "pipeline": {
            "raw_readings": 134_380_371,        # phase1_audit.md
            "study_readings": 3_515_554,        # phase2 curation log: landed, study cities
            "curated_readings": 2_875_844,      # phase2_curation.md
            "analysed_readings": ablation["n_rows"],
            "sensors": sum(g["sensors"] for g in graph.values()),
            "cities": 3,
            "manifest_generated": manifest.get("generated"),
        },
        "effects": effects,
        "ablation": {k: {"rmse": {m: rnd(100 * v, 2) for m, v in r["rmse"].items()},
                         "rows": r["n_rows"],
                         "improvement": {c: {kk: rnd(100 * vv, 2) if kk != "pct_of_baseline" else rnd(vv, 2)
                                             for kk, vv in d.items()}
                                         for c, d in r["improvement"].items()}}
                     for k, r in ablation["results"].items()},
        "coverage": graph,
        "storms": storms,
    }


def main() -> int:
    EXPORT.mkdir(parents=True, exist_ok=True)
    storms = {}
    for city in STORMS:
        if not (EXPORT / f"graph_{city}.json").exists():
            print(f"FAIL: run raincheck.graph.build_road_graph first ({city})")
            return 1
        storms[city] = replay(city)
    (EXPORT / "summary.json").write_text(json.dumps(summary(storms), indent=1))
    sizes = {p.name: round(p.stat().st_size / 1e6, 2) for p in sorted(EXPORT.glob("*.json"))}
    print(f"wrote {EXPORT}: {sizes}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
