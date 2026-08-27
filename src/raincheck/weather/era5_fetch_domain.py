"""Phase 4 / L2b -- fetch the ERA5 fields spateGAN-ERA5 actually needs.

The Phase-1 pre-check pulled `total_precipitation` over a ~30 km city box, which
was right for counting rain events and useless for downscaling. spateGAN needs
something different on all three axes:

  variables  convective + large-scale precipitation, NOT total. The model was
             trained on the two-component split because convective and
             stratiform rain have different spatial structure, and that
             structure is exactly what it reconstructs at 2 km.
  extent     >= 672 x 672 km. The model emits a +-168 km patch around the
             centre and needs surrounding context to do it, so the request has
             to be an order of magnitude wider than the city.
  sequence   >= 16 hours contiguous.

City bboxes in conf/cities.yml are therefore unusable here and a domain is
derived per city instead: a square in degrees, centred on the city, sized from
the km requirement at that latitude.

Downloads are cached by filename -- reruns are free.

Run:  python -m raincheck.weather.era5_fetch_domain
"""

from __future__ import annotations

import argparse
import json
import math
import sys

from raincheck import config

# spateGAN needs a 436 km radius (872 km across) around the patch centre. Ask
# for considerably more, because two things silently eat the margin:
#   - CDS snaps the requested `area` outward/inward to the 0.25 deg grid;
#   - the model re-centres the patch onto its UTM grid, shifting it by up to
#     ~0.15 deg from the requested centre.
# A 900 km domain looked sufficient on paper and failed for Torino with
# "Insufficient coverage south: 424.21 km < 436.00 km". ERA5 at 0.25 deg is
# tiny (tens of MB), so the margin is nearly free -- buy plenty.
DOMAIN_KM = 1200.0
KM_PER_DEG_LAT = 111.32

VARIABLES = ["convective_precipitation", "large_scale_precipitation"]

ERA5_DOMAIN_RAW = config.LAKE_ROOT / "era5" / "domain"


def city_centre(bbox: list[float]) -> tuple[float, float]:
    """conf/cities.yml stores bboxes as [N, W, S, E]."""
    north, west, south, east = bbox
    return (north + south) / 2.0, (west + east) / 2.0


def domain_bbox(lat: float, lon: float, km: float = DOMAIN_KM) -> list[float]:
    """Square-ish domain in degrees around a centre.

    Longitude degrees shrink with latitude, so the lon half-width is divided by
    cos(lat). At Manchester's 53.5 N that is a factor of 1.7 -- omitting it
    would yield a domain 40% too narrow east-west and silently violate the
    model's minimum extent.
    """
    half_lat = (km / 2.0) / KM_PER_DEG_LAT
    half_lon = (km / 2.0) / (KM_PER_DEG_LAT * math.cos(math.radians(lat)))
    return [
        round(lat + half_lat, 2),
        round(lon - half_lon, 2),
        round(lat - half_lat, 2),
        round(lon + half_lon, 2),
    ]


def months_in_range(y0: int, m0: int, y1: int, m1: int):
    out, y, m = [], y0, m0
    while (y, m) <= (y1, m1):
        out.append(m)
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def fetch(client, city: str, spec: dict, centre: tuple[float, float]):
    ERA5_DOMAIN_RAW.mkdir(parents=True, exist_ok=True)
    target = ERA5_DOMAIN_RAW / f"{city}_cp_lsp.nc"
    if target.exists():
        size_mb = target.stat().st_size / 1e6
        print(f"  {city}: cached ({size_mb:.1f} MB) -> {target.name}")
        return target

    lat, lon = centre
    bbox = domain_bbox(lat, lon)
    first, last = spec["first_day"], spec["last_day"]
    y0, m0 = int(first[:4]), int(first[5:7])
    y1, m1 = int(last[:4]), int(last[5:7])

    # Widen by a month either side. run_downscaling pads each run by a day so
    # the local/UTC offset is covered at the edges, and a padding day that
    # falls just outside the window's own months would otherwise have no source
    # data at all -- which silently truncates the final run and leaves the last
    # few hours of the last data day without rainfall.
    if m0 == 1:
        y0, m0 = y0 - 1, 12
    else:
        m0 -= 1
    if m1 == 12:
        y1, m1 = y1 + 1, 1
    else:
        m1 += 1

    years = sorted({str(y) for y in range(y0, y1 + 1)})
    months = sorted({f"{m:02d}" for m in months_in_range(y0, m0, y1, m1)})

    print(f"  {city}: centre=({lat:.2f}, {lon:.2f}) domain={bbox}")
    print(f"           {first}..{last}  ({len(years)}y x {len(months)}m x 24h)")
    client.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable": VARIABLES,
            "year": years,
            "month": months,
            "day": [f"{d:02d}" for d in range(1, 32)],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": bbox,
            "format": "netcdf",
        },
        str(target),
    )
    return target


def make_client():
    """Same credential resolution as the Phase-1 pre-check."""
    import cdsapi

    env = config.load_env()
    url, key = env.get("CDS_API_URL"), env.get("CDS_API_KEY")
    if key and not key.startswith("your-"):
        return cdsapi.Client(url=url or "https://cds.climate.copernicus.eu/api", key=key)
    from pathlib import Path

    if (Path.home() / ".cdsapirc").exists():
        return cdsapi.Client()
    raise RuntimeError(
        "No CDS credentials. Put CDS_API_URL / CDS_API_KEY in .env (see .env.example)."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--city", help="fetch a single city (default: all study cities)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the domains that would be requested and exit",
    )
    args = parser.parse_args()

    conf = config.load_cities_conf()
    windows_path = config.REPORTS_DIR / "phase1_windows.json"
    if not windows_path.exists():
        print(f"missing {windows_path} -- run the W2 audit first")
        return 1
    windows = json.loads(windows_path.read_text(encoding="utf-8"))

    cities = [args.city] if args.city else list(conf["study"].keys())
    manifest = {}

    for city in cities:
        spec = windows.get(city)
        if not spec or not spec.get("bbox"):
            print(f"  {city}: no window/bbox -- skipped")
            continue
        lat, lon = city_centre(spec["bbox"])
        manifest[city] = {
            "centre_lat": round(lat, 4),
            "centre_lon": round(lon, 4),
            "domain_bbox": domain_bbox(lat, lon),
            "first_day": spec["first_day"],
            "last_day": spec["last_day"],
        }

    if args.dry_run:
        print(json.dumps(manifest, indent=2))
        return 0

    try:
        client = make_client()
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: {exc}")
        return 1

    ok = True
    for city, spec in manifest.items():
        try:
            path = fetch(client, city, windows[city], (spec["centre_lat"], spec["centre_lon"]))
            spec["path"] = str(path)
            spec["size_mb"] = round(path.stat().st_size / 1e6, 1)
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"  {city}: FAILED -- {exc}")
            ok = False

    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS_DIR / "phase4_domains.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"wrote {out}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
