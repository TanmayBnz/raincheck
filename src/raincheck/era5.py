"""ERA5 acquisition and reshaping (L2b, first half).

Pulls hourly single-level reanalysis from the Copernicus CDS and flattens the
netCDF cube into long-form rows ready to join against detectors.
"""

import calendar
import datetime as dt

import pandas as pd
import xarray as xr

# N / W / S / E. The six study cities span only 238 x 653 km, but this box is
# roughly 722 x 860 km, which also clears spateGAN-ERA5's 672 x 672 km minimum
# extent - so a later downscaling phase can reuse this exact download.
AREA = [56.0, -4.0, 49.5, 8.5]

DATASET = "reanalysis-era5-single-levels"

VARIABLES = [
    "total_precipitation",
    # spateGAN-ERA5 requires convective and large-scale precipitation as
    # separate inputs. Unused by this slice, fetched now to avoid a re-pull.
    "convective_precipitation",
    "large_scale_precipitation",
    "2m_temperature",
    "2m_dewpoint_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
]

HOURS = [f"{h:02d}:00" for h in range(24)]

# Short names as they appear inside the netCDF, mapped to output columns.
# Precipitation is metres -> millimetres; temperatures Kelvin -> Celsius.
_SCALE_MM = ("tp", "cp", "lsp")

# Without these the output table is structurally valid and contains no rainfall,
# which is indistinguishable from "it never rained". The CDS delivers them in a
# separate netCDF stream from the instantaneous fields, so losing them is easy.
REQUIRED_VARS = ("tp", "cp", "lsp")
_KELVIN = ("t2m", "d2m")
_PASSTHROUGH = ("u10", "v10")


def build_monthly_requests(start: dt.date, end: dt.date) -> list[dict]:
    """One CDS request per calendar month, with exact day lists.

    Deliberately not a single year/month/day request: the CDS treats those lists
    as a cross product, so a range from 25 August to 19 November would also pull
    1-24 August and 20-30 November.
    """
    requests = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        first = 1 if (year, month) != (start.year, start.month) else start.day
        last = (
            calendar.monthrange(year, month)[1]
            if (year, month) != (end.year, end.month)
            else end.day
        )
        requests.append(
            {
                "product_type": ["reanalysis"],
                "variable": list(VARIABLES),
                "year": f"{year:04d}",
                "month": f"{month:02d}",
                "day": [f"{d:02d}" for d in range(first, last + 1)],
                "time": list(HOURS),
                "area": list(AREA),
                "data_format": "netcdf",
                "download_format": "unarchived",
            }
        )
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return requests


def era5_to_long(ds: xr.Dataset) -> pd.DataFrame:
    """Flatten an ERA5 cube to one row per (grid cell, hour), in SI-friendly units."""
    missing = [v for v in REQUIRED_VARS if v not in ds.data_vars]
    if missing:
        raise ValueError(
            f"ERA5 cube is missing required variables {missing}; "
            f"present: {sorted(ds.data_vars)}. The CDS splits accumulated fields "
            "(tp/cp/lsp) into their own netCDF inside the returned zip - all "
            "members must be merged, not just the first."
        )
    time_coord = "valid_time" if "valid_time" in ds.coords else "time"
    frame = ds.to_dataframe().reset_index()

    out = pd.DataFrame(
        {
            # Rounded so they compare equal to the detector-side snapping in
            # rain.assign_grid_cell. netCDF floats are not exact multiples of
            # 0.25, and an equality join on them matches nothing.
            "grid_lat": frame["latitude"].astype("float64").round(4),
            "grid_lon": frame["longitude"].astype("float64").round(4),
            # Microseconds, not the pandas-default nanoseconds: Spark's Parquet
            # reader rejects INT64 TIMESTAMP(NANOS) with "Illegal Parquet type".
            "ts_utc": frame[time_coord].astype("datetime64[us]"),
        }
    )
    for name in _SCALE_MM:
        if name in frame:
            out[f"{name}_mm"] = frame[name].astype("float64") * 1000.0
    for name in _KELVIN:
        if name in frame:
            out[f"{name}_c"] = frame[name].astype("float64") - 273.15
    for name in _PASSTHROUGH:
        if name in frame:
            out[name] = frame[name].astype("float64")
    return out
