import datetime as dt

import numpy as np
import pytest
import xarray as xr

from raincheck.era5 import AREA, VARIABLES, build_monthly_requests, era5_to_long


def test_monthly_requests_cover_partial_first_and_last_months():
    # The lead-in starts mid-August and the window ends mid-November, so the
    # edge months must carry partial day lists. A year/month/day cross product
    # over the whole range would silently fetch all of August.
    reqs = build_monthly_requests(dt.date(2017, 8, 25), dt.date(2017, 11, 19))

    assert [(r["year"], r["month"]) for r in reqs] == [
        ("2017", "08"), ("2017", "09"), ("2017", "10"), ("2017", "11"),
    ]
    assert reqs[0]["day"] == ["25", "26", "27", "28", "29", "30", "31"]
    assert len(reqs[1]["day"]) == 30           # September
    assert len(reqs[2]["day"]) == 31           # October
    assert reqs[3]["day"][-1] == "19"          # November truncated


def test_every_request_asks_for_the_full_domain_and_all_hours():
    reqs = build_monthly_requests(dt.date(2017, 9, 1), dt.date(2017, 9, 2))

    req = reqs[0]
    assert req["area"] == AREA
    assert len(req["time"]) == 24
    assert set(req["variable"]) == set(VARIABLES)
    # spateGAN needs convective and large-scale precipitation separately, so
    # they are fetched now even though this slice only uses total precipitation.
    assert "convective_precipitation" in req["variable"]
    assert "large_scale_precipitation" in req["variable"]


def _dataset(time_coord: str) -> xr.Dataset:
    return xr.Dataset(
        {
            "tp": ((time_coord, "latitude", "longitude"), np.array([[[0.0025]]])),
            "cp": ((time_coord, "latitude", "longitude"), np.array([[[0.001]]])),
            "lsp": ((time_coord, "latitude", "longitude"), np.array([[[0.0015]]])),
            "t2m": ((time_coord, "latitude", "longitude"), np.array([[[283.15]]])),
            "d2m": ((time_coord, "latitude", "longitude"), np.array([[[278.15]]])),
            "u10": ((time_coord, "latitude", "longitude"), np.array([[[3.0]]])),
            "v10": ((time_coord, "latitude", "longitude"), np.array([[[-4.0]]])),
        },
        coords={
            time_coord: [np.datetime64("2017-09-08T15:00:00")],
            "latitude": [53.5],
            "longitude": [-2.25],
        },
    )


def test_precipitation_converts_metres_to_millimetres_and_kelvin_to_celsius():
    # ERA5 ships tp in metres accumulated over the hour ENDING at the stamp, so
    # tp * 1000 is already mm for that hour - i.e. mm/h, with no division.
    long = era5_to_long(_dataset("valid_time"))

    row = long.iloc[0]
    assert row["tp_mm"] == 2.5
    assert row["cp_mm"] == 1.0
    assert row["lsp_mm"] == 1.5
    assert row["t2m_c"] == 10.0
    assert row["d2m_c"] == 5.0
    assert row["grid_lat"] == 53.5
    assert row["grid_lon"] == -2.25


def test_long_form_accepts_either_time_coordinate_name():
    # The current CDS names the hourly coordinate `valid_time`; older archives
    # and some tooling still emit `time`.
    for coord in ("valid_time", "time"):
        long = era5_to_long(_dataset(coord))
        assert list(long["ts_utc"]) == [np.datetime64("2017-09-08T15:00:00")]


def test_grid_coordinates_are_rounded_so_they_join_exactly():
    # Detector-side snapping produces exact multiples of 0.25. If the netCDF
    # stores 53.499996 the equality join silently matches nothing, which looks
    # like "no rain data" rather than a bug.
    ds = _dataset("valid_time").assign_coords(latitude=[53.499996], longitude=[-2.2500001])

    long = era5_to_long(ds)

    assert long.iloc[0]["grid_lat"] == 53.5
    assert long.iloc[0]["grid_lon"] == -2.25


def test_missing_precipitation_is_an_error_not_an_empty_column():
    # The CDS splits its netCDF into instantaneous and accumulated streams; the
    # accumulated one holds tp/cp/lsp. Losing it produced a table that looked
    # fine and contained no rainfall at all, so absence must be loud.
    instant_only = _dataset("valid_time").drop_vars(["tp", "cp", "lsp"])

    with pytest.raises(ValueError, match="tp"):
        era5_to_long(instant_only)


def test_timestamps_are_microsecond_precision_for_spark():
    # pandas defaults to nanoseconds, and Spark's Parquet reader rejects
    # INT64 TIMESTAMP(NANOS) outright: "Illegal Parquet type".
    long = era5_to_long(_dataset("valid_time"))

    assert long["ts_utc"].dtype == np.dtype("datetime64[us]")
