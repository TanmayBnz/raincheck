"""NDW (Netherlands) DATEX II adapter.

Parses the two open feeds published at https://opendata.ndw.nu/ :

* ``measurement_current.xml.gz`` - a ``MeasurementSiteTablePublication``, the
  detector metadata (one ``Point`` record per site).
* ``trafficspeed.xml.gz`` - a ``MeasuredDataPublication``, one minute of
  measured values per site.

Structural facts this module relies on are recorded in
``reports/phase1_nl_audit.md``.
"""
from __future__ import annotations

import datetime as dt
import xml.etree.ElementTree as ET
from dataclasses import dataclass


def _local(tag: str) -> str:
    """Local name of a possibly namespace-qualified tag."""
    return tag.rsplit("}", 1)[-1]


def _find(element, name: str):
    """First descendant (or self) whose local name is ``name``."""
    for sub in element.iter():
        if _local(sub.tag) == name:
            return sub
    return None


def _text(element, name: str) -> str | None:
    """Text of the first ``name`` descendant, unwrapping DATEX II <values>.

    Several fields wrap their content as ``<values><value lang="nl">x</value>``,
    so reading ``.text`` on the named element itself yields None.
    """
    found = _find(element, name)
    if found is None:
        return None
    if found.text and found.text.strip():
        return found.text.strip()
    value = _find(found, "value")
    if value is not None and value.text:
        return value.text.strip()
    return None


MISSING_SENTINEL = -1.0
"""NDW encodes "no measurement" as -1 inside an otherwise numeric field."""


def _measured(value: str | None) -> float | None:
    """Numeric text to float, mapping the -1 sentinel to None."""
    if value is None:
        return None
    number = float(value)
    return None if number == MISSING_SENTINEL else number


XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"


def _is_point(record) -> bool:
    """Is this a detector site rather than a travel-time section?

    Itinerary records also carry openlrCoordinate elements, so location type -
    not the presence of coordinates - is what distinguishes the 20,519 detector
    sites from the 80,709 ItineraryByIndexedLocations sections.
    """
    location = _find(record, "measurementSiteLocation")
    return location is not None and location.get(XSI_TYPE) == "Point"


def _reading(container, value_tag: str) -> float | None:
    """Numeric reading from a ``measuredValue``, or None if absent or flagged.

    ``dataError`` is honoured because it can accompany a perfectly legal-looking
    value - the live feed pairs ``<dataError>true</dataError>`` with
    ``<vehicleFlowRate>0</vehicleFlowRate>``, and unlike the -1 sentinel a zero
    flow is indistinguishable from genuinely empty road without the flag.
    """
    if container is None:
        return None
    if (_text(container, "dataError") or "").lower() == "true":
        return None
    return _measured(_text(container, value_tag))


def _timestamp(value: str | None) -> dt.datetime | None:
    """DATEX II UTC instant to a naive-UTC datetime.

    Stored naive rather than tz-aware because the feed is already UTC and the
    Spark session runs with spark.sql.session.timeZone=UTC; a tz-aware value
    would be converted a second time.
    """
    if value is None:
        return None
    return (
        dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        .astimezone(dt.timezone.utc)
        .replace(tzinfo=None)
    )


def _aggregate_indices(record) -> dict[str, str]:
    """Map ``trafficFlow``/``trafficSpeed`` to this site's ``anyVehicle`` index.

    The index is site-specific - it depends on lane count multiplied by the
    number of vehicle-length classes, and was observed at 4, 8 and 16 across
    real sites. Hardcoding one would read a length-classed lane as if it were
    the aggregate, so indices are always resolved from the site table.
    """
    found: dict[str, str] = {}
    for characteristics in record:
        if _local(characteristics.tag) != "measurementSpecificCharacteristics":
            continue
        index = characteristics.get("index")
        inner = characteristics[0] if len(characteristics) else None
        if index is None or inner is None:
            continue
        if _text(inner, "vehicleType") != "anyVehicle":
            continue
        value_type = _text(inner, "specificMeasurementValueType")
        if value_type in ("trafficFlow", "trafficSpeed"):
            found.setdefault(value_type, index)
    return found


@dataclass(frozen=True)
class Site:
    """One NDW measurement point."""

    site_id: str
    lat: float
    lon: float
    name: str | None
    equipment: str | None
    computation_method: str | None
    frc: str | None
    flow_index: str | None
    speed_index: str | None


@dataclass(frozen=True)
class SiteTable:
    """A parsed ``MeasurementSiteTable``."""

    version: str
    sites: dict[str, Site]


def parse_site_table(source) -> SiteTable:
    """Parse a MeasurementSiteTablePublication into a SiteTable."""
    sites: dict[str, Site] = {}
    version = ""

    for _, element in ET.iterparse(source, events=("end",)):
        name = _local(element.tag)

        if name == "measurementSiteTable":
            version = element.get("version", "")
            continue
        if name != "measurementSiteRecord":
            continue

        site_id = element.get("id")
        lat, lon = _text(element, "latitude"), _text(element, "longitude")
        indices = _aggregate_indices(element)
        if site_id and lat and lon and _is_point(element):
            sites[site_id] = Site(
                site_id=site_id,
                lat=float(lat),
                lon=float(lon),
                name=_text(element, "measurementSiteName"),
                equipment=_text(element, "measurementEquipmentTypeUsed"),
                computation_method=_text(element, "computationMethod"),
                frc=_text(element, "openlrFunctionalRoadClass"),
                flow_index=indices.get("trafficFlow"),
                speed_index=indices.get("trafficSpeed"),
            )
        element.clear()

    return SiteTable(version=version, sites=sites)


def parse_measurements(source, table: SiteTable) -> list[dict]:
    """Parse a MeasuredDataPublication into one row per site."""
    rows: list[dict] = []

    for _, element in ET.iterparse(source, events=("end",)):
        if _local(element.tag) == "measurementSiteTableReference":
            published = element.get("version", "")
            if published != table.version:
                raise ValueError(
                    f"measurements reference site table version {published!r} but the "
                    f"parsed table is version {table.version!r}; measurement indices "
                    f"are table-specific and would be mis-resolved"
                )
            continue
        if _local(element.tag) != "siteMeasurements":
            continue

        reference = _find(element, "measurementSiteReference")
        site = table.sites.get(reference.get("id")) if reference is not None else None
        if site is None:
            element.clear()
            continue

        indexed = {
            value.get("index"): value
            for value in element
            if _local(value.tag) == "measuredValue"
        }
        speed = indexed.get(site.speed_index)
        flow = indexed.get(site.flow_index)
        average = _find(speed, "averageVehicleSpeed") if speed is not None else None

        rows.append({
            "source": "ndw",
            "country": "NL",
            "segment_id": site.site_id,
            "ts_utc": _timestamp(_text(element, "measurementTimeDefault")),
            "speed": _reading(speed, "speed"),
            "flow": _reading(flow, "vehicleFlowRate"),
            "quality_weight": (
                _measured(average.get("numberOfInputValuesUsed"))
                if average is not None else None
            ),
            "frc": site.frc,
            "lat": site.lat,
            "lon": site.lon,
            "computation_method": site.computation_method,
            "equipment": site.equipment,
        })
        element.clear()

    return rows
