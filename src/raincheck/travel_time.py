"""NDW travel-time adapter: DATEX II TravelTimeData to the canonical schema.

Travel time is a second, largely **independent** measurement of the same network:
90.5% of the 80,709 sections are floating car data, 9.1% are loop-derived and
0.4% are number-plate recognition. Because section length is published, it
converts exactly to a space-mean speed - see ``raincheck.monitor``.

Sections whose ``equipment`` is ``lus`` are derived from the same loops as the
speed feed. They must be excluded, or flagged, whenever travel time is treated as
independent evidence, or the two series double-count.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass

from raincheck.ndw import _find, _local, _text, _timestamp

MISSING_SENTINEL = -1.0

# Section equipment that is *not* independent of the loop-detector speed feed.
LOOP_DERIVED_EQUIPMENT = "lus"


def _duration(node) -> float | None:
    """Duration in seconds, or None when absent, flagged or sentinel."""
    if node is None:
        return None
    if (_text(node, "dataError") or "").lower() == "true":
        return None
    value = _text(node, "duration")
    if value is None:
        return None
    seconds = float(value)
    return None if seconds <= 0 or seconds == MISSING_SENTINEL else seconds


def _travel_times(block):
    """The measured and reference ``travelTime`` elements of one section.

    Both elements are identically shaped; only nesting inside
    ``measuredValueExtension`` distinguishes the static reference from the
    measurement. Taking the first match in document order would work by luck and
    fail silently if the order ever changed - and swapping the two inverts every
    delay ratio.
    """
    extension = next(
        (e for e in block.iter() if _local(e.tag) == "measuredValueExtension"), None)
    reference_ids = {id(e) for e in extension.iter()} if extension is not None else set()

    measured = reference = None
    for element in block.iter():
        if _local(element.tag) != "travelTime":
            continue
        if id(element) in reference_ids:
            reference = element
        else:
            measured = element
    return measured, reference


def parse_travel_times(source) -> list[dict]:
    """Parse a TravelTimeData publication into one row per section."""
    rows: list[dict] = []

    for _, element in ET.iterparse(source, events=("end",)):
        if _local(element.tag) != "siteMeasurements":
            continue

        reference_node = _find(element, "measurementSiteReference")
        section_id = reference_node.get("id") if reference_node is not None else None
        measured, static = _travel_times(element)

        rows.append({
            "source": "ndw",
            "country": "NL",
            "section_id": section_id,
            "ts_utc": _timestamp(_text(element, "measurementTimeDefault")),
            "duration_s": _duration(measured),
            "reference_duration_s": _duration(static),
            "travel_time_type": _text(element, "travelTimeType"),
            "support": (
                float(measured.get("numberOfInputValuesUsed"))
                if measured is not None
                and measured.get("numberOfInputValuesUsed") is not None
                else None
            ),
        })
        element.clear()

    return rows


XSI_TYPE = "{http://www.w3.org/2001/XMLSchema-instance}type"


@dataclass(frozen=True)
class Section:
    """One travel-time section: a chain of links with a published total length."""

    section_id: str
    length_m: float
    equipment: str | None
    lat: float | None
    lon: float | None
    n_links: int

    @property
    def is_loop_derived(self) -> bool:
        """True when this section comes from the same loops as the speed feed."""
        return self.equipment == LOOP_DERIVED_EQUIPMENT


def parse_sections(source) -> dict[str, Section]:
    """Parse ItineraryByIndexedLocations records from the measurement site table.

    Length is the **sum** over the itinerary's links: a section is a chain of
    Linear locations, each carrying its own ``lengthAffected``. Taking only the
    first gives 290 m where the section is 445 m, and since speed is length over
    time, a truncated length understates speed by exactly that proportion.

    Point records are skipped - those are the 20,519 loop detectors, parsed by
    ``raincheck.ndw`` as a different measurement layer.
    """
    sections: dict[str, Section] = {}

    for _, element in ET.iterparse(source, events=("end",)):
        if _local(element.tag) != "measurementSiteRecord":
            continue

        location = _find(element, "measurementSiteLocation")
        if location is None or location.get(XSI_TYPE) != "ItineraryByIndexedLocations":
            element.clear()
            continue

        lengths = [float(e.text) for e in element.iter()
                   if _local(e.tag) == "lengthAffected" and e.text]
        section_id = element.get("id")
        if section_id:
            sections[section_id] = Section(
                section_id=section_id,
                length_m=sum(lengths),
                equipment=_text(element, "measurementEquipmentTypeUsed"),
                lat=float(lat) if (lat := _text(element, "latitude")) else None,
                lon=float(lon) if (lon := _text(element, "longitude")) else None,
                n_links=len(lengths),
            )
        element.clear()

    return sections
