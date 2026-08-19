"""Tests for the NDW travel-time adapter.

Structure verified against the live feed, 2026-08-19: 80,709 sections, one
DATEX II TravelTimeData value each, with a static reference nested inside
measuredValueExtension.
"""
import io

from raincheck.travel_time import parse_sections, parse_travel_times

D2 = 'xmlns="http://datex2.eu/schema/2/2_0"'
XSI = 'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'


def travel_time_element(duration: str, inputs: str = "99",
                        error: bool = False) -> str:
    flag = "<dataError>true</dataError>" if error else ""
    return (
        f'<travelTime accuracy="100.0" numberOfInputValuesUsed="{inputs}"'
        f' supplierCalculatedDataQuality="100.0">'
        f"{flag}<duration>{duration}</duration></travelTime>"
    )


def measurements(blocks: str) -> io.BytesIO:
    return io.BytesIO(
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<d2LogicalModel {D2} {XSI}><payloadPublication '
        f'xsi:type="MeasuredDataPublication">'
        f"{blocks}</payloadPublication></d2LogicalModel>".encode()
    )


def section_block(section_id: str, measured: str, reference: str | None,
                  time: str = "2026-08-19T10:12:00Z") -> str:
    extension = (
        f"<measuredValueExtension><measuredValueExtended>"
        f"<basicDataReferenceValue>"
        f"<referenceValueType>staticReferenceValue</referenceValueType>"
        f"<travelTimeData>{reference}</travelTimeData>"
        f"</basicDataReferenceValue>"
        f"</measuredValueExtended></measuredValueExtension>"
    ) if reference else ""
    return (
        f"<siteMeasurements>"
        f'<measurementSiteReference id="{section_id}" version="41"/>'
        f"<measurementTimeDefault>{time}</measurementTimeDefault>"
        f'<measuredValue index="1"><measuredValue>'
        f'<basicData xsi:type="TravelTimeData">'
        f"<travelTimeType>reconstituted</travelTimeType>{measured}"
        f"</basicData>{extension}"
        f"</measuredValue></measuredValue></siteMeasurements>"
    )


def test_the_reference_value_is_not_mistaken_for_the_measurement():
    # Both are <travelTime> elements with identical shape; only nesting inside
    # measuredValueExtension distinguishes them. A depth-first search for the
    # first <travelTime> would work by luck on document order and break silently
    # if it ever changed - and swapping them inverts every delay ratio.
    xml = measurements(section_block(
        "S1", travel_time_element("24.0"), travel_time_element("20.0")))

    rows = parse_travel_times(xml)

    assert len(rows) == 1
    assert rows[0]["duration_s"] == 24.0
    assert rows[0]["reference_duration_s"] == 20.0
    assert rows[0]["section_id"] == "S1"
    assert rows[0]["travel_time_type"] == "reconstituted"


def test_the_minus_one_sentinel_and_dataError_null_both_values():
    # Fifth instance of this pattern in the project, after NDW speed's -1, KNMI's
    # 65534/65535 and the absent sample-size attribute.
    xml = measurements(section_block(
        "S1",
        travel_time_element("-1.0", inputs="0", error=True),
        travel_time_element("-1.0", inputs="0", error=True)))

    rows = parse_travel_times(xml)

    assert rows[0]["duration_s"] is None
    assert rows[0]["reference_duration_s"] is None


def test_a_section_without_a_reference_still_yields_its_measurement():
    xml = measurements(section_block("S1", travel_time_element("24.0"), None))

    rows = parse_travel_times(xml)

    assert rows[0]["duration_s"] == 24.0
    assert rows[0]["reference_duration_s"] is None


def site_table(records: str, version: str = "1727") -> io.BytesIO:
    return io.BytesIO(
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<d2LogicalModel {D2} {XSI}><payloadPublication '
        f'xsi:type="MeasurementSiteTablePublication">'
        f'<measurementSiteTable id="NDW01_MT" version="{version}">{records}'
        f"</measurementSiteTable></payloadPublication></d2LogicalModel>".encode()
    )


def itinerary_record(section_id: str, lengths: list[float],
                     equipment: str = "fcd") -> str:
    links = "".join(
        f'<locationContainedInItinerary index="{i}">'
        f'<location xsi:type="Linear"><locationForDisplay>'
        f"<latitude>{51.58 + i / 100}</latitude>"
        f"<longitude>{4.78 + i / 100}</longitude>"
        f"</locationForDisplay>"
        f"<lengthAffected>{length}</lengthAffected>"
        f"</location></locationContainedInItinerary>"
        for i, length in enumerate(lengths)
    )
    return (
        f'<measurementSiteRecord id="{section_id}" version="41">'
        f"<measurementEquipmentTypeUsed><values>"
        f'<value lang="nl">{equipment}</value>'
        f"</values></measurementEquipmentTypeUsed>"
        f'<measurementSiteLocation xsi:type="ItineraryByIndexedLocations">'
        f"{links}</measurementSiteLocation></measurementSiteRecord>"
    )


def point_record(site_id: str) -> str:
    return (
        f'<measurementSiteRecord id="{site_id}" version="1">'
        f'<measurementSiteLocation xsi:type="Point"><pointExtension>'
        f"<openlrCoordinate><latitude>52.0</latitude><longitude>4.0</longitude>"
        f"</openlrCoordinate></pointExtension></measurementSiteLocation>"
        f"</measurementSiteRecord>"
    )


def test_section_length_is_the_sum_of_its_itinerary_links():
    # A section is a chain of Linear locations, each with its own lengthAffected.
    # Taking only the first gives 290 m where the section is 445 m - and since
    # speed is length/time, a truncated length understates speed by the same
    # proportion. Point records must not appear here: they are the 20,519 loop
    # detectors, a different measurement layer entirely.
    xml = site_table(itinerary_record("S1", [290.308, 154.7])
                     + point_record("PZH01_MST_0029-00"))

    table = parse_sections(xml)

    assert list(table) == ["S1"]
    assert round(table["S1"].length_m, 3) == 445.008
    assert table["S1"].equipment == "fcd"
    assert table["S1"].n_links == 2


def test_loop_derived_sections_are_identifiable_so_they_can_be_excluded():
    # 7,373 of 80,709 sections are computed from the same loops as the speed
    # feed. Treating those as independent corroboration double-counts.
    xml = site_table(itinerary_record("FCD", [100.0])
                     + itinerary_record("LOOP", [100.0], equipment="lus"))

    table = parse_sections(xml)

    assert table["FCD"].is_loop_derived is False
    assert table["LOOP"].is_loop_derived is True
