"""Tests for the NDW DATEX II adapter.

Fixtures mirror the real structure verified in reports/phase1_nl_audit.md:
a single default namespace (http://datex2.eu/schema/2/2_0), coordinates nested
under pointExtension/openlrExtendedPoint, and per-site measurement indices.
"""
import datetime as dt
import io

import pytest

from raincheck.ndw import parse_measurements, parse_site_table

D2 = 'xmlns="http://datex2.eu/schema/2/2_0"'
XSI = 'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'


def site_table(records: str, version: str = "1727") -> io.BytesIO:
    return io.BytesIO(
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<d2LogicalModel {D2} {XSI}><payloadPublication '
        f'xsi:type="MeasurementSiteTablePublication">'
        f'<measurementSiteTable id="NDW01_MT" version="{version}">'
        f"{records}"
        f"</measurementSiteTable></payloadPublication></d2LogicalModel>".encode()
    )


def characteristic(index: str, value_type: str, lane: str = "lane1",
                   vehicle: str = "<vehicleType>anyVehicle</vehicleType>") -> str:
    return (
        f'<measurementSpecificCharacteristics index="{index}">'
        f"<measurementSpecificCharacteristics>"
        f"<accuracy>95</accuracy><period>60</period>"
        f"<specificLane>{lane}</specificLane>"
        f"<specificMeasurementValueType>{value_type}</specificMeasurementValueType>"
        f"<specificVehicleCharacteristics>{vehicle}</specificVehicleCharacteristics>"
        f"</measurementSpecificCharacteristics>"
        f"</measurementSpecificCharacteristics>"
    )


LENGTH_CLASS = (
    "<lengthCharacteristic>"
    "<comparisonOperator>lessThanOrEqualTo</comparisonOperator>"
    "<vehicleLength>5.6</vehicleLength>"
    "</lengthCharacteristic>"
)


def point_record(site_id: str, lat: str, lon: str, characteristics: str,
                 name: str = "N207 km 18.768 Re", equipment: str = "lus",
                 method: str = "arithmeticAverageOfSamplesInATimePeriod",
                 frc: str = "FRC3") -> str:
    frc_xml = (
        f"<openlrLineAttributes>"
        f"<openlrFunctionalRoadClass>{frc}</openlrFunctionalRoadClass>"
        f"</openlrLineAttributes>"
    ) if frc else ""
    return (
        f'<measurementSiteRecord id="{site_id}" version="13">'
        f"<computationMethod>{method}</computationMethod>"
        f"<measurementEquipmentTypeUsed><values>"
        f'<value lang="nl">{equipment}</value>'
        f"</values></measurementEquipmentTypeUsed>"
        f"<measurementSiteName><values>"
        f'<value lang="nl">{name}</value>'
        f"</values></measurementSiteName>"
        f"<measurementSiteNumberOfLanes>1</measurementSiteNumberOfLanes>"
        f"{characteristics}"
        f'<measurementSiteLocation xsi:type="Point"><pointExtension>'
        f"<openlrExtendedPoint><openlrPointLocationReference>"
        f"<openlrPointAlongLine><openlrLocationReferencePoint>"
        f"<openlrCoordinate>"
        f"<latitude>{lat}</latitude><longitude>{lon}</longitude>"
        f"</openlrCoordinate>{frc_xml}"
        f"</openlrLocationReferencePoint></openlrPointAlongLine>"
        f"</openlrPointLocationReference></openlrExtendedPoint>"
        f"</pointExtension></measurementSiteLocation>"
        f"</measurementSiteRecord>"
    )


def measured_value(index: str, kind: str, value: str, inputs: str = "12") -> str:
    if kind == "TrafficFlow":
        body = f"<vehicleFlow><vehicleFlowRate>{value}</vehicleFlowRate></vehicleFlow>"
    else:
        body = (
            f'<averageVehicleSpeed numberOfInputValuesUsed="{inputs}">'
            f"<speed>{value}</speed></averageVehicleSpeed>"
        )
    return (
        f'<measuredValue index="{index}"><measuredValue>'
        f'<basicData xsi:type="{kind}">{body}</basicData>'
        f"</measuredValue></measuredValue>"
    )


def measurements(site_blocks: str, version: str = "1727") -> io.BytesIO:
    return io.BytesIO(
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f'<d2LogicalModel {D2} {XSI}><payloadPublication '
        f'xsi:type="MeasuredDataPublication">'
        f"<publicationTime>2026-08-19T08:33:42.013Z</publicationTime>"
        f'<measurementSiteTableReference id="NDW01_MT" version="{version}"/>'
        f"{site_blocks}"
        f"</payloadPublication></d2LogicalModel>".encode()
    )


def site_block(site_id: str, values: str,
               time: str = "2026-08-19T08:32:00Z") -> str:
    return (
        f"<siteMeasurements>"
        f'<measurementSiteReference id="{site_id}" version="13"/>'
        f"<measurementTimeDefault>{time}</measurementTimeDefault>"
        f"{values}</siteMeasurements>"
    )


def one_site() -> io.BytesIO:
    """A fresh single-site table. Must be a function: BytesIO is consumed once."""
    return site_table(point_record(
        "A", "52.0", "4.0",
        characteristic("2", "trafficFlow") + characteristic("4", "trafficSpeed"),
    ))


def test_site_table_extracts_identity_location_and_equipment():
    # measurementEquipmentTypeUsed and measurementSiteName wrap their content in
    # <values><value lang="nl">, so a naive .text read returns None.
    xml = site_table(point_record(
        "PZH01_MST_0029-00", "52.0052032", "4.68347263",
        characteristic("1", "trafficFlow") + characteristic("2", "trafficSpeed"),
    ))

    table = parse_site_table(xml)

    site = table.sites["PZH01_MST_0029-00"]
    assert (site.lat, site.lon) == (52.0052032, 4.68347263)
    assert site.name == "N207 km 18.768 Re"
    assert site.equipment == "lus"
    assert site.computation_method == "arithmeticAverageOfSamplesInATimePeriod"
    assert site.frc == "FRC3"


def test_site_table_resolves_the_anyVehicle_indices_per_site():
    # The anyVehicle index is NOT fixed: it depends on lane count x vehicle-class
    # count, and was observed at 4, 8 and 16 across real sites. Here the
    # length-classed speed sits at index 3 and the aggregate at index 4, so any
    # implementation that takes the first speed index reads a lorry-only lane.
    xml = site_table(
        point_record(
            "A", "52.0", "4.0",
            characteristic("1", "trafficFlow", vehicle=LENGTH_CLASS)
            + characteristic("2", "trafficFlow")
            + characteristic("3", "trafficSpeed", vehicle=LENGTH_CLASS)
            + characteristic("4", "trafficSpeed"),
        )
        + point_record(
            "B", "52.1", "4.1",
            characteristic("7", "trafficSpeed") + characteristic("9", "trafficFlow"),
        )
    )

    table = parse_site_table(xml)

    assert (table.sites["A"].flow_index, table.sites["A"].speed_index) == ("2", "4")
    assert (table.sites["B"].flow_index, table.sites["B"].speed_index) == ("9", "7")


def test_minus_one_speed_is_null_not_a_measurement():
    # NDW encodes "no measurement" as -1 in a numeric field. Left as a number it
    # silently drags every mean downwards, so it must become None at parse time.
    table = parse_site_table(one_site())
    xml = measurements(site_block("A", measured_value("4", "TrafficSpeed", "-1")))

    rows = parse_measurements(xml, table)

    assert len(rows) == 1
    assert rows[0]["speed"] is None


def test_canonical_row_carries_identity_time_and_quality():
    # Index 3 is a length-classed speed decoy at an implausible value; the
    # aggregate anyVehicle speed is index 4. numberOfInputValuesUsed becomes
    # quality_weight, replacing UTD19's keep/drop error flag with a continuous
    # weight usable directly as GBTRegressor.weightCol.
    table = parse_site_table(site_table(point_record(
        "PZH01_MST_0029-00", "52.0052032", "4.68347263",
        characteristic("2", "trafficFlow")
        + characteristic("3", "trafficSpeed", vehicle=LENGTH_CLASS)
        + characteristic("4", "trafficSpeed"),
    )))
    xml = measurements(site_block(
        "PZH01_MST_0029-00",
        measured_value("2", "TrafficFlow", "780")
        + measured_value("3", "TrafficSpeed", "20")
        + measured_value("4", "TrafficSpeed", "87", inputs="12"),
    ))

    rows = parse_measurements(xml, table)

    assert rows == [{
        "source": "ndw",
        "country": "NL",
        "segment_id": "PZH01_MST_0029-00",
        "ts_utc": dt.datetime(2026, 8, 19, 8, 32),
        "speed": 87.0,
        "flow": 780.0,
        "quality_weight": 12.0,
        "frc": "FRC3",
        "lat": 52.0052032,
        "lon": 4.68347263,
        "computation_method": "arithmeticAverageOfSamplesInATimePeriod",
        "equipment": "lus",
    }]


def test_site_table_version_mismatch_is_refused():
    # Indices are resolved from the site table, so pairing measurements with a
    # different table version can silently re-map a lorry lane onto the
    # aggregate speed column. That must fail loudly, not quietly mis-parse.
    table = parse_site_table(one_site())                      # version 1727
    xml = measurements(site_block("A", measured_value("4", "TrafficSpeed", "87")),
                       version="1728")

    with pytest.raises(ValueError, match="1728"):
        parse_measurements(xml, table)


def test_dataError_nulls_an_otherwise_plausible_zero():
    # Observed in the live feed: <dataError>true</dataError> accompanied by
    # <vehicleFlowRate>0</vehicleFlowRate>. Unlike -1, a zero is a perfectly
    # legal flow, so only the flag distinguishes "no traffic" from "broken loop".
    table = parse_site_table(one_site())
    xml = measurements(site_block("A", (
        '<measuredValue index="2"><measuredValue>'
        '<basicData xsi:type="TrafficFlow">'
        '<vehicleFlow supplierCalculatedDataQuality="0">'
        "<dataError>true</dataError><vehicleFlowRate>0</vehicleFlowRate>"
        "</vehicleFlow></basicData></measuredValue></measuredValue>"
    )))

    rows = parse_measurements(xml, table)

    assert rows[0]["flow"] is None


def test_only_point_locations_are_treated_as_detector_sites():
    # The real table holds 20,519 Point records and 80,709
    # ItineraryByIndexedLocations (travel-time sections). Itineraries also carry
    # openlrCoordinate elements, so filtering on "has coordinates" admits them
    # and inflates the table five-fold, risking id collisions on lookup.
    itinerary = (
        '<measurementSiteRecord id="SECTION_1" version="1">'
        '<measurementSiteLocation xsi:type="ItineraryByIndexedLocations">'
        "<openlrCoordinate>"
        "<latitude>52.5</latitude><longitude>4.5</longitude>"
        "</openlrCoordinate>"
        "</measurementSiteLocation></measurementSiteRecord>"
    )
    xml = site_table(point_record(
        "A", "52.0", "4.0", characteristic("2", "trafficFlow")) + itinerary)

    table = parse_site_table(xml)

    assert list(table.sites) == ["A"]
