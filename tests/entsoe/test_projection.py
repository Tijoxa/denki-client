from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from denki_client.entsoe._projection import PROJECTION_SCHEMA, project_documents

NAMESPACE = "urn:iec62325:351:tc57wg16:451-3:publicationdocument:7:3"


def element(
    name: str,
    text: str | None = None,
    *children: dict[str, Any],
    attributes: list[dict[str, str | None]] | None = None,
    namespace: str | None = NAMESPACE,
) -> dict[str, Any]:
    return {
        "name": name,
        "namespace": namespace,
        "text": text,
        "attributes": attributes or [],
        "children": list(children),
    }


def qualified_path(*names: str) -> str:
    return "/".join(f"{{{NAMESPACE}}}{name}" for name in names)


def period(start: str, end: str, resolution: str, curve_type: str, *points: dict[str, Any]) -> dict[str, Any]:
    return element(
        "Period",
        None,
        element("timeInterval", None, element("start", start), element("end", end)),
        element("resolution", resolution),
        element("curveType", curve_type),
        *points,
    )


def publication_document(*series: dict[str, Any]) -> dict[str, Any]:
    return element(
        "Publication_MarketDocument",
        None,
        element("mRID", "document-1"),
        element("revisionNumber", "1"),
        element("type", "A44"),
        element("createdDateTime", "2025-01-01T00:00:00Z"),
        *series,
    )


def time_series(mrid: str, *children: dict[str, Any]) -> dict[str, Any]:
    return element(
        "TimeSeries",
        None,
        element("mRID", mrid),
        element(
            "in_Domain.mRID",
            "10YBE----------2",
            attributes=[{"name": "codingScheme", "namespace": None, "value": "A01"}],
        ),
        element("out_Domain.mRID", "10YNL----------L"),
        element("currency_Unit.name", "EUR"),
        element("price_Measure_Unit.name", "MWH"),
        *children,
    )


def test_a01_uses_one_interval_per_point_and_resets_each_period() -> None:
    document = publication_document(
        time_series(
            "series-1",
            period(
                "2025-01-01T00:00:00Z",
                "2025-01-01T01:00:00Z",
                "PT15M",
                "A01",
                element("Point", None, element("position", "1"), element("quantity", "1.25")),
                element("Point", None, element("position", "3"), element("quantity", "2.5")),
            ),
            period(
                "2025-01-01T01:00:00Z",
                "2025-01-01T01:30:00Z",
                "PT15M",
                "A01",
                element("Point", None, element("position", "1"), element("quantity", "3.75")),
            ),
        )
    )

    frame = project_documents([(None, document)])

    assert frame.schema == PROJECTION_SCHEMA
    assert frame.get_column("timestamp").to_list() == [
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 30, tzinfo=UTC),
        datetime(2025, 1, 1, 1, 0, tzinfo=UTC),
    ]
    assert frame.get_column("timestamp_end").to_list() == [
        datetime(2025, 1, 1, 0, 15, tzinfo=UTC),
        datetime(2025, 1, 1, 0, 45, tzinfo=UTC),
        datetime(2025, 1, 1, 1, 15, tzinfo=UTC),
    ]
    assert frame.get_column("quantity").to_list() == [1.25, 2.5, 3.75]
    assert frame.get_column("in_Domain.mRID").to_list() == ["10YBE----------2"] * 3
    assert frame.get_column("in_Domain.codingScheme").to_list() == ["A01"] * 3
    assert frame.get_column("currency_Unit.name").to_list() == ["EUR"] * 3


def test_a03_points_mark_block_starts_without_filling_gaps() -> None:
    document = publication_document(
        time_series(
            "series-1",
            period(
                "2025-03-30T00:00:00Z",
                "2025-03-30T04:00:00Z",
                "PT1H",
                "A03",
                element("Point", None, element("position", "1"), element("price.amount", "40.0")),
                element("Point", None, element("position", "3"), element("price.amount", "45.0")),
            ),
        )
    )

    frame = project_documents([(None, document)])

    assert frame.height == 2
    assert frame.get_column("timestamp").to_list() == [
        datetime(2025, 3, 30, 0, 0, tzinfo=UTC),
        datetime(2025, 3, 30, 2, 0, tzinfo=UTC),
    ]
    assert frame.get_column("timestamp_end").to_list() == [
        datetime(2025, 3, 30, 2, 0, tzinfo=UTC),
        datetime(2025, 3, 30, 4, 0, tzinfo=UTC),
    ]
    assert frame.get_column("price.amount").to_list() == [40.0, 45.0]


def test_point_value_variants_keep_source_columns_and_common_aliases() -> None:
    document = publication_document(
        time_series(
            "series-1",
            period(
                "2025-01-01T00:00:00Z",
                "2025-01-01T01:00:00Z",
                "PT1H",
                "A01",
                element(
                    "Point",
                    None,
                    element("position", "1"),
                    element("quantity.quantity", "12"),
                    element("secondaryQuantity", "3"),
                    element("unavailable_Quantity.quantity", "7"),
                    element("procurement_Price.amount", "5"),
                    element("congestionCost_Price.amount", "6"),
                    element("energy_Price.amount", "7"),
                    element("imbalance_Price.amount", "8"),
                    element("imbalance_Price.category", "A01"),
                    element("Financial_Price", None, element("amount", "9")),
                    element("Reason", None, element("code", "A01"), element("text", "Missing")),
                ),
            ),
        )
    )

    frame = project_documents([(None, document)])

    assert frame.get_column("quantity").item() == 12.0
    assert frame.get_column("quantity.quantity").item() == 12.0
    assert frame.get_column("secondaryQuantity").item() == 3.0
    assert frame.get_column("unavailable_Quantity.quantity").item() == 7.0
    assert frame.get_column("price.amount").item() == 5.0
    assert frame.get_column("procurement_Price.amount").item() == 5.0
    assert frame.get_column("congestionCost_Price.amount").item() == 6.0
    assert frame.get_column("energy_Price.amount").item() == 7.0
    assert frame.get_column("imbalance_Price.amount").item() == 8.0
    assert frame.get_column("imbalance_Price.category").item() == "A01"
    assert frame.get_column("Financial_Price.amount").item() == 9.0
    assert frame.get_column("point_reasons").to_list()[0] == [{"code": "A01", "text": "Missing"}]


def test_independent_repeated_flow_based_groups_remain_in_xml_extra() -> None:
    series = time_series(
        "flow-series",
        period(
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "PT1H",
            "A01",
            element(
                "Point",
                None,
                element("position", "1"),
                element(
                    "Constraint_Series",
                    None,
                    element("mRID", "constraint-1"),
                    element("businessType", "B40"),
                    element("Contingency_Series", None, element("mRID", "contingency-1")),
                    element("Monitored_Series", None, element("mRID", "monitored-1")),
                ),
                element(
                    "Constraint_Series",
                    None,
                    element("mRID", "constraint-2"),
                    element("businessType", "B40"),
                    element("Contingency_Series", None, element("mRID", "contingency-2")),
                    element("Monitored_Series", None, element("mRID", "monitored-2")),
                ),
            ),
        ),
    )
    document = element(
        "CriticalNetworkElement_MarketDocument",
        None,
        element("mRID", "flow-document"),
        element("RegisteredResource", None, element("mRID", "resource-1")),
        element("RegisteredResource", None, element("mRID", "resource-2")),
        series,
    )

    frame = project_documents([("flow-based.xml", document)])

    assert frame.height == 1
    assert frame.get_column("document_name").item() == "flow-based.xml"
    extras = frame.get_column("xml_extra").to_list()[0]
    values_by_path = {extra["path"]: extra["value"] for extra in extras}
    assert (
        values_by_path[qualified_path("CriticalNetworkElement_MarketDocument", "RegisteredResource[1]", "mRID")]
        == "resource-1"
    )
    assert (
        values_by_path[qualified_path("CriticalNetworkElement_MarketDocument", "RegisteredResource[2]", "mRID")]
        == "resource-2"
    )
    assert (
        values_by_path[
            qualified_path(
                "CriticalNetworkElement_MarketDocument", "TimeSeries", "Period", "Point", "Constraint_Series[1]", "mRID"
            )
        ]
        == "constraint-1"
    )
    assert (
        values_by_path[
            qualified_path(
                "CriticalNetworkElement_MarketDocument", "TimeSeries", "Period", "Point", "Constraint_Series[2]", "mRID"
            )
        ]
        == "constraint-2"
    )
    assert (
        values_by_path[
            qualified_path(
                "CriticalNetworkElement_MarketDocument",
                "TimeSeries",
                "Period",
                "Point",
                "Constraint_Series[1]",
                "Contingency_Series",
                "mRID",
            )
        ]
        == "contingency-1"
    )

    groups = frame.get_column("xml_groups").to_list()[0]
    group_paths = {group["path"] for group in groups}
    assert qualified_path("CriticalNetworkElement_MarketDocument", "RegisteredResource[1]") in group_paths
    assert qualified_path("CriticalNetworkElement_MarketDocument", "RegisteredResource[2]") in group_paths
    assert (
        qualified_path("CriticalNetworkElement_MarketDocument", "TimeSeries", "Period", "Point", "Constraint_Series[1]")
        in group_paths
    )
    assert (
        qualified_path("CriticalNetworkElement_MarketDocument", "TimeSeries", "Period", "Point", "Constraint_Series[2]")
        in group_paths
    )
    assert (
        qualified_path(
            "CriticalNetworkElement_MarketDocument",
            "TimeSeries",
            "Period",
            "Point",
            "Constraint_Series[1]",
            "Contingency_Series",
        )
        in group_paths
    )


def test_unavailability_projects_available_period_points_and_resource() -> None:
    document = element(
        "Unavailability_MarketDocument",
        None,
        element("mRID", "outage-document"),
        element(
            "unavailability_Time_Period.timeInterval",
            None,
            element("start", "2025-01-01T00:00:00Z"),
            element("end", "2025-01-01T04:00:00Z"),
        ),
        element(
            "TimeSeries",
            None,
            element("mRID", "outage-series"),
            element("start_DateAndOrTime.date", "2025-01-01"),
            element("start_DateAndOrTime.time", "00:00:00Z"),
            element("end_DateAndOrTime.date", "2025-01-01"),
            element("end_DateAndOrTime.time", "04:00:00Z"),
            element("production_RegisteredResource.mRID", "21W000000000999G"),
            element("production_RegisteredResource.pSRType.psrType", "B14"),
            element(
                "production_RegisteredResource.pSRType.powerSystemResources.mRID",
                "21W000000000999P",
                attributes=[{"name": "codingScheme", "namespace": None, "value": "A01"}],
            ),
            element("production_RegisteredResource.pSRType.powerSystemResources.name", "Generation unit"),
            element(
                "production_RegisteredResource.pSRType.powerSystemResources.nominalP",
                "200",
                attributes=[{"name": "unit", "namespace": None, "value": "MAW"}],
            ),
            element("quantity_Measure_Unit.name", "MAW"),
            element(
                "Available_Period",
                None,
                element(
                    "timeInterval",
                    None,
                    element("start", "2025-01-01T00:00:00Z"),
                    element("end", "2025-01-01T02:00:00Z"),
                ),
                element("resolution", "PT1H"),
                element("Point", None, element("position", "1"), element("quantity", "80")),
                element("Point", None, element("position", "2"), element("quantity", "70")),
            ),
            element(
                "Available_Period",
                None,
                element(
                    "timeInterval",
                    None,
                    element("start", "2025-01-01T02:00:00Z"),
                    element("end", "2025-01-01T04:00:00Z"),
                ),
                element("resolution", "PT1H"),
                element("Point", None, element("position", "1"), element("quantity", "0")),
            ),
        ),
    )

    frame = project_documents([("outage.xml", document)])

    assert frame.height == 3
    assert frame.get_column("row_kind").to_list() == ["point", "point", "point"]
    assert frame.get_column("period_start").to_list() == [
        datetime(2025, 1, 1, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 2, tzinfo=UTC),
    ]
    assert frame.get_column("timestamp").to_list() == [
        datetime(2025, 1, 1, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 1, tzinfo=UTC),
        datetime(2025, 1, 1, 2, tzinfo=UTC),
    ]
    assert frame.get_column("quantity").to_list() == [80.0, 70.0, 0.0]
    assert frame.get_column("registeredResource.mRID").to_list() == ["21W000000000999G"] * 3
    assert frame.get_column("PowerSystemResources.mRID").to_list() == ["21W000000000999P"] * 3
    assert frame.get_column("PowerSystemResources.codingScheme").to_list() == ["A01"] * 3
    assert frame.get_column("PowerSystemResources.name").to_list() == ["Generation unit"] * 3
    assert frame.get_column("PowerSystemResources.nominalP").to_list() == [200.0] * 3
    assert frame.get_column("PowerSystemResources.nominalP.unit").to_list() == ["MAW"] * 3
    assert frame.get_column("psrType").to_list() == ["B14"] * 3
    assert frame.get_column("document_start").to_list() == [datetime(2025, 1, 1, tzinfo=UTC)] * 3
    assert frame.get_column("series_start").to_list() == [datetime(2025, 1, 1, tzinfo=UTC)] * 3
    assert frame.get_column("quantity_Measure_Unit.name").to_list() == ["MAW"] * 3
    assert frame.get_column("period_end").to_list() == [
        datetime(2025, 1, 1, 2, tzinfo=UTC),
        datetime(2025, 1, 1, 2, tzinfo=UTC),
        datetime(2025, 1, 1, 4, tzinfo=UTC),
    ]


def test_master_and_omi_documents_have_stable_rows_and_reasons() -> None:
    master = element(
        "Configuration_MarketDocument",
        None,
        element(
            "TimeSeries",
            None,
            element("mRID", "unit-1"),
            element(
                "registeredResource.mRID",
                "eic-1",
                attributes=[{"name": "codingScheme", "namespace": None, "value": "A01"}],
            ),
            element("registeredResource.name", "Unit one"),
            element("registeredResource.location.name", "Offshore site"),
            element("implementation_DateAndOrTime.date", "2017-01-01"),
            element(
                "ControlArea_Domain",
                None,
                element(
                    "mRID", "control-eic", attributes=[{"name": "codingScheme", "namespace": None, "value": "A01"}]
                ),
            ),
            element(
                "MktPSRType",
                None,
                element("psrType", "B14"),
                element(
                    "GeneratingUnit_PowerSystemResources",
                    None,
                    element(
                        "mRID", "power-eic", attributes=[{"name": "codingScheme", "namespace": None, "value": "A01"}]
                    ),
                ),
                element(
                    "nominalIP_PowerSystemResources.nominalP",
                    "200",
                    attributes=[{"name": "unit", "namespace": None, "value": "MAW"}],
                ),
            ),
        ),
        element("TimeSeries", None, element("mRID", "unit-2")),
    )
    omi = element(
        "OtherTransparencyMarketInformation_MarketDocument",
        None,
        element("mRID", "omi-document"),
        element("publication_DateAndOrTime.dateTime", "2025-01-01T00:00:00Z"),
        element("start_DateAndOrTime.dateTime", "2024-12-31T22:00:00Z"),
        element("end_DateAndOrTime.dateTime", "2025-01-01T02:00:00Z"),
        element("reason.code", "A01"),
        element("reason.text", "Information"),
    )

    master_frame = project_documents([("units.xml", master)])
    omi_frame = project_documents([("omi.xml", omi)])
    empty_frame = project_documents([])

    assert master_frame.height == 2
    assert master_frame.get_column("row_kind").to_list() == ["series", "series"]
    assert master_frame.get_column("series_mRID").to_list() == ["unit-1", "unit-2"]
    assert master_frame.get_column("document_name").to_list() == ["units.xml", "units.xml"]
    assert master_frame.get_column("registeredResource.mRID").to_list()[0] == "eic-1"
    assert master_frame.get_column("registeredResource.codingScheme").to_list()[0] == "A01"
    assert master_frame.get_column("psrType").to_list()[0] == "B14"
    assert master_frame.get_column("PowerSystemResources.mRID").to_list()[0] == "power-eic"
    assert master_frame.get_column("GeneratingUnit_PowerSystemResources.mRID").to_list()[0] == "power-eic"
    assert master_frame.get_column("controlArea_Domain.mRID").to_list()[0] == "control-eic"
    assert master_frame.get_column("controlArea_Domain.codingScheme").to_list()[0] == "A01"
    assert master_frame.get_column("registeredResource.location.name").to_list()[0] == "Offshore site"
    assert master_frame.get_column("implementation_date").to_list()[0].isoformat() == "2017-01-01"
    assert master_frame.get_column("nominalIP_PowerSystemResources.nominalP").to_list()[0] == 200.0
    assert master_frame.get_column("nominalIP_PowerSystemResources.nominalP.unit").to_list()[0] == "MAW"
    assert omi_frame.height == 1
    assert omi_frame.get_column("row_kind").item() == "document"
    assert omi_frame.get_column("publication_datetime").item() == datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    assert omi_frame.get_column("start_datetime").item() == datetime(2024, 12, 31, 22, 0, tzinfo=UTC)
    assert omi_frame.get_column("end_datetime").item() == datetime(2025, 1, 1, 2, 0, tzinfo=UTC)
    assert omi_frame.get_column("document_reasons").to_list()[0] == [
        {"code": "A01", "text": "Information"},
    ]
    assert omi_frame.get_column("xml_extra").to_list()[0][0]["path"] == qualified_path(
        "OtherTransparencyMarketInformation_MarketDocument", "publication_DateAndOrTime.dateTime"
    )
    assert empty_frame.schema == PROJECTION_SCHEMA
    assert empty_frame.height == 0
