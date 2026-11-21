"""Project parsed ENTSO-E XML documents into a stable, loss-aware table.

Rows follow the deepest publication grain. Time-series documents emit one row
per point, or one row per period/series when no point exists. Scalar fields
that do not have a dedicated column remain in ``xml_extra`` with their full
qualified path, sibling position, value, and attributes. Repeated XML groups therefore
remain independent and can be exploded without combining their members.
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Any

import polars as pl
from dateutil.relativedelta import relativedelta

_ATTRIBUTE_SCHEMA = pl.Struct(
    {
        "name": pl.String,
        "namespace": pl.String,
        "value": pl.String,
    }
)
_XML_EXTRA_SCHEMA = pl.List(
    pl.Struct(
        {
            "path": pl.String,
            "position": pl.UInt32,
            "value": pl.String,
            "attributes": pl.List(_ATTRIBUTE_SCHEMA),
        }
    )
)
_XML_GROUP_SCHEMA = pl.List(
    pl.Struct(
        {
            "path": pl.String,
            "position": pl.UInt32,
            "fields": _XML_EXTRA_SCHEMA,
        }
    )
)
_REASON_SCHEMA = pl.List(pl.Struct({"code": pl.String, "text": pl.String}))
_GROUP_NODE_NAMES = {
    "Available_Period",
    "Constraint_Series",
    "Contingency_Series",
    "Measurements",
    "Monitored_Series",
    "MktPSRType",
    "Period",
    "PowerSystemResources",
    "PTDF_Domain",
    "Reason",
    "RegisteredResource",
    "TimeSeries",
}

# One schema is used for every response family and every page. Fields specific
# to a document family are represented as named columns when they are common,
# with the remaining XML preserved in xml_extra.
PROJECTION_SCHEMA = pl.Schema(
    {
        "document_name": pl.String,
        "document_root": pl.String,
        "document_namespace": pl.String,
        "document_mRID": pl.String,
        "document_revisionNumber": pl.String,
        "document_type": pl.String,
        "document_processType": pl.String,
        "document_businessType": pl.String,
        "document_createdDateTime": pl.Datetime(time_zone="UTC"),
        "document_start": pl.Datetime(time_zone="UTC"),
        "document_end": pl.Datetime(time_zone="UTC"),
        "publication_datetime": pl.Datetime(time_zone="UTC"),
        "start_datetime": pl.Datetime(time_zone="UTC"),
        "end_datetime": pl.Datetime(time_zone="UTC"),
        "document_status": pl.String,
        "document_reasons": _REASON_SCHEMA,
        "row_kind": pl.String,
        "series_mRID": pl.String,
        "businessType": pl.String,
        "product": pl.String,
        "in_Domain.mRID": pl.String,
        "in_Domain.codingScheme": pl.String,
        "out_Domain.mRID": pl.String,
        "out_Domain.codingScheme": pl.String,
        "controlArea_Domain.mRID": pl.String,
        "controlArea_Domain.codingScheme": pl.String,
        "area_Domain.mRID": pl.String,
        "area_Domain.codingScheme": pl.String,
        "connecting_Domain.mRID": pl.String,
        "connecting_Domain.codingScheme": pl.String,
        "acquiring_Domain.mRID": pl.String,
        "acquiring_Domain.codingScheme": pl.String,
        "biddingZone_Domain.mRID": pl.String,
        "biddingZone_Domain.codingScheme": pl.String,
        "inBiddingZone_Domain.mRID": pl.String,
        "inBiddingZone_Domain.codingScheme": pl.String,
        "outBiddingZone_Domain.mRID": pl.String,
        "outBiddingZone_Domain.codingScheme": pl.String,
        "psrType": pl.String,
        "PowerSystemResources.mRID": pl.String,
        "PowerSystemResources.codingScheme": pl.String,
        "PowerSystemResources.name": pl.String,
        "PowerSystemResources.nominalP": pl.Float64,
        "PowerSystemResources.nominalP.unit": pl.String,
        "GeneratingUnit_PowerSystemResources.mRID": pl.String,
        "GeneratingUnit_PowerSystemResources.codingScheme": pl.String,
        "nominalIP_PowerSystemResources.nominalP": pl.Float64,
        "nominalIP_PowerSystemResources.nominalP.unit": pl.String,
        "objectAggregation": pl.String,
        "implementation_date": pl.Date,
        "series_start": pl.Datetime(time_zone="UTC"),
        "series_end": pl.Datetime(time_zone="UTC"),
        "flowDirection.direction": pl.String,
        "registeredResource.mRID": pl.String,
        "registeredResource.codingScheme": pl.String,
        "registeredResource.name": pl.String,
        "registeredResource.location.name": pl.String,
        "production_unit": pl.String,
        "period_start": pl.Datetime(time_zone="UTC"),
        "period_end": pl.Datetime(time_zone="UTC"),
        "timestamp": pl.Datetime(time_zone="UTC"),
        "timestamp_end": pl.Datetime(time_zone="UTC"),
        "resolution": pl.String,
        "curveType": pl.String,
        "point_position": pl.UInt32,
        "quantity": pl.Float64,
        "quantity.quantity": pl.Float64,
        "secondaryQuantity": pl.Float64,
        "unavailable_Quantity.quantity": pl.Float64,
        "price.amount": pl.Float64,
        "procurement_price.amount": pl.Float64,
        "procurement_Price.amount": pl.Float64,
        "activation_Price.amount": pl.Float64,
        "congestionCost_Price.amount": pl.Float64,
        "energy_Price.amount": pl.Float64,
        "imbalance_Price.amount": pl.Float64,
        "imbalance_Price.category": pl.String,
        "Financial_Price.amount": pl.Float64,
        "point_reasons": _REASON_SCHEMA,
        "quantity_Measure_Unit.name": pl.String,
        "price_Measure_Unit.name": pl.String,
        "currency_Unit.name": pl.String,
        "xml_extra": _XML_EXTRA_SCHEMA,
        "xml_groups": _XML_GROUP_SCHEMA,
    }
)

_DURATION_PATTERN = re.compile(
    r"^P(?:(?P<years>\d+(?:\.\d+)?)Y)?(?:(?P<months>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<days>\d+(?:\.\d+)?)D)?(?:T(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def _children(node: dict[str, Any], name: str) -> list[dict[str, Any]]:
    return [
        child
        for child in node.get("children", [])
        if child.get("name") == name and child.get("namespace") == node.get("namespace")
    ]


def _qualified_name(node: dict[str, Any]) -> str:
    name = node.get("name", "")
    namespace = node.get("namespace")
    return f"{{{namespace}}}{name}" if namespace else name


def _child(node: dict[str, Any], name: str) -> dict[str, Any] | None:
    matches = _children(node, name)
    return matches[0] if matches else None


def _text(node: dict[str, Any] | None) -> str | None:
    if node is None:
        return None
    value = node.get("text")
    return value.strip() if isinstance(value, str) and value.strip() else None


def _value(node: dict[str, Any], path: str) -> str | None:
    current = node
    for part in path.split("/"):
        if current is None:
            return None
        current = _child(current, part)
    return _text(current)


def _first_value(node: dict[str, Any], *paths: str) -> str | None:
    return next((value for path in paths if (value := _value(node, path)) is not None), None)


def _identifier(node: dict[str, Any], name: str) -> tuple[str | None, str | None]:
    value_node = _child(node, name)
    if value_node is None:
        return None, None
    coding_scheme = next(
        (
            attribute.get("value")
            for attribute in value_node.get("attributes", [])
            if attribute.get("name") == "codingScheme"
        ),
        None,
    )
    return _text(value_node), coding_scheme


def _first_identifier(node: dict[str, Any], *paths: str) -> tuple[str | None, str | None]:
    for path in paths:
        current = node
        for part in path.split("/"):
            current = _child(current, part) if current is not None else None
        if current is None:
            continue
        coding_scheme = next(
            (
                attribute.get("value")
                for attribute in current.get("attributes", [])
                if attribute.get("name") == "codingScheme"
            ),
            None,
        )
        if _text(current) is not None:
            return _text(current), coding_scheme
    return None, None


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _combined_date_time(node: dict[str, Any], prefix: str) -> str | None:
    direct_value = _first_value(node, f"{prefix}.dateTime", f"{prefix}/dateTime")
    if direct_value is not None:
        return direct_value
    date_value = _first_value(node, f"{prefix}.date", f"{prefix}/date")
    time_value = _first_value(node, f"{prefix}.time", f"{prefix}/time")
    if date_value is None or time_value is None:
        return None
    return f"{date_value}T{time_value}"


def _parse_duration(value: str | None) -> timedelta | relativedelta | None:
    if not value:
        return None
    match = _DURATION_PATTERN.fullmatch(value)
    if match is None:
        return None
    parts = {name: float(number) for name, number in match.groupdict().items() if number is not None}
    if not parts:
        return None
    if "years" in parts or "months" in parts:
        if any(number % 1 for number in parts.values()):
            return None
        return relativedelta(**{name: int(number) for name, number in parts.items()})
    seconds = (
        parts.get("days", 0) * 86400
        + parts.get("hours", 0) * 3600
        + parts.get("minutes", 0) * 60
        + parts.get("seconds", 0)
    )
    return timedelta(seconds=seconds)


def _add_resolution(timestamp: datetime | None, resolution: str | None, periods: int = 1) -> datetime | None:
    duration = _parse_duration(resolution)
    if timestamp is None or duration is None:
        return None
    if isinstance(duration, relativedelta):
        return timestamp + duration * periods
    return timestamp + duration * periods


def _attributes(node: dict[str, Any]) -> list[dict[str, str | None]]:
    return [
        {
            "name": attribute.get("name"),
            "namespace": attribute.get("namespace"),
            "value": attribute.get("value"),
        }
        for attribute in node.get("attributes", [])
    ]


def _xml_values(
    node: dict[str, Any], prefix: str = "", excluded_children: set[str] | None = None
) -> list[dict[str, Any]]:
    """Flatten leaves without losing repeated siblings or dotted local names."""
    values: list[dict[str, Any]] = []
    children = node.get("children", [])
    excluded_children = excluded_children or set()
    current_path = prefix or _qualified_name(node)
    if node.get("attributes") or _text(node) is not None:
        values.append(
            {
                "path": current_path,
                "position": 1,
                "value": _text(node),
                "attributes": _attributes(node),
            }
        )
    totals = Counter(_qualified_name(child) for child in children)
    counts: Counter[str] = Counter()
    for child in children:
        qualified_name = _qualified_name(child)
        counts[qualified_name] += 1
        occurrence = counts[qualified_name]
        if child.get("name") in excluded_children and child.get("namespace") == node.get("namespace"):
            continue
        segment = f"{qualified_name}[{occurrence}]" if totals[qualified_name] > 1 else qualified_name
        path = f"{current_path}/{segment}"
        if child.get("children"):
            values.extend(_xml_values(child, path))
        elif _text(child) is not None or child.get("attributes"):
            values.append(
                {
                    "path": path,
                    "position": occurrence,
                    "value": _text(child),
                    "attributes": _attributes(child),
                }
            )
    return values


def _xml_groups(
    node: dict[str, Any], prefix: str = "", excluded_children: set[str] | None = None
) -> list[dict[str, Any]]:
    """Retain repeated XML collections as independent, explodable groups."""
    groups: list[dict[str, Any]] = []
    current_path = prefix or _qualified_name(node)
    excluded_children = excluded_children or set()
    children = node.get("children", [])
    totals = Counter(_qualified_name(child) for child in children)
    counts: Counter[str] = Counter()
    for child in children:
        name = child.get("name", "")
        qualified_name = _qualified_name(child)
        counts[qualified_name] += 1
        occurrence = counts[qualified_name]
        if name in excluded_children and child.get("namespace") == node.get("namespace"):
            continue
        segment = f"{qualified_name}[{occurrence}]" if totals[qualified_name] > 1 else qualified_name
        path = f"{current_path}/{segment}"
        if not child.get("children"):
            continue
        if name in _GROUP_NODE_NAMES:
            groups.append(
                {
                    "path": path,
                    "position": occurrence,
                    "fields": _xml_values(child, path),
                }
            )
        groups.extend(_xml_groups(child, path))
    return groups


def _base_record(document: dict[str, Any], document_name: str | None, row_kind: str) -> dict[str, Any]:
    record = {
        "document_name": document_name,
        "document_root": document.get("name"),
        "document_namespace": document.get("namespace"),
        "document_mRID": _value(document, "mRID"),
        "document_revisionNumber": _value(document, "revisionNumber"),
        "document_type": _value(document, "type"),
        "document_processType": _first_value(document, "process.processType", "processType"),
        "document_businessType": _value(document, "businessType"),
        "document_createdDateTime": _parse_datetime(_value(document, "createdDateTime")),
        "document_start": _parse_datetime(_first_value(document, "unavailability_Time_Period.timeInterval/start")),
        "document_end": _parse_datetime(_first_value(document, "unavailability_Time_Period.timeInterval/end")),
        "publication_datetime": _parse_datetime(_value(document, "publication_DateAndOrTime.dateTime")),
        "start_datetime": _parse_datetime(_value(document, "start_DateAndOrTime.dateTime")),
        "end_datetime": _parse_datetime(_value(document, "end_DateAndOrTime.dateTime")),
        "document_status": _value(document, "docStatus/value"),
        "document_reasons": _reason_values(document),
        "row_kind": row_kind,
        "series_mRID": None,
        "businessType": None,
        "product": None,
        "in_Domain.mRID": None,
        "in_Domain.codingScheme": None,
        "out_Domain.mRID": None,
        "out_Domain.codingScheme": None,
        "controlArea_Domain.mRID": None,
        "controlArea_Domain.codingScheme": None,
        "area_Domain.mRID": None,
        "area_Domain.codingScheme": None,
        "connecting_Domain.mRID": None,
        "connecting_Domain.codingScheme": None,
        "acquiring_Domain.mRID": None,
        "acquiring_Domain.codingScheme": None,
        "biddingZone_Domain.mRID": None,
        "biddingZone_Domain.codingScheme": None,
        "inBiddingZone_Domain.mRID": None,
        "inBiddingZone_Domain.codingScheme": None,
        "outBiddingZone_Domain.mRID": None,
        "outBiddingZone_Domain.codingScheme": None,
        "psrType": None,
        "PowerSystemResources.mRID": None,
        "PowerSystemResources.codingScheme": None,
        "PowerSystemResources.name": None,
        "PowerSystemResources.nominalP": None,
        "PowerSystemResources.nominalP.unit": None,
        "GeneratingUnit_PowerSystemResources.mRID": None,
        "GeneratingUnit_PowerSystemResources.codingScheme": None,
        "nominalIP_PowerSystemResources.nominalP": None,
        "nominalIP_PowerSystemResources.nominalP.unit": None,
        "objectAggregation": None,
        "implementation_date": None,
        "series_start": None,
        "series_end": None,
        "flowDirection.direction": None,
        "registeredResource.mRID": None,
        "registeredResource.codingScheme": None,
        "registeredResource.name": None,
        "registeredResource.location.name": None,
        "production_unit": None,
        "period_start": None,
        "period_end": None,
        "timestamp": None,
        "timestamp_end": None,
        "resolution": None,
        "curveType": None,
        "point_position": None,
        "quantity": None,
        "quantity.quantity": None,
        "secondaryQuantity": None,
        "unavailable_Quantity.quantity": None,
        "price.amount": None,
        "procurement_price.amount": None,
        "procurement_Price.amount": None,
        "activation_Price.amount": None,
        "congestionCost_Price.amount": None,
        "energy_Price.amount": None,
        "imbalance_Price.amount": None,
        "imbalance_Price.category": None,
        "Financial_Price.amount": None,
        "point_reasons": [],
        "quantity_Measure_Unit.name": None,
        "price_Measure_Unit.name": None,
        "currency_Unit.name": None,
        "xml_extra": [],
        "xml_groups": [],
    }
    _set_domain_fields(record, document)
    return record


def _set_domain_fields(record: dict[str, Any], node: dict[str, Any]) -> None:
    for domain in (
        "in_Domain",
        "out_Domain",
        "controlArea_Domain",
        "area_Domain",
        "connecting_Domain",
        "acquiring_Domain",
        "biddingZone_Domain",
        "inBiddingZone_Domain",
        "outBiddingZone_Domain",
    ):
        value, coding_scheme = _first_identifier(
            node,
            f"{domain}.mRID",
            f"{domain}/mRID",
            f"{domain[0].upper()}{domain[1:]}.mRID",
            f"{domain[0].upper()}{domain[1:]}/mRID",
        )
        if value is not None:
            record[f"{domain}.mRID"] = value
            record[f"{domain}.codingScheme"] = coding_scheme


def _set_series_fields(record: dict[str, Any], series: dict[str, Any]) -> None:
    record["series_mRID"] = _value(series, "mRID")
    record["implementation_date"] = _parse_date(_value(series, "implementation_DateAndOrTime.date"))
    record["series_start"] = _parse_datetime(_combined_date_time(series, "start_DateAndOrTime"))
    record["series_end"] = _parse_datetime(_combined_date_time(series, "end_DateAndOrTime"))
    for column in (
        "businessType",
        "product",
        "curveType",
        "objectAggregation",
        "flowDirection.direction",
        "registeredResource.name",
        "registeredResource.location.name",
        "production_unit",
        "quantity_Measure_Unit.name",
        "price_Measure_Unit.name",
        "currency_Unit.name",
    ):
        record[column] = _value(series, column)
    _set_domain_fields(record, series)

    record["psrType"] = _first_value(
        series,
        "psrType",
        "MktPSRType/psrType",
        "MktPSRType.psrType",
        "production_RegisteredResource.pSRType.psrType",
    )
    power_resource, power_resource_scheme = _first_identifier(
        series,
        "PowerSystemResources/mRID",
        "PowerSystemResources.mRID",
        "MktPSRType/PowerSystemResources/mRID",
        "MktPSRType/GeneratingUnit_PowerSystemResources/mRID",
        "GeneratingUnit_PowerSystemResources.mRID",
        "production_RegisteredResource.pSRType.powerSystemResources.mRID",
    )
    record["PowerSystemResources.mRID"] = power_resource
    record["PowerSystemResources.codingScheme"] = power_resource_scheme
    generating_resource, generating_scheme = _first_identifier(
        series,
        "MktPSRType/GeneratingUnit_PowerSystemResources/mRID",
        "GeneratingUnit_PowerSystemResources.mRID",
    )
    record["GeneratingUnit_PowerSystemResources.mRID"] = generating_resource
    record["GeneratingUnit_PowerSystemResources.codingScheme"] = generating_scheme
    resource_value, resource_scheme = _first_identifier(
        series,
        "registeredResource.mRID",
        "RegisteredResource/mRID",
        "registeredResource/mRID",
        "production_RegisteredResource.mRID",
        "production_RegisteredResource.pSRType.powerSystemResources.mRID",
    )
    record["registeredResource.mRID"] = resource_value or power_resource
    record["registeredResource.codingScheme"] = resource_scheme or power_resource_scheme
    record["registeredResource.name"] = record["registeredResource.name"] or _first_value(
        series,
        "RegisteredResource/name",
        "registeredResource/name",
    )
    record["PowerSystemResources.name"] = _first_value(
        series,
        "PowerSystemResources/name",
        "MktPSRType/PowerSystemResources/name",
        "MktPSRType/GeneratingUnit_PowerSystemResources/name",
        "production_RegisteredResource.pSRType.powerSystemResources.name",
    )

    nominal_node = None
    for path in (
        "nominalIP_PowerSystemResources.nominalP",
        "MktPSRType/nominalIP_PowerSystemResources.nominalP",
        "MktPSRType/PowerSystemResources/nominalP",
        "MktPSRType/GeneratingUnit_PowerSystemResources/nominalP",
        "GeneratingUnit_PowerSystemResources.nominalP",
        "production_RegisteredResource.pSRType.powerSystemResources.nominalP",
    ):
        current = series
        for part in path.split("/"):
            current = _child(current, part) if current is not None else None
        if current is not None:
            nominal_node = current
            break
    nominal_value = _text(nominal_node)
    if nominal_value is not None:
        try:
            numeric_nominal = float(nominal_value)
            record["nominalIP_PowerSystemResources.nominalP"] = numeric_nominal
            record["PowerSystemResources.nominalP"] = numeric_nominal
        except ValueError:
            pass
    if nominal_node is not None:
        nominal_unit = next(
            (
                attribute.get("value")
                for attribute in nominal_node.get("attributes", [])
                if attribute.get("name") == "unit"
            ),
            None,
        )
        record["nominalIP_PowerSystemResources.nominalP.unit"] = nominal_unit
        record["PowerSystemResources.nominalP.unit"] = nominal_unit


def _period_bounds(period: dict[str, Any]) -> tuple[datetime | None, datetime | None]:
    interval = _child(period, "timeInterval")
    if interval is None:
        return None, None
    return _parse_datetime(_value(interval, "start")), _parse_datetime(_value(interval, "end"))


def _reason_values(node: dict[str, Any]) -> list[dict[str, str | None]]:
    reasons = [{"code": _value(reason, "code"), "text": _value(reason, "text")} for reason in _children(node, "Reason")]
    if reasons:
        return reasons
    code = _first_value(node, "reason.code")
    text = _first_value(node, "reason.text")
    return [{"code": code, "text": text}] if code is not None or text is not None else []


def _point_fields(record: dict[str, Any], point: dict[str, Any], point_path: str) -> None:
    position = _value(point, "position")
    if position is not None:
        try:
            record["point_position"] = int(position)
        except ValueError:
            pass
    numeric_fields = (
        "quantity",
        "quantity.quantity",
        "secondaryQuantity",
        "unavailable_Quantity.quantity",
        "price.amount",
        "procurement_price.amount",
        "procurement_Price.amount",
        "activation_Price.amount",
        "congestionCost_Price.amount",
        "energy_Price.amount",
        "imbalance_Price.amount",
    )
    for column in numeric_fields:
        node = _child(point, column)
        value = _text(node)
        if node is not None and value is not None:
            try:
                record[column] = float(value)
            except ValueError:
                record["xml_extra"].extend(_xml_values(node, f"{point_path}/{_qualified_name(node)}"))
    financial_price = _first_value(point, "Financial_Price.amount", "Financial_Price/amount")
    if financial_price is not None:
        try:
            record["Financial_Price.amount"] = float(financial_price)
        except ValueError:
            pass
    record["imbalance_Price.category"] = _value(point, "imbalance_Price.category")
    record["point_reasons"] = _reason_values(point)
    if record["quantity"] is None:
        record["quantity"] = record["quantity.quantity"]
    if record["price.amount"] is None:
        for column in (
            "procurement_price.amount",
            "procurement_Price.amount",
            "activation_Price.amount",
            "congestionCost_Price.amount",
            "energy_Price.amount",
            "imbalance_Price.amount",
            "Financial_Price.amount",
        ):
            if record[column] is not None:
                record["price.amount"] = record[column]
                break


def _parent_xml_values(
    document: dict[str, Any],
    series: dict[str, Any] | None,
    period: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    document_path = _qualified_name(document)
    values = _xml_values(
        document,
        excluded_children={
            "TimeSeries",
            "mRID",
            "revisionNumber",
            "type",
            "process.processType",
            "businessType",
            "createdDateTime",
            "docStatus",
            "Reason",
            "reason.code",
            "reason.text",
        },
    )
    if series is not None:
        values.extend(
            _xml_values(
                series,
                f"{document_path}/{_qualified_name(series)}",
                {
                    "Period",
                    "Available_Period",
                    "mRID",
                    "businessType",
                    "product",
                    "curveType",
                    "in_Domain.mRID",
                    "out_Domain.mRID",
                    "controlArea_Domain.mRID",
                    "area_Domain.mRID",
                    "connecting_Domain.mRID",
                    "acquiring_Domain.mRID",
                    "biddingZone_Domain.mRID",
                    "psrType",
                    "flowDirection.direction",
                    "registeredResource.mRID",
                    "production_RegisteredResource.mRID",
                    "registeredResource.name",
                    "production_unit",
                    "quantity_Measure_Unit.name",
                    "price_Measure_Unit.name",
                    "currency_Unit.name",
                },
            )
        )
    if period is not None and series is not None:
        period_path = f"{document_path}/{_qualified_name(series)}/{_qualified_name(period)}"
        values.extend(
            _xml_values(
                period,
                period_path,
                {"timeInterval", "resolution", "curveType", "Point"},
            )
        )
    return values


def _parent_xml_groups(
    document: dict[str, Any],
    series: dict[str, Any] | None,
    period: dict[str, Any] | None,
    point: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    document_path = _qualified_name(document)
    groups = _xml_groups(document, excluded_children={"TimeSeries"})
    if series is not None:
        series_path = f"{document_path}/{_qualified_name(series)}"
        groups.extend(
            _xml_groups(
                series,
                series_path,
                {"Period", "Available_Period"},
            )
        )
    if period is not None and series is not None:
        period_path = f"{document_path}/{_qualified_name(series)}/{_qualified_name(period)}"
        groups.extend(_xml_groups(period, period_path, {"Point"}))
    if point is not None and series is not None and period is not None:
        point_path = "/".join(
            (
                document_path,
                _qualified_name(series),
                _qualified_name(period),
                _qualified_name(point),
            )
        )
        groups.extend(
            _xml_groups(
                point,
                point_path,
                {"position", "quantity", "price.amount", "procurement_price.amount", "activation_Price.amount"},
            )
        )
    return groups


def _project_time_series_document(document: dict[str, Any], document_name: str | None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    time_series = _children(document, "TimeSeries")
    if not time_series:
        record = _base_record(document, document_name, "document")
        record["xml_extra"] = _xml_values(
            document,
            excluded_children={
                "mRID",
                "revisionNumber",
                "type",
                "process.processType",
                "businessType",
                "createdDateTime",
                "docStatus",
                "Reason",
                "reason.code",
                "reason.text",
            },
        )
        record["xml_groups"] = _parent_xml_groups(document, None, None)
        return [record]

    for series in time_series:
        series_record = _base_record(document, document_name, "series")
        _set_series_fields(series_record, series)
        periods = _children(series, "Period") or _children(series, "Available_Period")
        if not periods:
            series_record["xml_extra"] = _parent_xml_values(document, series, None)
            series_record["xml_groups"] = _parent_xml_groups(document, series, None)
            records.append(series_record)
            continue

        for period in periods:
            start, end = _period_bounds(period)
            resolution = _value(period, "resolution")
            curve_type = _value(period, "curveType") or series_record["curveType"]
            period_record: dict[str, Any] = dict(
                series_record,
                row_kind="period",
                period_start=start,
                period_end=end,
                resolution=resolution,
                curveType=curve_type,
                xml_extra=_parent_xml_values(document, series, period),
            )
            points = _children(period, "Point")
            if not points:
                period_record["xml_groups"] = _parent_xml_groups(document, series, period)
                records.append(period_record)
                continue

            point_times: list[datetime | None] = []
            for point in points:
                position_text = _value(point, "position")
                try:
                    position = int(position_text) if position_text is not None else None
                except ValueError:
                    position = None
                point_times.append(
                    _add_resolution(start, resolution, position - 1) if position and position > 0 else None
                )

            block_ends = [end] * len(points)
            if curve_type == "A03":
                next_start = end
                for index, timestamp in reversed(list(enumerate(point_times))):
                    block_ends[index] = next_start
                    if timestamp is not None:
                        next_start = timestamp

            for index, point in enumerate(points):
                timestamp = point_times[index]
                timestamp_end = block_ends[index] if curve_type == "A03" else _add_resolution(timestamp, resolution)
                record: dict[str, Any] = dict(
                    period_record,
                    row_kind="point",
                    timestamp=timestamp,
                    timestamp_end=timestamp_end,
                    xml_extra=list(period_record["xml_extra"]),
                )
                point_path = "/".join(
                    (
                        _qualified_name(document),
                        _qualified_name(series),
                        _qualified_name(period),
                        _qualified_name(point),
                    )
                )
                _point_fields(record, point, point_path)
                record["xml_extra"].extend(
                    _xml_values(
                        point,
                        point_path,
                        {
                            "position",
                            "quantity",
                            "quantity.quantity",
                            "secondaryQuantity",
                            "unavailable_Quantity.quantity",
                            "price.amount",
                            "procurement_price.amount",
                            "procurement_Price.amount",
                            "activation_Price.amount",
                            "congestionCost_Price.amount",
                            "energy_Price.amount",
                            "imbalance_Price.amount",
                            "imbalance_Price.category",
                            "Reason",
                        },
                    )
                )
                record["xml_groups"] = _parent_xml_groups(document, series, period, point)
                records.append(record)
    return records


def project_documents(documents: list[tuple[str | None, dict[str, Any]]]) -> pl.DataFrame:
    """Project parsed XML roots, including ZIP member names, to a stable table.

    ``documents`` is a sequence of ``(member_name, root)`` pairs. Pass ``None``
    as the name for direct XML responses. An empty sequence produces an empty
    DataFrame with the same schema as every non-empty projection.
    """
    records: list[dict[str, Any]] = []
    for document_name, root in documents:
        records.extend(_project_time_series_document(root, document_name))
    return pl.DataFrame(records, schema=PROJECTION_SCHEMA, strict=False)
