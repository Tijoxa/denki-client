"""Shared HTTP transport, request windows and ENTSO-E response decoding."""

import logging
import re
from collections.abc import AsyncIterator, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from enum import Enum
from io import BytesIO
from typing import ClassVar, Literal, NamedTuple
from urllib.parse import quote, quote_plus
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement, tostring
from zipfile import BadZipFile, ZipFile
from zoneinfo import ZoneInfo

import httpx2
import polars as pl
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from denki_client._core import parse_xml
from denki_client.area import Area, lookup_area
from denki_client.exceptions import (
    EntsoeResponseError,
    InvalidBusinessParameterError,
    InvalidPSRTypeError,
    NoMatchingDataError,
    PaginationError,
    ParseError,
)
from denki_client.parsers import parse_date, parse_datetime, parse_freq
from denki_client.utils import yield_date_range

from ._projection import merge_window_results, project_documents

RequestMode = Literal["get", "get_header", "post"]
PageUnit = Literal["series", "documents", "archives"]
DateInput = date | str
DateTimeInput = datetime | str
WireParams = dict[str, str | int | bool | None]
WireValue = str | int | bool | Enum | None

_MAX_OFFSET = 4800
_ZIP_SIGNATURES = (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")
_TOKEN_QUERY = re.compile(r"(?i)([?&]securityToken=)[^&\s\"'<>]*")
_EIC = re.compile(r"[A-Z0-9-]{16}")
_STATUS_NS = "urn:iec62325.351:tc57wg16:451-5:statusrequestdocument:4:0"
_AREA_KEYS = (
    "out_domain",
    "connecting_domain",
    "outbiddingzone_domain",
    "in_domain",
    "inbiddingzone_domain",
    "biddingzone_domain",
    "controlarea_domain",
    "area_domain",
    "domain",
    "acquiring_domain",
    "ptdf_domain.mrid",
)


class _TokenLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = _TOKEN_QUERY.sub(r"\1<redacted>", message)
        if redacted != message:
            record.msg, record.args = redacted, ()
        return True


_TOKEN_LOG_FILTER = _TokenLogFilter()


def area_code(area: Area | str) -> str:
    """Resolve a known area name or accept an EIC as supplied by ENTSO-E."""
    if isinstance(area, str) and _EIC.fullmatch(area.upper()):
        return area.upper()
    return lookup_area(area).code


def _utc_datetime(value: DateTimeInput) -> datetime:
    if isinstance(value, str):
        if len(value) == 12 and value.isdecimal():
            return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=UTC)
        value = datetime.fromisoformat(value)
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
    return parse_datetime(value)


def format_datetime(value: DateTimeInput) -> str:
    return _utc_datetime(value).strftime("%Y-%m-%dT%H:%MZ")


def _area_timezone(params: WireParams, calendar_area: Area | None) -> tzinfo:
    if calendar_area is not None:
        return ZoneInfo(calendar_area.tz)
    domains = {key.casefold(): value for key, value in params.items()}
    for key in _AREA_KEYS:
        domain = domains.get(key)
        if isinstance(domain, str):
            try:
                return ZoneInfo(lookup_area(domain).tz)
            except ValueError:
                return UTC
    return UTC


def _local_midnights(window: tuple[date, date], zone: tzinfo) -> tuple[datetime, datetime]:
    start, end = window
    return datetime.combine(start, time.min, zone), datetime.combine(end, time.min, zone)


def _utc_wire_time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M")


def _split_point(
    start: datetime, end: datetime, publication_zone: tzinfo | None, allow_subday: bool
) -> datetime | None:
    if publication_zone is not None:
        first_day = start.astimezone(publication_zone).date()
        last_day = end.astimezone(publication_zone).date()
        middle_day = first_day + timedelta(days=((last_day - first_day).days + 1) // 2)
        midnight = datetime.combine(middle_day, time.min, publication_zone).astimezone(UTC)
        if start < midnight < end:
            return midnight
        if not allow_subday:
            return None
    if end - start >= timedelta(minutes=2):
        return (start + (end - start) / 2).replace(second=0, microsecond=0)
    return None


def _status_request(params: WireParams) -> bytes:
    start = datetime.strptime(str(params["periodStart"]), "%Y%m%d%H%M").replace(tzinfo=UTC)
    end = datetime.strptime(str(params["periodEnd"]), "%Y%m%d%H%M").replace(tzinfo=UTC)
    root = Element("StatusRequest_MarketDocument", {"xmlns": _STATUS_NS})
    SubElement(root, "mRID").text = uuid4().hex
    SubElement(root, "type").text = "A59"
    SubElement(root, "sender_MarketParticipant.mRID", {"codingScheme": "A01"}).text = "10X1001A1001A450"
    SubElement(root, "sender_MarketParticipant.marketRole.type").text = "A07"
    SubElement(root, "receiver_MarketParticipant.mRID", {"codingScheme": "A01"}).text = "10X1001A1001A450"
    SubElement(root, "receiver_MarketParticipant.marketRole.type").text = "A32"
    SubElement(root, "createdDateTime").text = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    for name, value in (
        ("DocumentType", params["documentType"]),
        ("ProcessType", params["processType"]),
        ("OutBiddingZone_Domain", params["outBiddingZone_Domain"]),
        ("TimeInterval", f"{format_datetime(start)}/{format_datetime(end)}"),
    ):
        component = SubElement(root, "AttributeInstanceComponent")
        SubElement(component, "attribute").text = name
        SubElement(component, "attributeValue").text = str(value)
    return tostring(root, encoding="utf-8", xml_declaration=True)


@dataclass(slots=True)
class _Document:
    name: str | None
    root: dict


@dataclass(slots=True)
class _Response:
    documents: list[_Document]
    archives: int

    def count(self, unit: PageUnit) -> int:
        if unit == "archives" and self.archives:
            return self.archives
        if unit == "series":
            return sum(
                child["name"] == "TimeSeries" and child["namespace"] == document.root["namespace"]
                for document in self.documents
                for child in document.root["children"]
            )
        return len(self.documents)


class _Pagination(NamedTuple):
    page_size: int | None
    page_unit: PageUnit
    offset_key: str
    max_units: int | None


def _decode(content: bytes, name: str | None = None) -> _Response:
    if content.startswith(_ZIP_SIGNATURES):
        documents = []
        archives = 0
        try:
            with ZipFile(BytesIO(content)) as archive:
                for entry in archive.infolist():
                    if entry.is_dir():
                        continue
                    member = f"{name}/{entry.filename}" if name else entry.filename
                    try:
                        body = archive.read(entry)
                    except (BadZipFile, RuntimeError, OSError, NotImplementedError) as exc:
                        raise ParseError(f"Invalid ZIP member {member!r}: {exc}") from exc
                    nested_zip = body.startswith(_ZIP_SIGNATURES)
                    if (
                        not nested_zip
                        and not entry.filename.lower().endswith((".xml", ".zip"))
                        and not body.lstrip().startswith((b"<", b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff"))
                    ):
                        continue
                    archives += nested_zip
                    documents.extend(_decode(body, member).documents)
        except BadZipFile as exc:
            raise ParseError(f"Invalid ZIP in {name or 'ENTSO-E response'}: {exc}") from exc
        return _Response(documents, archives)

    if name and name.lower().endswith(".zip"):
        raise ParseError(f"Invalid ZIP signature in {name!r}")
    try:
        root = parse_xml(content)
    except (ValueError, RuntimeError) as exc:
        raise ParseError(f"Invalid XML in {name or 'ENTSO-E response'}: {exc}") from exc
    return _Response([_Document(name, root)], 0)


def _reasons(root: dict):
    for item in root["children"]:
        if item["name"] == "Reason" and item["namespace"] == root["namespace"]:
            values = {field["name"]: field["text"] for field in item["children"]}
            yield values.get("code"), values.get("text")


def _reason_error(message: str) -> type[Exception] | None:
    text = message.casefold()
    if "no matching data found" in text:
        return NoMatchingDataError
    if "dependency tables" in text:
        return InvalidBusinessParameterError
    if "is not valid for this area" in text:
        return InvalidPSRTypeError
    if (
        "exceeds the allowed limit" in text
        or "exceeds allowed limit" in text
        or "date range exceeds permitted limit" in text
        or re.search(r"more than \d+ matching documents", text)
    ):
        return PaginationError
    return None


class _Transport:
    base_url: ClassVar[str] = "https://web-api.tp.entsoe.eu/api"

    def __init__(self, api_key: str, **httpx2_client_kwargs) -> None:
        """Create an asynchronous ENTSO-E client with options forwarded to HTTPX2."""
        self.api_key = api_key
        self.session = httpx2.AsyncClient(**httpx2_client_kwargs)
        self.logger = logging.getLogger(__package__ or __name__)
        for name in ("httpx2", "httpx"):
            logging.getLogger(name).addFilter(_TOKEN_LOG_FILTER)

    def _redact(self, message: str) -> str:
        if self.api_key:
            for secret in {self.api_key, quote(self.api_key, safe=""), quote_plus(self.api_key)}:
                message = message.replace(secret, "<redacted>")
        return _TOKEN_QUERY.sub(r"\1<redacted>", message)

    @retry(
        retry=retry_if_exception_type((httpx2.ConnectError, httpx2.ReadTimeout)),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        reraise=True,
    )
    async def _send(self, request: httpx2.Request, **send_kwargs) -> httpx2.Response:
        return await self.session.send(request, **send_kwargs)

    async def _request(self, params: WireParams, request_mode: RequestMode = "get") -> _Response:
        params = {key: value for key, value in params.items() if value is not None}
        headers = {}
        method, content = "GET", None
        if request_mode == "get":
            params["securityToken"] = self.api_key
        else:
            headers["SECURITY_TOKEN"] = self.api_key
            if request_mode == "post":
                method, content = "POST", _status_request(params)
                headers["Content-Type"] = "application/xml"
                params = {}
        self.logger.debug("ENTSO-E %s request", method)
        request = self.session.build_request(method, self.base_url, params=params, headers=headers, content=content)
        try:
            response = await self._send(request)
        except httpx2.RequestError as exc:
            raise type(exc)(self._redact(str(exc)), request=self._safe_request(request)) from None

        try:
            decoded = _decode(response.content)
        except ParseError as exc:
            safe_message = self._redact(str(exc))
            if not response.is_success:
                cause = exc if safe_message == str(exc) else ParseError(safe_message)
                raise self._http_error(response) from cause
            if safe_message != str(exc):
                raise ParseError(safe_message) from None
            raise

        documents = []
        no_matching_data = False
        for document in decoded.documents:
            if document.root["name"] == "text" and not response.is_success:
                message = self._redact(document.root["text"] or "")
                kind = _reason_error(message)
                if kind:
                    raise kind(f"ENTSO-E HTTP {response.status_code}: {message}")
            if document.root["name"] != "Acknowledgement_MarketDocument":
                documents.append(document)
                continue

            reasons = tuple(_reasons(document.root))
            message = self._redact("; ".join(f"{code or '?'}: {reason or ''}" for code, reason in reasons))
            context = f"ENTSO-E HTTP {response.status_code}"
            if document.name:
                context += f" in {self._redact(document.name)!r}"
            kinds = [_reason_error(reason or "") for _, reason in reasons]
            if kinds and all(kind is NoMatchingDataError for kind in kinds):
                no_matching_data = True
                continue
            specific = [kind for kind in kinds if kind is not NoMatchingDataError]
            if specific and specific[0] and all(kind is specific[0] for kind in specific):
                raise specific[0](f"{context}: {message}")
            raise EntsoeResponseError(response.status_code, f"{context}: {message or 'empty acknowledgement'}")

        if not documents and no_matching_data:
            raise NoMatchingDataError
        if not response.is_success:
            raise self._http_error(response)
        for document in documents:
            root_name = document.root["name"]
            if not root_name.endswith("MarketDocument") and root_name != "AllocationConfigurationDocument":
                raise ParseError(
                    f"Unexpected ENTSO-E XML root {self._redact(root_name)!r} in "
                    f"{self._redact(document.name or 'ENTSO-E response')}"
                )
        return _Response(documents, decoded.archives)

    @staticmethod
    def _safe_request(request: httpx2.Request) -> httpx2.Request:
        return httpx2.Request(request.method, request.url.copy_remove_param("securityToken"))

    def _http_error(self, response: httpx2.Response) -> httpx2.HTTPStatusError:
        request = self._safe_request(response.request)
        status = response.status_code
        return httpx2.HTTPStatusError(
            f"ENTSO-E HTTP {status}", request=request, response=httpx2.Response(status, request=request)
        )

    async def _query(
        self,
        document_type: str,
        start: DateInput | None = None,
        end: DateInput | None = None,
        *,
        wire_params: Mapping[str, WireValue] | None = None,
        calendar_area: Area | None = None,
        max_window: str | None = "1y",
        page_size: int | None = None,
        page_unit: PageUnit = "series",
        offset: int | None = None,
        offset_key: str = "offset",
        request_mode: RequestMode = "get",
        max_units: int | None = None,
        start_key: str = "periodStart",
        end_key: str = "periodEnd",
        document_key: str = "documentType",
        update_start: DateTimeInput | None = None,
        update_end: DateTimeInput | None = None,
        allow_subday: bool = True,
        **http_params: WireValue,
    ) -> pl.DataFrame | None:
        if request_mode not in ("get", "get_header", "post"):
            raise ValueError(f"Unknown request mode {request_mode!r}")
        if offset is not None and (type(offset) is not int or not 0 <= offset <= _MAX_OFFSET):
            raise ValueError(f"offset must be an integer between 0 and {_MAX_OFFSET}")
        filters = {**(wire_params or {}), **http_params}
        params = {key: value.name if isinstance(value, Enum) else value for key, value in filters.items()}
        params[document_key] = document_type
        if offset is not None:
            params[offset_key] = offset

        publication = self._publication_dates(start, end)
        update = self._update_window(update_start, update_end)
        frequency = parse_freq(max_window) if max_window else None
        zone = _area_timezone(params, calendar_area)
        keys = (start_key, end_key)
        windows: Iterable[tuple[datetime, datetime] | None]
        if update is not None:
            if publication is not None:
                params[start_key], params[end_key] = map(_utc_wire_time, _local_midnights(publication, zone))
            keys = ("PeriodStartUpdate", "PeriodEndUpdate")
            windows = yield_date_range(update[0], update[1], frequency) if frequency is not None else (update,)
        elif publication is not None:
            blocks = (
                yield_date_range(publication[0], publication[1], frequency) if frequency is not None else (publication,)
            )
            windows = (_local_midnights(block, zone) for block in blocks)
        else:
            windows = (None,)
        pagination = _Pagination(page_size, page_unit, offset_key, max_units)
        publication_zone = zone if publication is not None and update is None else None
        frames = []
        for block in windows:
            async for documents in self._query_window(
                params, block, keys, request_mode, pagination, publication_zone, allow_subday
            ):
                frames.append(project_documents([(document.name, document.root) for document in documents]))
        return merge_window_results(frames, document_type) if frames else None

    @staticmethod
    def _publication_dates(start: DateInput | None, end: DateInput | None) -> tuple[date, date] | None:
        if start is None and end is None:
            return None
        if start is None or end is None:
            raise ValueError("Both start and end must be supplied for a request window")
        first, last = parse_date(start, name="start"), parse_date(end, name="end")
        if first >= last:
            raise ValueError("end must be a date after start")
        return first, last

    @staticmethod
    def _update_window(start: DateTimeInput | None, end: DateTimeInput | None) -> tuple[datetime, datetime] | None:
        if start is None and end is None:
            return None
        if start is None or end is None:
            raise ValueError("Both update bounds must be supplied")
        first, last = (_utc_datetime(value).replace(second=0, microsecond=0) for value in (start, end))
        if first >= last:
            raise ValueError("The update window must end after it starts at minute precision")
        return first, last

    async def _query_window(
        self,
        params: WireParams,
        window: tuple[datetime, datetime] | None,
        keys: tuple[str, str],
        request_mode: RequestMode,
        pagination: _Pagination,
        publication_zone: tzinfo | None,
        allow_subday: bool,
    ) -> AsyncIterator[list[_Document]]:
        if window is not None:
            window = window[0].astimezone(UTC), window[1].astimezone(UTC)
            params = params | dict(zip(keys, map(_utc_wire_time, window), strict=True))
        try:
            documents = await self._query_pages(params, request_mode, *pagination)
        except PaginationError:
            # Offsets index the parent response. They cannot be translated to the child responses.
            if window is None or params.get(pagination.offset_key):
                raise
            start, end = window
            pivot = _split_point(start, end, publication_zone, allow_subday)
            if pivot is None:
                raise
            for block in ((start, pivot), (pivot, end)):
                async for batch in self._query_window(
                    params, block, keys, request_mode, pagination, publication_zone, allow_subday
                ):
                    yield batch
        else:
            if documents:
                yield documents

    async def _query_pages(
        self,
        params: WireParams,
        request_mode: RequestMode,
        page_size: int | None,
        page_unit: PageUnit,
        offset_key: str,
        max_units: int | None,
    ) -> list[_Document]:
        documents = []
        offset = int(params.get(offset_key) or 0)
        while True:
            if page_size and offset > _MAX_OFFSET:
                raise PaginationError(f"A full page reached ENTSO-E's maximum offset of {_MAX_OFFSET}")
            try:
                response = await self._request(params, request_mode)
            except NoMatchingDataError:
                return documents
            documents.extend(response.documents)
            count = response.count(page_unit)
            if max_units is not None and count > max_units:
                raise PaginationError(f"The response contains more than {max_units} {page_unit}")
            if not page_size or count < page_size or not response.documents:
                return documents
            offset += page_size
            params = params | {offset_key: offset}
