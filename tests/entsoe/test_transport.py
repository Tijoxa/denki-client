import logging
from datetime import UTC, date, datetime
from io import BytesIO
from itertools import pairwise
from zipfile import ZipFile

import httpx2
import pytest

from denki_client import EntsoeClient
from denki_client.area import Area
from denki_client.exceptions import EntsoeResponseError, PaginationError, ParseError

START = date(2025, 1, 1)
END = date(2025, 1, 3)
TOKEN = "T3st+/secret"
PUBLICATION = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"
ACKNOWLEDGEMENT = "urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0"


def document(
    value: int, *, series: str = "1", period_start: str = "2025-01-01T00:00Z", period_end: str = "2025-01-01T01:00Z"
):
    return f"""<Publication_MarketDocument xmlns="{PUBLICATION}">
        <mRID>Document-{value}</mRID><revisionNumber>1</revisionNumber>
        <TimeSeries><mRID>{series}</mRID><curveType>A01</curveType>
            <currency_Unit.name>EUR</currency_Unit.name>
            <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
            <Period>
                <timeInterval><start>{period_start}</start><end>{period_end}</end></timeInterval>
                <resolution>PT60M</resolution>
                <Point><position>1</position><price.amount>{value}</price.amount><quantity>{value}</quantity></Point>
            </Period>
        </TimeSeries>
    </Publication_MarketDocument>""".encode()


def acknowledgement(message: str) -> bytes:
    return f"""<a:Acknowledgement_MarketDocument xmlns:a="{ACKNOWLEDGEMENT}">
        <a:Reason><a:code>999</a:code><a:text>{message}</a:text></a:Reason>
    </a:Acknowledgement_MarketDocument>""".encode()


def zipped(**members: bytes) -> bytes:
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return output.getvalue()


@pytest.mark.parametrize("status", [200, 400])
async def test_no_matching_data_is_read_from_acknowledgement(status):
    def handler(_):
        return httpx2.Response(status, content=acknowledgement("No matching data found"))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        assert await client.query_load(country_code="FR", start=START, end=END) is None


async def test_business_acknowledgement_in_successful_zip_has_context_and_safe_logs(caplog):
    def handler(request):
        assert request.url.params["securityToken"] == TOKEN
        return httpx2.Response(
            200,
            content=zipped(**{"data.zip": zipped(**{"error.xml": acknowledgement(f"Invalid parameter {TOKEN}")})}),
        )

    caplog.set_level(logging.DEBUG)
    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        with pytest.raises(EntsoeResponseError, match=r"HTTP 200.*data\.zip/error\.xml.*999: Invalid parameter") as exc:
            await client.query_load("FR", START, END)
    assert TOKEN not in str(exc.value)
    assert TOKEN not in caplog.text
    assert "T3st%2B%2Fsecret" not in caplog.text


@pytest.mark.parametrize(
    ("content", "location"),
    [(b"PK\x03\x04broken", "ENTSO-E response"), (zipped(**{"malformed.xml": b"<bad"}), "malformed.xml")],
)
async def test_invalid_successful_content_retains_parse_cause(content, location):
    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(lambda _: httpx2.Response(200, content=content)))
    async with client.session:
        with pytest.raises(ParseError, match=location) as exc:
            await client.query_load("FR", START, END)
    assert exc.value.__cause__ is not None


async def test_http_error_has_status_and_redacted_request():
    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(lambda _: httpx2.Response(502, text="Bad gateway")))
    async with client.session:
        with pytest.raises(httpx2.HTTPStatusError) as exc:
            await client.query_load("FR", START, END)
    error = exc.value
    assert isinstance(error, httpx2.HTTPStatusError)
    assert error.response.status_code == 502
    assert error.request.url.params.get("documentType") == "A65"
    assert error.request.url.params.get("securityToken") is None


async def test_archive_pagination_counts_immediate_zips_before_rows():
    offsets = []

    def handler(request):
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        if offset == 2:
            return httpx2.Response(
                200,
                content=zipped(
                    **{
                        "one.zip": zipped(**{"a.xml": document(1), "b.xml": document(2)}),
                        "two.zip": zipped(**{"c.xml": document(3)}),
                    }
                ),
            )
        return httpx2.Response(200, content=zipped(**{"three.zip": zipped(**{"d.xml": document(4)})}))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        result = await client._query("A37", START, END, page_size=2, page_unit="archives", offset=2)
    assert offsets == [2, 4]
    assert result is not None
    assert result["quantity"].to_list() == [1.0, 2.0, 3.0, 4.0]


async def test_publication_window_bisected_on_pagination_error():
    windows = []

    def handler(request):
        params = request.url.params
        windows.append((params["periodStart"], params["periodEnd"]))
        if len(windows) == 1:
            return httpx2.Response(400, content=acknowledgement("Requested date range exceeds permitted limit"))
        return httpx2.Response(200, content=document(len(windows)))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        result = await client.query_load("FR", date(2025, 3, 30), date(2025, 4, 1))
    assert windows == [
        ("202503292300", "202503312200"),
        ("202503292300", "202503302200"),
        ("202503302200", "202503312200"),
    ]
    assert result is not None
    assert result["quantity"].to_list() == [2.0, 3.0]


async def test_full_page_at_maximum_offset_stops_before_out_of_range_request():
    offsets = []

    def handler(request):
        offsets.append(request.url.params["offset"])
        return httpx2.Response(200, content=document(1))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        with pytest.raises(PaginationError, match="maximum offset of 4800"):
            await client._query("A37", START, END, page_size=1, offset=4800)
    assert offsets == ["4800"]


async def test_automatic_pagination_restarts_saturated_window_on_smaller_blocks(monkeypatch):
    monkeypatch.setattr("denki_client.entsoe._transport._MAX_OFFSET", 2)
    requests = []

    def handler(request):
        params = request.url.params
        start, end, offset = (params[key] for key in ("periodStart", "periodEnd", "offset"))
        requests.append((start, end, offset))
        if (start, end) == ("202501010000", "202501030000"):
            return httpx2.Response(200, content=zipped(**{"one.xml": document(1), "two.xml": document(2)}))
        if start == "202501010000":
            return httpx2.Response(400, content=acknowledgement("No matching data found"))
        return httpx2.Response(200, content=zipped(**{"three.xml": document(3)}))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        frame = await client._query("A37", START, END, page_size=2, page_unit="documents", offset=0)

    assert requests == [
        ("202501010000", "202501030000", "0"),
        ("202501010000", "202501030000", "2"),
        ("202501010000", "202501020000", "0"),
        ("202501020000", "202501030000", "0"),
    ]
    assert frame is not None
    assert frame["quantity"].to_list() == [3.0]


async def test_full_day_publication_does_not_retry_at_subday_granularity():
    requests = []

    def handler(request):
        requests.append(request.url.params["periodStart"])
        return httpx2.Response(400, content=acknowledgement("More than 100 matching documents"))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        with pytest.raises(PaginationError, match="More than 100 matching documents"):
            await client.query_day_ahead_prices("FR", START, date(2025, 1, 2))
    assert requests == ["202412312300"]


async def test_saturated_update_window_stops_at_one_minute():
    windows = []

    def handler(request):
        params = request.url.params
        windows.append((params["PeriodStartUpdate"], params["PeriodEndUpdate"]))
        return httpx2.Response(400, content=acknowledgement("Amount of requested data exceeds allowed limit"))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        with pytest.raises(PaginationError, match="exceeds allowed limit"):
            await client.query_other_market_information(
                "FR", periodstartupdate="2025-01-01T09:42Z", periodendupdate="2025-01-01T09:45Z"
            )
    assert windows == [("202501010942", "202501010945"), ("202501010942", "202501010943")]


async def test_split_results_preserve_revisions_and_multiplicity_across_windows():
    original = document(1, period_start="2024-12-31T00:00Z", period_end="2024-12-31T01:00Z")
    revised = original.replace(b"<revisionNumber>1", b"<revisionNumber>2")
    sibling = document(2)

    def handler(request):
        if request.url.params["periodStart"] == "202501010000":
            body = zipped(**{"first.xml": original, "second.xml": original})
        else:
            body = zipped(**{"renamed.xml": original, "revised.xml": revised, "new.xml": sibling})
        return httpx2.Response(200, content=body)

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        large = await client._query("A37", START, END, max_window="1d")
        first = await client._query("A37", START, date(2025, 1, 2), max_window="1d")
        second = await client._query("A37", date(2025, 1, 2), END, max_window="1d")

    assert large is not None and first is not None and second is not None
    assert large.head(first.height).equals(first)
    assert large.tail(second.height - 1).equals(second.slice(1))
    assert large.select("document_revisionNumber", "quantity", "document_name").rows() == [
        ("1", 1.0, "first.xml"),
        ("1", 1.0, "second.xml"),
        ("2", 1.0, "revised.xml"),
        ("1", 2.0, "new.xml"),
    ]


async def test_crossborder_composition_respects_direction_and_aligns_neighbours():
    requests = []

    def handler(request):
        origin = request.url.params["out_Domain"]
        requests.append(origin)
        assert request.url.params["in_Domain"] == Area.ES.code
        assert request.url.params["periodStart"] == "202412312300"
        assert request.url.params["periodEnd"] == "202501022300"
        if origin == Area.FR.code:
            return httpx2.Response(200, content=document(40))
        assert origin == Area.PT.code
        return httpx2.Response(200, content=document(2))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        result = await client.query_physical_crossborder_allborders("ES", START, END, export=False, per_hour=True)
    assert requests == [Area.FR.code, Area.PT.code]
    assert result is not None
    assert result.select("timestamp", "FR", "PT", "sum").row(0) == (datetime(2025, 1, 1, tzinfo=UTC), 40, 2, 42)


@pytest.mark.parametrize(
    ("start", "end", "bounds"),
    [
        (date(2025, 3, 30), date(2025, 4, 1), ["202503292300", "202503302200", "202503312200"]),
        (date(2025, 10, 26), date(2025, 10, 28), ["202510252200", "202510262300", "202510272300"]),
    ],
)
async def test_local_day_windows_follow_dst(start, end, bounds):
    windows = []

    def handler(request):
        windows.append((request.url.params["periodStart"], request.url.params["periodEnd"]))
        return httpx2.Response(200, content=document(len(windows)))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        result = await client.query_actual_generation_by_unit(Area.FR.code, start=start, end=end)
    assert windows == list(pairwise(bounds))
    assert result is not None
    assert result["quantity"].to_list() == [1.0, 2.0]


async def test_publication_window_requires_dates():
    def handler(_):
        pytest.fail("A datetime reached the transport")

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        with pytest.raises(TypeError, match="must be a date"):
            await client.query_load("FR", datetime(2025, 1, 1, 12, tzinfo=UTC), END)
