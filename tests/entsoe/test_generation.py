from datetime import UTC, date, datetime

import httpx2
import polars as pl
import pytest

from denki_client import EntsoeClient
from denki_client.area import PsrType

GENERATION_RESPONSE = """<Publication_MarketDocument>
    <TimeSeries>
        <businessType>A04</businessType>
        <psrType>B14</psrType>
        <RegisteredResource>
            <mRID codingScheme="A01">11WGEN-UNIT00001A</mRID>
            <name>Example unit</name>
        </RegisteredResource>
        <Period>
            <timeInterval><start>2025-01-01T00:00Z</start><end>2025-01-02T00:00Z</end></timeInterval>
            <resolution>PT60M</resolution>
            <Point><position>1</position><quantity>12.5</quantity></Point>
        </Period>
    </TimeSeries>
</Publication_MarketDocument>"""


@pytest.mark.parametrize(
    ("method", "args", "kwargs", "document_type", "params", "request_mode"),
    [
        (
            "query_actual_total_load",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2), "request_mode": "get_header", "curve_type": "A01"},
            "A65",
            {"processType": "A16", "outBiddingZone_Domain": "10YFR-RTE------C", "curveType": "A01"},
            "get_header",
        ),
        (
            "query_actual_total_load",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2), "request_mode": "post"},
            "A65",
            {"processType": "A16", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "post",
        ),
        (
            "query_actual_total_load",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A65",
            {"processType": "A16", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_load_forecast",
            ("FR", date(2025, 1, 1), date(2025, 1, 2)),
            {"process_type": "A31"},
            "A65",
            {"processType": "A31", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_day_ahead_total_load_forecast",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A65",
            {"processType": "A01", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_week_ahead_total_load_forecast",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A65",
            {"processType": "A31", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_month_ahead_total_load_forecast",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A65",
            {"processType": "A32", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_year_ahead_total_load_forecast",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A65",
            {"processType": "A33", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_year_ahead_forecast_margin",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A70",
            {"processType": "A33", "outBiddingZone_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_installed_generation_capacity_by_type",
            ("FR", PsrType.B14),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A68",
            {"processType": "A33", "in_Domain": "10YFR-RTE------C", "PsrType": "B14"},
            "get",
        ),
        (
            "query_hydro_reservoir_storage",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A72",
            {"processType": "A16", "in_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_generation",
            ("FR", date(2025, 1, 1), date(2025, 1, 2), "B14"),
            {"nett": True},
            "A75",
            {"processType": "A16", "in_Domain": "10YFR-RTE------C", "psrType": "B14"},
            "get",
        ),
        (
            "query_generation_per_plant",
            ("FR", date(2025, 1, 1), date(2025, 1, 2), "B14"),
            {"include_eic": True, "eic_code": "11WGEN-UNIT00001A"},
            "A73",
            {
                "processType": "A16",
                "in_Domain": "10YFR-RTE------C",
                "PsrType": "B14",
                "RegisteredResource": "11WGEN-UNIT00001A",
            },
            "get",
        ),
        (
            "query_day_ahead_generation_forecast",
            ("FR",),
            {"start": date(2025, 1, 1), "end": date(2025, 1, 2)},
            "A71",
            {"processType": "A01", "in_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_generation_forecast",
            ("FR", date(2025, 1, 1), date(2025, 1, 2), "A01"),
            {"nett": True},
            "A71",
            {"processType": "A01", "in_Domain": "10YFR-RTE------C"},
            "get",
        ),
        (
            "query_wind_and_solar_forecast",
            ("FR", date(2025, 1, 1), date(2025, 1, 2), "B19", "A40"),
            {},
            "A69",
            {"processType": "A40", "in_Domain": "10YFR-RTE------C", "PsrType": "B19"},
            "get",
        ),
        (
            "query_installed_generation_capacity_per_unit",
            ("FR", date(2025, 1, 1), date(2025, 1, 2), "B14"),
            {},
            "A71",
            {"processType": "A33", "in_Domain": "10YFR-RTE------C", "PsrType": "B14"},
            "get",
        ),
    ],
)
async def test_generation_methods_send_expected_request(method, args, kwargs, document_type, params, request_mode):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, text=GENERATION_RESPONSE)

    client = EntsoeClient("secret", transport=httpx2.MockTransport(handler))
    async with client.session:
        frame = await getattr(client, method)(*args, **kwargs)

    assert isinstance(frame, pl.DataFrame)
    assert requests
    for request in requests:
        if request_mode == "post":
            assert "documentType" not in request.url.params
            assert "periodStart" not in request.url.params
            assert "periodEnd" not in request.url.params
        else:
            assert request.url.params["documentType"] == document_type
            assert request.url.params["periodStart"] == "202412312300"
            assert request.url.params["periodEnd"] == "202501012300"
            for key, value in params.items():
                assert request.url.params[key] == value

        if request_mode == "get":
            assert request.url.params["securityToken"] == "secret"
            assert "SECURITY_TOKEN" not in request.headers
        elif request_mode == "get_header":
            assert "securityToken" not in request.url.params
            assert request.headers["SECURITY_TOKEN"] == "secret"
        else:
            assert request.method == "POST"
            assert "securityToken" not in request.url.params
            assert request.headers["SECURITY_TOKEN"] == "secret"
            assert request.headers["content-type"] == "application/xml"
            body = request.content.decode()
            assert "StatusRequest_MarketDocument" in body
            assert "<attribute>DocumentType</attribute><attributeValue>A65</attributeValue>" in body
            assert "<attribute>ProcessType</attribute><attributeValue>A16</attributeValue>" in body
            assert "<attribute>OutBiddingZone_Domain</attribute>" in body
            assert "<attribute>TimeInterval</attribute>" in body
            assert "<attributeValue>2024-12-31T23:00Z/2025-01-01T23:00Z</attributeValue>" in body


async def test_load_and_forecast_outer_joins_timestamps():
    requests = []

    def handler(request):
        requests.append(request)
        value = "10" if request.url.params["processType"] == "A16" else "12"
        return httpx2.Response(
            200,
            text=f"""<Publication_MarketDocument>
                <TimeSeries><Period><timeInterval><start>2025-01-01T00:00Z</start></timeInterval>
                <resolution>PT60M</resolution><Point><position>1</position><quantity>{value}</quantity></Point>
                </Period></TimeSeries>
            </Publication_MarketDocument>""",
        )

    client = EntsoeClient("secret", transport=httpx2.MockTransport(handler))
    async with client.session:
        result = await client.query_load_and_forecast("FR", start=date(2025, 1, 1), end=date(2025, 1, 2))

    assert len(requests) == 2
    assert isinstance(result, pl.DataFrame)
    assert result.columns == ["timestamp", "actual_load", "forecast_load"]
    assert result.row(0) == (datetime(2025, 1, 1, tzinfo=UTC), 10.0, 12.0)


async def test_post_load_rejects_curve_type():
    client = EntsoeClient("secret", transport=httpx2.MockTransport(lambda _: httpx2.Response(200)))
    async with client.session:
        with pytest.raises(ValueError, match="curve_type is not supported"):
            await client.query_actual_total_load(
                "FR", start=date(2025, 1, 1), end=date(2025, 1, 2), request_mode="post", curve_type="A01"
            )


async def test_generation_nett_subtracts_consumption_from_generation():
    response = """<Publication_MarketDocument>
        <TimeSeries>
            <mRID>generation-series</mRID>
            <in_Domain.mRID codingScheme="A01">FR</in_Domain.mRID>
            <psrType>B10</psrType>
            <Period><timeInterval><start>2025-01-01T00:00Z</start><end>2025-01-02T00:00Z</end></timeInterval>
            <resolution>PT60M</resolution><Point><position>1</position><quantity>100</quantity></Point></Period>
        </TimeSeries>
        <TimeSeries>
            <mRID>consumption-series</mRID>
            <out_Domain.mRID codingScheme="A02">FR</out_Domain.mRID>
            <psrType>B10</psrType>
            <Period><timeInterval><start>2025-01-01T00:00Z</start><end>2025-01-02T00:00Z</end></timeInterval>
            <resolution>PT60M</resolution><Point><position>1</position><quantity>15</quantity></Point></Period>
        </TimeSeries>
        <TimeSeries>
            <mRID>consumption-only-series</mRID>
            <out_Domain.mRID codingScheme="A02">FR</out_Domain.mRID>
            <psrType>B10</psrType>
            <Period><timeInterval><start>2025-01-01T00:00Z</start><end>2025-01-02T00:00Z</end></timeInterval>
            <resolution>PT60M</resolution><Point><position>2</position><quantity>5</quantity></Point></Period>
        </TimeSeries>
    </Publication_MarketDocument>"""
    client = EntsoeClient("secret", transport=httpx2.MockTransport(lambda _: httpx2.Response(200, text=response)))
    async with client.session:
        result = await client.query_generation("FR", date(2025, 1, 1), date(2025, 1, 2), nett=True)

    assert isinstance(result, pl.DataFrame)
    assert result["quantity"].to_list() == [85.0, -5.0]
