from __future__ import annotations

from datetime import UTC, date, datetime
from io import BytesIO
from zipfile import ZipFile

import httpx2
import polars as pl
import pytest

from denki_client.entsoe import EntsoeClient

START = date(2025, 1, 1)
END = "2025-01-02"

MARKET_RESPONSE = """<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
  <mRID>market-test</mRID>
  <TimeSeries>
    <mRID>series-1</mRID>
    <out_Domain.mRID codingScheme="A01">10YFR-RTE------C</out_Domain.mRID>
    <in_Domain.mRID codingScheme="A01">10YBE----------2</in_Domain.mRID>
    <currency_Unit.name>EUR</currency_Unit.name>
    <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
    <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
    <Period>
      <timeInterval><start>2025-01-01T00:00Z</start><end>2025-01-01T01:00Z</end></timeInterval>
      <resolution>PT60M</resolution>
      <Point><position>1</position><price.amount>42.5</price.amount><quantity>42.5</quantity></Point>
    </Period>
  </TimeSeries>
</Publication_MarketDocument>"""


def zipped_market_response() -> bytes:
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("allocation.xml", MARKET_RESPONSE)
        archive.writestr("allocation-extra.xml", MARKET_RESPONSE.replace("series-1", "series-2"))
    return output.getvalue()


async def test_day_ahead_pages_count_time_series_and_return_polars():
    offsets = []

    def handle_request(request):
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        prices = range(100) if offset == 0 else [100]
        series = "".join(
            f"""<TimeSeries><mRID>{price}</mRID>
                <currency_Unit.name>EUR</currency_Unit.name><price_Measure_Unit.name>MWH</price_Measure_Unit.name>
                <Period><timeInterval><start>2024-12-31T23:00Z</start></timeInterval><resolution>PT60M</resolution>
                <Point><position>1</position><price.amount>{price}</price.amount></Point>
                </Period></TimeSeries>"""
            for price in prices
        )
        return httpx2.Response(200, text=f"<Publication_MarketDocument>{series}</Publication_MarketDocument>")

    client = EntsoeClient("test", transport=httpx2.MockTransport(handle_request))
    async with client.session:
        frame = await client.query_day_ahead_prices("FR", START, date(2025, 1, 2), include_currency=True)

    assert offsets == [0, 100]
    assert isinstance(frame, pl.DataFrame)
    assert frame.schema["timestamp"] == pl.Datetime("us", "UTC")
    assert frame.schema["price.amount"] == pl.Float64
    assert frame["timestamp"].first() == datetime(2024, 12, 31, 23, tzinfo=UTC)
    assert frame["price.amount"].to_list() == list(range(101))
    assert frame["currency_Unit.name"].unique().to_list() == ["EUR"]


@pytest.mark.parametrize(
    ("method", "args", "kwargs", "expected"),
    [
        (
            "query_congestion_income",
            ("FR", "BE", "A01"),
            {},
            {"documentType": "A25", "businessType": "B10", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_total_nominated_capacity",
            ("FR", "BE"),
            {},
            {"documentType": "A26", "businessType": "B08"},
        ),
        (
            "query_implicit_offered_transfer_capacity",
            ("FR", "BE", "A01", "A07"),
            {},
            {"documentType": "A31", "auction.Type": "A01", "contract_MarketAgreement.Type": "A07"},
        ),
        (
            "query_transfer_capacity_with_third_countries",
            ("FR", "BE", "A01", "A07"),
            {},
            {"documentType": "A94", "auction.Type": "A01", "contract_MarketAgreement.Type": "A07"},
        ),
        (
            "query_total_allocated_capacity",
            ("FR", "BE", "A01"),
            {},
            {"documentType": "A26", "businessType": "A29", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_explicit_offered_transfer_capacity",
            ("FR", "BE", "A02", "A07"),
            {},
            {"documentType": "A31", "auction.Type": "A02", "contract_MarketAgreement.Type": "A07"},
        ),
        (
            "query_transfer_capacity_use",
            ("FR", "BE", "A01"),
            {},
            {"documentType": "A25", "businessType": "B05", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_auction_revenue",
            ("FR", "BE", "A01"),
            {},
            {"documentType": "A25", "businessType": "B07", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_implicit_auction_net_positions",
            ("FR", "BE", "A01"),
            {},
            {"documentType": "A25", "businessType": "B09", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_flow_based_allocations",
            ("FR", "BE"),
            {},
            {"documentType": "B09", "processType": "A44"},
        ),
        (
            "query_flow_based_allocations",
            ("FR", "BE"),
            {"archive": True},
            {"documentType": "B09", "processType": "A32", "StorageType": "archive"},
        ),
        (
            "query_continuous_offered_transfer_capacity",
            ("FR", "BE", "A08", "A07"),
            {},
            {"DocumentType": "A31", "Auction.Type": "A08", "Contract_MarketAgreement.Type": "A07", "offset": "0"},
        ),
        (
            "query_energy_prices",
            ("FR", "BE", "A01"),
            {},
            {"documentType": "A44", "contract_MarketAgreement.type": "A01", "offset": "0"},
        ),
        (
            "query_cross_border_physical_flows",
            ("FR", "BE"),
            {},
            {"documentType": "A11"},
        ),
        (
            "query_forecast_transfer_capacity",
            ("FR", "BE", "A01"),
            {},
            {"documentType": "A61", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_commercial_schedules",
            ("FR", "BE"),
            {"contract_marketagreement_type": "A05"},
            {"documentType": "A09", "contract_MarketAgreement.Type": "A05"},
        ),
        (
            "query_commercial_schedule_net_positions",
            ("FR", "BE"),
            {},
            {"documentType": "A09", "businessType": "B09"},
        ),
        (
            "query_dc_link_intraday_transfer_limits",
            ("FR", "BE"),
            {},
            {"documentType": "A93"},
        ),
        (
            "query_congestion_management_costs",
            ("FR", "BE"),
            {},
            {"documentType": "A92"},
        ),
        (
            "query_internal_redispatch",
            ("FR", "BE"),
            {},
            {"documentType": "A63", "businessType": "A85"},
        ),
        (
            "query_cross_border_redispatch",
            ("FR", "BE"),
            {},
            {"documentType": "A63", "businessType": "A46"},
        ),
        (
            "query_expansion_dismantling_projects",
            ("FR", "BE"),
            {},
            {"documentType": "A90", "businessType": "B01"},
        ),
        (
            "query_countertrading",
            ("FR", "BE"),
            {},
            {"documentType": "A91"},
        ),
    ],
)
async def test_market_request_uses_catalogue_parameters(method, args, kwargs, expected):
    requests = []

    def handle_request(request):
        requests.append(request)
        if method == "query_flow_based_allocations":
            return httpx2.Response(200, content=zipped_market_response())
        return httpx2.Response(200, text=MARKET_RESPONSE)

    client = EntsoeClient("test-token", transport=httpx2.MockTransport(handle_request))
    async with client.session:
        frame = await getattr(client, method)(*args, **kwargs, start=START, end=END)

    assert isinstance(frame, pl.DataFrame)
    if method == "query_flow_based_allocations":
        assert frame.height == 2
    assert len(requests) == 1
    params = requests[0].url.params
    for key, value in expected.items():
        assert params[key] == value
    out_key = "Out_Domain" if "Out_Domain" in params else "out_Domain"
    in_key = "In_Domain" if "In_Domain" in params else "in_Domain"
    expected_in = "10YFR-RTE------C" if len(args) == 1 else "10YBE----------2"
    assert params[out_key] == "10YFR-RTE------C"
    assert params[in_key] == expected_in
    start_key = "PeriodStart" if "PeriodStart" in params else "periodStart"
    end_key = "PeriodEnd" if "PeriodEnd" in params else "periodEnd"
    assert params[start_key] == "202412312300"
    assert params[end_key] == "202501012300"
    if method == "query_flow_based_allocations" or method not in {
        "query_continuous_offered_transfer_capacity",
        "query_energy_prices",
    }:
        assert "offset" not in params


@pytest.mark.parametrize(
    ("method", "args", "kwargs", "expected"),
    [
        (
            "query_day_ahead_prices",
            ("FR",),
            {"sequence": 1},
            {
                "documentType": "A44",
                "contract_MarketAgreement.type": "A01",
                "classificationSequence_AttributeInstanceComponent.position": "1",
                "offset": "0",
            },
        ),
        (
            "query_intraday_prices",
            ("FR",),
            {"sequence": 2},
            {
                "documentType": "A44",
                "contract_MarketAgreement.type": "A07",
                "classificationSequence_AttributeInstanceComponent.position": "2",
                "offset": "0",
            },
        ),
        (
            "query_day_ahead_prices_local",
            ("FR",),
            {"sequence": 3},
            {
                "documentType": "A44",
                "contract_MarketAgreement.type": "A01",
                "classificationSequence_AttributeInstanceComponent.position": "3",
                "offset": "0",
            },
        ),
        (
            "query_net_position",
            ("FR",),
            {},
            {"documentType": "A25", "businessType": "B09", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_net_position",
            ("FR",),
            {"dayahead": False},
            {"documentType": "A25", "businessType": "B09", "contract_MarketAgreement.Type": "A07"},
        ),
        (
            "query_crossborder_flows",
            ("FR", "BE"),
            {},
            {"documentType": "A11"},
        ),
        (
            "query_scheduled_exchanges",
            ("FR", "BE"),
            {},
            {"documentType": "A09", "contract_MarketAgreement.Type": "A05"},
        ),
        (
            "query_scheduled_exchanges",
            ("FR", "BE"),
            {"dayahead": True},
            {"documentType": "A09", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_net_transfer_capacity_dayahead",
            ("FR", "BE"),
            {},
            {"documentType": "A61", "contract_MarketAgreement.Type": "A01"},
        ),
        (
            "query_net_transfer_capacity_weekahead",
            ("FR", "BE"),
            {},
            {"documentType": "A61", "contract_MarketAgreement.Type": "A02"},
        ),
        (
            "query_net_transfer_capacity_monthahead",
            ("FR", "BE"),
            {},
            {"documentType": "A61", "contract_MarketAgreement.Type": "A03"},
        ),
        (
            "query_net_transfer_capacity_yearahead",
            ("FR", "BE"),
            {},
            {"documentType": "A61", "contract_MarketAgreement.Type": "A04"},
        ),
        (
            "query_intraday_offered_capacity",
            ("FR", "BE"),
            {"implicit": True, "id_type": "IDCT"},
            {"DocumentType": "A31", "Auction.Type": "A08", "Contract_MarketAgreement.Type": "A07", "offset": "0"},
        ),
        (
            "query_intraday_offered_capacity",
            ("FR", "BE"),
            {"implicit": True, "id_type": "IDA2"},
            {
                "documentType": "A31",
                "auction.Type": "A01",
                "contract_MarketAgreement.Type": "A07",
                "ClassificationSequence_AttributeInstanceComponent.Position": "2",
            },
        ),
        (
            "query_intraday_offered_capacity",
            ("FR", "BE"),
            {"implicit": False},
            {"documentType": "A31", "auction.Type": "A02", "contract_MarketAgreement.Type": "A07"},
        ),
        (
            "query_offered_capacity",
            ("FR", "BE", "A03"),
            {},
            {"documentType": "A31", "auction.Type": "A01", "contract_MarketAgreement.Type": "A03", "offset": "0"},
        ),
        (
            "query_offered_capacity",
            ("FR", "BE", "A03"),
            {"implicit": False},
            {"documentType": "A25", "businessType": "B05", "contract_MarketAgreement.Type": "A03", "offset": "0"},
        ),
    ],
)
async def test_entsoe_py_aliases_use_the_expected_query(method, args, kwargs, expected):
    requests = []

    def handle_request(request):
        requests.append(request)
        return httpx2.Response(200, text=MARKET_RESPONSE)

    client = EntsoeClient("test-token", transport=httpx2.MockTransport(handle_request))
    async with client.session:
        call_args = args
        if method == "query_offered_capacity":
            call_args = (*args[:2], START, END, *args[2:])
            frame = await getattr(client, method)(*call_args, **kwargs)
        else:
            frame = await getattr(client, method)(*call_args, START, END, **kwargs)

    assert isinstance(frame, pl.DataFrame)
    assert len(requests) == 1
    params = requests[0].url.params
    for key, value in expected.items():
        assert params[key] == value
    out_key = "Out_Domain" if "Out_Domain" in params else "out_Domain"
    in_key = "In_Domain" if "In_Domain" in params else "in_Domain"
    expected_in = "10YFR-RTE------C" if len(args) == 1 else "10YBE----------2"
    assert params[out_key] == "10YFR-RTE------C"
    assert params[in_key] == expected_in
    start_key = "PeriodStart" if "PeriodStart" in params else "periodStart"
    end_key = "PeriodEnd" if "PeriodEnd" in params else "periodEnd"
    assert params[start_key] == "202412312300"
    assert params[end_key] == "202501012300"
