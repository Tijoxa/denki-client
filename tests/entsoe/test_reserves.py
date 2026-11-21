import httpx2
import polars as pl
import pytest

from denki_client import EntsoeClient
from denki_client.area import Area

START = "2025-01-01"
END = "2025-01-02"
PERIOD_START_UTC = "202412312300"
PERIOD_END_UTC = "202501012300"
FR = Area.FR.code


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs", "expected"),
    [
        (
            "query_contracted_reserve_volumes_prices",
            ("FR",),
            {"type_marketagreement_type": "A01"},
            {
                "documentType": "A81",
                "controlArea_Domain": FR,
                "businessType": "B95",
                "Type_MarketAgreement.Type": "A01",
                "offset": "0",
            },
        ),
        (
            "query_contracted_reserve_prices",
            ("FR", "A51", "A06"),
            {"psr_type": "B01", "offset": 100},
            {
                "documentType": "A81",
                "controlArea_Domain": FR,
                "businessType": "B95",
                "processType": "A51",
                "Type_MarketAgreement.Type": "A06",
                "psrType": "B01",
                "offset": "100",
            },
        ),
        (
            "query_contracted_reserve_prices_procured_capacity",
            (Area.FR, "A52", "A03"),
            {},
            {
                "documentType": "A81",
                "controlArea_Domain": FR,
                "businessType": "B95",
                "processType": "A52",
                "Type_MarketAgreement.Type": "A03",
                "offset": "0",
            },
        ),
        (
            "query_contracted_reserve_amount",
            ("FR", "A47", "A01"),
            {"offset": 200},
            {
                "documentType": "A81",
                "controlArea_Domain": FR,
                "businessType": "B95",
                "processType": "A47",
                "Type_MarketAgreement.Type": "A01",
                "offset": "200",
            },
        ),
        (
            "query_procured_balancing_capacity",
            ("FR", "A51"),
            {"type_marketagreement_type": "A06"},
            {
                "documentType": "A15",
                "area_Domain": FR,
                "processType": "A51",
                "Type_MarketAgreement.Type": "A06",
                "offset": "0",
            },
        ),
        (
            "query_fcr_total_capacity",
            ("FR",),
            {},
            {"documentType": "A26", "area_Domain": FR, "businessType": "A25"},
        ),
        (
            "query_fcr_capacity_shares",
            ("FR",),
            {"curve_type": "A01"},
            {"documentType": "A26", "area_Domain": FR, "businessType": "C23", "curveType": "A01"},
        ),
        (
            "query_fcr_sharing_between_synchronous_areas",
            ("FR",),
            {"offset": 100},
            {
                "documentType": "A26",
                "area_Domain": FR,
                "processType": "A52",
                "businessType": "C22",
                "offset": "100",
            },
        ),
        (
            "query_frr_rr_capacity_outlook",
            ("FR",),
            {},
            {"documentType": "A26", "area_Domain": FR, "processType": "A56", "businessType": "C76"},
        ),
        (
            "query_frr_rr_actual_capacity",
            ("FR",),
            {"curve_type": "A01"},
            {
                "documentType": "A26",
                "area_Domain": FR,
                "processType": "A56",
                "businessType": "C77",
                "curveType": "A01",
            },
        ),
        (
            "query_rr_capacity_outlook",
            ("FR",),
            {},
            {
                "documentType": "A26",
                "area_Domain": FR,
                "processType": "A46",
                "businessType": "C76",
                "offset": "0",
            },
        ),
        (
            "query_rr_actual_capacity",
            ("FR",),
            {"offset": 100},
            {
                "documentType": "A26",
                "area_Domain": FR,
                "processType": "A46",
                "businessType": "C77",
                "offset": "100",
            },
        ),
        (
            "query_balancing_expenses_income",
            ("FR",),
            {},
            {"documentType": "A87", "controlArea_Domain": FR},
        ),
        (
            "query_cross_zonal_balancing_capacity",
            ("FR", "DE"),
            {"type_marketagreement_type": "A06", "curve_type": "A01"},
            {
                "documentType": "A38",
                "processType": "A51",
                "Connecting_Domain": FR,
                "Acquiring_Domain": Area.DE.code,
                "Type_MarketAgreement.Type": "A06",
                "curveType": "A01",
            },
        ),
        (
            "query_permanent_hvdc_capacity_limits",
            ("FR", "DE"),
            {"registered_resource": "22WABC-TEST-----1", "curve_type": "A01"},
            {
                "documentType": "A99",
                "BusinessType": "B06",
                "processType": "A63",
                "Out_Domain": FR,
                "In_Domain": Area.DE.code,
                "registeredResource": "22WABC-TEST-----1",
                "curveType": "A01",
            },
        ),
        (
            "query_balancing_border_capacity_limits",
            ("FR", "DE"),
            {},
            {
                "documentType": "A31",
                "BusinessType": "A26",
                "processType": "A47",
                "Out_Domain": FR,
                "In_Domain": Area.DE.code,
            },
        ),
        (
            "query_shared_rr_frr_capacity",
            ("FR",),
            {"offset": 200},
            {
                "documentType": "A26",
                "BusinessType": "C22",
                "processType": "A51",
                "Area_Domain": FR,
                "curveType": "A01",
                "offset": "200",
            },
        ),
        (
            "query_exchanged_reserve_capacity",
            ("FR", "DE"),
            {"offset": 100},
            {
                "documentType": "A26",
                "processType": "A46",
                "BusinessType": "C21",
                "Acquiring_Domain": Area.DE.code,
                "Connecting_Domain": FR,
                "Offset": "100",
            },
        ),
    ],
)
async def test_reserve_queries_send_catalogue_parameters(method_name, args, kwargs, expected):
    requests = []

    def handle_request(request):
        requests.append(request)
        return httpx2.Response(
            400,
            text="""<Acknowledgement_MarketDocument>
                <Reason><code>999</code><text>No matching data found</text></Reason>
            </Acknowledgement_MarketDocument>""",
        )

    client = EntsoeClient("test", transport=httpx2.MockTransport(handle_request))
    async with client.session:
        result = await getattr(client, method_name)(
            *args,
            START,
            END,
            **kwargs,
        )

    assert result is None
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert dict(request.url.params) == {
        "securityToken": "test",
        "periodStart": PERIOD_START_UTC,
        "periodEnd": PERIOD_END_UTC,
        **expected,
    }


async def test_contracted_reserve_queries_keep_only_the_requested_measure():
    xml = """<Balancing_MarketDocument>
            <TimeSeries>
                <businessType>B95</businessType>
                <quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name>
                <currency_Unit.name>EUR</currency_Unit.name>
                <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
                <Period>
                    <timeInterval><start>2025-01-01T00:00Z</start><end>2025-01-01T01:00Z</end></timeInterval>
                    <resolution>PT60M</resolution>
                    <Point>
                        <position>1</position>
                        <quantity>12.5</quantity>
                        <procurement_Price.amount>3.25</procurement_Price.amount>
                    </Point>
                </Period>
            </TimeSeries>
        </Balancing_MarketDocument>"""
    client = EntsoeClient(
        "test",
        transport=httpx2.MockTransport(lambda _: httpx2.Response(200, text=xml)),
    )
    async with client.session:
        prices = await client.query_contracted_reserve_prices("FR", "A51", "A06", START, END)
        amount = await client.query_contracted_reserve_amount("FR", "A51", "A06", START, END)
        both = await client.query_contracted_reserve_prices_procured_capacity("FR", "A51", "A06", START, END)

    assert isinstance(prices, pl.DataFrame)
    assert prices["price.amount"].to_list() == [3.25]
    assert "quantity" not in prices.columns
    assert "procurement_price.amount" not in prices.columns
    assert isinstance(amount, pl.DataFrame)
    assert amount["quantity"].to_list() == [12.5]
    assert "price.amount" not in amount.columns
    assert "procurement_price.amount" not in amount.columns
    assert "procurement_Price.amount" not in amount.columns
    assert isinstance(both, pl.DataFrame)
    assert both.select("quantity", "price.amount").row(0) == (12.5, 3.25)


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    [
        (
            "query_contracted_reserve_volumes_prices",
            ("FR", START, END),
            {"type_marketagreement_type": "A05"},
        ),
        (
            "query_contracted_reserve_volumes_prices",
            ("FR", START, END),
            {"type_marketagreement_type": "A01", "process_type": "A60"},
        ),
        ("query_procured_balancing_capacity", ("FR", "A60", START, END), {}),
        ("query_fcr_total_capacity", ("FR", START, END), {"curve_type": "A02"}),
    ],
)
async def test_reserve_queries_reject_unsupported_filter_codes(method_name, args, kwargs):
    def handle_request(_):
        pytest.fail("An unsupported filter reached the transport")

    client = EntsoeClient(
        "test",
        transport=httpx2.MockTransport(handle_request),
    )
    async with client.session:
        with pytest.raises(ValueError):
            await getattr(client, method_name)(*args, **kwargs)
