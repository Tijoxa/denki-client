from __future__ import annotations

from datetime import date, datetime, time
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import httpx2
import polars as pl
import pytest
from pytz import UTC, timezone

from denki_client import EntsoeClient
from denki_client.area import lookup_area

TOKEN = "bids-test-token"
START = "2025-01-01"
END = "2025-01-02"

BALANCING = "urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:1"
RESERVE_BID = "urn:iec62325.351:tc57wg16:451-7:reservebiddocument:7:1"
BID_AVAILABILITY = "urn:iec62325.351:tc57wg16:451-n:bidavailabilitydocument:1:0"
IMBALANCE = "urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:4"
CRITERIA = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"


def wire_boundary(area: str, local_date: str) -> str:
    zone = timezone(lookup_area(area).tz)
    local_midnight = zone.localize(datetime.combine(date.fromisoformat(local_date), time.min))
    return local_midnight.astimezone(UTC).strftime("%Y%m%d%H%M")


def xml_document(root_name: str, namespace: str) -> bytes:
    return f"""<{root_name} xmlns="{namespace}">
      <mRID>test-document</mRID>
      <TimeSeries>
        <mRID>test-series</mRID>
        <process.processType>A63</process.processType>
        <businessType>A96</businessType>
        <quantity_Measure_Unit.name>MWH</quantity_Measure_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <currency_Unit.name>EUR</currency_Unit.name>
        <Period>
          <timeInterval><start>2025-01-01T00:00Z</start><end>2025-01-01T01:00Z</end></timeInterval>
          <resolution>PT60M</resolution>
          <Point><position>1</position><quantity>12.5</quantity><price.amount>18.25</price.amount></Point>
        </Period>
      </TimeSeries>
    </{root_name}>""".encode()


def zip_members(**members: bytes) -> bytes:
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return output.getvalue()


CASES = [
    pytest.param(
        "query_activated_balancing_energy_prices",
        ("TR", START, END),
        {"documentType": "A84", "processType": "A16", "controlArea_Domain": "10YTR-TEIAS----W"},
        "balancing",
        id="r47-activated-balancing-prices",
    ),
    pytest.param(
        "query_afrr_cross_border_marginal_prices",
        ("DE_50HZ", START, END, "A01"),
        {
            "documentType": "A84",
            "processType": "A67",
            "businessType": "A96",
            "Standard_MarketProduct": "A01",
            "controlArea_Domain": "10YDE-VE-------2",
        },
        "balancing",
        id="r48-afrr-cross-border-marginal-prices",
    ),
    pytest.param(
        "query_balancing_energy_bids",
        ("CZ", START, END),
        {"documentType": "A37", "businessType": "B74", "processType": "A47", "connecting_Domain": "10YCZ-CEPS-----N"},
        "reserve-bid",
        id="r49-balancing-bids",
    ),
    pytest.param(
        "query_balancing_energy_bids",
        ("BE", START, END),
        {
            "documentType": "A37",
            "businessType": "B74",
            "processType": "A47",
            "connecting_Domain": "10YBE----------2",
            "storageType": "archive",
        },
        "nested-archive",
        id="r50-balancing-bid-archives",
    ),
    pytest.param(
        "query_bid_availability_changes",
        ("DE_50HZ", START, END),
        {
            "documentType": "B45",
            "processType": "A47",
            "businessType": "C46",
            "Domain": "10YDE-VE-------2",
        },
        "bid-availability",
        id="r51-bid-availability-changes",
    ),
    pytest.param(
        "query_bid_availability_changes",
        ("DE_50HZ", START, END),
        {
            "documentType": "B45",
            "processType": "A47",
            "businessType": "C46",
            "Domain": "10YDE-VE-------2",
            "storageType": "archive",
        },
        "nested-archive-availability",
        id="r52-bid-availability-archives",
    ),
    pytest.param(
        "query_aggregated_bids",
        ("AT", START, END, "A51"),
        {"documentType": "A24", "processType": "A51", "area_Domain": "10YAT-APG------L"},
        "balancing",
        id="r53-aggregated-balancing-bids",
    ),
    pytest.param(
        "query_netted_exchanged_volumes",
        ("GB", "TR", START, END),
        {
            "documentType": "B17",
            "processType": "A63",
            "Acquiring_Domain": "10YGB----------A",
            "Connecting_Domain": "10YTR-TEIAS----W",
        },
        "archive",
        id="r54-netted-exchanged-volumes",
    ),
    pytest.param(
        "query_netted_exchanged_volumes_by_border",
        ("GB", "TR", START, END),
        {
            "documentType": "A30",
            "processType": "A60",
            "Acquiring_Domain": "10YGB----------A",
            "Connecting_Domain": "10YTR-TEIAS----W",
        },
        "balancing",
        id="r55-netted-exchanged-volumes-by-border",
    ),
    pytest.param(
        "query_elastic_demands",
        ("CZ", START, END),
        {
            "documentType": "A37",
            "businessType": "B75",
            "processType": "A47",
            "Acquiring_Domain": "10YCZ-CEPS-----N",
        },
        "reserve-bid",
        id="r56-elastic-demands",
    ),
    pytest.param(
        "query_imbalance_prices",
        ("NL", START, END),
        {"documentType": "A85", "controlArea_Domain": "10YNL----------L"},
        "archive",
        id="r72-imbalance-prices",
    ),
    pytest.param(
        "query_imbalance_volumes",
        ("AT", START, END),
        {"documentType": "A86", "controlArea_Domain": "10YAT-APG------L", "businessType": "A19"},
        "archive",
        id="r73-total-imbalance-volumes",
    ),
    pytest.param(
        "query_total_imbalance_volumes",
        ("AT", START, END),
        {"documentType": "A86", "controlArea_Domain": "10YAT-APG------L", "businessType": "A19"},
        "archive",
        id="r73-total-imbalance-volumes-catalogue-method",
    ),
    pytest.param(
        "query_current_balancing_state",
        ("HU", START, END),
        {"documentType": "A86", "businessType": "B33", "area_Domain": "10YHU-MAVIR----U"},
        "balancing",
        id="r74-current-balancing-state",
    ),
    pytest.param(
        "query_criteria_application_measurements",
        ("CZ", START, END),
        {"documentType": "A45", "processType": "A65", "area_domain": "10YCZ-CEPS-----N"},
        "criteria",
        id="r75-criteria-measurements",
    ),
]


def response_for(kind: str) -> bytes:
    if kind == "reserve-bid":
        return xml_document("ReserveBid_MarketDocument", RESERVE_BID)
    if kind == "bid-availability":
        return xml_document("BidAvailability_MarketDocument", BID_AVAILABILITY)
    if kind == "nested-archive":
        nested = zip_members(**{"offer.xml": xml_document("ReserveBid_MarketDocument", RESERVE_BID)})
        return zip_members(**{"offers.zip": nested})
    if kind == "nested-archive-availability":
        nested = zip_members(**{"availability.xml": xml_document("BidAvailability_MarketDocument", BID_AVAILABILITY)})
        return zip_members(**{"updates.zip": nested})
    if kind == "archive":
        body = xml_document("Balancing_MarketDocument", IMBALANCE)
        return zip_members(**{"data.xml": body})
    if kind == "criteria":
        return xml_document("Publication_MarketDocument", CRITERIA)
    return xml_document("Balancing_MarketDocument", BALANCING)


@pytest.mark.parametrize(("method_name", "args", "expected_params", "kind"), CASES)
async def test_balancing_catalogue_queries_use_documented_contract(method_name, args, expected_params, kind):
    requests = []

    def handler(request):
        requests.append(request)
        content_type = (
            "application/zip"
            if kind in {"nested-archive", "nested-archive-availability", "archive"}
            else "application/xml"
        )
        return httpx2.Response(200, content=response_for(kind), headers={"content-type": content_type})

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        kwargs = {"archive": True} if kind in {"nested-archive", "nested-archive-availability"} else {}
        result = await getattr(client, method_name)(*args, **kwargs)

    assert len(requests) == 1
    params = requests[0].url.params
    assert params["securityToken"] == TOKEN
    for key, value in expected_params.items():
        assert params[key] == value
    if method_name in {"query_netted_exchanged_volumes", "query_netted_exchanged_volumes_by_border"}:
        origin = args[1]
        start_date, end_date = args[2:4]
    else:
        origin = args[0]
        start_date, end_date = args[1:3]
    assert params["periodStart"] == wire_boundary(origin, start_date)
    assert params["periodEnd"] == wire_boundary(origin, end_date)
    assert result is not None
    assert isinstance(result, pl.DataFrame)
    assert result.height == 1
    assert result["quantity"].to_list() == [12.5]


async def test_aFRR_volume_query_uses_one_day_request_window():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, content=xml_document("Balancing_MarketDocument", BALANCING))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        await client.query_netted_exchanged_volumes("GB", "TR", "2025-01-01", "2025-01-03", process_type="A67")

    assert [(request.url.params["periodStart"], request.url.params["periodEnd"]) for request in requests] == [
        (wire_boundary("TR", "2025-01-01"), wire_boundary("TR", "2025-01-02")),
        (wire_boundary("TR", "2025-01-02"), wire_boundary("TR", "2025-01-03")),
    ]
    assert all(request.url.params["processType"] == "A67" for request in requests)


async def test_netted_exchanged_volumes_per_border_applies_aFRR_window_limit():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, content=xml_document("Balancing_MarketDocument", BALANCING))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        await client.query_netted_exchanged_volumes_by_border(
            "GB", "TR", "2025-01-01", "2025-01-03", process_type="A67"
        )

    assert len(requests) == 2
    assert all(request.url.params["documentType"] == "A30" for request in requests)
    assert all(request.url.params["processType"] == "A67" for request in requests)


async def test_historical_activated_balancing_energy_reads_official_a83_archive_xml():
    fixture = Path(__file__).parent / "fixtures" / "activated-balancing-energy-a83.xml"
    body = fixture.read_bytes()
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, content=body, headers={"content-type": "application/xml"})

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        result = await client.query_activated_balancing_energy(
            "DE_50HZ",
            "2013-12-18",
            "2013-12-19",
            "A95",
            psr_type="A04",
        )

    assert len(requests) == 1
    params = requests[0].url.params
    assert params["documentType"] == "A83"
    assert params["controlArea_Domain"] == "10YDE-VE-------2"
    assert params["periodStart"] == wire_boundary("DE_50HZ", "2013-12-18")
    assert params["periodEnd"] == wire_boundary("DE_50HZ", "2013-12-19")
    assert params["businessType"] == "A95"
    assert params["psrType"] == "A04"
    assert result is not None
    assert result.height == 8
    assert result["quantity"].to_list() == [100.0] * 8


async def test_aggregated_bids_reject_unsupported_process_type_before_request():
    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(lambda _: pytest.fail("unexpected request")))
    async with client.session:
        with pytest.raises(ValueError, match="process_type"):
            await client.query_aggregated_bids("AT", START, END, "A99")


async def test_activated_balancing_energy_prices_sends_all_named_optional_filters():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, content=xml_document("Balancing_MarketDocument", BALANCING))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        await client.query_activated_balancing_energy_prices(
            "FR",
            START,
            END,
            process_type="A60",
            psr_type="B01",
            business_type="A97",
            standard_market_product="A01",
            original_market_product="A02",
            export_type="A01",
            curve_type="A03",
        )

    params = requests[0].url.params
    assert params["processType"] == "A60"
    assert params["PsrType"] == "B01"
    assert params["businessType"] == "A97"
    assert params["Standard_MarketProduct"] == "A01"
    assert params["Original_MarketProduct"] == "A02"
    assert params["ExportType"] == "A01"
    assert params["curveType"] == "A03"


async def test_imbalance_volumes_exposes_resolution_only_when_requested():
    body = xml_document("Balancing_MarketDocument", IMBALANCE)
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, content=zip_members(**{"data.xml": body}))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        default = await client.query_imbalance_volumes("AT", START, END)
        detailed = await client.query_imbalance_volumes("AT", START, END, "A04", include_resolution=True)

    assert default is not None and "resolution" not in default.columns
    assert detailed is not None and detailed["resolution"].to_list() == ["PT60M"]
    assert requests[1].url.params["PsrType"] == "A04"


async def test_total_imbalance_volumes_accepts_business_and_curve_filters():
    requests = []
    body = xml_document("Balancing_MarketDocument", IMBALANCE)

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, content=zip_members(**{"data.xml": body}))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        await client.query_total_imbalance_volumes("AT", START, END, "A19", curve_type="A01")

    assert requests[0].url.params["documentType"] == "A86"
    assert requests[0].url.params["businessType"] == "A19"
    assert requests[0].url.params["curveType"] == "A01"


async def test_current_balancing_state_preserves_source_quantity_sign():
    body = xml_document("Balancing_MarketDocument", BALANCING)
    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(lambda _: httpx2.Response(200, content=body)))
    async with client.session:
        result = await client.query_current_balancing_state("HU", START, END)

    assert result is not None
    assert result["quantity"].to_list() == [12.5]
