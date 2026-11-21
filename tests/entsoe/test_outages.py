from __future__ import annotations

from datetime import UTC, datetime
from io import BytesIO
from zipfile import ZipFile

import httpx2
import polars as pl
import pytest

from denki_client import EntsoeClient
from denki_client.area import BsnType

START = "2025-01-01"
END = "2025-01-02"
TOKEN = "outage-test-token"


def market_document(document_type: str) -> bytes:
    root = "Configuration_MarketDocument" if document_type == "A95" else "Outage_MarketDocument"
    namespace = "urn:iec62325.351:tc57wg16:451-6:outagedocument:3:0"
    return f"""<e:{root} xmlns:e="{namespace}">
      <e:mRID>document-123</e:mRID>
      <e:revisionNumber>1</e:revisionNumber>
      <e:type>{document_type}</e:type>
      <e:createdDateTime>2025-01-01T00:00:00Z</e:createdDateTime>
      <e:docStatus><e:value>A05</e:value></e:docStatus>
      <e:TimeSeries><e:mRID>series-1</e:mRID><e:businessType>A53</e:businessType></e:TimeSeries>
    </e:{root}>""".encode()


def zipped_document(document_type: str) -> bytes:
    archive_bytes = BytesIO()
    with ZipFile(archive_bytes, "w") as archive:
        archive.writestr("document.xml", market_document(document_type))
    return archive_bytes.getvalue()


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs", "document_type", "expected_params", "archive"),
    [
        (
            "query_production_unit_unavailability",
            ("FR", START, END),
            {
                "business_type": "A54",
                "docstatus": "A05",
                "registered_resource": "FR0ABC",
                "periodstartupdate": "2025-01-03T00:00Z",
                "periodendupdate": "2025-01-04T00:00Z",
                "offset": 200,
            },
            "A77",
            {
                "BiddingZone_Domain": "10YFR-RTE------C",
                "BusinessType": "A54",
                "DocStatus": "A05",
                "RegisteredResource": "FR0ABC",
                "periodStart": "202412312300",
                "periodEnd": "202501012300",
                "PeriodStartUpdate": "202501030000",
                "PeriodEndUpdate": "202501040000",
                "offset": "200",
            },
            True,
        ),
        (
            "query_unavailability_of_generation_units",
            (None, START, END),
            {"mRID": "document-123", "offset": 400},
            "A80",
            {"mRID": "document-123", "periodStart": "202501010000", "periodEnd": "202501020000", "offset": "400"},
            True,
        ),
        (
            "query_aggregated_consumption_unit_unavailability",
            ("DE_LU", START, END),
            {"business_type": BsnType.A53, "curve_type": "A01"},
            "A76",
            {
                "BiddingZone_Domain": "10Y1001A1001A82H",
                "BusinessType": "A53",
                "curveType": "A01",
                "periodStart": "202412312300",
                "periodEnd": "202501012300",
            },
            False,
        ),
        (
            "query_unavailability_transmission",
            ("GB", "FR", START, END),
            {"business_type": "A54", "docstatus": "A09"},
            "A78",
            {
                "Out_Domain": "10YGB----------A",
                "In_Domain": "10YFR-RTE------C",
                "BusinessType": "A54",
                "DocStatus": "A09",
                "periodStart": "202501010000",
                "periodEnd": "202501020000",
            },
            True,
        ),
        (
            "query_transmission_unavailability_available_capacity",
            ("FR", START, END),
            {"asset_registered_resource_mrid": "FR0ABC", "offset": 200},
            "A78",
            {
                "ControlArea_Domain": "10YFR-RTE------C",
                "Asset_RegisteredResource.mRID": "FR0ABC",
                "periodStart": "202412312300",
                "periodEnd": "202501012300",
                "offset": "200",
            },
            True,
        ),
        (
            "query_transmission_unavailability_net_position_impact",
            ("FR", START, END),
            {"asset_registered_resource_mrid": "FR0ABC"},
            "A78",
            {
                "pTDF_Domain.mRID": "10YFR-RTE------C",
                "Asset_RegisteredResource.mRID": "FR0ABC",
                "periodStart": "202412312300",
                "periodEnd": "202501012300",
            },
            True,
        ),
        (
            "query_offshore_grid_unavailability",
            ("DE_LU",),
            {
                "periodstartupdate": "2025-01-03T00:00Z",
                "periodendupdate": "2025-01-04T00:00Z",
                "docstatus": "A05",
                "offset": 200,
            },
            "A79",
            {
                "BiddingZone_Domain": "10Y1001A1001A82H",
                "DocStatus": "A05",
                "PeriodStartUpdate": "202501030000",
                "PeriodEndUpdate": "202501040000",
                "offset": "200",
            },
            True,
        ),
        (
            "query_fall_backs",
            ("BE", START, END),
            {"docstatus": "A13", "offset": 100},
            "A53",
            {
                "ProcessType": "A51",
                "BusinessType": "C47",
                "BiddingZone_Domain": "10YBE----------2",
                "DocStatus": "A13",
                "periodStart": "202412312300",
                "periodEnd": "202501012300",
                "offset": "100",
            },
            True,
        ),
        (
            "query_production_and_generation_units",
            ("FR", datetime(2025, 1, 1, tzinfo=UTC)),
            {"psr_type": "B16"},
            "A95",
            {
                "BiddingZone_Domain": "10YFR-RTE------C",
                "BusinessType": "B11",
                "Implementation_DateAndOrTime": "2025-01-01",
                "psrType": "B16",
            },
            False,
        ),
        (
            "query_other_market_information",
            (None,),
            {
                "periodstartupdate": "2025-01-03T00:00Z",
                "periodendupdate": "2025-01-04T00:00Z",
                "docstatus": "A05",
                "mRID": "omi-123",
                "offset": 200,
            },
            "B47",
            {
                "PeriodStartUpdate": "202501030000",
                "PeriodEndUpdate": "202501040000",
                "DocStatus": "A05",
                "mRID": "omi-123",
                "Offset": "200",
            },
            True,
        ),
    ],
)
async def test_catalogue_queries_use_http_transport(method_name, args, kwargs, document_type, expected_params, archive):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(
            200,
            content=zipped_document(document_type) if archive else market_document(document_type),
        )

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        result = await getattr(client, method_name)(*args, **kwargs)

    assert len(requests) == 1
    params = requests[0].url.params
    assert params["securityToken"] == TOKEN
    for name, expected in expected_params.items():
        assert params[name] == expected
    assert isinstance(result, pl.DataFrame)
    assert result.height == 1
    assert result["document_type"][0] == document_type


async def test_reference_date_accepts_date_only_format():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx2.Response(200, content=market_document("A95"))

    client = EntsoeClient(TOKEN, transport=httpx2.MockTransport(handler))
    async with client.session:
        await client.query_production_and_generation_units("FR", "2025-01-01")

    assert requests[0].url.params["Implementation_DateAndOrTime"] == "2025-01-01"
    assert "periodStart" not in requests[0].url.params
    assert "periodEnd" not in requests[0].url.params
