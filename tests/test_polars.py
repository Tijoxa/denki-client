from datetime import UTC, datetime

import httpx2
import polars as pl
import pytest

from denki_client import EntsoeClient
from denki_client.area import Area, Neighbours, lookup_area


async def test_day_ahead_pages_return_polars():
    offsets = []

    def handle_request(request):
        offset = request.url.params["offset"]
        offsets.append(offset)
        if offset == "200":
            return httpx2.Response(400, text="<text>No matching data found</text>")

        day = "01" if offset == "0" else "02"
        return httpx2.Response(
            200,
            text=f"""<Publication_MarketDocument>
                <TimeSeries>
                    <currency_Unit.name>EUR</currency_Unit.name>
                    <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
                    <period>
                        <timeInterval><start>2025-01-{day}T00:00Z</start></timeInterval>
                        <resolution>PT60M</resolution>
                        <Point><position>1</position><price.amount>52</price.amount></Point>
                        <Point><position>2</position><price.amount>53.25</price.amount></Point>
                    </period>
                </TimeSeries>
            </Publication_MarketDocument>""",
        )

    client = EntsoeClient("test", transport=httpx2.MockTransport(handle_request))
    async with client.session:
        df = await client.query_day_ahead_price("FR", start="20250101", end="20250104")

    assert offsets == ["0", "100", "200"]
    assert isinstance(df, pl.DataFrame)
    assert df.schema["timestamp"] == pl.Datetime("us", "UTC")
    assert df.schema["price.amount"] == pl.Float64
    assert df.schema["resolution"] == pl.Enum(["PT60M", "PT30M", "PT15M"])
    assert df["timestamp"].to_list() == [datetime(2025, 1, day, hour, tzinfo=UTC) for day in (1, 2) for hour in (0, 1)]
    assert df["price.amount"].to_list() == [52.0, 53.25, 52.0, 53.25]


@pytest.mark.parametrize("direction_code, direction", [("A01", "Up"), ("A03", "Symmetric")])
async def test_balancing_volume_decodes_codes(direction_code, direction):
    response = httpx2.Response(
        200,
        text=f"""<Publication_MarketDocument>
            <TimeSeries>
                <flowDirection.direction>{direction_code}</flowDirection.direction>
                <businessType>A96</businessType>
                <quantity_Measure_Unit.name>MWH</quantity_Measure_Unit.name>
                <period>
                    <timeInterval><start>2025-01-01T00:00Z</start></timeInterval>
                    <resolution>PT15M</resolution>
                    <Point><position>1</position><quantity>12.5</quantity></Point>
                </period>
            </TimeSeries>
        </Publication_MarketDocument>""",
    )
    client = EntsoeClient("test", transport=httpx2.MockTransport(lambda _: response))
    async with client.session:
        df = await client.query_activated_balancing_energy_volume("FR", start="20250101", end="20250102")

    assert isinstance(df, pl.DataFrame)
    assert df.select("flowDirection.direction", "businessType", "quantity").row(0) == (direction, "aFRR", 12.5)


async def test_area_lookup_uses_iceland_eic_in_requests():
    def handle_request(request):
        assert request.url.params["in_Domain"] == "10Y1001A1001A958"
        assert request.url.params["out_Domain"] == "10Y1001A1001A958"
        return httpx2.Response(400, text="<text>No matching data found</text>")

    client = EntsoeClient("test", transport=httpx2.MockTransport(handle_request))
    async with client.session:
        assert await client.query_day_ahead_price("is", start="20250101", end="20250102") is None


def test_northern_ireland_alias_resolves_to_existing_area_and_neighbours():
    area = lookup_area("nie")

    assert area is Area.GB_NIR is Area.NIE
    assert lookup_area("10Y1001A1001A016") is area
    assert Neighbours[area.name] is Neighbours.NIE
    assert "NIE" in Neighbours.IE.value


@pytest.mark.parametrize("query", ["query_day_ahead_price", "query_actual_generation_per_production_type"])
async def test_no_matching_data_returns_none(query):
    transport = httpx2.MockTransport(lambda _: httpx2.Response(400, text="<text>No matching data found</text>"))
    client = EntsoeClient("test", transport=transport)
    async with client.session:
        assert await getattr(client, query)("FR", start="20250101", end="20250103") is None
