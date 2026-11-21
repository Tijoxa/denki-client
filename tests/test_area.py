import httpx2

from denki_client import EntsoeClient
from denki_client.area import Area, Neighbours, lookup_area


async def test_area_lookup_uses_iceland_eic_in_requests():
    def handle_request(request):
        assert request.url.params["in_Domain"] == "10Y1001A1001A958"
        assert request.url.params["out_Domain"] == "10Y1001A1001A958"
        assert request.url.params["periodStart"] == "202501010000"
        return httpx2.Response(
            400,
            text="<Acknowledgement_MarketDocument><Reason><text>No matching data found</text></Reason>"
            "</Acknowledgement_MarketDocument>",
        )

    client = EntsoeClient("test", transport=httpx2.MockTransport(handle_request))
    async with client.session:
        assert await client.query_day_ahead_prices("is", "2025-01-01", "2025-01-02") is None


def test_northern_ireland_alias_resolves_to_existing_area_and_neighbours():
    area = lookup_area("nie")

    assert area is Area.GB_NIR is Area.NIE
    assert lookup_area("10Y1001A1001A016") is area
    assert Neighbours[area.name] is Neighbours.NIE
    assert "NIE" in Neighbours.IE.value
