import os

import narwhals as nw
import pytest
from httpx_limiter import AsyncRateLimitedTransport, Rate

from denki_client.entsoe import EntsoeClient


@pytest.fixture(scope="session")
def client():
    return EntsoeClient(
        os.environ["API_KEY_ENTSOE"],
        backend="polars",
        transport=AsyncRateLimitedTransport.create(rate=Rate.create(magnitude=1)),
    )


async def test_query_day_ahead_prices(client: EntsoeClient):
    df = await client.query_day_ahead_prices("FR", start="20250101", end="20250103")
    assert isinstance(df, nw.DataFrame)


async def test_query_activated_balancing_energy_prices(client: EntsoeClient):
    df = await client.query_activated_balancing_energy_prices("FR", "A16", "A95", start="20250101", end="20250103")
    assert isinstance(df, nw.DataFrame)


async def test_query_installed_capacity_per_production_type(client: EntsoeClient):
    df = await client.query_installed_capacity_per_production_type("FR", "B01", start="20250101", end="20250103")
    assert isinstance(df, nw.DataFrame)


async def test_query_actual_generation_per_production_type(client: EntsoeClient):
    df = await client.query_actual_generation_per_production_type("FR", "B01", start="20250101", end="20250103")
    assert isinstance(df, nw.DataFrame)
