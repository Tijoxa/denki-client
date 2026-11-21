import os

import polars as pl
import pytest

from denki_client.entsoe import EntsoeClient

pytestmark = pytest.mark.live


@pytest.fixture(scope="session")
async def client():
    api_key = os.environ.get("API_KEY_ENTSOE")
    if not api_key:
        pytest.skip("API_KEY_ENTSOE is required for live ENTSO-e tests")

    from httpx_limiter import Rate
    from httpx_limiter.aiolimiter import AiolimiterAsyncLimiter
    from httpx_limiter.httpx2 import AsyncRateLimitedTransport

    limiter = AiolimiterAsyncLimiter.create(Rate.create(magnitude=1))
    client = EntsoeClient(
        api_key,
        transport=AsyncRateLimitedTransport.create(limiter=limiter),
    )
    async with client.session:
        yield client


async def test_query_day_ahead_price(client: EntsoeClient):
    df = await client.query_day_ahead_price("FR", start="20250101", end="20250103")
    assert isinstance(df, pl.DataFrame)


async def test_query_activated_balancing_energy_price(client: EntsoeClient):
    df = await client.query_activated_balancing_energy_price("FR", "A16", "A95", start="20250101", end="20250103")
    assert isinstance(df, pl.DataFrame)


async def test_query_activated_balancing_energy_volume(client: EntsoeClient):
    df = await client.query_activated_balancing_energy_volume("FR", "A16", "A95", start="20250101", end="20250103")
    assert isinstance(df, pl.DataFrame)


async def test_query_actual_generation_per_production_type(client: EntsoeClient):
    df = await client.query_actual_generation_per_production_type("FR", "B01", start="20250101", end="20250103")
    assert isinstance(df, pl.DataFrame)


async def test_query_actual_generation_per_generation_unit(client: EntsoeClient):
    df = await client.query_actual_generation_per_generation_unit("FR", "B01", start="20250101", end="20250103")
    assert isinstance(df, pl.DataFrame)


async def test_query_installed_capacity_per_production_type(client: EntsoeClient):
    df = await client.query_installed_capacity_per_production_type("FR", "B01", start="20240101", end="20250103")
    assert isinstance(df, pl.DataFrame)


async def test_query_installed_capacity_per_production_unit(client: EntsoeClient):
    df = await client.query_installed_capacity_per_production_unit("FR", "B01", start="20240101", end="20250103")
    assert isinstance(df, pl.DataFrame)
