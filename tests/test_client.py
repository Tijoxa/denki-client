import asyncio
import os

import narwhals as nw
import pytest

from denki_client.client import Client


@pytest.fixture
def client():
    return Client(os.environ["API_KEY_ENTSOE"], backend="polars")


def test_query_day_ahead_prices(client: Client):
    df = asyncio.run(client.query_day_ahead_prices("FR", start="20250101", end="20250103"))
    assert isinstance(df, nw.DataFrame)


def test_query_activated_balancing_energy_prices(client: Client):
    df = asyncio.run(
        client.query_activated_balancing_energy_prices(
            "FR",
            "A16",
            "A95",
            start="20250101",
            end="20250103",
        )
    )
    assert isinstance(df, nw.DataFrame)
