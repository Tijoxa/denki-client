import asyncio
import os

import pytest

from entsoe_client.client import Client


@pytest.fixture
def client():
    return Client(os.getenv(("API_KEY_ENTSOE")), backend="polars")


def test_query_day_ahead_prices(client: Client):
    _df = asyncio.run(client.query_day_ahead_prices("FR", start="20250101", end="20250103"))
