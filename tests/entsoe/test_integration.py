"""Small live ENTSO-E smoke suite backed by a persistent raw-response cache."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import date
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, quote_plus
from zipfile import ZipFile

import httpx2
import polars as pl
import pytest

from denki_client.entsoe import EntsoeClient

_CACHE_API_KEY = "denki-client-hishel-live-cache-key"
_IMBALANCE_NS = "urn:iec62325.351:tc57wg16:451-6:balancingdocument:4:4"


class _TokenBoundaryTransport(httpx2.AsyncBaseTransport):
    """Add the real API key only after the cache has inspected the request."""

    def __init__(self, api_key: str, transport: httpx2.AsyncBaseTransport | None = None) -> None:
        self._api_key = api_key
        self._transport = transport or httpx2.AsyncHTTPTransport()
        self.network_requests = 0
        self.cache_request_used_placeholder: bool | None = None

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        cache_key_token = request.url.params.get("securityToken")
        self.cache_request_used_placeholder = cache_key_token != self._api_key
        network_url = request.url.copy_set_param("securityToken", self._api_key)
        network_request = httpx2.Request(
            request.method,
            network_url,
            headers=request.headers,
            content=request.content,
            extensions=dict(request.extensions),
        )
        try:
            self.network_requests += 1
            response = await self._transport.handle_async_request(network_request)
        except httpx2.RequestError as exc:
            safe_message = str(exc)
            for secret in {self._api_key, quote(self._api_key, safe=""), quote_plus(self._api_key)}:
                safe_message = safe_message.replace(secret, "<redacted>")
            raise type(exc)(safe_message, request=request) from None

        # Preserve the raw response stream and ensure Hishel sees only the placeholder URL.
        response.request = request
        return response

    async def aclose(self) -> None:
        await self._transport.aclose()


@pytest.fixture(scope="session")
async def live_context(pytestconfig: pytest.Config) -> AsyncIterator[tuple[EntsoeClient, _TokenBoundaryTransport]]:
    api_key = os.environ.get("API_KEY_ENTSOE")
    if not api_key:
        pytest.skip("API_KEY_ENTSOE is required for live ENTSO-E tests")

    from hishel import AsyncSqliteStorage
    from hishel.httpx2 import AsyncCacheTransport

    cache_dir = Path(pytestconfig.cache.mkdir("hishel-entsoe-live"))
    database_path = cache_dir / "responses.sqlite"
    network_transport = _TokenBoundaryTransport(api_key)
    cache_transport = AsyncCacheTransport(
        next_transport=network_transport,
        storage=AsyncSqliteStorage(database_path=database_path),
    )
    client = EntsoeClient(_CACHE_API_KEY, transport=cache_transport)
    try:
        async with client.session:
            yield client, network_transport
    finally:
        # Keep this check silent: neither the token nor request URL is included in failures.
        for path in (database_path, Path(f"{database_path}-wal"), Path(f"{database_path}-shm")):
            if path.exists() and api_key.encode() in path.read_bytes():
                pytest.fail("The ENTSO-E API key was written to the persistent HTTP cache")


@pytest.fixture(scope="session")
async def client(live_context: tuple[EntsoeClient, _TokenBoundaryTransport]) -> EntsoeClient:
    return live_context[0]


def _assert_data(frame: pl.DataFrame | None) -> pl.DataFrame:
    assert isinstance(frame, pl.DataFrame)
    assert not frame.is_empty()
    assert {"timestamp", "quantity"} <= set(frame.columns)
    return frame


@pytest.mark.live
async def test_day_ahead_prices(
    live_context: tuple[EntsoeClient, _TokenBoundaryTransport],
) -> None:
    client, network_transport = live_context
    original = _assert_data(await client.query_day_ahead_prices("FR", date(2025, 1, 1), date(2025, 1, 2)))
    assert original["price.amount"].count() > 0
    requests_before_replay = network_transport.network_requests
    cached_frame = _assert_data(await client.query_day_ahead_prices("FR", date(2025, 1, 1), date(2025, 1, 2)))
    assert cached_frame.equals(original)
    assert network_transport.network_requests == requests_before_replay


@pytest.mark.live
async def test_actual_total_load(client: EntsoeClient) -> None:
    _assert_data(await client.query_actual_total_load("FR", start=date(2025, 1, 1), end=date(2025, 1, 2)))


@pytest.mark.live
async def test_actual_generation(client: EntsoeClient) -> None:
    frame = _assert_data(await client.query_generation("FR", date(2025, 1, 1), date(2025, 1, 2)))
    assert frame["psrType"].count() > 0


@pytest.mark.live
async def test_directed_physical_flows(client: EntsoeClient) -> None:
    frame = _assert_data(
        await client.query_cross_border_physical_flows("FR", "BE", start=date(2025, 1, 1), end=date(2025, 1, 2))
    )
    assert {"in_Domain.mRID", "out_Domain.mRID"} <= set(frame.columns)


@pytest.mark.live
async def test_imbalance_prices_zip_response(client: EntsoeClient) -> None:
    # R72's recorded Postman example is NL on 2023-11-01 and returns a ZIP.
    _assert_data(await client.query_imbalance_prices("NL", date(2023, 11, 1), date(2023, 11, 2)))


def _imbalance_zip() -> bytes:
    document = f"""<Balancing_MarketDocument xmlns="{_IMBALANCE_NS}">
      <mRID>cache-test-document</mRID>
      <TimeSeries>
        <mRID>cache-test-series</mRID>
        <businessType>A96</businessType>
        <Period>
          <timeInterval><start>2023-11-01T00:00Z</start><end>2023-11-01T01:00Z</end></timeInterval>
          <resolution>PT60M</resolution>
          <Point><position>1</position><quantity>42.5</quantity></Point>
        </Period>
      </TimeSeries>
    </Balancing_MarketDocument>""".encode()
    archive = BytesIO()
    with ZipFile(archive, "w") as zipped:
        zipped.writestr("imbalance.xml", document)
    return archive.getvalue()


async def test_zip_response_persists_and_replays_without_real_token(tmp_path: Path) -> None:
    from hishel import AsyncSqliteStorage
    from hishel.httpx2 import AsyncCacheTransport

    api_key = "offline-test-api-secret"
    database_path = tmp_path / "responses.sqlite"
    upstream_tokens_were_real: list[bool] = []

    def upstream(request: httpx2.Request) -> httpx2.Response:
        upstream_tokens_were_real.append(request.url.params.get("securityToken") == api_key)
        return httpx2.Response(200, content=_imbalance_zip(), headers={"content-type": "application/zip"})

    first_network = _TokenBoundaryTransport(api_key, transport=httpx2.MockTransport(upstream))
    first_cache = AsyncCacheTransport(
        next_transport=first_network,
        storage=AsyncSqliteStorage(database_path=database_path),
    )
    first_client = EntsoeClient(_CACHE_API_KEY, transport=first_cache)
    async with first_client.session:
        original = _assert_data(await first_client.query_imbalance_prices("NL", date(2023, 11, 1), date(2023, 11, 2)))

    assert upstream_tokens_were_real == [True]
    assert first_network.cache_request_used_placeholder is True
    assert first_network.network_requests == 1
    assert original["quantity"].to_list() == [42.5]

    def fail_if_network_is_used(_: httpx2.Request) -> httpx2.Response:
        pytest.fail("A persisted Hishel response was not replayed")

    replay_network = _TokenBoundaryTransport(api_key, transport=httpx2.MockTransport(fail_if_network_is_used))
    replay_cache = AsyncCacheTransport(
        next_transport=replay_network,
        storage=AsyncSqliteStorage(database_path=database_path),
    )
    replay_client = EntsoeClient(_CACHE_API_KEY, transport=replay_cache)
    async with replay_client.session:
        replayed = _assert_data(await replay_client.query_imbalance_prices("NL", date(2023, 11, 1), date(2023, 11, 2)))

    assert replay_network.network_requests == 0
    assert replayed.equals(original)
    for path in (database_path, Path(f"{database_path}-wal"), Path(f"{database_path}-shm")):
        if path.exists():
            assert api_key.encode() not in path.read_bytes()
