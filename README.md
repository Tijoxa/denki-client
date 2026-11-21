# denki-client
Asynchronous Python client for the [ENTSO-e API](https://documenter.getpostman.com/view/7009892/2s93JtP3F6) that returns Polars DataFrames.

## Installation
Install with uv from the [Tijoxa Python package index](https://tijoxa.github.io/python-packages/simple/). Add the index and source to your `pyproject.toml`.

```toml
[[tool.uv.index]]
name = "tijoxa"
url = "https://tijoxa.github.io/python-packages/simple/"
explicit = true

[tool.uv.sources]
denki-client = { index = "tijoxa" }
```

```bash
uv add denki-client
```

## Usage
```python
import asyncio
import os
from datetime import date

from denki_client import EntsoeClient

async def main():
    client = EntsoeClient(os.environ["API_KEY_ENTSOE"])
    async with client.session:
        return await client.query_day_ahead_prices(
            country_code="FR",
            start=date(2025, 1, 1),
            end=date(2025, 1, 2),
            include_currency=True,
        )


df = asyncio.run(main())
```

Use an HTTPX2 transport if you pass a custom `transport=` to `EntsoeClient`.

## API coverage

The [request catalogue](plan.md#catalogue-des-requêtes) lists the 77 ENTSO-E requests and the matching Python methods. `query_actual_total_load` accepts `request_mode="get"`, `"get_header"`, and `"post"`. `query_flow_based_allocations`, `query_balancing_energy_bids`, and `query_bid_availability_changes` accept `archive=True`.

Methods accept an `Area`, an area name such as `"FR"`, or an EIC. Pass `datetime.date` values or ISO date strings such as `"2025-01-01"` for `start` and `end`. Both bounds are local midnights and `end` is exclusive. Directed queries use the origin’s timezone. All-border queries use the selected country’s timezone for every border. Queries without a known area use UTC. The client converts these bounds to UTC for ENTSO-E and accounts for daylight saving time when splitting a request.

### Polars results

Queries return a `pl.DataFrame`, or `None` when ENTSO-E reports no matching data. Point rows carry their document, series and period metadata, including source units and currencies. Timestamps are UTC. On A03 curves, `timestamp` and `timestamp_end` bound a block, so a long block occupies one row. Additional XML fields are available in `xml_extra`, a `List(Struct)` of qualified paths, positions, values and attributes.

| Response family | Row grain |
| --- | --- |
| Load, generation, market, transmission and balancing | One row per point within a series and period |
| Outages | One row per availability point, with the asset, outage interval, document revision and status |
| Flow-based allocations | One row per point, with resource, constraint and contingency groups available in `xml_groups` |
| Production and generation unit master data | One row per series, with the resource identifiers and effective date |
| Other market information | One row per document, with its publication interval and reasons |

Periods and series with missing children retain a row with null child fields. `xml_groups` keeps repeated groups together so they can be explored independently.

The [entsoe-py compatibility methods](plan.md#compatibilité-avec-entsoe-py) accept the upstream keyword arguments. Results use Polars columns and UTC timestamps. Price and net-position methods accept explicit resolution options where listed. `query_load_and_forecast` joins by timestamp. `query_physical_crossborder_allborders` returns one column per available neighbour and a `sum` column. `per_hour=True` takes the first aligned result of each UTC hour.

Use `query_day_ahead_prices` for day-ahead prices and `query_installed_generation_capacity_per_unit` for installed capacity per unit.

### Historical activated balancing energy

`query_activated_balancing_energy` implements the historical A83 request. ENTSO-E discontinued this publication on 11 December 2025 and lists it under `TP_Legacy_Publications/ActivatedBalancingEnergy_17.1.E_r2` in the File Library. File Library downloads require their own bearer token. For recent data, use `query_aggregated_balancing_energy_bids` or `query_aggregated_bids` (R53), which return aggregated bid rows with their own filters. [ENTSO-E archive notice](https://transparencyplatform.zendesk.com/hc/en-us/articles/41986091964305-Archives-for-legacy-Transparency-Platform-publications) · [File Library guide](https://transparencyplatform.zendesk.com/hc/en-us/articles/35960137882129-File-Library-Guide)

## Tests

```bash
uv run pytest -m "not live"
uv run pytest -m live
```

Set `API_KEY_ENTSOE` for the `live` tests. Hishel stores raw XML and ZIP responses in the local pytest cache, so subsequent runs exercise the decoder and Polars projection from those responses. The API key is added at the network boundary and stays out of the cache. [Hishel HTTPX2 integration](https://hishel.com/httpx.html)

## Release wheels

Run **Release Python wheels** manually on `main` in GitHub Actions. The workflow tests and uploads wheels as run artifacts, then publishes them to a GitHub Release for the version in `pyproject.toml`.

Run **Release Python wheels to PyPI** manually on `main` to build, test, and publish the four wheels to PyPI with `uv publish`. The publish step uses the `PYPI_API_KEY_DENKI_CLIENT` repository secret.

## References
Inspired by [entsoe-py](https://github.com/EnergieID/entsoe-py) repository.
