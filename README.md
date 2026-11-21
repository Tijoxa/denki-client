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
from denki_client import EntsoeClient

async def main():
    client = EntsoeClient("API_KEY_ENTSOE")
    async with client.session:
        return await client.query_day_ahead_price("FR", start="20250101", end="20250201")


df = asyncio.run(main())
```

Use an HTTPX2 transport if you pass a custom `transport=` to `EntsoeClient`.

## Features
- Asynchronous HTTP requests with [HTTPX2](https://pydantic.dev/docs/httpx2/).
- Native [Polars](https://pola.rs/) DataFrames, or `None` when there is no matching data.

## Release wheels

Run **Release Python wheels** manually on `main` in GitHub Actions. The workflow tests and uploads wheels as run artifacts, then publishes them to a GitHub Release for the version in `pyproject.toml`.

Run **Release Python wheels to PyPI** manually on `main` to build, test, and publish the four wheels to PyPI with `uv publish`. The publish step uses the `PYPI_API_KEY_DENKI_CLIENT` repository secret.

## References
Inspired by [entsoe-py](https://github.com/EnergieID/entsoe-py) repository.
