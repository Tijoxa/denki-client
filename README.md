# denki-client
Python client to retreive data from ENTSO-e API.

## Installation
Install it from [PyPI](https://pypi.org/project/denki-client/):

```bash
pip install denki-client
```

## Usage
```python
import asyncio
from denki_client import EntsoeClient

client = EntsoeClient("API_KEY_ENTSOE", backend="polars")

df = asyncio.run(client.query_day_ahead_prices("FR", start="20250101", end="20250201"))
df.to_native()
```

## Features
- asynchronous client (use of [httpx](https://github.com/encode/httpx))
- agnostic DataFrame library (use of [narwhals](https://github.com/narwhals-dev/narwhals))

## References
Inspired by [entsoe-py](https://github.com/EnergieID/entsoe-py) repository.
