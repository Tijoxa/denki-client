import pytest

from entsoe_client.utils import yield_date_range


@pytest.fixture
def sample_dates():
    return {"start": "20230101", "end": "20250102", "freq": "1y"}


@pytest.mark.parametrize(
    "start, end, freq, expected",
    [
        (
            "20230101",
            "20250102",
            "1y",
            [
                ("2023-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"),
                ("2024-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"),
                ("2025-01-01T00:00:00+00:00", "2025-01-02T00:00:00+00:00"),
            ],
        ),
    ],
)
def test_yield_date_range(start, end, freq, expected):
    result = list(yield_date_range(start, end, freq))
    assert result == expected
