from datetime import UTC, datetime

import pytest
from dateutil.relativedelta import relativedelta

from entsoe_client.utils import yield_date_range


@pytest.mark.parametrize(
    "start, end, freq, expected",
    [
        (
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 2, tzinfo=UTC),
            relativedelta(years=1),
            [
                (datetime(2023, 1, 1, tzinfo=UTC), datetime(2024, 1, 1, tzinfo=UTC)),
                (datetime(2024, 1, 1, tzinfo=UTC), datetime(2025, 1, 1, tzinfo=UTC)),
                (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 1, 2, tzinfo=UTC)),
            ],
        ),
        (
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 3, 2, tzinfo=UTC),
            relativedelta(months=1),
            [
                (datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 2, 1, tzinfo=UTC)),
                (datetime(2023, 2, 1, tzinfo=UTC), datetime(2023, 3, 1, tzinfo=UTC)),
                (datetime(2023, 3, 1, tzinfo=UTC), datetime(2023, 3, 2, tzinfo=UTC)),
            ],
        ),
        (
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 1, 3, 23, tzinfo=UTC),
            relativedelta(days=1),
            [
                (datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 1, 2, tzinfo=UTC)),
                (datetime(2023, 1, 2, tzinfo=UTC), datetime(2023, 1, 3, tzinfo=UTC)),
                (datetime(2023, 1, 3, tzinfo=UTC), datetime(2023, 1, 3, 23, tzinfo=UTC)),
            ],
        ),
        (
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 1, 1, 2, 45, tzinfo=UTC),
            relativedelta(hours=1),
            [
                (datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, 1, tzinfo=UTC)),
                (datetime(2023, 1, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, 2, tzinfo=UTC)),
                (datetime(2023, 1, 1, 2, tzinfo=UTC), datetime(2023, 1, 1, 2, 45, tzinfo=UTC)),
            ],
        ),
        (
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 1, 1, 0, 2, 45, tzinfo=UTC),
            relativedelta(minutes=1),
            [
                (datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, 0, 1, tzinfo=UTC)),
                (datetime(2023, 1, 1, 0, 1, tzinfo=UTC), datetime(2023, 1, 1, 0, 2, tzinfo=UTC)),
                (datetime(2023, 1, 1, 0, 2, tzinfo=UTC), datetime(2023, 1, 1, 0, 2, 45, tzinfo=UTC)),
            ],
        ),
        (
            datetime(2023, 1, 1, tzinfo=UTC),
            datetime(2023, 1, 1, 0, 0, 2, tzinfo=UTC),
            relativedelta(seconds=1),
            [
                (datetime(2023, 1, 1, tzinfo=UTC), datetime(2023, 1, 1, 0, 0, 1, tzinfo=UTC)),
                (datetime(2023, 1, 1, 0, 0, 1, tzinfo=UTC), datetime(2023, 1, 1, 0, 0, 2, tzinfo=UTC)),
            ],
        ),
    ],
)
def test_yield_date_range(start, end, freq, expected):
    result = list(yield_date_range(start, end, freq))
    assert result == expected
