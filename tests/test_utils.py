from datetime import UTC, date, datetime

import pytest
from dateutil.relativedelta import relativedelta

from denki_client.utils import yield_date_range


@pytest.mark.parametrize(
    "start, end, freq, expected",
    [
        (
            date(2024, 2, 29),
            date(2025, 3, 2),
            relativedelta(years=1),
            [(date(2024, 2, 29), date(2025, 2, 28)), (date(2025, 2, 28), date(2025, 3, 2))],
        ),
        (
            date(2024, 1, 31),
            date(2024, 3, 1),
            relativedelta(months=1),
            [(date(2024, 1, 31), date(2024, 2, 29)), (date(2024, 2, 29), date(2024, 3, 1))],
        ),
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


@pytest.mark.parametrize(
    ("freq", "message"),
    [(relativedelta(hours=12), "whole-day frequency"), (relativedelta(), "must advance")],
)
def test_date_windows_require_a_frequency_that_advances_by_whole_days(freq, message):
    with pytest.raises(ValueError, match=message):
        list(yield_date_range(date(2025, 1, 1), date(2025, 1, 3), freq))
