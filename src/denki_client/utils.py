from collections.abc import Iterator
from datetime import date, datetime

from dateutil.relativedelta import relativedelta


def yield_date_range[Boundary: (datetime, date)](
    start: Boundary, end: Boundary, freq: relativedelta
) -> Iterator[tuple[Boundary, Boundary]]:
    """Yield contiguous half-open windows, preserving dates or timestamps."""
    if not isinstance(start, datetime):
        freq = freq.normalized()
        if any((freq.hours, freq.minutes, freq.seconds, freq.microseconds)) or any(
            part is not None for part in (freq.hour, freq.minute, freq.second, freq.microsecond)
        ):
            raise ValueError("Date windows require a whole-day frequency")
    while start < end:
        next_start = min(start + freq, end)
        if next_start <= start:
            raise ValueError("The window frequency must advance the start")
        yield start, next_start
        start = next_start
