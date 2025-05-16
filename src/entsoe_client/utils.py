import logging
from datetime import UTC, datetime
from functools import wraps

import narwhals as nw
from dateutil.relativedelta import relativedelta

from entsoe_client.parsers import parse_freq

from .exceptions import NoMatchingDataError, PaginationError

logger = logging.getLogger(__name__)


def paginated(func):
    """Catches a PaginationError, splits the requested period in two and tries
    again. Finally it concatenates the results."""

    @wraps(func)
    async def pagination_wrapper(*args, start, end, **kwargs):
        try:
            frame: nw.DataFrame | None = await func(*args, start=start, end=end, **kwargs)

        except PaginationError:
            pivot = start + (end - start) / 2
            frame1: nw.DataFrame | None = await pagination_wrapper(*args, start=start, end=pivot, **kwargs)
            frame2: nw.DataFrame | None = await pagination_wrapper(*args, start=pivot, end=end, **kwargs)

            if frame1 is None and frame2 is None:
                frame = None
            else:
                frame = nw.concat([frame1, frame2], how="diagonal")
        return frame

    return pagination_wrapper


def documents_limited(n: int = 100):
    """Deals with calls where you cannot query more than n documents at a
    time, by offsetting per n documents."""

    def decorator(func):
        @wraps(func)
        async def documents_wrapper(*args, **kwargs):
            frames = []
            for offset in range(0, 4800 + n, n):
                try:
                    frame: nw.DataFrame | None = await func(*args, offset=offset, **kwargs)
                    if frame is not None:
                        frames.append(frame)

                except NoMatchingDataError:
                    logger.debug(f"NoMatchingDataError: for offset {offset}")
                    break

            if frames == []:
                logger.debug("All the data returned are void")

            df = nw.concat(frames, how="diagonal")
            return df

        return documents_wrapper

    return decorator


def yield_date_range(start: datetime | str, end: datetime | str, freq: relativedelta | str):
    """Create a date_range iterator from `start` to `end` at given frequency.

    :param datetime | str start: if str, must be isoformat.
    :param datetime | str end: if str, must be isoformat.
    :param relativedelta | str freq:
    :yield str: _start, isoformat
    :yield str: _end, isoformat
    """
    if isinstance(start, str):
        start = datetime.fromisoformat(start)
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
    if isinstance(end, str):
        end = datetime.fromisoformat(end)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)
    if isinstance(freq, str):
        freq = parse_freq(freq)

    current = start
    if current > end:
        yield start.isoformat(), end.isoformat()
        return

    while current < end:
        next_one = min(current + freq, end)
        yield current.isoformat(), next_one.isoformat()
        current = next_one


def split_query(freq: relativedelta | str):
    """Deals with calls where you cannot query more than a given frequency,
    by splitting the call up in blocks."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, start: datetime | str, end: datetime | str, **kwargs):
            blocks = yield_date_range(start, end, freq)
            frames = []
            for _start, _end in blocks:
                try:
                    frame: nw.DataFrame | None = await func(*args, start=_start, end=_end, **kwargs)
                    if frame is not None:
                        frames.append(frame)

                except NoMatchingDataError:
                    logger.debug(f"NoMatchingDataError: between {_start} and {_end}")

            if frames == []:
                logger.debug("All the data returned are void")

            df = nw.concat(frames, how="diagonal")
            return df

        return wrapper

    return decorator
