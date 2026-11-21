import logging
from datetime import datetime
from functools import wraps
from typing import Literal

import polars as pl
from dateutil.relativedelta import relativedelta

from denki_client.area import Area, lookup_area
from denki_client.parsers import parse_datetime, parse_freq

from .exceptions import NoMatchingDataError, PaginationError

logger = logging.getLogger(__name__)


def parse_inputs(func):
    """Parses function inputs.

    - area: `Area` | `str` -> `Area`
    - start: `datetime` | `str` -> `str`
    - end: `datetime` | `str` -> `str`
    """

    @wraps(func)
    async def parse_inputs_wrapper(self, area: Area | str, *args, start: datetime | str, end: datetime | str, **kwargs):
        area = lookup_area(area)
        start = parse_datetime(start, area.tz)
        end = parse_datetime(end, area.tz)
        return await func(self, area, *args, start=start, end=end, **kwargs)

    return parse_inputs_wrapper


def paginated(func):
    """Catches a PaginationError, splits the requested period in two and tries
    again. Finally it concatenates the results.
    """

    @wraps(func)
    async def pagination_wrapper(*args, start: datetime, end: datetime, **kwargs):
        try:
            return await func(*args, start=start, end=end, **kwargs)

        except PaginationError:
            pivot = start + (end - start) / 2
            left = await pagination_wrapper(*args, start=start, end=pivot, **kwargs)
            right = await pagination_wrapper(*args, start=pivot, end=end, **kwargs)
            frames = [frame for frame in (left, right) if frame is not None]
            return pl.concat(frames, how="diagonal") if frames else None

    return pagination_wrapper


def documents_limited(n: int = 100):
    """Deals with calls where you cannot query more than n documents at a
    time, by offsetting per `n` documents. Function needs `offset` kwarg.

    :param int n: maximum number of documents to query, defaults to 100.
    """

    def decorator(func):
        @wraps(func)
        async def documents_wrapper(*args, **kwargs):
            frames: list[pl.DataFrame] = []
            for _offset in range(0, 4800 + n, n):
                try:
                    frame = await func(*args, offset=_offset, **kwargs)
                    if frame is not None:
                        frames.append(frame)

                except NoMatchingDataError:
                    logger.debug(f"NoMatchingDataError: for offset {_offset}")
                    break

            if not frames:
                logger.debug("All the data returned are void")
                return None

            return pl.concat(frames, how="diagonal")

        return documents_wrapper

    return decorator


def yield_date_range(start: datetime, end: datetime, freq: relativedelta):
    """Create a date_range iterator from `start` to `end` at given frequency.

    :param datetime start:
    :param datetime end:
    :param relativedelta freq:
    :yield datetime: _start
    :yield datetime: _end
    """

    _t0 = start
    while _t0 < end:
        _t1 = min(_t0 + freq, end)
        yield _t0, _t1
        _t0 = _t1


def split_query(freq: relativedelta | str):
    """Deals with calls where you cannot query more than a given frequency,
    by splitting the call up in blocks. Function needs `start` and `end` kwargs.

    :param relativedelta | str freq: split frequency compatible with `parse_freq`
    """
    freq: relativedelta = parse_freq(freq)

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, start: datetime, end: datetime, **kwargs):
            blocks = yield_date_range(start, end, freq)
            frames: list[pl.DataFrame] = []
            for _start, _end in blocks:
                try:
                    frame = await func(*args, start=_start, end=_end, **kwargs)
                    if frame is not None:
                        frames.append(frame)

                except NoMatchingDataError:
                    logger.debug(f"NoMatchingDataError: between {_start} and {_end}")

            if not frames:
                logger.debug("All the data returned are void")
                return None

            return pl.concat(frames, how="diagonal")

        return wrapper

    return decorator


def inclusive(resolution: relativedelta | str, closed: Literal["both", "left", "right", "neither"]):
    """Truncate `start` and `end` arguments for calls.

    :param relativedelta | str resolution:
    :param Literal["both", "left", "right", "neither"] closed: where the interval is closed
    """
    resolution = parse_freq(resolution)
    resolution: relativedelta

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, start: datetime, end: datetime, **kwargs):
            match closed:
                case "both":
                    _start = start
                    _end = end
                case "left":
                    _start = start
                    _end = end - resolution
                case "right":
                    _start = start + resolution
                    _end = end
                case "neither":
                    _start = start + resolution
                    _end = end - resolution
            return await func(*args, start=_start, end=_end, **kwargs)

        return wrapper

    return decorator
