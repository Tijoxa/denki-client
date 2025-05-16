import logging
from datetime import datetime
from functools import wraps
from typing import Literal

import narwhals as nw
from dateutil.relativedelta import relativedelta

from entsoe_client.parsers import parse_freq

from .exceptions import NoMatchingDataError, PaginationError

logger = logging.getLogger(__name__)


def paginated(func):
    """Catches a PaginationError, splits the requested period in two and tries
    again. Finally it concatenates the results."""

    @wraps(func)
    async def pagination_wrapper(*args, start: datetime, end: datetime, **kwargs):
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
    time, by offsetting per `n` documents.

    :param int n: maximum number of documents to query, defaults to 100"""

    def decorator(func):
        @wraps(func)
        async def documents_wrapper(*args, _offset: int, **kwargs):
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
    by splitting the call up in blocks.

    :param relativedelta | str freq: split frequency compatible with `parse_freq`
    """
    freq = parse_freq(freq)
    freq: relativedelta

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, start: datetime, end: datetime, **kwargs):
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


def inclusive(granularity: relativedelta | str, closed: Literal["both", "left", "right", "neither"] = "left"):
    """Truncate `start` and `end` arguments for calls.

    :param relativedelta | str granularity:
    :param Literal["both", "left", "right", "neither"] closed: where the interval is closed, defaults to "left"
    """
    granularity = parse_freq(granularity)
    granularity: relativedelta

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, start: datetime, end: datetime, **kwargs):
            match closed:
                case "both":
                    _start = start
                    _end = end
                case "left":
                    _start = start
                    _end = end - granularity
                case "right":
                    _start = start + granularity
                    _end = end
                case "neither":
                    _start = start + granularity
                    _end = end - granularity
            return await func(*args, start=_start, end=_end, **kwargs)

        return wrapper

    return decorator
