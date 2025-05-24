import re
from datetime import UTC, datetime, tzinfo

from dateutil.relativedelta import relativedelta
from pytz import timezone

from .exceptions import ParseError, TzNaiveError


def parse_datetime(date: datetime | str, target_tz: tzinfo | str = UTC) -> datetime:
    """Parse Datetime with timezone.

    :param datetime | str date: datetime to parse. If datetime, must be tz-aware
    :param tzinfo target_tz: target timezone, defaults to UTC
    :raises TzNaiveError:
    :return datetime: Datetime in format yyyyMMddHHmm
    """
    if isinstance(target_tz, str):
        target_tz = timezone(target_tz)
    if isinstance(date, datetime):
        if date.tzinfo is None:
            raise TzNaiveError
        date = date.astimezone(target_tz)
    elif isinstance(date, str):
        date = datetime.fromisoformat(date).astimezone(target_tz)
    return date


def parse_freq(freq: relativedelta | str) -> relativedelta:
    """Parse a time string e.g. (2h13m) into a relativedelta object.

    :param relativedelta | str freq: A relativedelta or a string identifying a duration. (eg. 2h13m)
    :return relativedelta:
    """
    if isinstance(freq, str):
        regex = re.compile(
            r"^((?P<years>[\.\d]+?)y)?((?P<months>[\.\d]+?)mo)?((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$"
        )
        parts = regex.match(freq)
        if parts is None:
            raise ParseError(
                f"Could not parse any time information from '{freq}'. Examples of valid strings: '8h', '2d8h5m20s','2m4s', '1y2mo'"
            )
        time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return relativedelta(**time_params)
