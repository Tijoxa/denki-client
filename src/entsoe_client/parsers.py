import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta, tzinfo

import httpx
import narwhals as nw
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from pytz import timezone

from .exceptions import ParseError, TzNaiveError


def resolution_to_timedelta(res_text: str) -> timedelta | relativedelta:
    """Convert an Entsoe resolution to Timedelta or RelativeDelta.

    :param str res_text: rseolution text
    :raises NotImplementedError: raised an error if res_text is unrecognized
    :return timedelta | relativedelta:
    """
    resolutions = {
        "PT60M": timedelta(minutes=60),
        "P1Y": relativedelta(years=1),
        "PT15M": timedelta(minutes=15),
        "PT30M": timedelta(minutes=30),
        "P1D": timedelta(days=1),
        "P7D": timedelta(days=7),
        "P1M": relativedelta(months=1),
        "PT1M": timedelta(minutes=1),
    }
    delta = resolutions.get(res_text)
    if delta is None:
        raise NotImplementedError("Unrecognized Timedelta")
    return delta


def parse_timeseries_generic(
    soup: BeautifulSoup, label: str, *, period_name: str = "period", schema: nw.Schema | None, backend: str
) -> nw.DataFrame:
    """Parse Timeseries.

    :param BeautifulSoup soup:
    :param str label:
    :param str period_name: defaults to "period"
    :param str backend:
    :return DataFrame:
    """
    data = defaultdict(list)

    for period in soup.find_all(period_name):
        start_text = period.find("start").text
        start = datetime.fromisoformat(start_text)
        delta_text = period.find("resolution").text
        delta = resolution_to_timedelta(delta_text)
        for point in period.find_all("point"):
            value_text: str = point.find(label).text
            value = float(value_text)
            position = int(point.find("position").text)
            data[delta_text + "_timestamp"].append(start + (position - 1) * delta)
            data[delta_text + "_value"].append(value)

    return nw.from_dict(data, schema, backend=backend)


def parse_timeseries_generic_whole(
    response: httpx.Response, label: str, *, backend: str, schema: nw.Schema | None
) -> nw.DataFrame:
    """Parse whole Timeseries.

    :param httpx.Response response:
    :param str label: defaults to "quantity"
    :param str backend:
    :return DataFrame:
    """
    data_all = []
    soup = BeautifulSoup(response.text, "html.parser")
    for timeseries in soup.find_all("timeseries"):
        data = parse_timeseries_generic(timeseries, label=label, schema=schema, backend=backend)
        data_all.append(data)

    return nw.concat(data_all, how="diagonal")


def parse_datetime(date: datetime | str, target_tz: tzinfo | str = UTC) -> str:
    """Parse Datetime.

    :param datetime | str date: datetime to parse. If datetime, must be tz-aware
    :param tzinfo target_tz: target timezone, defaults to UTC
    :raises TzNaiveError:
    :return str: Datetime in format yyyyMMddHHmm
    """
    if isinstance(target_tz, str):
        target_tz = timezone(target_tz)
    if isinstance(date, datetime):
        if date.tzinfo is None:
            raise TzNaiveError
        date = date.astimezone(target_tz)
    elif isinstance(date, str):
        date = datetime.fromisoformat(date).astimezone(target_tz)
    return date.strftime("%Y%m%d%H%M")


def parse_freq(freq: str) -> relativedelta:
    """Parse a time string e.g. (2h13m) into a relativedelta object.

    :param str freq: A string identifying a duration. (eg. 2h13m)
    :return relativedelta:
    """
    regex = re.compile(
        r"^((?P<years>[\.\d]+?)y)?((?P<months>[\.\d]+?)m)?((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$"
    )
    parts = regex.match(freq)
    if parts is None:
        raise ParseError(
            f"Could not parse any time information from '{freq}'. Examples of valid strings: '8h', '2d8h5m20s','2m4s', '1y2m'"
        )
    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return relativedelta(**time_params)
