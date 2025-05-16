import logging
import warnings
from datetime import datetime

import httpx
import narwhals as nw
from bs4 import XMLParsedAsHTMLWarning
from tenacity import retry, retry_unless_exception_type, stop_after_attempt, wait_fixed

from entsoe_client.area import Area, lookup_area
from entsoe_client.exceptions import (
    InvalidBusinessParameterError,
    InvalidParameterError,
    InvalidPSRTypeError,
    NoMatchingDataError,
    PaginationError,
    raise_response_error,
)
from entsoe_client.parsers import parse_datetime, parse_timeseries_generic_whole
from entsoe_client.schemas import DAY_AHEAD_SCHEMA
from entsoe_client.utils import documents_limited, split_query

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class Client:
    def __init__(self, api_key: str, backend: str, **httpx_client_kwargs) -> None:
        """Client to Entsoe API.

        :param str api_key: API key obtained by creating an account on the website.

        API doc: `https://documenter.getpostman.com/view/7009892/2s93JtP3F6`.
        """
        self.api_key = api_key
        self.base_url = "https://web-api.tp.entsoe.eu/api"
        self.session = httpx.AsyncClient(**httpx_client_kwargs)
        self.logger = logging.getLogger(__name__)
        self.backend = backend

    @retry(
        retry=retry_unless_exception_type(
            (
                PaginationError,
                NoMatchingDataError,
                InvalidPSRTypeError,
                InvalidBusinessParameterError,
                InvalidParameterError,
            )
        ),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
    )
    async def _base_request(self, params: dict, start_str: str, end_str: str) -> httpx.Response:
        """Base Request.

        :param dict params: parameters dictionnary. See documentation for more details.
        :param str start_str: Pattern yyyyMMddHHmm e.g. 201601010000. Considered timezones is the local one.
        :param str end_str: Pattern yyyyMMddHHmm e.g. 201601010000 Considered timezones is the local one.
        :return httpx.Response:
        """
        base_params = {
            "securityToken": self.api_key,
            "periodStart": start_str,
            "periodEnd": end_str,
        }
        params.update(base_params)
        self.logger.debug(f"Request with {params=}")
        response = await self.session.get(self.base_url, params=params)
        raise_response_error(response)
        return response

    @split_query("1y")
    @documents_limited(100)
    async def query_day_ahead_prices(
        self, country_code: Area | str, *, start: datetime | str, end: datetime | str, offset: int = 0
    ) -> nw.DataFrame | None:
        """Query day-ahead prices.

        :param datetime start: Start of the query. Must be tz-aware
        :param datetime end: End of the query. Must be tz-aware
        :return nw.DataFrame | None:
        """
        domain = lookup_area(country_code)
        params = {
            "documentType": "A44",
            "in_Domain": domain.code,
            "out_Domain": domain.code,
            "offset": offset,
        }
        start_str = parse_datetime(start, domain.tz)
        end_str = parse_datetime(end, domain.tz)
        response = await self._base_request(params, start_str, end_str)
        data_all = parse_timeseries_generic_whole(response, label="price.amount")
        if data_all == []:
            return None
        df = nw.concat([nw.from_dict(e, DAY_AHEAD_SCHEMA, backend=self.backend) for e in data_all], how="diagonal")
        return df
