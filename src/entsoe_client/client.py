import logging
import warnings
from datetime import datetime

import httpx
import narwhals as nw
from bs4 import XMLParsedAsHTMLWarning
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from entsoe_client._core import parse_timeseries_generic
from entsoe_client.area import Area, lookup_area
from entsoe_client.exceptions import raise_response_error
from entsoe_client.parsers import parse_datetime
from entsoe_client.schemas import DAY_AHEAD_SCHEMA
from entsoe_client.utils import documents_limited, inclusive, split_query

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class BaseClient:
    def __init__(self, api_key: str, backend: str, **httpx_client_kwargs) -> None:
        """Client to ENTSO-e API.

        :param str api_key: API key obtained by creating an account on the website.
        :param str backend: Narwhals's compatible backend.
        :param dict httpx_client_kwargs: Additional keyword arguments to pass to the httpx client.

        API doc: `https://documenter.getpostman.com/view/7009892/2s93JtP3F6`.
        """
        self.api_key = api_key
        self.base_url = "https://web-api.tp.entsoe.eu/api"
        self.session = httpx.AsyncClient(**httpx_client_kwargs)
        self.logger = logging.getLogger(__name__)
        self.backend = backend

    @retry(
        retry=retry_if_exception_type(httpx.ConnectError),
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
    @inclusive("1d", "left")
    async def query_day_ahead_prices(
        self, domain_code: str, *, start: datetime, end: datetime, offset: int
    ) -> nw.DataFrame | None:
        """Query day-ahead prices.

        :param str domain_code:
        :param datetime start: start of the query
        :param datetime end: end of the query
        :param int offset:
        :return nw.DataFrame | None:
        """
        params = {
            "documentType": "A44",
            "in_Domain": domain_code,
            "out_Domain": domain_code,
            "offset": offset,
        }
        start_str = start.strftime("%Y%m%d%H%M")
        end_str = end.strftime("%Y%m%d%H%M")
        response = await self._base_request(params, start_str, end_str)
        data_all = parse_timeseries_generic(response.text, "price.amount", "period")
        if data_all == {}:
            return None
        df = nw.from_dict(data_all, DAY_AHEAD_SCHEMA, backend=self.backend)
        return df


class Client(BaseClient):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    async def query_day_ahead_prices(
        self, country_code: Area | str, *, start: datetime | str, end: datetime | str
    ) -> nw.DataFrame | None:
        """Query day-ahead prices.

        :param  Area | str country_code:
        :param datetime | str start: start of the query
        :param datetime | str end: end of the query
        :return nw.DataFrame | None:
        """
        domain = lookup_area(country_code)
        start = parse_datetime(start, domain.tz)
        end = parse_datetime(end, domain.tz)
        return await super().query_day_ahead_prices(domain.code, start=start, end=end, offset=0)

    async def query_activated_balancing_energy_prices(
        self, country_code: Area | str, *, start: datetime | str, end: datetime | str
    ) -> nw.DataFrame | None: ...
