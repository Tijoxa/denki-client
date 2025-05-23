import logging
import warnings
from datetime import datetime
from typing import Literal

import httpx
import narwhals as nw
from bs4 import XMLParsedAsHTMLWarning
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from entsoe_client._core import parse_timeseries_generic
from entsoe_client.area import Area
from entsoe_client.exceptions import raise_response_error
from entsoe_client.schemas import ACTIVATED_BALANCING_ENERGY_PRICES_SCHEMA, DAY_AHEAD_SCHEMA
from entsoe_client.utils import documents_limited, inclusive, parse_inputs, split_query

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


class Client:
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

    @parse_inputs
    @split_query("1y")
    @documents_limited(100)
    @inclusive("1d", "left")
    async def query_day_ahead_prices(
        self, country_code: Area | str, *, start: datetime | str, end: datetime | str, offset: int = 0
    ) -> nw.DataFrame | None:
        """Query day-ahead prices.

        API documentation: `https://documenter.getpostman.com/view/7009892/2s93JtP3F6#3b383df0-ada2-49fe-9a50-98b1bb201c6b`

        :param  Area | str country_code:
        :param datetime | str start: start of the query
        :param datetime | str end: end of the query
        :param int offset: defaults to 0
        :return nw.DataFrame | None:
        """
        if isinstance(country_code, str):
            raise TypeError(f"{type(country_code)=} instead of Area. Consider using the `parse_inputs` decorator.")

        if isinstance(start, str) or isinstance(end, str):
            raise TypeError(
                f"(type(start), type(end)) = ({type(start)}, {type(end)}) instead of (str, str). Consider using the `parse_inputs` decorator."
            )

        params = {
            "documentType": "A44",
            "in_Domain": country_code.code,
            "out_Domain": country_code.code,
            "contract_MarketAgreement.type": "A01",
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

    @parse_inputs
    @split_query("1y")
    @inclusive("1d", "left")
    async def query_activated_balancing_energy_prices(
        self,
        country_code: Area | str,
        process_type: Literal["A16", "A60", "A61", "A68"],
        business_type: Literal["A95", "A96", "A97", "A98"],
        psr_type: Literal["A04", "A05"],
        original_market_product: Literal["A02", "A04"],
        *,
        start: datetime | str,
        end: datetime | str,
    ) -> nw.DataFrame | None:
        """Query activated balancing energy prices.

        API documentation: `https://documenter.getpostman.com/view/7009892/2s93JtP3F6#c301d91e-53ac-4aca-8e18-f29e9146c4a6`

        :param  Area | str country_code:
        :param Literal['A16', 'A60', 'A61', 'A68'] process_type:
        - A16 = Realised
        - A60 = Scheduled activation mFRR
        - A61 = Direct activation mFRR
        - A68 = Local Selection aFRR
        :param Literal['A95', 'A96', 'A97', 'A98'] business_type:
        - A95 = Frequency containment reserve
        - A96 = Automatic frequency restoration reserve
        - A97 = Manual frequency restoration reserve
        - A98 = Replacement reserve
        :param Literal['A04', 'A05'] psr_type:
        - A04 = Generation
        - A05 = Load
        :param Literal['A02', 'A04'] original_market_product:
        - A02 = Specific
        - A04 = Local
        :param datetime | str start: start of the query
        :param datetime | str end: end of the query
        ...
        """
        if isinstance(country_code, str):
            raise TypeError(f"{type(country_code)=} instead of Area. Consider using the `parse_inputs` decorator.")

        if isinstance(start, str) or isinstance(end, str):
            raise TypeError(
                f"(type(start), type(end)) = ({type(start)}, {type(end)}) instead of (str, str). Consider using the `parse_inputs` decorator."
            )

        params = {
            "documentType": "A84",
            "processType": process_type,
            "controlArea_Domain": country_code.code,
            "businessType": business_type,
            "PsrType": psr_type,
            "Standard_MarketProduct": "A01",
            "Original_MarketProduct": original_market_product,
        }
        start_str = start.strftime("%Y%m%d%H%M")
        end_str = end.strftime("%Y%m%d%H%M")
        response = await self._base_request(params, start_str, end_str)
        data_all = parse_timeseries_generic(response.text, ..., ...)
        if data_all == {}:
            return None
        df = nw.from_dict(data_all, ACTIVATED_BALANCING_ENERGY_PRICES_SCHEMA, backend=self.backend)
        return df
