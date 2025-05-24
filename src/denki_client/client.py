import logging
from datetime import datetime
from types import ModuleType
from typing import Literal

import httpx
import narwhals as nw
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from denki_client._core import parse_timeseries_generic
from denki_client.area import Area
from denki_client.exceptions import raise_response_error
from denki_client.schemas import ACTIVATED_BALANCING_ENERGY_PRICES_SCHEMA, DAY_AHEAD_SCHEMA
from denki_client.utils import documents_limited, inclusive, parse_inputs, split_query


class Client:
    def __init__(self, api_key: str, backend: ModuleType | nw.Implementation | str, **httpx_client_kwargs) -> None:
        """Client to ENTSO-e API.

        :param str api_key: API key obtained by creating an account on the website.
        :param ModuleType | Implementation | str backend: Narwhals's compatible backend.
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
        :param str start_str: Pattern yyyyMMddHHmm e.g. 201601010000. Considered timezone is the local one.
        :param str end_str: Pattern yyyyMMddHHmm e.g. 201601010000 Considered timezone is the local one.
        :return httpx.Response:
        """
        base_params = {
            "securityToken": self.api_key,
            "periodStart": start_str,
            "periodEnd": end_str,
        }
        params.update(base_params)
        params = {k: v for k, v in params.items() if v is not None}
        self.logger.debug(f"Request with {params=}")
        response = await self.session.get(self.base_url, params=params)
        raise_response_error(response)
        return response

    def _prepare_inputs(
        self, country_code: Area | str, start: datetime | str, end: datetime | str
    ) -> tuple[str, str, str]:
        if isinstance(country_code, str):
            raise TypeError(f"{type(country_code)=} instead of Area. Consider using the `parse_inputs` decorator.")

        if isinstance(start, str) or isinstance(end, str):
            raise TypeError(
                f"(type(start), type(end)) = ({type(start)}, {type(end)}) instead of (datetime, datetime). Consider using the `parse_inputs` decorator."
            )

        start_str = start.strftime("%Y%m%d%H%M")
        end_str = end.strftime("%Y%m%d%H%M")
        return country_code.code, start_str, end_str

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
        :return nw.DataFrame | None: DataFrame with the following columns:
        - timestamp: in UTC
        - price.amount: in €/MWh
        - resolution
        """
        domain_code, start_str, end_str = self._prepare_inputs(country_code, start, end)

        params = {
            "documentType": "A44",
            "in_Domain": domain_code,
            "out_Domain": domain_code,
            "contract_MarketAgreement.type": "A01",
            "offset": offset,
        }
        response = await self._base_request(params, start_str, end_str)
        data = parse_timeseries_generic(
            response.text,
            ["price.amount"],
            [],
            "period",
        )
        if data == {}:
            return None
        df = nw.from_dict(data, DAY_AHEAD_SCHEMA, backend=self.backend)
        return df

    @parse_inputs
    @split_query("1y")
    async def query_activated_balancing_energy_prices(
        self,
        country_code: Area | str,
        process_type: Literal["A16", "A60", "A61", "A68", None],
        business_type: Literal["A95", "A96", "A97", "A98", None],
        *,
        start: datetime | str,
        end: datetime | str,
    ) -> nw.DataFrame | None:
        """Query activated balancing energy prices.

        API documentation: `https://documenter.getpostman.com/view/7009892/2s93JtP3F6#c301d91e-53ac-4aca-8e18-f29e9146c4a6`

        :param  Area | str country_code:
        :param Literal['A16', 'A60', 'A61', 'A68', None] process_type:
        - A16: Realised
        - A60: Scheduled activation mFRR
        - A61: Direct activation mFRR
        - A68: Local Selection aFRR
        - None: select all
        :param Literal['A95', 'A96', 'A97', 'A98', None] business_type:
        - A95: Frequency containment reserve
        - A96: Automatic frequency restoration reserve
        - A97: Manual frequency restoration reserve
        - A98: Replacement reserve
        - None: select all
        :param datetime | str start: start of the query
        :param datetime | str end: end of the query
        :return nw.DataFrame | None: DataFrame with the following columns:
        - timestamp: in UTC
        - activation_Price.amount: in €/MWh
        - flowDirection.direction: Up is A01 and Down is A02
        - businessType
        - resolution
        """
        domain_code, start_str, end_str = self._prepare_inputs(country_code, start, end)

        params = {
            "documentType": "A84",
            "processType": process_type,
            "controlArea_Domain": domain_code,
            "businessType": business_type,
            "psrType": None,
            "standardMarketProduct": None,
            "originalMarketProduct": None,
        }
        response = await self._base_request(params, start_str, end_str)
        data = parse_timeseries_generic(
            response.text,
            ["activation_Price.amount"],
            ["flowDirection.direction", "businessType"],
            "period",
        )
        if data == {}:
            return None
        df = nw.from_dict(data, ACTIVATED_BALANCING_ENERGY_PRICES_SCHEMA, backend=self.backend)
        return df
