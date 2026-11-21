from __future__ import annotations

from datetime import date
from typing import Literal

import polars as pl

from denki_client.area import Area

from ._transport import _Transport, area_code

_AREA_PARAMETERS = {
    "Acquiring_Domain",
    "area_Domain",
    "BiddingZone_Domain",
    "Connecting_Domain",
    "ControlArea_Domain",
    "in_Domain",
    "In_Domain",
    "out_Domain",
    "Out_Domain",
}


class MarketQueries(_Transport):
    """Market and transmission query methods for :class:`EntsoeClient`."""

    async def _market_query(
        self,
        document_type: str,
        start: date | str,
        end: date | str,
        *,
        max_window: str = "1y",
        page_size: int | None = None,
        offset: int | None = None,
        offset_key: str = "offset",
        max_units: int | None = None,
        document_key: str = "documentType",
        start_key: str = "periodStart",
        end_key: str = "periodEnd",
        **params: Area | str | int | None,
    ) -> pl.DataFrame | None:
        wire_params = {
            key: area_code(value)
            if isinstance(value, Area) or (isinstance(value, str) and key in _AREA_PARAMETERS)
            else value
            for key, value in params.items()
        }
        # ENTSO-E returns whole delivery days (or a longer delivery period) for these publications.
        whole_period = document_type in {"A25", "A31", "A44", "A94", "B09"} or (
            document_type == "A26" and params.get("businessType") == "A29"
        )
        return await self._query(
            document_type,
            start,
            end,
            max_window=max_window,
            page_size=page_size,
            offset=offset,
            offset_key=offset_key,
            max_units=max_units,
            document_key=document_key,
            start_key=start_key,
            end_key=end_key,
            allow_subday=not whole_period,
            wire_params=wire_params,
        )

    async def query_congestion_income(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A25",
            start,
            end,
            businessType="B10",
            **{
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "curveType": curve_type,
            },
        )

    async def query_total_nominated_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A26",
            start,
            end,
            businessType="B08",
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            curveType=curve_type,
        )

    async def query_implicit_offered_transfer_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        auction_type: str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        update_date_and_or_time: str | None = None,
        classification_sequence_position: int | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A31",
            start,
            end,
            **{
                "auction.Type": auction_type,
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "Update_DateAndOrTime": update_date_and_or_time,
                "ClassificationSequence_AttributeInstanceComponent.Position": classification_sequence_position,
                "curveType": curve_type,
            },
        )

    async def query_transfer_capacity_with_third_countries(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        auction_type: str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        auction_category: str | None = None,
        classification_sequence_position: int | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A94",
            start,
            end,
            **{
                "auction.Type": auction_type,
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "auction.Category": auction_category,
                "classificationSequence_AttributeInstanceComponent.Position": classification_sequence_position,
                "curveType": curve_type,
            },
        )

    async def query_total_allocated_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        auction_category: str | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A26",
            start,
            end,
            **{
                "businessType": "A29",
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "auction.Category": auction_category,
                "curveType": curve_type,
            },
        )

    async def query_explicit_offered_transfer_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        auction_type: str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        auction_category: str | None = None,
        update_date_and_or_time: str | None = None,
        classification_sequence_position: int | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A31",
            start,
            end,
            **{
                "auction.Type": auction_type,
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "auction.Category": auction_category,
                "Update_DateAndOrTime": update_date_and_or_time,
                "ClassificationSequence_AttributeInstanceComponent.Position": classification_sequence_position,
                "curveType": curve_type,
            },
        )

    async def query_transfer_capacity_use(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        auction_category: str | None = None,
        classification_sequence_position: int | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A25",
            start,
            end,
            **{
                "businessType": "B05",
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "Auction.Category": auction_category,
                "ClassificationSequence_AttributeInstanceComponent.Position": classification_sequence_position,
                "curveType": curve_type,
            },
        )

    async def query_auction_revenue(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A25",
            start,
            end,
            **{
                "businessType": "B07",
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "curveType": curve_type,
            },
        )

    async def query_implicit_auction_net_positions(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A25",
            start,
            end,
            **{
                "businessType": "B09",
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "curvetype": curve_type,
            },
        )

    async def query_flow_based_allocations(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        archive: bool = False,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "B09",
            start,
            end,
            processType="A32" if archive else "A44",
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            StorageType="archive" if archive else None,
        )

    async def query_continuous_offered_transfer_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        auction_type: str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        update_date_and_or_time: str | None = None,
        curve_type: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A31",
            start,
            end,
            page_size=100,
            offset=offset,
            document_key="DocumentType",
            start_key="PeriodStart",
            end_key="PeriodEnd",
            **{
                "Auction.Type": auction_type,
                "Out_Domain": country_code_from,
                "In_Domain": country_code_to,
                "Contract_MarketAgreement.Type": contract_marketagreement_type,
                "Update_DateAndOrTime": update_date_and_or_time,
                "curveType": curve_type,
            },
        )

    async def query_energy_prices(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        contract_marketagreement_type: str,
        *,
        start: date | str,
        end: date | str,
        classification_sequence_position: int | None = None,
        offset: int = 0,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A44",
            start,
            end,
            page_size=100,
            offset=offset,
            **{
                "contract_MarketAgreement.type": contract_marketagreement_type,
                "classificationSequence_AttributeInstanceComponent.position": classification_sequence_position,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "curveType": curve_type,
            },
        )

    async def query_cross_border_physical_flows(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._physical_flows(country_code_from, country_code_to, start, end, curve_type=curve_type)

    async def _physical_flows(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        *,
        calendar_area: Area | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._query(
            "A11",
            start,
            end,
            calendar_area=calendar_area,
            out_Domain=area_code(country_code_from),
            in_Domain=area_code(country_code_to),
            curveType=curve_type,
        )

    async def query_forecast_transfer_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        contract_marketagreement_type: str = "A01",
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A61",
            start,
            end,
            **{
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "curveType": curve_type,
            },
        )

    async def query_commercial_schedules(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        contract_marketagreement_type: str | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A09",
            start,
            end,
            **{
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "curveType": curve_type,
            },
        )

    async def query_commercial_schedule_net_positions(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        contract_marketagreement_type: str | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A09",
            start,
            end,
            **{
                "businessType": "B09",
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
                "curveType": curve_type,
            },
        )

    async def query_dc_link_intraday_transfer_limits(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A93",
            start,
            end,
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            curveType=curve_type,
        )

    async def query_congestion_management_costs(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A92",
            start,
            end,
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            curveType=curve_type,
        )

    async def query_internal_redispatch(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A63",
            start,
            end,
            businessType="A85",
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            curveType=curve_type,
        )

    async def query_cross_border_redispatch(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A63",
            start,
            end,
            businessType="A46",
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            curveType=curve_type,
        )

    async def query_expansion_dismantling_projects(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        business_type: str = "B01",
        doc_status: str | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A90",
            start,
            end,
            businessType=business_type,
            DocStatus=doc_status,
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            curveType=curve_type,
        )

    async def query_countertrading(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        return await self._market_query(
            "A91",
            start,
            end,
            out_Domain=country_code_from,
            in_Domain=country_code_to,
            curveType=curve_type,
        )

    async def query_day_ahead_prices(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        offset: int = 0,
        sequence: int | None = None,
        resolution: str | None = None,
        include_currency: bool = False,
    ) -> pl.DataFrame | None:
        frame = await self.query_energy_prices(
            country_code,
            country_code,
            "A01",
            start=start,
            end=end,
            classification_sequence_position=sequence,
            offset=offset,
        )
        return self._with_price_compatibility(frame, include_currency, resolution)

    async def query_intraday_prices(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        sequence: int,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        return await self.query_energy_prices(
            country_code,
            country_code,
            "A07",
            start=start,
            end=end,
            classification_sequence_position=sequence,
            offset=offset,
        )

    async def query_day_ahead_prices_local(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        sequence: int,
        resolution: str = "60min",
    ) -> pl.DataFrame | None:
        frame = await self.query_energy_prices(
            country_code,
            country_code,
            "A01",
            start=start,
            end=end,
            classification_sequence_position=sequence,
        )
        return self._with_price_compatibility(frame, False, resolution)

    async def query_net_position(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        dayahead: bool = True,
        resolution: str | None = None,
    ) -> pl.DataFrame | None:
        frame = await self.query_implicit_auction_net_positions(
            country_code,
            country_code,
            "A01" if dayahead else "A07",
            start=start,
            end=end,
        )
        if frame is None:
            return None
        return self._with_resolution(frame, resolution)

    async def query_crossborder_flows(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
    ) -> pl.DataFrame | None:
        return await self.query_cross_border_physical_flows(country_code_from, country_code_to, start=start, end=end)

    async def query_scheduled_exchanges(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        dayahead: bool = False,
    ) -> pl.DataFrame | None:
        return await self.query_commercial_schedules(
            country_code_from,
            country_code_to,
            start=start,
            end=end,
            contract_marketagreement_type="A01" if dayahead else "A05",
        )

    async def query_net_transfer_capacity_dayahead(
        self, country_code_from: Area | str, country_code_to: Area | str, start: date | str, end: date | str
    ) -> pl.DataFrame | None:
        return await self.query_forecast_transfer_capacity(
            country_code_from, country_code_to, "A01", start=start, end=end
        )

    async def query_net_transfer_capacity_weekahead(
        self, country_code_from: Area | str, country_code_to: Area | str, start: date | str, end: date | str
    ) -> pl.DataFrame | None:
        return await self.query_forecast_transfer_capacity(
            country_code_from, country_code_to, "A02", start=start, end=end
        )

    async def query_net_transfer_capacity_monthahead(
        self, country_code_from: Area | str, country_code_to: Area | str, start: date | str, end: date | str
    ) -> pl.DataFrame | None:
        return await self.query_forecast_transfer_capacity(
            country_code_from, country_code_to, "A03", start=start, end=end
        )

    async def query_net_transfer_capacity_yearahead(
        self, country_code_from: Area | str, country_code_to: Area | str, start: date | str, end: date | str
    ) -> pl.DataFrame | None:
        return await self.query_forecast_transfer_capacity(
            country_code_from, country_code_to, "A04", start=start, end=end
        )

    async def query_intraday_offered_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        implicit: bool = True,
        id_type: Literal["IDCT", "IDA1", "IDA2", "IDA3"] = "IDCT",
    ) -> pl.DataFrame | None:
        if id_type not in {"IDCT", "IDA1", "IDA2", "IDA3"}:
            raise ValueError("id_type must be one of 'IDCT', 'IDA1', 'IDA2', or 'IDA3'")

        if implicit and id_type == "IDCT":
            return await self.query_continuous_offered_transfer_capacity(
                country_code_from,
                country_code_to,
                "A08",
                "A07",
                start=start,
                end=end,
            )

        auction_type = "A01" if implicit else "A02"
        method = (
            self.query_implicit_offered_transfer_capacity if implicit else self.query_explicit_offered_transfer_capacity
        )
        sequence = {"IDA1": 1, "IDA2": 2, "IDA3": 3}.get(id_type) if implicit else None
        return await method(
            country_code_from,
            country_code_to,
            auction_type,
            "A07",
            start=start,
            end=end,
            classification_sequence_position=sequence,
        )

    async def query_offered_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        contract_marketagreement_type: str,
        implicit: bool = True,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        if implicit:
            return await self._market_query(
                "A31",
                start,
                end,
                page_size=100,
                offset=offset,
                **{
                    "auction.Type": "A01",
                    "contract_MarketAgreement.Type": contract_marketagreement_type,
                    "out_Domain": country_code_from,
                    "in_Domain": country_code_to,
                },
            )
        return await self._market_query(
            "A25",
            start,
            end,
            page_size=100,
            offset=offset,
            **{
                "businessType": "B05",
                "contract_MarketAgreement.Type": contract_marketagreement_type,
                "out_Domain": country_code_from,
                "in_Domain": country_code_to,
            },
        )

    @staticmethod
    def _with_price_compatibility(
        frame: pl.DataFrame | None, include_currency: bool, resolution: str | None
    ) -> pl.DataFrame | None:
        if frame is None:
            return None
        frame = MarketQueries._with_resolution(frame, resolution)
        if frame is None:
            return None
        if not include_currency and "currency_Unit.name" in frame.columns:
            frame = frame.drop("currency_Unit.name")
        return frame

    @staticmethod
    def _with_resolution(frame: pl.DataFrame, resolution: str | None) -> pl.DataFrame:
        if resolution is None or "resolution" not in frame.columns:
            return frame
        normalized = {
            "15MIN": "PT15M",
            "30MIN": "PT30M",
            "60MIN": "PT60M",
            "15T": "PT15M",
            "30T": "PT30M",
            "60T": "PT60M",
        }.get(resolution.upper(), resolution.upper())
        return frame.filter(pl.col("resolution") == normalized)
