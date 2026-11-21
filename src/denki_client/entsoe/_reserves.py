"""Queries for ENTSO-E reserve and balancing-capacity data."""

from __future__ import annotations

from datetime import date
from enum import Enum
from typing import Literal

import polars as pl

from denki_client.area import Area, MarketAgreementType, ProcessType, PsrType

from ._transport import _Transport, area_code

ContractedReserveProcess = Literal["A46", "A47", "A51", "A52"]
ProcuredCapacityProcess = Literal["A46", "A47", "A51", "A52"]
CurveType = Literal["A01", "A03"]
CONTRACTED_RESERVE_MARKET_AGREEMENTS = {"A01", "A02", "A03", "A04", "A06", "A13"}
CROSS_ZONAL_MARKET_AGREEMENTS = {"A01", "A02", "A06"}


def _validate_process_type(
    process_type: ProcessType | str | None,
    allowed: set[str],
    parameter: str,
) -> None:
    code = process_type.name if isinstance(process_type, ProcessType) else process_type
    if code is not None and code not in allowed:
        values = ", ".join(sorted(allowed))
        raise ValueError(f"{parameter} must be one of {values}")


def _validate_market_agreement_type(
    market_agreement_type: MarketAgreementType | str,
    allowed: set[str] | None = None,
) -> None:
    code = market_agreement_type.name if isinstance(market_agreement_type, Enum) else market_agreement_type
    valid_codes = allowed or set(MarketAgreementType.__members__)
    if code not in valid_codes:
        values = ", ".join(sorted(valid_codes))
        raise ValueError(f"type_marketagreement_type must be one of {values}")


def _validate_psr_type(psr_type: PsrType | str | None) -> None:
    code = psr_type.name if isinstance(psr_type, PsrType) else psr_type
    if code is not None and code not in PsrType.__members__:
        values = ", ".join(PsrType.__members__)
        raise ValueError(f"psr_type must be one of {values}")


def _validate_curve_type(curve_type: str | None) -> None:
    if curve_type is not None and curve_type not in {"A01", "A03"}:
        raise ValueError("curve_type must be 'A01' or 'A03'")


def _select_reserve_values(
    data: pl.DataFrame | None,
    *value_columns: str,
) -> pl.DataFrame | None:
    """Return one or both reserve measures with all shared metadata."""
    if data is None:
        return None

    all_value_columns = {"quantity", "price.amount", "procurement_price.amount", "procurement_Price.amount"}
    value_column_set = set(value_columns)
    return data.select(
        column for column in data.columns if column not in all_value_columns or column in value_column_set
    )


class ReserveQueries(_Transport):
    """Reserve query methods that use ``EntsoeClient._query`` for transport."""

    async def query_contracted_reserve_volumes_prices(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        *,
        type_marketagreement_type: MarketAgreementType | str,
        process_type: ProcessType | ContractedReserveProcess | str | None = None,
        psr_type: PsrType | str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query contracted reserve volumes and prices from R57."""
        _validate_market_agreement_type(type_marketagreement_type, CONTRACTED_RESERVE_MARKET_AGREEMENTS)
        _validate_process_type(process_type, {"A46", "A47", "A51", "A52"}, "process_type")
        _validate_psr_type(psr_type)
        return await self._query(
            "A81",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="series",
            offset=offset,
            controlArea_Domain=area_code(country_code),
            businessType="B95",
            processType=process_type,
            **{"Type_MarketAgreement.Type": type_marketagreement_type, "psrType": psr_type},
        )

    async def query_contracted_reserve_prices(
        self,
        country_code: Area | str,
        process_type: ProcessType | ContractedReserveProcess | str,
        type_marketagreement_type: MarketAgreementType | str,
        start: date | str,
        end: date | str,
        psr_type: PsrType | str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query contracted reserve prices from R57."""
        data = await self.query_contracted_reserve_volumes_prices(
            country_code,
            start,
            end,
            type_marketagreement_type=type_marketagreement_type,
            process_type=process_type,
            psr_type=psr_type,
            offset=offset,
        )
        return _select_reserve_values(data, "price.amount")

    async def query_contracted_reserve_prices_procured_capacity(
        self,
        country_code: Area | str,
        process_type: ProcessType | ContractedReserveProcess | str,
        type_marketagreement_type: MarketAgreementType | str,
        start: date | str,
        end: date | str,
        psr_type: PsrType | str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query contracted reserve quantities and prices from R57."""
        return await self.query_contracted_reserve_volumes_prices(
            country_code,
            start,
            end,
            type_marketagreement_type=type_marketagreement_type,
            process_type=process_type,
            psr_type=psr_type,
            offset=offset,
        )

    async def query_contracted_reserve_amount(
        self,
        country_code: Area | str,
        process_type: str,
        type_marketagreement_type: str,
        start: date | str,
        end: date | str,
        psr_type: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query contracted reserve amounts from R57."""
        data = await self.query_contracted_reserve_volumes_prices(
            country_code,
            start,
            end,
            type_marketagreement_type=type_marketagreement_type,
            process_type=process_type,
            psr_type=psr_type,
            offset=offset,
        )
        return _select_reserve_values(data, "quantity")

    async def query_procured_balancing_capacity(
        self,
        country_code: Area | str,
        process_type: ProcessType | ProcuredCapacityProcess | str,
        start: date | str,
        end: date | str,
        type_marketagreement_type: MarketAgreementType | str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query procured balancing capacity from R58."""
        _validate_process_type(process_type, {"A46", "A47", "A51", "A52"}, "process_type")
        if type_marketagreement_type is not None:
            _validate_market_agreement_type(type_marketagreement_type)
        return await self._query(
            "A15",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="series",
            offset=offset,
            area_Domain=area_code(country_code),
            processType=process_type,
            **{"Type_MarketAgreement.Type": type_marketagreement_type},
        )

    async def query_fcr_total_capacity(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query total FCR capacity from R59."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A26",
            start,
            end,
            max_window="1y",
            area_Domain=area_code(country_code),
            businessType="A25",
            curveType=curve_type,
        )

    async def query_fcr_capacity_shares(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query FCR capacity shares from R60."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A26",
            start,
            end,
            max_window="1y",
            area_Domain=area_code(country_code),
            businessType="C23",
            curveType=curve_type,
        )

    async def query_fcr_sharing_between_synchronous_areas(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        offset: int = 0,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query FCR sharing between synchronous areas from R61."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A26",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="series",
            offset=offset,
            area_Domain=area_code(country_code),
            processType="A52",
            businessType="C22",
            curveType=curve_type,
        )

    async def query_frr_rr_capacity_outlook(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
    ) -> pl.DataFrame | None:
        """Query FRR and RR capacity outlook from R62."""
        return await self._query(
            "A26",
            start,
            end,
            max_window="1y",
            area_Domain=area_code(country_code),
            processType="A56",
            businessType="C76",
        )

    async def query_frr_rr_actual_capacity(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query actual FRR and RR capacity from R63."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A26",
            start,
            end,
            max_window="1y",
            area_Domain=area_code(country_code),
            processType="A56",
            businessType="C77",
            curveType=curve_type,
        )

    async def query_rr_capacity_outlook(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query RR capacity outlook from R64."""
        return await self._query(
            "A26",
            start,
            end,
            max_window=None,
            offset=offset,
            area_Domain=area_code(country_code),
            processType="A46",
            businessType="C76",
        )

    async def query_rr_actual_capacity(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query actual RR capacity from R65."""
        return await self._query(
            "A26",
            start,
            end,
            max_window=None,
            offset=offset,
            area_Domain=area_code(country_code),
            processType="A46",
            businessType="C77",
        )

    async def query_balancing_expenses_income(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query balancing financial expenses and income from R66."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A87",
            start,
            end,
            max_window="1y",
            controlArea_Domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_cross_zonal_balancing_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        type_marketagreement_type: MarketAgreementType | str | None = None,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query cross-zonal balancing capacity allocation from R67."""
        if type_marketagreement_type is not None:
            _validate_market_agreement_type(type_marketagreement_type, CROSS_ZONAL_MARKET_AGREEMENTS)
        _validate_curve_type(curve_type)
        # ENTSO-E enforces the instance cap. An instance can span multiple series and files.
        return await self._query(
            "A38",
            start,
            end,
            max_window="1y",
            processType="A51",
            Connecting_Domain=area_code(country_code_from),
            Acquiring_Domain=area_code(country_code_to),
            **{
                "Type_MarketAgreement.Type": type_marketagreement_type,
                "curveType": curve_type,
            },
        )

    async def query_permanent_hvdc_capacity_limits(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        registered_resource: str | None = None,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query permanent HVDC capacity limitations from R68."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A99",
            start,
            end,
            max_window="1y",
            page_unit="series",
            max_units=100,
            BusinessType="B06",
            processType="A63",
            Out_Domain=area_code(country_code_from),
            In_Domain=area_code(country_code_to),
            registeredResource=registered_resource,
            curveType=curve_type,
        )

    async def query_balancing_border_capacity_limits(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        registered_resource: str | None = None,
        curve_type: CurveType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query balancing border capacity limitations from R69."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A31",
            start,
            end,
            max_window="1y",
            BusinessType="A26",
            processType="A47",
            Out_Domain=area_code(country_code_from),
            In_Domain=area_code(country_code_to),
            registeredResource=registered_resource,
            curveType=curve_type,
        )

    async def query_shared_rr_frr_capacity(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        curve_type: CurveType | str = "A01",
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query shared RR and FRR capacity from R70."""
        _validate_curve_type(curve_type)
        return await self._query(
            "A26",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="series",
            offset=offset,
            BusinessType="C22",
            processType="A51",
            Area_Domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_exchanged_reserve_capacity(
        self,
        country_code_from: Area | str,
        country_code_to: Area | str,
        start: date | str,
        end: date | str,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query exchanged reserve capacity from R71."""
        return await self._query(
            "A26",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="series",
            offset=offset,
            offset_key="Offset",
            processType="A46",
            BusinessType="C21",
            Acquiring_Domain=area_code(country_code_to),
            Connecting_Domain=area_code(country_code_from),
        )
