from __future__ import annotations

from datetime import date, datetime

import polars as pl

from denki_client.area import Area, BsnType, DocStatus

from ._transport import WireValue, _Transport
from ._transport import area_code as _area_code


def _code(value: BsnType | DocStatus | str | None, name: str, allowed: tuple[str, ...]) -> str | None:
    code = value.name if isinstance(value, (BsnType, DocStatus)) else value
    if code is not None and code not in allowed:
        raise ValueError(f"{name} must be one of {', '.join(allowed)}")
    return code


def _area_if_no_mrid(area: Area | str | None, mrid: str | None, name: str) -> str | None:
    if mrid is not None:
        return None
    if area is None:
        raise ValueError(f"{name} is required when mRID is omitted")
    return _area_code(area)


class OutageQueries(_Transport):
    """Queries for ENTSO-E outage and fallback documents."""

    async def _query_outage(
        self,
        document_type: str,
        start: date | str | None,
        end: date | str | None,
        *,
        page_size: int | None = 200,
        offset: int = 0,
        update_start: datetime | str | None = None,
        update_end: datetime | str | None = None,
        **params: WireValue,
    ) -> pl.DataFrame | None:
        return await self._query(
            document_type,
            start,
            end,
            max_window="1y",
            page_size=page_size,
            page_unit="documents",
            offset=offset,
            update_start=update_start,
            update_end=update_end,
            wire_params=params,
        )

    async def query_production_unit_unavailability(
        self,
        country_code: Area | str | None,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        registered_resource: str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query planned or actual availability of production units (R39)."""
        return await self._query_outage(
            "A77",
            start,
            end,
            offset=offset,
            update_start=periodstartupdate,
            update_end=periodendupdate,
            BiddingZone_Domain=_area_if_no_mrid(country_code, mRID, "country_code"),
            BusinessType=_code(business_type, "business_type", ("A53", "A54")),
            DocStatus=_code(docstatus, "docstatus", ("A05", "A09", "A13")),
            RegisteredResource=registered_resource,
            mRID=mRID,
        )

    async def query_generation_unit_unavailability(
        self,
        country_code: Area | str | None,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        registered_resource: str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query planned or actual availability of generation units (R40)."""
        return await self._query_outage(
            "A80",
            start,
            end,
            offset=offset,
            update_start=periodstartupdate,
            update_end=periodendupdate,
            BiddingZone_Domain=_area_if_no_mrid(country_code, mRID, "country_code"),
            BusinessType=_code(business_type, "business_type", ("A53", "A54")),
            DocStatus=_code(docstatus, "docstatus", ("A05", "A09", "A13")),
            RegisteredResource=registered_resource,
            mRID=mRID,
        )

    async def query_aggregated_consumption_unit_unavailability(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        *,
        business_type: BsnType | str | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query aggregated consumption unit unavailability (R41)."""
        return await self._query(
            "A76",
            start,
            end,
            max_window="1y",
            BiddingZone_Domain=_area_code(country_code),
            BusinessType=_code(business_type, "business_type", ("A53", "A54")),
            curveType=curve_type,
        )

    async def query_transmission_unavailability(
        self,
        country_code_from: Area | str | None,
        country_code_to: Area | str | None,
        start: date | str,
        end: date | str,
        *,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query unavailability of transmission infrastructure (R42)."""
        return await self._query_outage(
            "A78",
            start,
            end,
            offset=offset,
            update_start=periodstartupdate,
            update_end=periodendupdate,
            Out_Domain=_area_if_no_mrid(country_code_from, mRID, "country_code_from"),
            In_Domain=_area_if_no_mrid(country_code_to, mRID, "country_code_to"),
            BusinessType=_code(business_type, "business_type", ("A53", "A54")),
            DocStatus=_code(docstatus, "docstatus", ("A05", "A09", "A13")),
            mRID=mRID,
        )

    async def query_transmission_unavailability_available_capacity(
        self,
        country_code: Area | str | None,
        start: date | str,
        end: date | str,
        *,
        asset_registered_resource_mrid: str | None = None,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query available transmission capacity (R43)."""
        return await self._query_outage(
            "A78",
            start,
            end,
            offset=offset,
            update_start=periodstartupdate,
            update_end=periodendupdate,
            ControlArea_Domain=_area_if_no_mrid(country_code, mRID, "country_code"),
            BusinessType=_code(business_type, "business_type", ("A53", "A54")),
            DocStatus=_code(docstatus, "docstatus", ("A05", "A09", "A13")),
            mRID=mRID,
            **{"Asset_RegisteredResource.mRID": asset_registered_resource_mrid},
        )

    async def query_transmission_unavailability_net_position_impact(
        self,
        ptdf_domain: Area | str | None,
        start: date | str,
        end: date | str,
        *,
        asset_registered_resource_mrid: str | None = None,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query the impact of transmission unavailability on net position (R44)."""
        return await self._query_outage(
            "A78",
            start,
            end,
            offset=offset,
            update_start=periodstartupdate,
            update_end=periodendupdate,
            BusinessType=_code(business_type, "business_type", ("A53", "A54")),
            DocStatus=_code(docstatus, "docstatus", ("A05", "A09", "A13")),
            mRID=mRID,
            **{
                "pTDF_Domain.mRID": _area_if_no_mrid(ptdf_domain, mRID, "ptdf_domain"),
                "Asset_RegisteredResource.mRID": asset_registered_resource_mrid,
            },
        )

    async def query_offshore_grid_unavailability(
        self,
        area_code: Area | str | None,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query offshore grid infrastructure unavailability (R45)."""
        return await self._query_outage(
            "A79",
            start,
            end,
            offset=offset,
            update_start=periodstartupdate,
            update_end=periodendupdate,
            BiddingZone_Domain=_area_if_no_mrid(area_code, mRID, "area_code"),
            DocStatus=_code(docstatus, "docstatus", ("A05", "A09", "A13")),
            mRID=mRID,
        )

    async def query_fall_backs(
        self,
        country_code: Area | str | None,
        start: date | str,
        end: date | str,
        *,
        docstatus: DocStatus | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query fallback events for balancing services (R46)."""
        return await self._query_outage(
            "A53",
            start,
            end,
            page_size=100,
            offset=offset,
            ProcessType="A51",
            BusinessType="C47",
            BiddingZone_Domain=_area_if_no_mrid(country_code, mRID, "country_code"),
            DocStatus=_code(docstatus, "docstatus", ("A13",)),
            mRID=mRID,
        )

    async def query_withdrawn_unavailability_of_generation_units(
        self,
        country_code: Area | str | None,
        start: date | str,
        end: date | str,
        *,
        mRID: str | None = None,
    ) -> pl.DataFrame | None:
        """Query withdrawn generation unit documents (R40 with DocStatus A13)."""
        return await self.query_generation_unit_unavailability(
            country_code,
            start,
            end,
            docstatus="A13",
            mRID=mRID,
        )

    async def query_unavailability_of_production_units(
        self,
        country_code: Area | str | None,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        registered_resource: str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        return await self.query_production_unit_unavailability(
            country_code,
            start,
            end,
            business_type=business_type,
            docstatus=docstatus,
            periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate,
            registered_resource=registered_resource,
            mRID=mRID,
            offset=offset,
        )

    async def query_unavailability_of_generation_units(
        self,
        country_code: Area | str | None,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        registered_resource: str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        return await self.query_generation_unit_unavailability(
            country_code,
            start,
            end,
            business_type=business_type,
            docstatus=docstatus,
            periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate,
            registered_resource=registered_resource,
            mRID=mRID,
            offset=offset,
        )

    async def query_unavailability_transmission(
        self,
        country_code_from: Area | str | None,
        country_code_to: Area | str | None,
        start: date | str,
        end: date | str,
        *,
        business_type: BsnType | str | None = None,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        return await self.query_transmission_unavailability(
            country_code_from,
            country_code_to,
            start,
            end,
            business_type=business_type,
            docstatus=docstatus,
            periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate,
            mRID=mRID,
            offset=offset,
        )

    async def query_unavailability_of_offshore_grid(
        self,
        area_code: Area | str | None,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        return await self.query_offshore_grid_unavailability(
            area_code,
            start,
            end,
            docstatus=docstatus,
            periodstartupdate=periodstartupdate,
            periodendupdate=periodendupdate,
            mRID=mRID,
            offset=offset,
        )
