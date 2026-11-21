from __future__ import annotations

from datetime import date, datetime

import polars as pl

from denki_client.area import Area, DocStatus, PsrType
from denki_client.parsers import parse_date

from ._transport import _Transport, area_code


def _code(value: DocStatus | PsrType | str | None, name: str, allowed: tuple[str, ...]) -> str | None:
    code = value.name if isinstance(value, (DocStatus, PsrType)) else value
    if code is not None and code not in allowed:
        raise ValueError(f"{name} must be one of {', '.join(allowed)}")
    return code


def _area_if_no_mrid(area: Area | str | None, mrid: str | None) -> str | None:
    if mrid is not None:
        return None
    if area is None:
        raise ValueError("country_code is required when mRID is omitted")
    return area_code(area)


class ReferenceQueries(_Transport):
    """Queries for production unit master data and other market information."""

    async def query_production_and_generation_units(
        self,
        country_code: Area | str,
        implementation_date_and_or_time: date | str,
        *,
        psr_type: PsrType | str | None = None,
    ) -> pl.DataFrame | None:
        """Query production and generation unit master data on a reference date (R76)."""
        return await self._query(
            "A95",
            None,
            None,
            max_window=None,
            BiddingZone_Domain=area_code(country_code),
            BusinessType="B11",
            Implementation_DateAndOrTime=parse_date(
                implementation_date_and_or_time, name="implementation_date_and_or_time"
            ).isoformat(),
            psrType=_code(psr_type, "psr_type", tuple(PsrType.__members__)),
        )

    async def query_other_market_information(
        self,
        country_code: Area | str | None,
        start: date | str | None = None,
        end: date | str | None = None,
        *,
        docstatus: DocStatus | str | None = None,
        periodstartupdate: datetime | str | None = None,
        periodendupdate: datetime | str | None = None,
        mRID: str | None = None,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query other market information documents (R77)."""
        return await self._query(
            "B47",
            start,
            end,
            max_window="1y",
            page_size=200,
            page_unit="documents",
            offset=offset,
            offset_key="Offset",
            update_start=periodstartupdate,
            update_end=periodendupdate,
            ControlArea_Domain=_area_if_no_mrid(country_code, mRID),
            DocStatus=_code(docstatus, "docstatus", ("A02", "A05", "A09", "A13")),
            mRID=mRID,
        )
