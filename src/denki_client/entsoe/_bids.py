"""Balancing market query methods for :class:`EntsoeClient`."""

from __future__ import annotations

from datetime import date

import polars as pl

from denki_client.area import Area

from ._transport import _Transport, area_code

_AGGREGATED_BID_PROCESS_TYPES = {"A46", "A47", "A51", "A60", "A61", "A67", "A68"}


class BidQueries(_Transport):
    """Queries for balancing energy, bids, volumes, and prices.

    Methods return the transport's Polars projection with its original units,
    currencies, identifiers, and timestamps in UTC. No time-zone conversion or
    resampling is applied.
    """

    async def query_activated_balancing_energy_prices(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        process_type: str = "A16",
        psr_type: str | None = None,
        business_type: str | None = None,
        standard_market_product: str | None = None,
        original_market_product: str | None = None,
        export_type: str | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query activated balancing energy prices (R47).

        Parameters follow ``entsoe-py`` names. The returned rows retain the
        XML point grain and the source price, unit, and currency fields.
        """
        return await self._query(
            "A84",
            start,
            end,
            max_window="1y",
            processType=process_type,
            controlArea_Domain=area_code(country_code),
            businessType=business_type,
            ExportType=export_type,
            PsrType=psr_type,
            Standard_MarketProduct=standard_market_product,
            Original_MarketProduct=original_market_product,
            curveType=curve_type,
        )

    async def query_afrr_cross_border_marginal_prices(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        standard_market_product: str,
    ) -> pl.DataFrame | None:
        """Query cross-border marginal prices for aFRR central selection (R48)."""
        return await self._query(
            "A84",
            start,
            end,
            max_window="1d",
            processType="A67",
            businessType="A96",
            Standard_MarketProduct=standard_market_product,
            controlArea_Domain=area_code(country_code),
        )

    async def query_balancing_energy_bids(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        *,
        archive: bool = False,
        offset: int = 0,
        standard_market_product: str | None = None,
        original_market_product: str | None = None,
        direction: str | None = None,
    ) -> pl.DataFrame | None:
        """Query balancing energy bids, optionally from the archive (R49–R50)."""
        return await self._query(
            "A37",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="archives" if archive else "series",
            offset=offset,
            max_units=100,
            businessType="B74",
            processType="A47",
            connecting_Domain=area_code(country_code),
            Standard_MarketProduct=standard_market_product,
            Original_MarketProduct=original_market_product,
            Direction=direction,
            storageType="archive" if archive else None,
        )

    async def query_bid_availability_changes(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        *,
        archive: bool = False,
        business_type: str = "C46",
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query changes to balancing bid availability (R51–R52)."""
        return await self._query(
            "B45",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="archives" if archive else "series",
            offset=offset,
            max_units=100,
            processType="A47",
            Domain=area_code(country_code),
            businessType=business_type,
            storageType="archive" if archive else None,
        )

    async def query_aggregated_balancing_energy_bids(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        process_type: str,
    ) -> pl.DataFrame | None:
        """Query aggregated balancing energy bids (R53)."""
        if process_type not in _AGGREGATED_BID_PROCESS_TYPES:
            allowed = ", ".join(sorted(_AGGREGATED_BID_PROCESS_TYPES))
            raise ValueError(f"process_type must be one of {allowed}")

        return await self._query(
            "A24",
            start,
            end,
            max_window="1y",
            processType=process_type,
            area_Domain=area_code(country_code),
        )

    async def query_aggregated_bids(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        process_type: str,
    ) -> pl.DataFrame | None:
        """Compatibility alias for ``query_aggregated_balancing_energy_bids``."""
        return await self.query_aggregated_balancing_energy_bids(country_code, start, end, process_type)

    async def query_netted_exchanged_volumes(
        self,
        acquiring_country_code: Area | str,
        connecting_country_code: Area | str,
        start: date | str,
        end: date | str,
        process_type: str = "A63",
    ) -> pl.DataFrame | None:
        """Query netted and exchanged volumes (R54), retaining both areas."""
        if process_type not in {"A60", "A61", "A63", "A67", "A68"}:
            raise ValueError("process_type must be one of A60, A61, A63, A67, A68")

        return await self._query(
            "B17",
            start,
            end,
            max_window="1d" if process_type in {"A67", "A68"} else "1y",
            processType=process_type,
            Acquiring_Domain=area_code(acquiring_country_code),
            Connecting_Domain=area_code(connecting_country_code),
        )

    async def query_netted_exchanged_volumes_by_border(
        self,
        acquiring_country_code: Area | str,
        connecting_country_code: Area | str,
        start: date | str,
        end: date | str,
        process_type: str = "A60",
    ) -> pl.DataFrame | None:
        """Query netted and exchanged volumes per border (R55)."""
        if process_type not in {"A60", "A61", "A63", "A67", "A68"}:
            raise ValueError("process_type must be one of A60, A61, A63, A67, A68")

        return await self._query(
            "A30",
            start,
            end,
            max_window="1d" if process_type in {"A67", "A68"} else "1y",
            processType=process_type,
            Acquiring_Domain=area_code(acquiring_country_code),
            Connecting_Domain=area_code(connecting_country_code),
        )

    async def query_elastic_demands(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        *,
        offset: int = 0,
    ) -> pl.DataFrame | None:
        """Query elastic demands (R56)."""
        return await self._query(
            "A37",
            start,
            end,
            max_window="1y",
            page_size=100,
            page_unit="series",
            offset=offset,
            max_units=100,
            businessType="B75",
            processType="A47",
            Acquiring_Domain=area_code(country_code),
        )

    async def query_imbalance_prices(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query imbalance prices (R72), returned at XML point resolution."""
        return await self._query(
            "A85",
            start,
            end,
            max_window="1y",
            controlArea_Domain=area_code(country_code),
            PsrType=psr_type,
        )

    async def query_total_imbalance_volumes(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        business_type: str = "A19",
        psr_type: str | None = None,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query total imbalance volumes (R73) with the catalogue filters."""
        return await self._query(
            "A86",
            start,
            end,
            max_window="1y",
            controlArea_Domain=area_code(country_code),
            businessType=business_type,
            PsrType=psr_type,
            curveType=curve_type,
        )

    async def query_imbalance_volumes(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | None = None,
        include_resolution: bool = False,
    ) -> pl.DataFrame | None:
        """Compatibility query for total imbalance volumes (R73)."""
        frame = await self.query_total_imbalance_volumes(
            country_code,
            start,
            end,
            psr_type=psr_type,
        )
        if frame is not None and not include_resolution and "resolution" in frame.columns:
            frame = frame.drop("resolution")
        return frame

    async def query_current_balancing_state(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
    ) -> pl.DataFrame | None:
        """Query the current balancing state (R74), with a 100-day window.

        The signed quantity from the XML is preserved. ``entsoe-py`` negates
        this measure in its Pandas projection.
        """
        return await self._query(
            "A86",
            start,
            end,
            max_window="100d",
            businessType="B33",
            area_Domain=area_code(country_code),
        )

    async def query_criteria_application_measurements(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query results of the criteria application process (R75)."""
        return await self._query(
            "A45",
            start,
            end,
            max_window="1y",
            processType="A65",
            area_domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_activated_balancing_energy(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        business_type: str,
        psr_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query historical activated balancing energy quantities (A83).

        ENTSO-E ended A83 publication on 11 December 2025. Use
        `list_activated_balancing_energy_archives` to find File Library extracts.
        Recent activations use R53, with a different grain and set of filters.
        """
        return await self._query(
            "A83",
            start,
            end,
            max_window="1y",
            controlArea_Domain=area_code(country_code),
            businessType=business_type,
            psrType=psr_type,
        )
