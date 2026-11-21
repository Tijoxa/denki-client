from __future__ import annotations

import asyncio
from datetime import date
from typing import Literal

import polars as pl

from denki_client.area import Area, PsrType
from denki_client.entsoe._transport import _Transport, area_code

LoadForecastProcess = Literal["A01", "A31", "A32", "A33"]
WindSolarForecastProcess = Literal["A01", "A40"]


def _code(value: str | PsrType | None) -> str | None:
    if isinstance(value, PsrType):
        return value.name
    return value


def _net_generation(frame: pl.DataFrame | None) -> pl.DataFrame | None:
    if frame is None or "out_Domain.mRID" not in frame.columns:
        return frame

    grouping_columns = [
        name
        for name in (
            "timestamp",
            "timestamp_end",
            "resolution",
            "psrType",
            "quantity_Measure_Unit.name",
            "registeredResource.mRID",
            "registeredResource.name",
            "production_unit",
        )
        if name in frame.columns
    ]
    rows = (
        frame.with_columns(pl.col("out_Domain.mRID").is_not_null().alias("_is_consumption"))
        .group_by(grouping_columns, maintain_order=True)
        .agg(
            pl.col("quantity").filter(~pl.col("_is_consumption")).sum().alias("_generation"),
            pl.col("quantity").filter(pl.col("_is_consumption")).sum().alias("_consumption"),
            pl.col("quantity").filter(~pl.col("_is_consumption")).count().alias("_generation_count"),
            pl.col("quantity").count().alias("_value_count"),
        )
    )
    return (
        rows.with_columns(
            pl.when(pl.col("_value_count") == 0)
            .then(pl.lit(None, dtype=pl.Float64))
            .when(pl.col("_generation_count") == 0)
            .then(-pl.col("_consumption").fill_null(0))
            .otherwise(pl.col("_generation").fill_null(0) - pl.col("_consumption").fill_null(0))
            .alias("quantity")
        )
        .drop("_generation", "_consumption", "_generation_count", "_value_count")
        .sort("timestamp")
    )


class GenerationQueries(_Transport):
    """Load and generation queries from the ENTSO-E Transparency Platform."""

    async def query_actual_total_load(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        request_mode: Literal["get", "get_header", "post"] = "get",
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query actual total load using one of the three supported transports."""
        if request_mode == "post" and curve_type is not None:
            raise ValueError("curve_type is not supported by the POST load request")
        if request_mode not in {"get", "get_header", "post"}:
            raise ValueError("request_mode must be 'get', 'get_header' or 'post'")

        return await self._query(
            "A65",
            start,
            end,
            max_window="1y",
            request_mode=request_mode,
            processType="A16",
            outBiddingZone_Domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_load(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        *,
        request_mode: Literal["get", "get_header", "post"] = "get",
    ) -> pl.DataFrame | None:
        """Compatibility alias for actual total load."""
        return await self.query_actual_total_load(
            country_code,
            start=start,
            end=end,
            request_mode=request_mode,
        )

    async def _query_load_forecast(
        self,
        country_code: Area | str,
        process_type: LoadForecastProcess,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        if process_type not in {"A01", "A31", "A32", "A33"}:
            raise ValueError("process_type must be one of A01, A31, A32 or A33")
        return await self._query(
            "A65",
            start,
            end,
            max_window="1y",
            processType=process_type,
            outBiddingZone_Domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_day_ahead_total_load_forecast(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query the day-ahead total load forecast (R17)."""
        return await self._query_load_forecast(country_code, "A01", start=start, end=end, curve_type=curve_type)

    async def query_week_ahead_total_load_forecast(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query the week-ahead total load forecast (R18)."""
        return await self._query_load_forecast(country_code, "A31", start=start, end=end, curve_type=curve_type)

    async def query_month_ahead_total_load_forecast(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query the month-ahead total load forecast (R19)."""
        return await self._query_load_forecast(country_code, "A32", start=start, end=end, curve_type=curve_type)

    async def query_year_ahead_total_load_forecast(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query the year-ahead total load forecast (R20)."""
        return await self._query_load_forecast(country_code, "A33", start=start, end=end, curve_type=curve_type)

    async def query_load_forecast(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        process_type: LoadForecastProcess = "A01",
    ) -> pl.DataFrame | None:
        """Compatibility query for day, week, month or year-ahead load forecasts."""
        return await self._query_load_forecast(country_code, process_type, start=start, end=end)

    async def query_load_and_forecast(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
    ) -> pl.DataFrame | None:
        """Join actual total load and day-ahead forecast by UTC timestamp."""
        actual, forecast = await asyncio.gather(
            self.query_actual_total_load(country_code, start=start, end=end),
            self.query_day_ahead_total_load_forecast(country_code, start=start, end=end),
        )
        if actual is None and forecast is None:
            return None

        frames = []
        if actual is not None:
            frames.append(actual.select("timestamp", pl.col("quantity").alias("actual_load")))
        if forecast is not None:
            frames.append(forecast.select("timestamp", pl.col("quantity").alias("forecast_load")))
        if len(frames) == 1:
            return frames[0]
        return frames[0].join(frames[1], on="timestamp", how="full", coalesce=True).sort("timestamp")

    async def query_year_ahead_forecast_margin(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query the year-ahead forecast margin (R21)."""
        return await self._query(
            "A70",
            start,
            end,
            max_window="1y",
            processType="A33",
            outBiddingZone_Domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_installed_generation_capacity_by_type(
        self,
        country_code: Area | str,
        psr_type: str | PsrType | None = None,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query installed generation capacity by production type (R22)."""
        return await self._query(
            "A68",
            start,
            end,
            max_window="1y",
            processType="A33",
            in_Domain=area_code(country_code),
            PsrType=_code(psr_type),
            curveType=curve_type,
        )

    async def query_installed_generation_capacity(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | PsrType | None = None,
    ) -> pl.DataFrame | None:
        """Compatibility alias for installed capacity by production type."""
        return await self.query_installed_generation_capacity_by_type(country_code, psr_type, start=start, end=end)

    async def query_hydro_reservoir_storage(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query hydro reservoir and storage levels (R23)."""
        return await self._query(
            "A72",
            start,
            end,
            max_window="1y",
            processType="A16",
            in_Domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_aggregate_water_reservoirs_and_hydro_storage(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
    ) -> pl.DataFrame | None:
        """Compatibility alias for aggregate reservoir and hydro storage data."""
        return await self.query_hydro_reservoir_storage(country_code, start=start, end=end)

    async def query_actual_generation_by_production_type(
        self,
        country_code: Area | str,
        psr_type: str | PsrType | None = None,
        *,
        start: date | str,
        end: date | str,
        nett: bool = False,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query actual generation by production type (R24)."""
        frame = await self._query(
            "A75",
            start,
            end,
            max_window="1y",
            processType="A16",
            in_Domain=area_code(country_code),
            psrType=_code(psr_type),
            curveType=curve_type,
        )
        return _net_generation(frame) if nett else frame

    async def query_generation(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | PsrType | None = None,
        nett: bool = False,
    ) -> pl.DataFrame | None:
        """Compatibility query for actual generation by production type."""
        frame = await self.query_actual_generation_by_production_type(
            country_code, psr_type, start=start, end=end, nett=nett
        )
        return frame

    async def query_actual_generation_by_unit(
        self,
        country_code: Area | str,
        psr_type: str | PsrType | None = None,
        registered_resource: str | None = None,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query actual generation by unit (R25)."""
        return await self._query(
            "A73",
            start,
            end,
            max_window="1d",
            processType="A16",
            in_Domain=area_code(country_code),
            PsrType=_code(psr_type),
            RegisteredResource=registered_resource,
            curveType=curve_type,
        )

    async def query_generation_per_plant(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | PsrType | None = None,
        include_eic: bool = False,
        eic_code: str | None = None,
    ) -> pl.DataFrame | None:
        """Compatibility query for actual generation by production unit."""
        frame = await self.query_actual_generation_by_unit(
            country_code,
            psr_type,
            eic_code,
            start=start,
            end=end,
        )
        if frame is not None and not include_eic:
            identifiers = [name for name in ("registeredResource.mRID",) if name in frame.columns]
            if identifiers:
                frame = frame.drop(identifiers)
        return frame

    async def query_day_ahead_generation_forecast(
        self,
        country_code: Area | str,
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query day-ahead generation forecast (R26)."""
        return await self._query(
            "A71",
            start,
            end,
            max_window="1y",
            processType="A01",
            in_Domain=area_code(country_code),
            curveType=curve_type,
        )

    async def query_generation_forecast(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        process_type: str = "A01",
        *,
        nett: bool = False,
    ) -> pl.DataFrame | None:
        """Compatibility query for day-ahead generation forecasts."""
        frame = await self._query(
            "A71",
            start,
            end,
            max_window="1y",
            processType=process_type,
            in_Domain=area_code(country_code),
        )
        return _net_generation(frame) if nett else frame

    async def query_wind_solar_generation_forecast(
        self,
        country_code: Area | str,
        psr_type: str | PsrType | None = None,
        process_type: WindSolarForecastProcess = "A01",
        *,
        start: date | str,
        end: date | str,
        curve_type: str | None = None,
    ) -> pl.DataFrame | None:
        """Query wind and solar generation forecast or intraday updates (R27)."""
        if process_type not in {"A01", "A40"}:
            raise ValueError("process_type must be 'A01' or 'A40'")
        return await self._query(
            "A69",
            start,
            end,
            max_window="1y",
            processType=process_type,
            in_Domain=area_code(country_code),
            PsrType=_code(psr_type),
            curveType=curve_type,
        )

    async def query_wind_and_solar_forecast(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | PsrType | None = None,
        process_type: WindSolarForecastProcess = "A01",
    ) -> pl.DataFrame | None:
        """Compatibility query for wind and solar forecasts."""
        return await self.query_wind_solar_generation_forecast(
            country_code, psr_type, process_type, start=start, end=end
        )

    async def query_intraday_wind_and_solar_forecast(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | PsrType | None = None,
    ) -> pl.DataFrame | None:
        """Compatibility query for intraday wind and solar forecasts."""
        return await self.query_wind_solar_generation_forecast(country_code, psr_type, "A40", start=start, end=end)

    async def query_installed_generation_capacity_per_unit(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        psr_type: str | PsrType | None = None,
    ) -> pl.DataFrame | None:
        """Compatibility alias for installed capacity per production unit."""
        return await self._query(
            "A71",
            start,
            end,
            max_window=None,
            processType="A33",
            in_Domain=area_code(country_code),
            PsrType=_code(psr_type),
        )
