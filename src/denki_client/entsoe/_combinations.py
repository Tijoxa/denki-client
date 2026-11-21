"""Composition of queries that span several datasets or borders."""

from datetime import date

import polars as pl

from denki_client.area import Area, Neighbours, lookup_area

from ._generation import GenerationQueries
from ._market import MarketQueries


class CombinationQueries(GenerationQueries, MarketQueries):
    async def query_physical_crossborder_allborders(
        self,
        country_code: Area | str,
        start: date | str,
        end: date | str,
        export: bool,
        per_hour: bool = False,
    ) -> pl.DataFrame | None:
        """Align physical flows in one direction and return each border and the total in MW."""
        country = lookup_area(country_code)
        borders = Neighbours.__members__.get(country.name)
        if borders is None:
            raise ValueError(f"No neighbours are configured for {country.name}")

        flows = {}
        for neighbour in borders.value:
            origin, destination = (country, neighbour) if export else (neighbour, country)
            frame = await self._physical_flows(origin, destination, start, end, calendar_area=country)
            if frame is None:
                continue
            values = (
                frame.filter(pl.col("timestamp").is_not_null())
                .group_by("timestamp")
                .agg(
                    pl.col("timestamp_end").min(),
                    pl.when(pl.col("quantity").count() > 0).then(pl.col("quantity").sum()).alias(neighbour),
                )
            )
            if not values.is_empty():
                flows[neighbour] = values.sort("timestamp")

        if not flows:
            return None
        timestamps = [frame.select("timestamp") for frame in flows.values()]
        if per_hour:
            timestamps.extend(
                frame.select(
                    pl.datetime_ranges(
                        pl.col("timestamp").dt.truncate("1h").dt.offset_by("1h"),
                        pl.col("timestamp_end"),
                        interval="1h",
                        closed="left",
                    )
                    .explode()
                    .alias("timestamp")
                ).drop_nulls()
                for frame in flows.values()
            )
        aligned = pl.concat(timestamps).unique().sort("timestamp")
        for neighbour, frame in flows.items():
            aligned = (
                aligned.join_asof(frame, on="timestamp")
                .with_columns(
                    pl.when(pl.col("timestamp") < pl.col("timestamp_end")).then(pl.col(neighbour)).alias(neighbour)
                )
                .drop("timestamp_end")
            )
        any_flow = pl.any_horizontal(pl.col(neighbour).is_not_null() for neighbour in flows)
        aligned = aligned.with_columns(pl.when(any_flow).then(pl.sum_horizontal(*flows)).alias("sum"))
        if per_hour:
            return aligned.group_by_dynamic("timestamp", every="1h").agg(pl.exclude("timestamp").first())
        return aligned

    async def query_import(self, country_code: Area | str, start: date | str, end: date | str) -> pl.DataFrame | None:
        """Return each inbound border and the total in MW."""
        return await self.query_physical_crossborder_allborders(country_code, start, end, export=False)

    async def query_generation_import(
        self, country_code: Area | str, start: date | str, end: date | str
    ) -> pl.DataFrame | None:
        """Align net production and physical imports by UTC timestamp."""
        generation = await self.query_generation(country_code=country_code, start=start, end=end, nett=True)
        imports = await self.query_import(country_code=country_code, start=start, end=end)
        if imports is None:
            return generation
        imports = imports.rename({name: f"import.{name}" for name in imports.columns if name != "timestamp"})
        if generation is None:
            return imports
        return generation.join(imports, on="timestamp", how="full", coalesce=True).sort("timestamp")
