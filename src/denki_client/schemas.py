import polars as pl

from denki_client.area import BusinessType, FlowDirection, PsrType

DAY_AHEAD_SCHEMA = pl.Schema(
    {
        "timestamp": pl.Datetime(time_zone="UTC"),
        "price.amount": pl.Float64,
        "currency_Unit.name": pl.Enum(["EUR"]),
        "price_Measure_Unit.name": pl.Enum(["MWH"]),
        "resolution": pl.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTIVATED_BALANCING_ENERGY_PRICE_SCHEMA = pl.Schema(
    {
        "timestamp": pl.Datetime(time_zone="UTC"),
        "activation_Price.amount": pl.Float64,
        "flowDirection.direction": pl.Enum([key.name for key in FlowDirection]),
        "businessType": pl.Enum([key.name for key in BusinessType]),
        "currency_Unit.name": pl.Enum(["EUR"]),
        "price_Measure_Unit.name": pl.Enum(["MWH"]),
        "resolution": pl.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTIVATED_BALANCING_ENERGY_VOLUME_SCHEMA = pl.Schema(
    {
        "timestamp": pl.Datetime(time_zone="UTC"),
        "quantity": pl.Float64,
        "flowDirection.direction": pl.Enum([key.name for key in FlowDirection]),
        "businessType": pl.Enum([key.name for key in BusinessType]),
        "quantity_Measure_Unit.name": pl.Enum(["MWH"]),
        "resolution": pl.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTUAL_GENERATION_PER_PRODUCTION_TYPE = pl.Schema(
    {
        "timestamp": pl.Datetime(time_zone="UTC"),
        "quantity": pl.Float64,
        "quantity_Measure_Unit.name": pl.Enum(["MAW"]),
        "psrType": pl.Enum([key.name for key in PsrType]),
        "resolution": pl.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTUAL_GENERATION_PER_GENERATION_UNIT = pl.Schema(
    {
        "timestamp": pl.Datetime(time_zone="UTC"),
        "quantity": pl.Float64,
        "quantity_Measure_Unit.name": pl.Enum(["MAW"]),
        "psrType": pl.Enum([key.name for key in PsrType]),
        "resolution": pl.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

INSTALLED_CAPACITY_PER_PRODUCTION_TYPE = pl.Schema(
    {
        "timestamp": pl.Datetime(time_zone="UTC"),
        "quantity": pl.Float64,
        "quantity_Measure_Unit.name": pl.Enum(["MAW"]),
        "psrType": pl.Enum([key.name for key in PsrType]),
        "resolution": pl.Enum(["P1Y"]),
    },
)

INSTALLED_CAPACITY_PER_PRODUCTION_UNIT = pl.Schema(
    {
        "timestamp": pl.Datetime(time_zone="UTC"),
        "quantity": pl.Float64,
        "quantity_Measure_Unit.name": pl.Enum(["MAW"]),
        "psrType": pl.Enum([key.name for key in PsrType]),
        "resolution": pl.Enum(["P1Y"]),
    },
)
