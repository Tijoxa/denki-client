from datetime import UTC

import narwhals as nw

from denki_client.area import BusinessType, FlowDirection, PsrType

DAY_AHEAD_SCHEMA = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "price.amount": nw.Float64,
        "currency_Unit.name": nw.Enum(["EUR"]),
        "price_Measure_Unit.name": nw.Enum(["MWH"]),
        "resolution": nw.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTIVATED_BALANCING_ENERGY_PRICES_SCHEMA = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "activation_Price.amount": nw.Float64,
        "flowDirection.direction": nw.Enum([key.name for key in FlowDirection]),
        "businessType": nw.Enum([key.name for key in BusinessType]),
        "currency_Unit.name": nw.Enum(["EUR"]),
        "price_Measure_Unit.name": nw.Enum(["MWH"]),
        "resolution": nw.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTUAL_GENERATION_PER_GENERATION_UNIT = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "quantity": nw.Int64,
        "quantity_Measure_Unit.name": nw.Enum(["MAW"]),
        "psrType": nw.Enum([key.name for key in PsrType]),
        "resolution": nw.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

INSTALLED_CAPACITY_PER_PRODUCTION_TYPE = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "quantity": nw.Int64,
        "quantity_Measure_Unit.name": nw.Enum(["MAW"]),
        "psrType": nw.Enum([key.name for key in PsrType]),
        "resolution": nw.Enum(["P1Y"]),
    },
)
