from datetime import UTC

import narwhals as nw

DAY_AHEAD_SCHEMA = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "price.amount": nw.Float64,
        "resolution": nw.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTIVATED_BALANCING_ENERGY_PRICES_SCHEMA = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "activation_Price.amount": nw.Float64,
        "flowDirection.direction": nw.Enum(["A01", "A02"]),
        "businessType": nw.Enum(["A95", "A96", "A97", "A98"]),
        "resolution": nw.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)
