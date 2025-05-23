from datetime import UTC

import narwhals as nw

DAY_AHEAD_SCHEMA = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "value": nw.Float64,
        "resolution": nw.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)

ACTIVATED_BALANCING_ENERGY_PRICES_SCHEMA = nw.Schema(
    {
        "timestamp": nw.Datetime(time_zone=UTC),
        "Price": nw.Float64,
        "Direction": nw.Enum(["Up", "Down"]),
        "ReserveType": nw.Enum(["FCR", "AFRR", "MFRR", "RR"]),
        "resolution": nw.Enum(["PT60M", "PT30M", "PT15M"]),
    }
)
