from datetime import UTC

import narwhals as nw

DAY_AHEAD_SCHEMA = nw.Schema(
    {
        "PT60M_timestamp": nw.Datetime(time_zone=UTC),
        "PT60M_value": nw.Float64,
    }
)
