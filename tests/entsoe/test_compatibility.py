from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

import httpx2
import pytest

from denki_client import EntsoeClient
from denki_client.area import lookup_area

START = date(2025, 1, 1)
END = "2025-01-02"
END_DATE = date(2025, 1, 2)
NO_DATA = b"""<Acknowledgement_MarketDocument
    xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">
    <Reason><code>999</code><text>No matching data found</text></Reason>
</Acknowledgement_MarketDocument>"""

COUNTRY = {"country_code": "FR"}
BORDER = {"country_code_from": "FR", "country_code_to": "BE"}
RESERVE = {"process_type": "A52", "type_marketagreement_type": "A01"}


def expected_utc_bounds(params, calendar_area=None):
    area_parameter_names = (
        "out_Domain",
        "Out_Domain",
        "outBiddingZone_Domain",
        "BiddingZone_Domain",
        "controlArea_Domain",
        "ControlArea_Domain",
        "area_Domain",
        "in_Domain",
        "In_Domain",
        "Acquiring_Domain",
        "Connecting_Domain",
    )
    origin_code = calendar_area or next(params[name] for name in area_parameter_names if params.get(name))
    timezone = ZoneInfo(lookup_area(origin_code).tz)

    def utc_midnight(day):
        return datetime.combine(day, time.min, tzinfo=timezone).astimezone(UTC).strftime("%Y%m%d%H%M")

    return utc_midnight(START), utc_midnight(END_DATE)


@pytest.mark.parametrize(
    ("method", "keywords"),
    [
        ("query_day_ahead_prices", COUNTRY),
        ("query_intraday_prices", COUNTRY | {"sequence": 1}),
        ("query_day_ahead_prices_local", COUNTRY | {"sequence": 1}),
        ("query_net_position", COUNTRY),
        ("query_load", COUNTRY),
        ("query_load_forecast", COUNTRY),
        ("query_load_and_forecast", COUNTRY),
        ("query_generation_forecast", COUNTRY),
        ("query_wind_and_solar_forecast", COUNTRY),
        ("query_intraday_wind_and_solar_forecast", COUNTRY),
        ("query_generation", COUNTRY),
        ("query_generation_per_plant", COUNTRY),
        ("query_installed_generation_capacity", COUNTRY),
        ("query_installed_generation_capacity_per_unit", COUNTRY),
        ("query_aggregate_water_reservoirs_and_hydro_storage", COUNTRY),
        ("query_crossborder_flows", BORDER),
        ("query_scheduled_exchanges", BORDER),
        ("query_net_transfer_capacity_dayahead", BORDER),
        ("query_net_transfer_capacity_weekahead", BORDER),
        ("query_net_transfer_capacity_monthahead", BORDER),
        ("query_net_transfer_capacity_yearahead", BORDER),
        ("query_intraday_offered_capacity", BORDER),
        ("query_offered_capacity", BORDER | {"contract_marketagreement_type": "A01"}),
        ("query_aggregated_bids", COUNTRY | {"process_type": "A51"}),
        ("query_activated_balancing_energy_prices", COUNTRY),
        ("query_activated_balancing_energy", COUNTRY | {"business_type": "A96"}),
        ("query_imbalance_prices", COUNTRY),
        ("query_imbalance_volumes", COUNTRY),
        ("query_current_balancing_state", COUNTRY),
        ("query_procured_balancing_capacity", COUNTRY | {"process_type": "A51"}),
        ("query_contracted_reserve_prices", COUNTRY | RESERVE),
        ("query_contracted_reserve_prices_procured_capacity", COUNTRY | RESERVE),
        ("query_contracted_reserve_amount", COUNTRY | RESERVE),
        ("query_unavailability_of_generation_units", COUNTRY),
        ("query_withdrawn_unavailability_of_generation_units", COUNTRY),
        ("query_unavailability_of_production_units", COUNTRY),
        ("query_unavailability_transmission", BORDER),
        ("query_unavailability_of_offshore_grid", {"area_code": "FR"}),
        ("query_physical_crossborder_allborders", COUNTRY | {"export": True}),
        ("query_import", COUNTRY),
        ("query_generation_import", COUNTRY),
    ],
)
async def test_entsoe_py_keyword_calls(method, keywords):
    def handler(request):
        params = request.url.params
        assert params["securityToken"] == "test-token"
        start_key = "PeriodStart" if "PeriodStart" in params else "periodStart"
        end_key = "PeriodEnd" if "PeriodEnd" in params else "periodEnd"
        calendar_area = (
            keywords["country_code"]
            if method in {"query_physical_crossborder_allborders", "query_import", "query_generation_import"}
            else None
        )
        expected_bounds = expected_utc_bounds(params, calendar_area)
        assert (params[start_key], params[end_key]) == expected_bounds, (params, expected_bounds)
        return httpx2.Response(200, content=NO_DATA)

    client = EntsoeClient("test-token", transport=httpx2.MockTransport(handler))
    async with client.session:
        assert await getattr(client, method)(start=START, end=END, **keywords) is None
