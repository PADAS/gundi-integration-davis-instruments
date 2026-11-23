from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app.actions.client as client
import app.actions.handlers as handlers
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig

STATION = client.Station(station_id=167888, station_name="Naboisho HQ", latitude=-1.4, longitude=35.3)

ARCHIVE_RECORD = {
    "ts": 1795446600, "tz_offset": 10800, "arch_int": 300,
    "temp_last": 75.2, "temp_hi": 75.5, "temp_lo": 75.1, "hum_last": 53.7,
    "wind_speed_avg": 5.3, "wind_speed_hi": 12.94, "wind_dir_of_prevail": 112,
    "rainfall_mm": 0.2, "rain_rate_hi_mm": 1.0, "heat_index_last": 75.3, "dew_point_last": 57.3,
    "temp_hi_at": 1795446500, "crc_errors": 0, "solar_rad_avg": None,
}
BAROMETER_RECORD = {"ts": 1795446600, "bar_sea_level": 30.159, "bar_absolute": 24.5}
HEALTH_RECORD = {"ts": 1795446600, "battery_voltage": 4016, "ip_v4_address": "192.168.1.190"}


# Shortly after the fixture readings, so tests don't depend on the real clock.
FROZEN_NOW = datetime(2026, 11, 23, 15, 30, tzinfo=timezone.utc)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return FROZEN_NOW if tz else FROZEN_NOW.replace(tzinfo=None)


@pytest.fixture(autouse=True)
def frozen_clock(mocker):
    mocker.patch.object(handlers, "datetime", FrozenDatetime)


def station_data(*sensors):
    return client.StationData(station_id=STATION.station_id, sensors=[
        client.SensorData(sensor_type=0, data_structure_type=dst, data=records) for dst, records in sensors
    ])


def integration(**auth):
    auth = auth or {"api_key": "key", "api_secret": "secret"}
    return SimpleNamespace(
        id="b6b9f6a4-0000-0000-0000-000000000000",
        base_url=None,
        configurations=[SimpleNamespace(action=SimpleNamespace(value="auth"), data=auth)],
    )


@pytest.fixture
def gundi(mocker):
    """Stands in for Gundi, Redis and the activity log."""
    state = {}

    async def get_state(integration_id, action_id, source_id="no-source"):
        return state.get((action_id, source_id), {})

    async def set_state(integration_id, action_id, state_value, source_id="no-source"):
        state[(action_id, source_id)] = state_value

    async def set_state_by_keyword(integration_id, action_id, state, source_id="no-source"):
        await set_state(integration_id, action_id, state, source_id)

    mocker.patch.object(handlers.state_manager, "get_state", side_effect=get_state)
    mocker.patch.object(handlers.state_manager, "set_state", side_effect=set_state_by_keyword)

    async def set_if_absent(integration_id, action_id, *, ttl_seconds, source_id="no-source"):
        if (action_id, source_id) in state:
            return False
        state[(action_id, source_id)] = 1
        return True

    mocker.patch.object(handlers.state_manager, "set_if_absent", side_effect=set_if_absent)
    sent = mocker.patch.object(handlers, "send_observations_to_gundi", new=AsyncMock(return_value={}))
    activity = mocker.patch.object(handlers, "log_action_activity", new=AsyncMock())
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    return SimpleNamespace(state=state, sent=sent, activity=activity)


def sent_observations(gundi):
    return [o for call in gundi.sent.await_args_list for o in call.kwargs["observations"]]


def test_only_renamed_weather_fields_are_sent_in_metric_units():
    readings = handlers.merge_records_by_time(station_data(
        (24, [ARCHIVE_RECORD]), (20, [BAROMETER_RECORD]), (27, [HEALTH_RECORD]),
    ))

    observation = handlers.transform(STATION, 1795446600, readings[1795446600])

    assert observation == {
        "source": "167888",
        "source_name": "Naboisho HQ",
        "type": "stationary-object",
        "subject_type": "weather_station",
        "recorded_at": "2026-11-23T15:10:00+00:00",
        "location": {"lat": -1.4, "lon": 35.3},
        "additional": {
            "rainfall_mm": 0.2,
            "rain_rate_max_mm_per_hr": 1.0,
            "temperature_c": 24.0,
            "temperature_max_c": 24.2,
            "temperature_min_c": 23.9,
            "humidity_pct": 53.7,
            "wind_speed_kmh": 8.5,
            "wind_gust_kmh": 20.8,
            "wind_direction_deg": 112,
            "heat_index_c": 24.1,
            "dew_point_c": 14.1,
            "pressure_hpa": 1021.3,
        },
    }


def test_current_conditions_names_are_understood_too():
    values = {
        "temp": 68.0, "hum": 40, "wind_speed_avg_last_10_min": 10, "wind_speed_hi_last_10_min": 20,
        "wind_dir_scalar_avg_last_10_min": 90, "rainfall_last_15_min_mm": 0.4,
        "rain_rate_hi_last_15_min_mm": 2.0, "heat_index": 68.0, "dew_point": 50.0,
    }

    assert handlers.weather_values(values) == {
        "rainfall_mm": 0.4, "rain_rate_max_mm_per_hr": 2.0, "temperature_c": 20.0, "humidity_pct": 40,
        "wind_speed_kmh": 16.1, "wind_gust_kmh": 32.2, "wind_direction_deg": 90,
        "heat_index_c": 20.0, "dew_point_c": 10.0,
    }


def test_time_windows_stay_within_one_day():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    windows = list(handlers.time_windows(start, start + timedelta(hours=50)))

    assert len(windows) == 3
    assert windows[0] == (start, start + timedelta(days=1))
    assert windows[-1][1] == start + timedelta(hours=50)


@pytest.mark.asyncio
async def test_pull_sends_history_and_saves_the_cursor(gundi, mocker):
    mocker.patch.object(handlers.client, "get_historic", new=AsyncMock(return_value=station_data(
        (24, [ARCHIVE_RECORD, {**ARCHIVE_RECORD, "ts": 1795446900}]),
        (20, [{"ts": 1795446000, "bar_absolute": 24.5}]),  # no field that is sent: skipped
    )))

    sent = await handlers.pull_station(integration(), STATION, "key", "secret", lookback_hours=1)

    assert sent == 2
    assert [o["recorded_at"] for o in sent_observations(gundi)] == [
        "2026-11-23T15:10:00+00:00", "2026-11-23T15:15:00+00:00",
    ]
    assert gundi.state[("pull_observations", "167888")] == {"last_ts": 1795446900}


@pytest.mark.asyncio
async def test_pull_starts_from_the_saved_cursor(gundi, mocker):
    gundi.state[("pull_observations", "167888")] = {"last_ts": 1795446600}
    get_historic = mocker.patch.object(
        handlers.client, "get_historic", new=AsyncMock(return_value=station_data()))

    await handlers.pull_station(integration(), STATION, "key", "secret", lookback_hours=24)

    assert get_historic.await_count == 1
    assert get_historic.await_args.kwargs["start"] == datetime.fromtimestamp(1795446600, tz=timezone.utc)
    assert get_historic.await_args.kwargs["end"] == FROZEN_NOW


@pytest.mark.asyncio
async def test_basic_plan_stations_send_the_latest_reading(gundi, mocker):
    get_historic = mocker.patch.object(handlers.client, "get_historic", new=AsyncMock(
        side_effect=client.WeatherLinkPermissionError("no access", 403)))
    mocker.patch.object(handlers.client, "get_current", new=AsyncMock(return_value=station_data(
        (23, [{"ts": 1795446600, "temp": 68.0}]),
        (19, [{"ts": 1795446300, "bar_sea_level": 30.0}]),
    )))

    sent = await handlers.pull_station(integration(), STATION, "key", "secret", lookback_hours=1)

    assert sent == 1
    observation = sent_observations(gundi)[0]
    assert observation["recorded_at"] == "2026-11-23T15:10:00+00:00"
    assert observation["additional"] == {"temperature_c": 20.0, "pressure_hpa": 1015.9}

    # The same reading isn't sent twice, and historic isn't asked again.
    gundi.sent.reset_mock()
    assert await handlers.pull_station(integration(), STATION, "key", "secret", lookback_hours=1) == 0
    gundi.sent.assert_not_awaited()
    assert get_historic.await_count == 1


@pytest.mark.asyncio
async def test_auth_reports_rejected_credentials(gundi, mocker):
    mocker.patch.object(handlers.client, "get_stations", new=AsyncMock(
        side_effect=client.WeatherLinkAuthError("Invalid key", 401)))

    result = await handlers.action_auth(
        integration(), action_config=AuthenticateConfig(api_key="bad", api_secret="bad"))

    assert result["valid_credentials"] is False


@pytest.mark.asyncio
async def test_auth_lists_the_stations(gundi, mocker):
    mocker.patch.object(handlers.client, "get_stations", new=AsyncMock(return_value=[STATION]))

    result = await handlers.action_auth(
        integration(), action_config=AuthenticateConfig(api_key="key", api_secret="secret"))

    assert result == {"valid_credentials": True, "stations": [{"station_id": 167888, "station_name": "Naboisho HQ"}]}


@pytest.mark.asyncio
async def test_one_failing_station_does_not_stop_the_others(gundi, mocker):
    other = client.Station(station_id=2, station_name="Airstrip", latitude=-1.5, longitude=35.4)
    no_location = client.Station(station_id=3, station_name="Workshop")
    mocker.patch.object(handlers.client, "get_stations", new=AsyncMock(return_value=[STATION, other, no_location]))
    mocker.patch.object(handlers, "pull_station", new=AsyncMock(
        side_effect=[client.WeatherLinkError("down", 500), 5]))

    result = await handlers.action_pull_observations(integration(), action_config=PullObservationsConfig())

    assert result == {"stations": 2, "stations_failed": ["Naboisho HQ"], "observations_sent": 5}
    titles = [call.kwargs["title"] for call in gundi.activity.await_args_list]
    assert titles == [
        "Readings from station Naboisho HQ weren't fetched because WeatherLink answered with an error.",
        "Readings from station Workshop weren't sent because WeatherLink has no location for it.",
    ]


@pytest.mark.asyncio
async def test_a_long_outage_reaches_back_at_most_seven_days(gundi, mocker):
    gundi.state[("pull_observations", "167888")] = {"last_ts": 1700000000}  # months ago
    get_historic = mocker.patch.object(
        handlers.client, "get_historic", new=AsyncMock(return_value=station_data()))

    await handlers.pull_station(integration(), STATION, "key", "secret", lookback_hours=24)

    assert get_historic.await_count == 7
    assert get_historic.await_args_list[0].kwargs["start"] == FROZEN_NOW - timedelta(days=7)


@pytest.mark.asyncio
async def test_a_network_error_or_lost_log_entry_does_not_stop_the_other_stations(gundi, mocker):
    other = client.Station(station_id=2, station_name="Airstrip", latitude=-1.5, longitude=35.4)
    mocker.patch.object(handlers.client, "get_stations", new=AsyncMock(return_value=[STATION, other]))
    mocker.patch.object(handlers, "pull_station", new=AsyncMock(
        side_effect=[client.WeatherLinkConnectionError("WeatherLink didn't answer (ReadTimeout)"), 4]))
    gundi.activity.side_effect = RuntimeError("pubsub down")

    result = await handlers.action_pull_observations(integration(), action_config=PullObservationsConfig())

    assert result == {"stations": 2, "stations_failed": ["Naboisho HQ"], "observations_sent": 4}
    assert gundi.activity.await_args.kwargs["title"] == (
        "Readings from station Naboisho HQ weren't fetched because WeatherLink didn't answer.")


def test_air_quality_sensors_do_not_replace_outdoor_weather():
    airlink = {"ts": 1795446600, "temp_hi": 90.0, "temp_lo": 80.0, "hum_last": 20.0}
    readings = handlers.merge_records_by_time(station_data((24, [ARCHIVE_RECORD]), (17, [airlink])))

    values = handlers.weather_values(readings[1795446600])

    assert values["temperature_max_c"] == 24.2
    assert values["humidity_pct"] == 53.7


def test_no_window_shorter_than_a_second():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert list(handlers.time_windows(start, start + timedelta(milliseconds=400))) == []


@pytest.mark.asyncio
async def test_a_rate_limit_stops_the_run(gundi, mocker):
    others = [client.Station(station_id=i, station_name=f"S{i}", latitude=-1.5, longitude=35.4) for i in (2, 3)]
    mocker.patch.object(handlers.client, "get_stations", new=AsyncMock(return_value=[STATION, *others]))
    pull = mocker.patch.object(handlers, "pull_station", new=AsyncMock(
        side_effect=[client.WeatherLinkRateLimitError("slow down", 429), 1, 1]))

    result = await handlers.action_pull_observations(integration(), action_config=PullObservationsConfig())

    assert pull.await_count == 1
    assert result["stations_failed"] == ["Naboisho HQ"]
    assert gundi.activity.await_count == 1


def test_older_consoles_are_read_with_their_own_names_and_direction_codes():
    vantage_archive = {
        "ts": 1795446600, "temp_out": 68.0, "temp_out_hi": 70.0, "temp_out_lo": 66.0, "hum_out": 40,
        "dew_point_out": 50.0, "heat_index_out": 68.0, "bar": 30.0, "rainfall_mm": 0.2,
        "wind_speed_avg": 10, "wind_speed_hi": 20, "wind_dir_of_prevail": 4, "temp_in": 75.0,
    }
    readings = handlers.merge_records_by_time(station_data((4, [vantage_archive])))

    assert handlers.weather_values(readings[1795446600]) == {
        "rainfall_mm": 0.2, "temperature_c": 20.0, "temperature_max_c": 21.1, "temperature_min_c": 18.9,
        "humidity_pct": 40, "wind_speed_kmh": 16.1, "wind_gust_kmh": 32.2, "wind_direction_deg": 90.0,
        "heat_index_c": 20.0, "dew_point_c": 10.0, "pressure_hpa": 1015.9,
    }


@pytest.mark.asyncio
async def test_current_conditions_keep_a_sensor_older_than_the_cursor(gundi, mocker):
    gundi.state[("pull_observations", "167888")] = {"last_ts": 1795446600}  # 15:10
    gundi.state[("pull_observations", "no-historic-167888")] = 1
    mocker.patch.object(handlers.client, "get_current", new=AsyncMock(return_value=station_data(
        (23, [{"ts": 1795446900, "temp": 68.0}]),  # 15:15
        (19, [{"ts": 1795446300, "bar_sea_level": 30.0}]),  # 15:05, before the cursor
    )))

    sent = await handlers.pull_station(integration(), STATION, "key", "secret", lookback_hours=1)

    assert sent == 1
    assert sent_observations(gundi)[0]["additional"] == {"temperature_c": 20.0, "pressure_hpa": 1015.9}


def test_a_value_that_is_not_a_number_is_left_out():
    values = {"temp_last": "N/A", "temp": 68.0, "hum_last": None, "hum": True, "bar_sea_level": float("nan"), "rainfall_mm": 0}

    assert handlers.weather_values(values) == {"temperature_c": 20.0, "rainfall_mm": 0}


@pytest.mark.asyncio
async def test_a_rate_limit_on_the_station_list_stops_cleanly(gundi, mocker):
    mocker.patch.object(handlers.client, "get_stations", new=AsyncMock(
        side_effect=client.WeatherLinkRateLimitError("slow down", 429)))
    pull = mocker.patch.object(handlers, "pull_station", new=AsyncMock())

    result = await handlers.action_pull_observations(integration(), action_config=PullObservationsConfig())

    assert result == {"stations": 0, "stations_failed": [], "observations_sent": 0}
    pull.assert_not_awaited()
    assert gundi.activity.await_args.kwargs["title"] == (
        "Readings weren't fetched because the WeatherLink key reached its limit of calls.")
