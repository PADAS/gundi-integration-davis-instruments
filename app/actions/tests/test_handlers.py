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

    assert get_historic.await_args.kwargs["start"] == datetime.fromtimestamp(1795446600, tz=timezone.utc)


@pytest.mark.asyncio
async def test_basic_plan_stations_send_the_latest_reading(gundi, mocker):
    mocker.patch.object(handlers.client, "get_historic", new=AsyncMock(
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

    # The same reading isn't sent twice.
    gundi.sent.reset_mock()
    assert await handlers.pull_station(integration(), STATION, "key", "secret", lookback_hours=1) == 0
    gundi.sent.assert_not_awaited()


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

    assert result == {"stations": 3, "stations_failed": ["Naboisho HQ"], "observations_sent": 5}
    titles = [call.kwargs["title"] for call in gundi.activity.await_args_list]
    assert titles == [
        "Readings from station Naboisho HQ weren't fetched because WeatherLink answered with an error.",
        "Readings from station Workshop weren't sent because WeatherLink has no location for it.",
    ]
