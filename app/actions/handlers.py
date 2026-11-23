import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

from gundi_core.schemas.v2 import LogLevel

import app.actions.client as client
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, get_auth_config
from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()

# Max observations per request to Gundi; larger requests can time out.
OBSERVATION_BATCH_SIZE = 200

# Console and gateway health records (battery, wifi, IP addresses). They
# describe the device, not the weather, so they aren't sent.
HEALTH_DATA_STRUCTURE_TYPES = {15, 27}

# Fields that describe the record rather than the weather.
SKIPPED_FIELDS = {"ts", "tz_offset", "arch_int"}


def weatherlink_base_url(integration) -> str:
    return integration.base_url or client.WEATHERLINK_BASE_URL


def merge_records_by_time(station_data: client.StationData) -> Dict[int, dict]:
    """One dict of weather values per timestamp, merging all weather sensors
    (outdoor sensors, barometer, indoor temperature...) of the station."""
    readings: Dict[int, dict] = defaultdict(dict)
    for sensor in station_data.sensors:
        if sensor.data_structure_type in HEALTH_DATA_STRUCTURE_TYPES:
            continue
        for record in sensor.data:
            ts = record.get("ts")
            if ts is None:
                continue
            readings[ts].update(
                {key: value for key, value in record.items() if key not in SKIPPED_FIELDS and value is not None}
            )
    return {ts: values for ts, values in readings.items() if values}


def f_to_c(value):
    return round((value - 32) * 5 / 9, 1)


def mph_to_kmh(value):
    return round(value * 1.609344, 1)


def inhg_to_hpa(value):
    return round(value * 33.8639, 1)


def same(value):
    return value


# Weather values sent to Gundi: (name sent, conversion, Davis fields to read
# from, first match wins). Historic archive records come first, then the names
# current conditions use (stations shared on a Basic plan).
WEATHER_FIELDS = [
    ("rainfall_mm", same, ["rainfall_mm", "rainfall_last_15_min_mm"]),
    ("rain_rate_max_mm_per_hr", same, ["rain_rate_hi_mm", "rain_rate_hi_last_15_min_mm"]),
    ("temperature_c", f_to_c, ["temp_last", "temp"]),
    ("temperature_max_c", f_to_c, ["temp_hi"]),
    ("temperature_min_c", f_to_c, ["temp_lo"]),
    ("humidity_pct", same, ["hum_last", "hum"]),
    ("wind_speed_kmh", mph_to_kmh, ["wind_speed_avg", "wind_speed_avg_last_10_min"]),
    ("wind_gust_kmh", mph_to_kmh, ["wind_speed_hi", "wind_speed_hi_last_10_min"]),
    ("wind_direction_deg", same, ["wind_dir_of_prevail", "wind_dir_scalar_avg_last_10_min"]),
    ("heat_index_c", f_to_c, ["heat_index_last", "heat_index"]),
    ("dew_point_c", f_to_c, ["dew_point_last", "dew_point"]),
    ("pressure_hpa", inhg_to_hpa, ["bar_sea_level"]),
]


def weather_values(values: dict) -> dict:
    """The readings conservation teams use, with clear names in metric units."""
    result = {}
    for name, convert, davis_fields in WEATHER_FIELDS:
        value = next((values[f] for f in davis_fields if values.get(f) is not None), None)
        if value is not None:
            result[name] = convert(value)
    return result


def transform(station: client.Station, ts: int, values: dict) -> dict:
    return {
        "source": str(station.station_id),
        "source_name": station.station_name or str(station.station_id),
        "type": "stationary-object",
        "subject_type": "weather_station",
        "recorded_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "location": {
            "lat": station.latitude,
            "lon": station.longitude,
        },
        "additional": weather_values(values),
    }


def time_windows(start: datetime, end: datetime, size_seconds: int = client.MAX_HISTORIC_RANGE_SECONDS) -> Iterable[tuple]:
    """Split (start, end] into windows WeatherLink accepts."""
    window_start = start
    while window_start < end:
        window_end = min(window_start + timedelta(seconds=size_seconds), end)
        yield window_start, window_end
        window_start = window_end


async def get_cursor(integration_id, station_id: int) -> Optional[datetime]:
    state = await state_manager.get_state(
        integration_id=integration_id,
        action_id="pull_observations",
        source_id=str(station_id),
    )
    if not state or not state.get("last_ts"):
        return None
    return datetime.fromtimestamp(state["last_ts"], tz=timezone.utc)


async def save_cursor(integration_id, station_id: int, last_ts: int):
    await state_manager.set_state(
        integration_id=integration_id,
        action_id="pull_observations",
        state={"last_ts": last_ts},
        source_id=str(station_id),
    )


async def send_readings(integration_id, station: client.Station, readings: Dict[int, dict]) -> int:
    observations = [transform(station, ts, values) for ts, values in sorted(readings.items())]
    # A record holding only fields that aren't sent (for example a barometer
    # record on its own) would arrive empty, so skip it.
    observations = [o for o in observations if o["additional"]]
    for batch in generate_batches(observations, OBSERVATION_BATCH_SIZE):
        await send_observations_to_gundi(observations=list(batch), integration_id=integration_id)
    return len(observations)


async def pull_station(integration, station: client.Station, api_key: str, api_secret: str, lookback_hours: int) -> int:
    """Send every reading since the station's cursor. Returns how many were sent."""
    base_url = weatherlink_base_url(integration)
    now = datetime.now(timezone.utc)
    cursor = await get_cursor(integration.id, station.station_id)
    start = cursor or now - timedelta(hours=lookback_hours)
    sent = 0
    try:
        for window_start, window_end in time_windows(start, now):
            station_data = await client.get_historic(
                station_id=station.station_id,
                start=window_start,
                end=window_end,
                api_key=api_key,
                api_secret=api_secret,
                base_url=base_url,
            )
            readings = merge_records_by_time(station_data)
            if readings:
                sent += await send_readings(integration.id, station, readings)
                await save_cursor(integration.id, station.station_id, max(readings))
    except client.WeatherLinkPermissionError:
        # Stations shared on a Basic plan have no historic data: send the
        # latest reading instead, once per new timestamp.
        logger.info(f"No historic access for station {station.station_id}, using current conditions.")
        station_data = await client.get_current(
            station_id=station.station_id, api_key=api_key, api_secret=api_secret, base_url=base_url
        )
        readings = merge_records_by_time(station_data)
        if cursor:
            cutoff = int(cursor.timestamp())
            readings = {ts: values for ts, values in readings.items() if ts > cutoff}
        if readings:
            # Sensors report at slightly different times; send one reading
            # stamped with the newest time, holding every sensor's latest values.
            latest_ts = max(readings)
            merged = {}
            for ts in sorted(readings):
                merged.update(readings[ts])
            sent += await send_readings(integration.id, station, {latest_ts: merged})
            await save_cursor(integration.id, station.station_id, latest_ts)
    return sent


@activity_logger()
async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id}...")
    try:
        stations = await client.get_stations(
            api_key=action_config.api_key.get_secret_value(),
            api_secret=action_config.api_secret.get_secret_value(),
            base_url=weatherlink_base_url(integration),
        )
    except client.WeatherLinkAuthError as e:
        return {"valid_credentials": False, "status_code": e.status_code, "message": str(e)}
    return {
        "valid_credentials": True,
        "stations": [{"station_id": s.station_id, "station_name": s.station_name} for s in stations],
    }


@crontab_schedule("*/5 * * * *")
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id}...")
    auth_config = get_auth_config(integration)
    api_key = auth_config.api_key.get_secret_value()
    api_secret = auth_config.api_secret.get_secret_value()

    stations = await client.get_stations(
        api_key=api_key, api_secret=api_secret, base_url=weatherlink_base_url(integration)
    )

    observations_sent = 0
    stations_failed: List[str] = []
    for station in stations:
        name = station.station_name or str(station.station_id)
        if not station.active:
            continue
        if station.latitude is None or station.longitude is None:
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_observations",
                level=LogLevel.WARNING,
                title=f"Readings from station {name} weren't sent because WeatherLink has no location for it.",
                data={"station_id": station.station_id},
            )
            continue
        try:
            observations_sent += await pull_station(
                integration, station, api_key, api_secret, action_config.default_lookback_hours
            )
        except client.WeatherLinkError as e:
            # One broken station shouldn't stop the others.
            stations_failed.append(name)
            logger.warning(f"Station {station.station_id} failed: {e!r}")
            await log_action_activity(
                integration_id=integration.id,
                action_id="pull_observations",
                level=LogLevel.WARNING,
                title=f"Readings from station {name} weren't fetched because WeatherLink answered with an error.",
                data={"station_id": station.station_id, "status_code": e.status_code, "message": str(e)},
            )

    return {
        "stations": len(stations),
        "stations_failed": stations_failed,
        "observations_sent": observations_sent,
    }
