"""Client for the Davis Instruments WeatherLink v2 API.

https://weatherlink.github.io/v2-api

Plain functions with plain arguments, so this module can be used on its own
(for example from a script). It raises its own exceptions and leaves retries
to the caller.
"""
from datetime import datetime, timezone
from typing import List, Optional

import logging

import httpx
import pydantic

# WeatherLink only takes the API key in the query string, and httpx logs every
# request URL at INFO. Keep httpx at WARNING so the key stays out of the logs.
logging.getLogger("httpx").setLevel(logging.WARNING)

WEATHERLINK_BASE_URL = "https://api.weatherlink.com/v2"

# WeatherLink refuses historic requests longer than one day.
MAX_HISTORIC_RANGE_SECONDS = 86400

DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


class WeatherLinkError(Exception):
    """WeatherLink answered with an error."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        self.status_code = status_code
        super().__init__(message)


class WeatherLinkAuthError(WeatherLinkError):
    """WeatherLink didn't accept the API key and secret."""


class WeatherLinkConnectionError(WeatherLinkError):
    """WeatherLink couldn't be reached or didn't answer in time."""


class WeatherLinkPermissionError(WeatherLinkError):
    """The key can't read this data, for example historic data on a Basic plan."""


class WeatherLinkRateLimitError(WeatherLinkError):
    """Too many calls for this key (1,000 per hour, 10 per second by default)."""


class Station(pydantic.BaseModel):
    station_id: int
    station_id_uuid: Optional[str] = None
    station_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation: Optional[float] = None
    time_zone: Optional[str] = None
    active: bool = True
    recording_interval: Optional[int] = None
    subscription_type: Optional[str] = None
    product_number: Optional[str] = None
    gateway_type: Optional[str] = None

    class Config:
        extra = "ignore"


class SensorData(pydantic.BaseModel):
    lsid: Optional[int] = None
    sensor_type: int
    data_structure_type: int
    data: List[dict] = []

    class Config:
        extra = "ignore"


class StationData(pydantic.BaseModel):
    station_id: int
    sensors: List[SensorData] = []

    class Config:
        extra = "ignore"


def _raise_for_status(response: httpx.Response):
    if response.is_success:
        return
    try:
        body = response.json()
    except ValueError:
        body = None
    message = (body.get("message") if isinstance(body, dict) else None) or response.text
    message = (message or "").strip()[:500]
    status = response.status_code
    if status == 401:
        raise WeatherLinkAuthError(message, status)
    if status == 403:
        raise WeatherLinkPermissionError(message, status)
    if status == 429:
        raise WeatherLinkRateLimitError(message, status)
    raise WeatherLinkError(message, status)


async def _get(base_url: str, path: str, api_key: str, api_secret: str, params: Optional[dict] = None) -> dict:
    query = {"api-key": api_key, **(params or {})}
    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.get(
                f"{base_url.rstrip('/')}/{path.lstrip('/')}",
                params=query,
                headers={"X-Api-Secret": api_secret},
            )
    except httpx.TransportError as e:
        # The message names the error type only: httpx's text can quote the
        # request URL, which carries the API key.
        raise WeatherLinkConnectionError(f"WeatherLink didn't answer ({type(e).__name__})") from e
    _raise_for_status(response)
    try:
        body = response.json()
    except ValueError as e:
        raise WeatherLinkError("WeatherLink answered with something that isn't JSON", response.status_code) from e
    if not isinstance(body, dict):
        raise WeatherLinkError("WeatherLink answered with an unexpected JSON shape", response.status_code)
    return body


def _parse(model, data):
    try:
        return model.parse_obj(data)
    except pydantic.ValidationError as e:
        raise WeatherLinkError(f"WeatherLink answered with an unexpected {model.__name__}: {e}") from e


async def get_stations(api_key: str, api_secret: str, base_url: str = WEATHERLINK_BASE_URL) -> List[Station]:
    """Every station the key can see, owned or shared."""
    body = await _get(base_url, "/stations", api_key, api_secret)
    return [_parse(Station, item) for item in body.get("stations") or []]


async def get_current(
    station_id: int, api_key: str, api_secret: str, base_url: str = WEATHERLINK_BASE_URL
) -> StationData:
    """The latest reading of each sensor at the station."""
    body = await _get(base_url, f"/current/{station_id}", api_key, api_secret)
    return _parse(StationData, body)


async def get_historic(
    station_id: int,
    start: datetime,
    end: datetime,
    api_key: str,
    api_secret: str,
    base_url: str = WEATHERLINK_BASE_URL,
) -> StationData:
    """Archive records with a timestamp after `start` and up to `end`.
    The range must be one day or less."""
    start_ts = int(start.astimezone(timezone.utc).timestamp())
    end_ts = int(end.astimezone(timezone.utc).timestamp())
    if end_ts <= start_ts:
        raise ValueError("end must be after start")
    if end_ts - start_ts > MAX_HISTORIC_RANGE_SECONDS:
        raise ValueError(f"WeatherLink allows at most {MAX_HISTORIC_RANGE_SECONDS} seconds per historic request")
    body = await _get(
        base_url,
        f"/historic/{station_id}",
        api_key,
        api_secret,
        params={"start-timestamp": start_ts, "end-timestamp": end_ts},
    )
    return _parse(StationData, body)
