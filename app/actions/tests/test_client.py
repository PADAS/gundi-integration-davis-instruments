from datetime import datetime, timedelta, timezone

import httpx
import pytest

import app.actions.client as client


@pytest.fixture
def weatherlink(monkeypatch):
    """Answers WeatherLink calls with `responses[path]` and records each request."""
    responses = {}
    requests = []

    def handler(request: httpx.Request):
        requests.append(request)
        status, body = responses[request.url.path]
        return httpx.Response(status, json=body)

    real_client = httpx.AsyncClient

    def fake_client(*args, **kwargs):
        return real_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", fake_client)
    return responses, requests


@pytest.mark.asyncio
async def test_the_key_goes_in_the_query_and_the_secret_in_a_header(weatherlink):
    responses, requests = weatherlink
    responses["/v2/stations"] = (200, {"stations": [
        {"station_id": 1, "station_name": "HQ", "latitude": -1.4, "longitude": 35.3, "extra": "ignored"},
    ]})

    stations = await client.get_stations(api_key="key", api_secret="secret")

    assert [(s.station_id, s.station_name, s.latitude, s.longitude) for s in stations] == [(1, "HQ", -1.4, 35.3)]
    assert requests[0].url.params["api-key"] == "key"
    assert "secret" not in str(requests[0].url)
    assert requests[0].headers["X-Api-Secret"] == "secret"


@pytest.mark.asyncio
@pytest.mark.parametrize("status, error", [
    (401, client.WeatherLinkAuthError),
    (403, client.WeatherLinkPermissionError),
    (429, client.WeatherLinkRateLimitError),
    (500, client.WeatherLinkError),
])
async def test_errors_are_raised_by_kind(weatherlink, status, error):
    responses, _ = weatherlink
    responses["/v2/current/1"] = (status, {"code": str(status), "message": "nope"})

    with pytest.raises(error) as raised:
        await client.get_current(station_id=1, api_key="key", api_secret="secret")

    assert raised.value.status_code == status
    assert str(raised.value) == "nope"


@pytest.mark.asyncio
async def test_historic_sends_unix_timestamps(weatherlink):
    responses, requests = weatherlink
    responses["/v2/historic/1"] = (200, {"station_id": 1, "sensors": [
        {"lsid": 9, "sensor_type": 37, "data_structure_type": 24, "data": [{"ts": 1700000300, "temp_last": 70}]},
    ]})
    start = datetime.fromtimestamp(1700000000, tz=timezone.utc)

    data = await client.get_historic(1, start, start + timedelta(hours=1), api_key="key", api_secret="secret")

    assert data.sensors[0].data == [{"ts": 1700000300, "temp_last": 70}]
    assert requests[0].url.params["start-timestamp"] == "1700000000"
    assert requests[0].url.params["end-timestamp"] == "1700003600"


@pytest.mark.asyncio
async def test_historic_refuses_more_than_one_day():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        await client.get_historic(1, start, start + timedelta(days=1, seconds=1), api_key="k", api_secret="s")


@pytest.mark.asyncio
async def test_a_timeout_becomes_a_connection_error_without_the_key(monkeypatch):
    real_client = httpx.AsyncClient

    def handler(request):
        raise httpx.ReadTimeout("timed out reading https://x/?api-key=key", request=request)

    monkeypatch.setattr(
        client.httpx, "AsyncClient",
        lambda *a, **kw: real_client(*a, transport=httpx.MockTransport(handler), **kw),
    )

    with pytest.raises(client.WeatherLinkConnectionError) as raised:
        await client.get_stations(api_key="key", api_secret="secret")

    assert "key" not in str(raised.value)


@pytest.mark.asyncio
async def test_an_error_body_that_is_not_an_object_still_raises_a_weatherlink_error(weatherlink):
    responses, _ = weatherlink
    responses["/v2/stations"] = (502, ["bad gateway"])

    with pytest.raises(client.WeatherLinkError) as raised:
        await client.get_stations(api_key="key", api_secret="secret")

    assert raised.value.status_code == 502


@pytest.mark.asyncio
async def test_a_success_that_is_not_json_raises_a_weatherlink_error(monkeypatch):
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        client.httpx, "AsyncClient",
        lambda *a, **kw: real_client(
            *a, transport=httpx.MockTransport(lambda r: httpx.Response(200, text="<html>maintenance</html>")), **kw),
    )

    with pytest.raises(client.WeatherLinkError):
        await client.get_current(station_id=1, api_key="key", api_secret="secret")


@pytest.mark.asyncio
async def test_an_unexpected_shape_raises_a_weatherlink_error(weatherlink):
    responses, _ = weatherlink
    responses["/v2/stations"] = (200, {"stations": [{"station_name": "no id"}]})

    with pytest.raises(client.WeatherLinkError):
        await client.get_stations(api_key="key", api_secret="secret")
