import pydantic

from app.services.errors import ConfigurationNotFound
from app.services.utils import FieldWithUIOptions, GlobalUISchemaOptions, find_config_for_action
from .core import AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin


MAX_LOOKBACK_HOURS = 168  # 7 days


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="WeatherLink API Key",
        description="The API Key v2 from the WeatherLink account page (weatherlink.com/account).",
        format="password",
    )
    api_secret: pydantic.SecretStr = FieldWithUIOptions(
        ...,
        title="WeatherLink API Secret",
        description="The API Secret generated together with the API Key.",
        format="password",
    )

    ui_global_options: GlobalUISchemaOptions = GlobalUISchemaOptions(
        order=[
            "api_key",
            "api_secret",
        ],
    )


class PullObservationsConfig(PullActionConfiguration):
    default_lookback_hours: int = FieldWithUIOptions(
        default=24,
        title="Data Retrieval Period (Hours)",
        description=(
            "How far back to fetch weather readings the first time a station is pulled "
            "(max 168 hours = 7 days). Stations on a Basic plan only share their latest reading."
        ),
        ge=1,
        le=MAX_LOOKBACK_HOURS,
    )


def get_auth_config(integration) -> AuthenticateConfig:
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth",
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)
