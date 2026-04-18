# ruff: noqa: RUF009
from __future__ import annotations

import ipaddress
import os
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Literal, get_args

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import Any, Self

ProxyProtocolMode = Literal['off', 'v1', 'v2']
_PROXY_PROTOCOL_MODES = get_args(ProxyProtocolMode)
CONFIG_PATH_ENV_VAR = 'H2CORN_CONFIG'
_OPTION_METADATA_KEY = 'h2corn_option'


def _identity(value):
    return value


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {'1', 'true', 'yes', 'on'}:
        return True
    if normalized in {'0', 'false', 'no', 'off'}:
        return False
    raise ValueError(f'invalid boolean value: {value!r}')


def _parse_octal(value: str) -> int:
    return int(value, 8)


def _parse_csv_tuple(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(',') if item.strip())


def _validate_forwarded_allow_ip(value: str) -> str:
    if value in {'*', 'unix'}:
        return value
    try:
        if '/' in value:
            ipaddress.ip_network(value, strict=False)
        else:
            ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError(f'invalid forwarded_allow_ips entry: {value!r}') from exc
    return value


def _normalize_optional_path(value: object) -> Path | None:
    if value is None or isinstance(value, Path):
        return value
    return Path(value)


def _normalize_str_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return _parse_csv_tuple(value)
    return tuple(str(item) for item in value)


def _normalize_forwarded_allow_ips(value: object) -> tuple[str, ...]:
    return tuple(
        _validate_forwarded_allow_ip(entry) for entry in _normalize_str_tuple(value)
    )


def _normalize_proxy_protocol(value: object) -> ProxyProtocolMode:
    if value not in _PROXY_PROTOCOL_MODES:
        raise ValueError(f'invalid proxy_protocol mode: {value!r}')
    return value


def _minimum(name: str, minimum: int):
    def normalize(value):
        if value < minimum:
            raise ValueError(f'{name} must be at least {minimum}')
        return value

    return normalize


def _non_negative(name: str):
    def normalize(value):
        if value < 0:
            raise ValueError(f'{name} must be non-negative')
        return value

    return normalize


@dataclass(frozen=True, slots=True)
class OptionMetadata:
    doc: str
    env_parse: Callable[[str], Any]
    normalize: Callable[[Any], Any] = _identity
    cli_flags: tuple[str, ...] = ()
    cli_type: Callable[[str], Any] | None = None
    cli_action: str | None = None
    cli_choices: tuple[str, ...] | None = None
    cli_metavar: str | None = None
    toml_key: str | None = None


@dataclass(frozen=True, slots=True)
class ConfigOption:
    name: str
    metadata: OptionMetadata
    default: Any

    @property
    def env_var(self) -> str:
        return f'H2CORN_{self.name.upper()}'

    @property
    def toml_key(self) -> str:
        return self.metadata.toml_key or self.name

    @property
    def cli_flags(self) -> tuple[str, ...]:
        return self.metadata.cli_flags or (f'--{self.name.replace("_", "-")}',)

    def help_text(self) -> str:
        return f'{self.metadata.doc} [env: {self.env_var}]'


def _option(
    *,
    default=MISSING,
    default_factory=MISSING,
    doc: str,
    env_parse: Callable[[str], Any],
    normalize: Callable[[Any], Any] = _identity,
    cli_flags: tuple[str, ...] = (),
    cli_type: Callable[[str], Any] | None = None,
    cli_action: str | None = None,
    cli_choices: tuple[str, ...] | None = None,
    cli_metavar: str | None = None,
    toml_key: str | None = None,
):
    metadata = {
        _OPTION_METADATA_KEY: OptionMetadata(
            doc=doc,
            env_parse=env_parse,
            normalize=normalize,
            cli_flags=cli_flags,
            cli_type=cli_type,
            cli_action=cli_action,
            cli_choices=cli_choices,
            cli_metavar=cli_metavar,
            toml_key=toml_key,
        )
    }
    kwargs = {'metadata': metadata}
    if default_factory is not MISSING:
        kwargs['default_factory'] = default_factory
    else:
        kwargs['default'] = default
    return field(**kwargs)


def config_options() -> tuple[ConfigOption, ...]:
    options = []
    for config_field in fields(Config):
        if _OPTION_METADATA_KEY not in config_field.metadata:
            continue
        metadata = config_field.metadata[_OPTION_METADATA_KEY]
        if config_field.default is not MISSING:
            default = config_field.default
        else:
            default = config_field.default_factory()
        options.append(
            ConfigOption(
                name=config_field.name,
                metadata=metadata,
                default=default,
            )
        )
    return tuple(options)


def _env_values(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = env or os.environ
    values: dict[str, Any] = {}
    for option in config_options():
        if option.env_var in env:
            values[option.name] = option.metadata.env_parse(env[option.env_var])
    return values


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    """
    Configuration parameters for the ASGI server.

    This immutable structure dictates network bindings, concurrency, proxy trust,
    and operational bounds. Unix socket paths (`uds`) and permissions are not
    supported on Windows.
    """

    host: str = _option(
        default='127.0.0.1',
        doc='The IP address or hostname to bind.',
        env_parse=str,
    )
    port: int = _option(
        default=8000,
        doc='The TCP port to listen on.',
        env_parse=int,
        cli_flags=('-p', '--port'),
        cli_type=int,
    )
    fd: int | None = _option(
        default=None,
        doc='Adopt an already-bound listening socket by file descriptor.',
        env_parse=int,
        normalize=lambda value: value if value is None else _minimum('fd', 0)(value),
        cli_type=int,
    )
    uds: Path | None = _option(
        default=None,
        doc='Path to a Unix Domain Socket to bind. (Not supported on Windows).',
        env_parse=Path,
        normalize=_normalize_optional_path,
        cli_type=Path,
    )
    uds_permissions: int | None = _option(
        default=None,
        doc='Octal mask for Unix Domain Socket permissions.',
        env_parse=_parse_octal,
        cli_type=_parse_octal,
    )
    backlog: int = _option(
        default=1024,
        doc='The maximum number of queued connections allowed on the socket.',
        env_parse=int,
        normalize=_minimum('backlog', 1),
        cli_type=int,
    )
    workers: int = _option(
        default=1,
        doc='The number of child worker processes to spawn.',
        env_parse=int,
        normalize=_minimum('workers', 1),
        cli_flags=('-w', '--workers'),
        cli_type=int,
    )
    max_requests: int = _option(
        default=0,
        doc='Maximum number of requests or WebSocket sessions a worker should complete before retiring. Use 0 to disable.',
        env_parse=int,
        normalize=_non_negative('max_requests'),
        cli_type=int,
    )
    max_requests_jitter: int = _option(
        default=0,
        doc='Maximum jitter added to max_requests to stagger worker retirements. Use 0 to disable.',
        env_parse=int,
        normalize=_non_negative('max_requests_jitter'),
        cli_type=int,
    )
    timeout_worker_healthcheck: float = _option(
        default=0.0,
        doc='Maximum time between worker healthcheck heartbeats before the supervisor replaces the worker. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_worker_healthcheck'),
        cli_type=float,
    )
    http1: bool = _option(
        default=True,
        doc='Whether HTTP/1.1 is supported. Intended for development purposes only; disable in production.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    access_log: bool = _option(
        default=True,
        doc='Whether requests should be logged to stderr.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    root_path: str = _option(
        default='',
        doc='ASGI root path (to mount the application at a subpath).',
        env_parse=str,
        cli_flags=('-r', '--root-path'),
    )
    max_concurrent_streams: int = _option(
        default=256,
        doc='Maximum active HTTP/2 streams per connection.',
        env_parse=int,
        normalize=_non_negative('max_concurrent_streams'),
        cli_type=int,
    )
    limit_request_line: int = _option(
        default=0,
        doc='The maximum size of the HTTP/1.1 request line in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('limit_request_line'),
        cli_type=int,
    )
    limit_request_fields: int = _option(
        default=0,
        doc='Limit the number of HTTP/1.1 header fields in a request. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('limit_request_fields'),
        cli_type=int,
    )
    limit_request_field_size: int = _option(
        default=0,
        doc='Limit the size of an individual HTTP/1.1 header field in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('limit_request_field_size'),
        cli_type=int,
    )
    h2_max_header_list_size: int = _option(
        default=0,
        doc='Maximum decoded HTTP/2 header list size in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('h2_max_header_list_size'),
        cli_type=int,
    )
    max_request_body_size: int = _option(
        default=0,
        doc='Maximum request body size in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('max_request_body_size'),
        cli_type=int,
    )
    timeout_handshake: float = _option(
        default=5.0,
        doc='Time limit to establish a connection/handshake (seconds).',
        env_parse=float,
        normalize=_non_negative('timeout_handshake'),
        cli_type=float,
    )
    timeout_graceful_shutdown: float = _option(
        default=30.0,
        doc='Time allowed for workers to finish existing requests on stop.',
        env_parse=float,
        normalize=_non_negative('timeout_graceful_shutdown'),
        cli_type=float,
    )
    timeout_keep_alive: float = _option(
        default=5.0,
        doc='Idle keep-alive timeout in seconds. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_keep_alive'),
        cli_type=float,
    )
    timeout_read: float = _option(
        default=0.0,
        doc='Timeout in seconds for reading an HTTP request head or body after the connection is established. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_read'),
        cli_type=float,
    )
    limit_concurrency: int = _option(
        default=0,
        doc='Maximum number of concurrent ASGI request/session tasks per worker. Use 0 to disable.',
        env_parse=int,
        normalize=_non_negative('limit_concurrency'),
        cli_type=int,
    )
    timeout_lifespan_startup: float = _option(
        default=60.0,
        doc='Maximum time to wait for ASGI lifespan startup in seconds. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_lifespan_startup'),
        cli_type=float,
    )
    timeout_lifespan_shutdown: float = _option(
        default=30.0,
        doc='Maximum time to wait for ASGI lifespan shutdown in seconds. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_lifespan_shutdown'),
        cli_type=float,
    )
    websocket_max_message_size: int | None = _option(
        default=16_777_216,
        doc='Maximum WebSocket message size in bytes. Defaults to 16 MiB. Set to `None` to inherit `max_request_body_size`. Use 0 for no limit.',
        env_parse=int,
        normalize=lambda value: (
            value
            if value is None
            else _non_negative('websocket_max_message_size')(value)
        ),
        cli_type=int,
    )
    websocket_per_message_deflate: bool = _option(
        default=True,
        doc='Whether to negotiate permessage-deflate for WebSockets when the client offers it.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    websocket_ping_interval: float = _option(
        default=0.0,
        doc='Interval in seconds between server WebSocket ping frames. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('websocket_ping_interval'),
        cli_type=float,
    )
    websocket_ping_timeout: float = _option(
        default=0.0,
        doc='Time limit in seconds to wait for a pong after a server WebSocket ping. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('websocket_ping_timeout'),
        cli_type=float,
    )
    proxy_headers: bool = _option(
        default=False,
        doc='Trust proxy headers (e.g., Forwarded, X-Forwarded-*) if the client IP is in `forwarded_allow_ips`.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    forwarded_allow_ips: tuple[str, ...] = _option(
        default_factory=lambda: ('127.0.0.1', '::1', 'unix'),
        doc="Allowed IPs or networks (in CIDR notation) for proxy headers. Use '*' to trust all.",
        env_parse=_parse_csv_tuple,
        normalize=_normalize_forwarded_allow_ips,
        cli_type=_parse_csv_tuple,
    )
    proxy_protocol: ProxyProtocolMode = _option(
        default='off',
        doc="Expect HAProxy's PROXY protocol on inbound connections.",
        env_parse=_normalize_proxy_protocol,
        normalize=_normalize_proxy_protocol,
        cli_choices=_PROXY_PROTOCOL_MODES,
    )
    server_header: bool = _option(
        default=False,
        doc='Whether h2corn should add a default Server header when the application did not set one.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    date_header: bool = _option(
        default=True,
        doc='Whether h2corn should add a default Date header when the application did not set one.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    response_headers: tuple[str, ...] = _option(
        default_factory=tuple,
        doc='Additional default response headers in `name: value` form. Repeat the flag to add more headers.',
        env_parse=_parse_csv_tuple,
        normalize=_normalize_str_tuple,
        cli_flags=('--header',),
        cli_action='append',
        cli_type=str,
        cli_metavar='HEADER',
    )
    _fd_is_unix: bool | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _control_write_fd: int | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        for option in config_options():
            normalized = option.metadata.normalize(getattr(self, option.name))
            object.__setattr__(self, option.name, normalized)
        if self.fd is not None and self.uds is not None:
            raise ValueError('fd and uds are mutually exclusive')

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        return cls(**_env_values(env))

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> Self:
        option_map = {option.toml_key: option for option in config_options()}
        if unknown := sorted(data.keys() - option_map.keys()):
            raise ValueError(f'unknown config keys: {", ".join(unknown)}')

        values = {option_map[key].name: value for key, value in data.items()}
        return cls(**values)

    @classmethod
    def from_toml(cls, path: Path) -> Self:
        import tomllib

        with path.open('rb') as handle:
            data = tomllib.load(handle)
        return cls.from_mapping(data)
