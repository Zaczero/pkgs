# ruff: noqa: RUF009
from __future__ import annotations

import ipaddress
import os
from collections.abc import Callable
from dataclasses import MISSING, dataclass, field, fields
from functools import cache
from pathlib import Path
from typing import Any, Literal, get_args

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Self

ProxyProtocolMode = Literal['off', 'v1', 'v2']
LifespanMode = Literal['auto', 'on', 'off']
_PROXY_PROTOCOL_MODES = get_args(ProxyProtocolMode)
_LIFESPAN_MODES = get_args(LifespanMode)
CONFIG_PATH_ENV_VAR = 'H2CORN_CONFIG'
_OPTION_METADATA_KEY = 'h2corn_option'
_H2_MIN_FRAME_SIZE = 16_384
_H2_MAX_FRAME_SIZE = 16_777_215
_U32_MAX = (1 << 32) - 1
_DEFAULT_BIND = ('127.0.0.1:8000',)
_CONVENIENCE_KEYS = frozenset({'host', 'port'})
_Normalize = Callable[[Any], Any]
_Parse = Callable[[str], Any]


def _identity(value):
    return value


def _parse_bool(value: str):
    normalized = value.strip().lower()
    if normalized in {'1', 'true', 'yes', 'on'}:
        return True
    if normalized in {'0', 'false', 'no', 'off'}:
        return False
    raise ValueError(f'invalid boolean value: {value!r}')


def _parse_octal(value: str):
    return int(value, 8)


def _parse_csv_tuple(value: str):
    return tuple(item.strip() for item in value.split(',') if item.strip())


def _parse_bind_env(value: str):
    return tuple(
        item.strip()
        for line in value.splitlines()
        for item in line.split(',')
        if item.strip()
    )


def _parse_optional_non_negative_int(value: str):
    if value.strip().lower() == 'inherit':
        return None
    return int(value)


def _validate_forwarded_allow_ip(value: str):
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


def _invalid_bind_target(kind: str, value: str, detail: str):
    return ValueError(f'invalid {kind} bind target {value!r}: {detail}')


def _normalize_str_tuple(value):
    if value is None:
        return ()
    if isinstance(value, str):
        return _parse_csv_tuple(value)
    if isinstance(value, tuple | list | set | frozenset):
        return tuple(str(item) for item in value)
    raise TypeError('expected a string or an iterable of strings')


def _normalize_forwarded_allow_ips(value):
    return tuple(
        _validate_forwarded_allow_ip(entry) for entry in _normalize_str_tuple(value)
    )


@dataclass(frozen=True, slots=True)
class TcpBindSpec:
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class UnixBindSpec:
    path: Path


@dataclass(frozen=True, slots=True)
class FdBindSpec:
    fd: int


BindSpec = TcpBindSpec | UnixBindSpec | FdBindSpec


def _parse_bind_spec(value: str):
    value = value.strip()
    if not value:
        raise ValueError('bind entries must not be empty')
    if value.startswith('unix:'):
        raw_path = value[5:]
        if not raw_path:
            raise _invalid_bind_target('unix', value, 'path must not be empty')
        return UnixBindSpec(Path(raw_path))
    if value.startswith('fd://'):
        fd = int(value[5:])
        if fd < 0:
            raise _invalid_bind_target('fd', value, 'must be non-negative')
        return FdBindSpec(fd)
    if value.startswith('['):
        host, sep, port = value[1:].rpartition(']:')
    else:
        host, sep, port = value.rpartition(':')
    if not sep:
        raise _invalid_bind_target('TCP', value, 'expected host:port')
    if not host:
        raise _invalid_bind_target('TCP', value, 'host must not be empty')
    tcp_port = int(port)
    if not 0 <= tcp_port <= 65_535:
        raise _invalid_bind_target('TCP', value, 'port must be between 0 and 65535')
    return TcpBindSpec(host, tcp_port)


def _format_tcp_bind(host: str, port: int):
    if ':' in host and not host.startswith('['):
        return f'[{host}]:{port}'
    return f'{host}:{port}'


def _normalize_bind_specs(value):
    if value is None:
        return _DEFAULT_BIND
    if isinstance(value, str):
        items = _parse_bind_env(value)
    elif isinstance(value, tuple | list | set | frozenset):
        items = tuple(item for item in (str(item).strip() for item in value) if item)
    else:
        raise TypeError('bind must be a string or an iterable of bind addresses')
    if not items:
        raise ValueError('bind must contain at least one entry')
    normalized = []
    tcp_ports: set[int] = set()
    for item in items:
        match _parse_bind_spec(item):
            case TcpBindSpec(host, port):
                normalized.append(_format_tcp_bind(host, port))
                tcp_ports.add(port)
            case UnixBindSpec(path):
                normalized.append(f'unix:{path}')
            case FdBindSpec(fd):
                normalized.append(f'fd://{fd}')
    if 0 in tcp_ports and len(tcp_ports) > 1:
        raise ValueError('bind cannot mix port 0 with explicit ports')
    return tuple(normalized)


def _bind_from_convenience(
    host: str | None,
    port: int | None,
):
    if host is None and port is None:
        return None
    return (_format_tcp_bind(host or '127.0.0.1', 8000 if port is None else port),)


def _sync_bind_convenience_fields(config: Config):
    host = port = None
    if len(config.bind) == 1 and isinstance(
        spec := _parse_bind_spec(config.bind[0]),
        TcpBindSpec,
    ):
        host, port = spec.host, spec.port
    object.__setattr__(config, 'host', host)
    object.__setattr__(config, 'port', port)


def _normalize_proxy_protocol(value):
    if value not in _PROXY_PROTOCOL_MODES:
        raise ValueError(f'invalid proxy_protocol mode: {value!r}')
    return value


def _normalize_lifespan(value):
    if value not in _LIFESPAN_MODES:
        raise ValueError(f'invalid lifespan mode: {value!r}')
    return value


def _minimum(name: str, minimum: int):
    def normalize(value: int):
        if value < minimum:
            raise ValueError(f'{name} must be at least {minimum}')
        return value

    return normalize


def _non_negative(name: str):
    def normalize(value: int | float):
        if value < 0:
            raise ValueError(f'{name} must be non-negative')
        return value

    return normalize


def _non_negative_u32(name: str):
    def normalize(value: int):
        if value < 0:
            raise ValueError(f'{name} must be non-negative')
        if value > _U32_MAX:
            raise ValueError(f'{name} must be at most {_U32_MAX}')
        return value

    return normalize


def _h2_frame_size(name: str):
    def normalize(value: int):
        if not _H2_MIN_FRAME_SIZE <= value <= _H2_MAX_FRAME_SIZE:
            raise ValueError(
                f'{name} must be between {_H2_MIN_FRAME_SIZE} and {_H2_MAX_FRAME_SIZE}'
            )
        return value

    return normalize


def _optional_non_negative(name: str):
    normalize = _non_negative(name)

    def validate(value: int | None):
        return None if value is None else normalize(value)

    return validate


@dataclass(frozen=True, slots=True)
class OptionMetadata:
    doc: str
    env_parse: _Parse
    normalize: _Normalize = _identity
    cli_flags: tuple[str, ...] = ()
    cli_type: _Parse | None = None
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
    env_parse: _Parse,
    normalize: _Normalize = _identity,
    cli_flags: tuple[str, ...] = (),
    cli_type: _Parse | None = None,
    cli_action: str | None = None,
    cli_choices: tuple[str, ...] | None = None,
    cli_metavar: str | None = None,
    toml_key: str | None = None,
) -> Any:
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
    if default_factory is not MISSING:
        return field(
            default_factory=default_factory,
            metadata=metadata,
        )
    return field(default=default, metadata=metadata)


@cache
def config_options() -> tuple[ConfigOption, ...]:
    options: list[ConfigOption] = []
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


def _env_values(env: Mapping[str, str] | None = None):
    if env is None:
        env = os.environ
    values = {
        option.name: option.metadata.env_parse(raw)
        for option in config_options()
        if (raw := env.get(option.env_var)) is not None
    }
    if 'bind' not in values:
        values |= {
            key: parse(raw)
            for key, parse in (('host', str), ('port', int))
            if (raw := env.get(f'H2CORN_{key.upper()}')) is not None
        }
    return values


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    """
    Configuration parameters for the ASGI server.

    This immutable structure dictates network bindings, concurrency, proxy trust,
    and operational bounds. Unix listeners (`unix:PATH`) and their permissions
    are not supported on Windows.
    """

    bind: tuple[str, ...] = _option(
        default_factory=lambda: _DEFAULT_BIND,
        doc='Listener addresses to bind. Repeat the flag to add more listeners. Supports HOST:PORT, [IPv6]:PORT, unix:PATH, and fd://N.',
        env_parse=_parse_bind_env,
        normalize=_normalize_bind_specs,
        cli_action='append',
        cli_type=str,
        cli_metavar='ADDRESS',
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
        default=30.0,
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
        normalize=_non_negative_u32('max_concurrent_streams'),
        cli_type=int,
    )
    limit_request_head_size: int = _option(
        default=1_048_576,
        doc='Limit the total size of an HTTP/1.1 request head in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('limit_request_head_size'),
        cli_type=int,
    )
    limit_request_line: int = _option(
        default=16_384,
        doc='The maximum size of the HTTP/1.1 request line in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('limit_request_line'),
        cli_type=int,
    )
    limit_request_fields: int = _option(
        default=100,
        doc='Limit the number of HTTP/1.1 header fields in a request. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('limit_request_fields'),
        cli_type=int,
    )
    limit_request_field_size: int = _option(
        default=32_768,
        doc='Limit the size of an individual HTTP/1.1 header field in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('limit_request_field_size'),
        cli_type=int,
    )
    h2_max_header_list_size: int = _option(
        default=1_048_576,
        doc='Maximum decoded HTTP/2 header list size in bytes. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative_u32('h2_max_header_list_size'),
        cli_type=int,
    )
    h2_max_header_block_size: int = _option(
        default=1_048_576,
        doc='Maximum compressed HTTP/2 header block size in bytes while collecting HEADERS and CONTINUATION frames. Use 0 for no limit.',
        env_parse=int,
        normalize=_non_negative('h2_max_header_block_size'),
        cli_type=int,
    )
    h2_max_inbound_frame_size: int = _option(
        default=65_536,
        doc='Maximum inbound HTTP/2 frame payload size to accept and advertise via SETTINGS_MAX_FRAME_SIZE.',
        env_parse=int,
        normalize=_h2_frame_size('h2_max_inbound_frame_size'),
        cli_type=int,
    )
    max_request_body_size: int = _option(
        default=1_073_741_824,
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
        default=120.0,
        doc='Idle keep-alive timeout in seconds. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_keep_alive'),
        cli_type=float,
    )
    timeout_request_header: float = _option(
        default=10.0,
        doc='Idle timeout in seconds while reading an HTTP request head or an HTTP/2 header block. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_request_header'),
        cli_type=float,
    )
    timeout_request_body_idle: float = _option(
        default=60.0,
        doc='Idle timeout in seconds while reading an HTTP request body. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('timeout_request_body_idle'),
        cli_type=float,
    )
    limit_concurrency: int = _option(
        default=0,
        doc='Maximum number of concurrent ASGI request/session tasks per worker. Use 0 to disable.',
        env_parse=int,
        normalize=_non_negative('limit_concurrency'),
        cli_type=int,
    )
    limit_connections: int = _option(
        default=0,
        doc='Maximum number of live client connections per worker. Use 0 to disable.',
        env_parse=int,
        normalize=_non_negative('limit_connections'),
        cli_type=int,
    )
    runtime_threads: int = _option(
        default=2,
        doc='Number of Tokio runtime worker threads per worker process.',
        env_parse=int,
        normalize=_minimum('runtime_threads', 1),
        cli_type=int,
    )
    lifespan: LifespanMode = _option(
        default='auto',
        doc='ASGI lifespan handling mode.',
        env_parse=_normalize_lifespan,
        normalize=_normalize_lifespan,
        cli_choices=_LIFESPAN_MODES,
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
        doc="Maximum WebSocket message size in bytes. Defaults to 16 MiB. Use 'inherit' to follow `max_request_body_size`, or 0 for no limit.",
        env_parse=_parse_optional_non_negative_int,
        normalize=_optional_non_negative('websocket_max_message_size'),
        cli_type=_parse_optional_non_negative_int,
    )
    websocket_per_message_deflate: bool = _option(
        default=True,
        doc='Whether to negotiate permessage-deflate for WebSockets when the client offers it.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    websocket_ping_interval: float = _option(
        default=60.0,
        doc='Interval in seconds between server WebSocket ping frames. Use 0 to disable.',
        env_parse=float,
        normalize=_non_negative('websocket_ping_interval'),
        cli_type=float,
    )
    websocket_ping_timeout: float = _option(
        default=30.0,
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
    _control_write_fd: int | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    host: str | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    port: int | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    _owned_socket_paths: tuple[Path, ...] = field(
        default=(),
        init=False,
        repr=False,
        compare=False,
    )
    _bind_fd_is_unix: tuple[bool, ...] = field(
        default=(),
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self):
        for option in config_options():
            normalized = option.metadata.normalize(getattr(self, option.name))
            object.__setattr__(self, option.name, normalized)
        convenience_bind = _bind_from_convenience(self.host, self.port)
        if convenience_bind is not None:
            if self.bind != _DEFAULT_BIND:
                raise ValueError('bind cannot be combined with host or port')
            object.__setattr__(self, 'bind', convenience_bind)
        _sync_bind_convenience_fields(self)
        if self.websocket_ping_timeout > 0 and self.websocket_ping_interval == 0:
            raise ValueError('websocket_ping_timeout requires websocket_ping_interval')

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        return cls(**_env_values(env))

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> Self:
        option_map = {option.toml_key: option for option in config_options()}
        if unknown := sorted(data.keys() - option_map.keys() - _CONVENIENCE_KEYS):
            raise ValueError(f'unknown config keys: {", ".join(unknown)}')

        values = {
            option.name: option.metadata.env_parse(value)
            if isinstance(value, str)
            else value
            for key, value in data.items()
            if (option := option_map.get(key)) is not None
        }
        values |= {key: data[key] for key in _CONVENIENCE_KEYS & data.keys()}
        return cls(**values)

    @classmethod
    def from_toml(cls, path: str | os.PathLike[str]) -> Self:
        import tomllib

        path = Path(path)
        with path.open('rb') as handle:
            data = tomllib.load(handle)
        return cls.from_mapping(data)
