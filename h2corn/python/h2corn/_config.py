from __future__ import annotations

import importlib.util
import ipaddress
import os
import socket
from collections.abc import Callable
from dataclasses import MISSING, dataclass, field, fields
from functools import cache
from pathlib import Path
from typing import (
    Any,
    Literal,
    TypeVar,
    cast,
    dataclass_transform,
    get_args,
    overload,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Self

ProxyProtocolMode = Literal['off', 'v1', 'v2']
LifespanMode = Literal['auto', 'on', 'off']
CertReqsMode = Literal['none', 'optional', 'required']
LoopImpl = Literal['auto', 'asyncio', 'uvloop']
_CliAction = Literal['bool', 'append']
_StringCollection = tuple[object, ...] | list[object] | set[object] | frozenset[object]
_StringTupleValue = str | _StringCollection | None
_PathValue = str | os.PathLike[str] | Path | None
_PrincipalValue = str | int | None
_BindValue = str | _StringCollection | None
_PROXY_PROTOCOL_MODES = get_args(ProxyProtocolMode)
_LIFESPAN_MODES = get_args(LifespanMode)
_CERT_REQS_MODES = get_args(CertReqsMode)
_LOOP_IMPLS = get_args(LoopImpl)
CONFIG_PATH_ENV_VAR = 'H2CORN_CONFIG'
_OPTION_METADATA_KEY = 'h2corn_option'
_H2_MIN_FRAME_SIZE = 16_384
_H2_MAX_FRAME_SIZE = 16_777_215
_H2_MIN_WINDOW_SIZE = 65_535
_H2_MAX_WINDOW_SIZE = 2_147_483_647
_U32_MAX = (1 << 32) - 1
DEFAULT_BIND = ('127.0.0.1:8000',)
CONVENIENCE_KEYS = frozenset({'host', 'port'})
_Normalize = Callable[[Any], Any]
_Parse = Callable[[str], Any]
_T = TypeVar('_T')
_InputT = TypeVar('_InputT')
_ClassT = TypeVar('_ClassT')


def _identity(value: Any) -> Any:
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
    return tuple(stripped for item in value.split(',') if (stripped := item.strip()))


def _parse_bind_env(value: str):
    return tuple(
        stripped
        for line in value.splitlines()
        for item in line.split(',')
        if (stripped := item.strip())
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


def _normalize_str_tuple(value: _StringTupleValue):
    if value is None:
        return ()
    if isinstance(value, str):
        return _parse_csv_tuple(value)
    return tuple(str(item) for item in value)


def _normalize_forwarded_allow_ips(value: _StringTupleValue) -> tuple[str, ...]:
    return tuple(
        _validate_forwarded_allow_ip(entry) for entry in _normalize_str_tuple(value)
    )


def _optional_path(value: _PathValue):
    if value is None or isinstance(value, Path):
        return value
    return Path(value)


def _optional_principal(name: str):
    def normalize(value: _PrincipalValue):
        if value is None:
            return None
        if isinstance(value, int):
            if value < 0:
                raise ValueError(f'{name} must be non-negative')
            return value
        if not value:
            raise ValueError(f'{name} must not be empty')
        return int(value) if value.isdecimal() else value

    return normalize


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


def parse_bind_spec(value: str) -> BindSpec:
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


def format_tcp_bind(host: str, port: int) -> str:
    if ':' in host and not host.startswith('['):
        return f'[{host}]:{port}'
    return f'{host}:{port}'


def _normalize_bind_specs(value: _BindValue):
    if value is None:
        return DEFAULT_BIND
    if isinstance(value, str):
        items = _parse_bind_env(value)
    else:
        items = tuple(item for item in (str(item).strip() for item in value) if item)
    if not items:
        raise ValueError('bind must contain at least one entry')
    normalized: list[str] = []
    tcp_ports: set[int] = set()
    for item in items:
        spec = parse_bind_spec(item)
        match spec:
            case TcpBindSpec(host, port):
                normalized.append(format_tcp_bind(host, port))
                tcp_ports.add(port)
            case UnixBindSpec(path):
                normalized.append(f'unix:{path.as_posix()}')
            case FdBindSpec(fd):
                normalized.append(f'fd://{fd}')
    if 0 in tcp_ports and len(tcp_ports) > 1:
        raise ValueError('bind cannot mix port 0 with explicit ports')
    return tuple(normalized)


def bind_from_convenience(
    host: str | None,
    port: int | None,
):
    if host is None and port is None:
        return None
    return (format_tcp_bind(host or '127.0.0.1', 8000 if port is None else port),)


def sync_bind_convenience_fields(config: Config) -> None:
    host = port = None
    if len(config.bind) == 1 and isinstance(
        spec := parse_bind_spec(config.bind[0]),
        TcpBindSpec,
    ):
        host, port = spec.host, spec.port
    object.__setattr__(config, 'host', host)
    object.__setattr__(config, 'port', port)


def _normalize_proxy_protocol(value: str) -> ProxyProtocolMode:
    if value not in _PROXY_PROTOCOL_MODES:
        raise ValueError(f'invalid proxy_protocol mode: {value!r}')
    return cast('ProxyProtocolMode', value)


def _normalize_lifespan(value: str) -> LifespanMode:
    if value not in _LIFESPAN_MODES:
        raise ValueError(f'invalid lifespan mode: {value!r}')
    return cast('LifespanMode', value)


def _normalize_cert_reqs(value: str) -> CertReqsMode:
    if value not in _CERT_REQS_MODES:
        raise ValueError(f'invalid cert_reqs mode: {value!r}')
    return cast('CertReqsMode', value)


def _normalize_loop(value: str) -> LoopImpl:
    if value not in _LOOP_IMPLS:
        raise ValueError(f'invalid loop implementation: {value!r}')
    return cast('LoopImpl', value)


def _minimum(name: str, minimum: int):
    def normalize(value: int):
        if value < minimum:
            raise ValueError(f'{name} must be at least {minimum}')
        return value

    return normalize


def _non_negative_int(name: str) -> Callable[[int], int]:
    def normalize(value: int) -> int:
        if value < 0:
            raise ValueError(f'{name} must be non-negative')
        return value

    return normalize


def _non_negative_float(name: str) -> Callable[[float], float]:
    def normalize(value: float) -> float:
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


def _h2_window_size(name: str):
    def normalize(value: int):
        if not _H2_MIN_WINDOW_SIZE <= value <= _H2_MAX_WINDOW_SIZE:
            raise ValueError(
                f'{name} must be between {_H2_MIN_WINDOW_SIZE} and {_H2_MAX_WINDOW_SIZE}'
            )
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
    normalize = _non_negative_int(name)

    def validate(value: int | None):
        return None if value is None else normalize(value)

    return validate


def _optional_octal_mask(name: str):
    def normalize(value: int | None):
        if value is None:
            return None
        if value < 0:
            raise ValueError(f'{name} must be non-negative')
        if value > 0o777:
            raise ValueError(f'{name} must be at most 0o777')
        return value

    return normalize


@dataclass(frozen=True, slots=True)
class OptionMetadata:
    doc: str
    env_parse: _Parse
    normalize: _Normalize = _identity
    cli_flags: tuple[str, ...] = ()
    cli_type: _Parse | None = None
    cli_action: _CliAction | None = None
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


@overload
def _option(
    *,
    default: _T,
    doc: str,
    env_parse: _Parse,
    converter: None = None,
    cli_flags: tuple[str, ...] = (),
    cli_type: _Parse | None = None,
    cli_action: _CliAction | None = None,
    cli_choices: tuple[str, ...] | None = None,
    cli_metavar: str | None = None,
    toml_key: str | None = None,
) -> _T: ...


@overload
def _option(
    *,
    default: _InputT,
    doc: str,
    env_parse: _Parse,
    converter: Callable[[_InputT], _T],
    cli_flags: tuple[str, ...] = (),
    cli_type: _Parse | None = None,
    cli_action: _CliAction | None = None,
    cli_choices: tuple[str, ...] | None = None,
    cli_metavar: str | None = None,
    toml_key: str | None = None,
) -> _T: ...


@overload
def _option(
    *,
    default_factory: Callable[[], _T],
    doc: str,
    env_parse: _Parse,
    converter: None = None,
    cli_flags: tuple[str, ...] = (),
    cli_type: _Parse | None = None,
    cli_action: _CliAction | None = None,
    cli_choices: tuple[str, ...] | None = None,
    cli_metavar: str | None = None,
    toml_key: str | None = None,
) -> _T: ...


@overload
def _option(
    *,
    default_factory: Callable[[], _InputT],
    doc: str,
    env_parse: _Parse,
    converter: Callable[[_InputT], _T],
    cli_flags: tuple[str, ...] = (),
    cli_type: _Parse | None = None,
    cli_action: _CliAction | None = None,
    cli_choices: tuple[str, ...] | None = None,
    cli_metavar: str | None = None,
    toml_key: str | None = None,
) -> _T: ...


def _option(
    *,
    default: Any = MISSING,
    default_factory: Any = MISSING,
    doc: str,
    env_parse: _Parse,
    converter: _Normalize | None = None,
    cli_flags: tuple[str, ...] = (),
    cli_type: _Parse | None = None,
    cli_action: _CliAction | None = None,
    cli_choices: tuple[str, ...] | None = None,
    cli_metavar: str | None = None,
    toml_key: str | None = None,
) -> Any:
    metadata = {
        _OPTION_METADATA_KEY: OptionMetadata(
            doc=doc,
            env_parse=env_parse,
            normalize=_identity if converter is None else converter,
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


@dataclass_transform(
    frozen_default=True,
    kw_only_default=True,
    field_specifiers=(field, _option),
)
def _config_dataclass(cls: type[_ClassT]) -> type[_ClassT]:
    return dataclass(frozen=True, slots=True, kw_only=True)(cls)


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
            default_factory = cast('Callable[[], Any]', config_field.default_factory)
            default = default_factory()
        options.append(
            ConfigOption(
                name=config_field.name,
                metadata=metadata,
                default=default,
            )
        )
    return tuple(options)


@dataclass(frozen=True, slots=True)
class OptionGroup:
    title: str
    blurb: str
    options: tuple[str, ...]


# Logical grouping of options by topic — the single source consumed by both
# the CLI parser sections (`_cli.py`) and the generated configuration docs
# (`scripts/gen_config_reference.py`). Both consumers enforce completeness:
# the CLI asserts every option is claimed by a group, and the docs page
# surfaces unclaimed options under an "Other" section.
OPTION_GROUPS: tuple[OptionGroup, ...] = (
    OptionGroup(
        'Application',
        'Where the ASGI app lives and how it is loaded.',
        (
            'root_path',
            'lifespan',
            'timeout_lifespan_startup',
            'timeout_lifespan_shutdown',
        ),
    ),
    OptionGroup(
        'Listeners',
        'How `h2corn` accepts connections.',
        ('bind', 'uds_permissions', 'backlog', 'reuse_port'),
    ),
    OptionGroup(
        'TLS',
        'Direct TLS for TCP listeners. Leave unset when terminating TLS at a proxy.',
        ('certfile', 'keyfile', 'ca_certs', 'cert_reqs'),
    ),
    OptionGroup(
        'Process and workers',
        'Process identity, worker pool, and lifecycle on Unix.',
        (
            'pid',
            'user',
            'group',
            'umask',
            'workers',
            'runtime_threads',
            'loop',
            'loop_threads',
            'max_requests',
            'max_requests_jitter',
            'timeout_worker_healthcheck',
        ),
    ),
    OptionGroup(
        'HTTP and resource limits',
        'Protocol behavior and per-connection bounds.',
        (
            'http1',
            'access_log',
            'max_concurrent_streams',
            'limit_request_head_size',
            'limit_request_line',
            'limit_request_fields',
            'limit_request_field_size',
            'h2_max_header_list_size',
            'h2_max_header_block_size',
            'h2_max_inbound_frame_size',
            'h2_initial_stream_window_size',
            'h2_initial_connection_window_size',
            'max_request_body_size',
            'limit_concurrency',
            'limit_connections',
        ),
    ),
    OptionGroup(
        'Timeouts',
        'Connection-level timeouts. All values are in seconds; `0` disables.',
        (
            'timeout_handshake',
            'timeout_graceful_shutdown',
            'timeout_keep_alive',
            'timeout_request_header',
            'timeout_request_body_idle',
            'h2_timeout_response_stall',
        ),
    ),
    OptionGroup(
        'WebSocket',
        'Limits and keep-alive for RFC 6455 and RFC 8441 WebSockets.',
        (
            'websocket_max_message_size',
            'websocket_per_message_deflate',
            'websocket_ping_interval',
            'websocket_ping_timeout',
        ),
    ),
    OptionGroup(
        'Proxy and response headers',
        'Trust boundaries with the upstream proxy and default response headers.',
        (
            'proxy_headers',
            'forwarded_allow_ips',
            'proxy_protocol',
            'server_header',
            'date_header',
            'response_headers',
        ),
    ),
)


def env_values(env: Mapping[str, str] | None = None) -> dict[str, Any]:
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


@_config_dataclass
class Config:
    """
    Immutable server configuration.

    Every option is also exposed as a CLI flag and an `H2CORN_*` environment
    variable. The full list and defaults are documented in the configuration
    reference of the project documentation.

    Resolution order (highest precedence first):

    1. CLI flags
    2. Environment variables
    3. TOML file (via `--config` or `H2CORN_CONFIG`)
    4. Built-in defaults

    Validation runs in `__post_init__`. Invalid combinations (for example,
    `certfile` without `keyfile`, or a Unix listener with TLS) raise
    `ValueError` at construction time.

    Unix listeners (`unix:PATH`) and the `user`/`group`/`umask` options
    are POSIX-only.
    """

    root_path: str = _option(
        default='',
        doc='ASGI root path (to mount the application at a subpath).',
        env_parse=str,
        cli_flags=('-r', '--root-path'),
    )
    lifespan: LifespanMode = _option(
        default='auto',
        doc='ASGI lifespan handling mode.',
        env_parse=_normalize_lifespan,
        converter=_normalize_lifespan,
        cli_choices=_LIFESPAN_MODES,
    )
    timeout_lifespan_startup: float = _option(
        default=60.0,
        doc='Maximum time to wait for ASGI lifespan startup in seconds. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('timeout_lifespan_startup'),
        cli_type=float,
    )
    timeout_lifespan_shutdown: float = _option(
        default=30.0,
        doc='Maximum time to wait for ASGI lifespan shutdown in seconds. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('timeout_lifespan_shutdown'),
        cli_type=float,
    )
    bind: tuple[str, ...] = _option(
        default_factory=lambda: DEFAULT_BIND,
        doc='Listener addresses to bind. Repeat the flag to add more listeners. Supports HOST:PORT, [IPv6]:PORT, unix:PATH, and fd://N.',
        env_parse=_parse_bind_env,
        converter=_normalize_bind_specs,
        cli_flags=('-b', '--bind'),
        cli_action='append',
        cli_type=str,
        cli_metavar='ADDRESS',
    )
    uds_permissions: int | None = _option(
        default=None,
        doc='Octal mask for Unix Domain Socket permissions.',
        env_parse=_parse_octal,
        converter=_optional_octal_mask('uds_permissions'),
        cli_type=_parse_octal,
    )
    backlog: int = _option(
        default=1024,
        doc='Maximum number of queued connections on the listening socket.',
        env_parse=int,
        converter=_minimum('backlog', 1),
        cli_type=int,
    )
    reuse_port: bool = _option(
        default=False,
        doc='Set SO_REUSEPORT on the TCP listeners so other server processes can bind the same port — for zero-downtime deploys or independent processes. Workers of one server always share the inherited listener. TCP listeners only.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    certfile: Path | None = _option(
        default=None,
        doc='PEM certificate chain file for direct TLS.',
        env_parse=Path,
        converter=_optional_path,
        cli_type=Path,
    )
    keyfile: Path | None = _option(
        default=None,
        doc='PEM private key file for direct TLS. Encrypted keys are not supported.',
        env_parse=Path,
        converter=_optional_path,
        cli_type=Path,
    )
    ca_certs: Path | None = _option(
        default=None,
        doc='PEM CA bundle used to verify client certificates for direct TLS.',
        env_parse=Path,
        converter=_optional_path,
        cli_type=Path,
    )
    cert_reqs: CertReqsMode = _option(
        default='none',
        doc='Client certificate verification mode for direct TLS.',
        env_parse=_normalize_cert_reqs,
        converter=_normalize_cert_reqs,
        cli_choices=_CERT_REQS_MODES,
    )
    pid: Path | None = _option(
        default=None,
        doc='Write the server process PID to this file.',
        env_parse=Path,
        converter=_optional_path,
        cli_type=Path,
    )
    user: str | int | None = _option(
        default=None,
        doc='User name or numeric UID for worker processes and created Unix sockets.',
        env_parse=str,
        converter=_optional_principal('user'),
        cli_flags=('-u', '--user'),
        cli_type=str,
    )
    group: str | int | None = _option(
        default=None,
        doc='Group name or numeric GID for worker processes and created Unix sockets.',
        env_parse=str,
        converter=_optional_principal('group'),
        cli_flags=('-g', '--group'),
        cli_type=str,
    )
    umask: int | None = _option(
        default=None,
        doc='Octal process umask to apply before creating files and sockets. Leave unset to preserve the inherited umask.',
        env_parse=_parse_octal,
        converter=_optional_octal_mask('umask'),
        cli_flags=('-m', '--umask'),
        cli_type=_parse_octal,
    )
    workers: int = _option(
        default=1,
        doc='Number of worker processes to spawn.',
        env_parse=int,
        converter=_minimum('workers', 1),
        cli_flags=('-w', '--workers'),
        cli_type=int,
    )
    runtime_threads: int = _option(
        default=2,
        doc='Number of Tokio runtime worker threads per worker process.',
        env_parse=int,
        converter=_minimum('runtime_threads', 1),
        cli_type=int,
    )
    loop: LoopImpl = _option(
        default='auto',
        doc="Python event-loop implementation: 'auto' uses uvloop when installed (the 'h2corn[uvloop]' extra), otherwise the stdlib asyncio loop. h2corn runs its I/O in Rust, so this only affects how application callbacks are scheduled.",
        env_parse=_normalize_loop,
        converter=_normalize_loop,
        cli_choices=_LOOP_IMPLS,
    )
    loop_threads: int = _option(
        default=1,
        doc="Number of Python event-loop threads per worker, balanced round-robin. Requires a free-threaded (no-GIL) interpreter; ignored on GIL builds. Secondary loops require the built-in asyncio or uvloop factory (the default 'auto' resolves to one); multiple loop threads are rejected when embedded in a custom loop. Each loop runs its own ASGI lifespan and state. Composes with workers — keep workers x loop_threads at or below the core count.",
        env_parse=int,
        converter=_minimum('loop_threads', 1),
        cli_type=int,
    )
    max_requests: int = _option(
        default=0,
        doc='Maximum number of requests or WebSocket sessions a worker should complete before retiring. Use 0 to disable.',
        env_parse=int,
        converter=_non_negative_int('max_requests'),
        cli_type=int,
    )
    max_requests_jitter: int = _option(
        default=0,
        doc='Maximum jitter added to max_requests to stagger worker retirements. Use 0 to disable.',
        env_parse=int,
        converter=_non_negative_int('max_requests_jitter'),
        cli_type=int,
    )
    timeout_worker_healthcheck: float = _option(
        default=30.0,
        doc='Maximum time between worker healthcheck heartbeats before the supervisor replaces the worker. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('timeout_worker_healthcheck'),
        cli_type=float,
    )
    http1: bool = _option(
        default=True,
        doc='Whether to accept HTTP/1.1 connections. Intended for development; disable in production.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    access_log: bool = _option(
        default=True,
        doc='Whether to log requests to stderr.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    max_concurrent_streams: int = _option(
        default=256,
        doc='Maximum active HTTP/2 streams per connection.',
        env_parse=int,
        converter=_non_negative_u32('max_concurrent_streams'),
        cli_type=int,
    )
    limit_request_head_size: int = _option(
        default=1_048_576,
        doc='Maximum total HTTP/1.1 request head size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('limit_request_head_size'),
        cli_type=int,
    )
    limit_request_line: int = _option(
        default=16_384,
        doc='Maximum HTTP/1.1 request line size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('limit_request_line'),
        cli_type=int,
    )
    limit_request_fields: int = _option(
        default=100,
        doc='Maximum number of request header fields; HTTP/2 counts every decoded field, including duplicates. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('limit_request_fields'),
        cli_type=int,
    )
    limit_request_field_size: int = _option(
        default=32_768,
        doc='Maximum individual HTTP/1.1 header field size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('limit_request_field_size'),
        cli_type=int,
    )
    h2_max_header_list_size: int = _option(
        default=1_048_576,
        doc='Maximum decoded HTTP/2 header list size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_u32('h2_max_header_list_size'),
        cli_type=int,
    )
    h2_max_header_block_size: int = _option(
        default=1_048_576,
        doc='Maximum compressed HTTP/2 header block size in bytes while collecting HEADERS and CONTINUATION frames. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('h2_max_header_block_size'),
        cli_type=int,
    )
    h2_max_inbound_frame_size: int = _option(
        default=65_536,
        doc='Maximum inbound HTTP/2 frame payload size to accept and advertise via SETTINGS_MAX_FRAME_SIZE.',
        env_parse=int,
        converter=_h2_frame_size('h2_max_inbound_frame_size'),
        cli_type=int,
    )
    h2_initial_stream_window_size: int = _option(
        default=1_048_576,
        doc='HTTP/2 per-stream receive flow-control window in bytes (SETTINGS_INITIAL_WINDOW_SIZE). Bounds worst-case buffered upload bytes per stream; raise for high-bandwidth-delay uploads.',
        env_parse=int,
        converter=_h2_window_size('h2_initial_stream_window_size'),
        cli_type=int,
    )
    h2_initial_connection_window_size: int = _option(
        default=2_097_152,
        doc='HTTP/2 connection-wide receive flow-control window in bytes. Bounds total buffered upload bytes per connection.',
        env_parse=int,
        converter=_h2_window_size('h2_initial_connection_window_size'),
        cli_type=int,
    )
    max_request_body_size: int = _option(
        default=1_073_741_824,
        doc='Maximum request body size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('max_request_body_size'),
        cli_type=int,
    )
    limit_concurrency: int = _option(
        default=0,
        doc='Maximum number of concurrent ASGI request or session tasks per worker. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('limit_concurrency'),
        cli_type=int,
    )
    limit_connections: int = _option(
        default=0,
        doc='Maximum number of live client connections per worker. Use 0 for no limit.',
        env_parse=int,
        converter=_non_negative_int('limit_connections'),
        cli_type=int,
    )
    timeout_handshake: float = _option(
        default=5.0,
        doc='Maximum time to establish a connection and TLS handshake, in seconds. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('timeout_handshake'),
        cli_type=float,
    )
    timeout_graceful_shutdown: float = _option(
        default=30.0,
        doc='Maximum time for workers to finish in-flight requests on shutdown, in seconds.',
        env_parse=float,
        converter=_non_negative_float('timeout_graceful_shutdown'),
        cli_type=float,
    )
    timeout_keep_alive: float = _option(
        default=120.0,
        doc='Idle keep-alive timeout in seconds. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('timeout_keep_alive'),
        cli_type=float,
    )
    timeout_request_header: float = _option(
        default=10.0,
        doc='Idle timeout in seconds while reading an HTTP request head or an HTTP/2 header block. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('timeout_request_header'),
        cli_type=float,
    )
    timeout_request_body_idle: float = _option(
        default=60.0,
        doc='Idle timeout in seconds while reading an HTTP request body. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('timeout_request_body_idle'),
        cli_type=float,
    )
    h2_timeout_response_stall: float = _option(
        default=60.0,
        doc='Timeout in seconds for HTTP/2 response body bytes pinned behind peer flow control. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('h2_timeout_response_stall'),
        cli_type=float,
    )
    websocket_max_message_size: int | None = _option(
        default=16_777_216,
        doc="Maximum WebSocket message size in bytes. Defaults to 16 MiB. Use 'inherit' to follow `max_request_body_size`, or 0 for no limit.",
        env_parse=_parse_optional_non_negative_int,
        converter=_optional_non_negative('websocket_max_message_size'),
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
        converter=_non_negative_float('websocket_ping_interval'),
        cli_type=float,
    )
    websocket_ping_timeout: float = _option(
        default=30.0,
        doc='Time limit in seconds to wait for a pong after a server WebSocket ping. Use 0 to disable.',
        env_parse=float,
        converter=_non_negative_float('websocket_ping_timeout'),
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
        converter=_normalize_forwarded_allow_ips,
        cli_type=_parse_csv_tuple,
    )
    proxy_protocol: ProxyProtocolMode = _option(
        default='off',
        doc="Expect HAProxy's PROXY protocol on inbound connections.",
        env_parse=_normalize_proxy_protocol,
        converter=_normalize_proxy_protocol,
        cli_choices=_PROXY_PROTOCOL_MODES,
    )
    server_header: bool = _option(
        default=False,
        doc='Whether to add a default Server header when the application sets none.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    date_header: bool = _option(
        default=True,
        doc='Whether to add a default Date header when the application sets none.',
        env_parse=_parse_bool,
        cli_action='bool',
    )
    response_headers: tuple[str, ...] = _option(
        default_factory=tuple,
        doc='Additional default response headers in `name: value` form. Repeat the flag to add more headers.',
        env_parse=_parse_csv_tuple,
        converter=_normalize_str_tuple,
        cli_flags=('--header',),
        cli_action='append',
        cli_type=str,
        cli_metavar='HEADER',
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

    def __post_init__(self) -> None:
        for option in config_options():
            normalized = option.metadata.normalize(getattr(self, option.name))
            object.__setattr__(self, option.name, normalized)
        convenience_bind = bind_from_convenience(self.host, self.port)
        if convenience_bind is not None:
            if self.bind != DEFAULT_BIND:
                raise ValueError('bind cannot be combined with host or port')
            object.__setattr__(self, 'bind', convenience_bind)
        sync_bind_convenience_fields(self)
        if self.reuse_port:
            if not hasattr(socket, 'SO_REUSEPORT'):
                raise ValueError('reuse_port is not supported on this platform')
            for bind in self.bind:
                if bind.startswith(('unix:', 'fd://')):
                    raise ValueError('reuse_port supports TCP listeners only')
        if self.loop == 'uvloop' and importlib.util.find_spec('uvloop') is None:
            raise ValueError(
                "loop='uvloop' requires the uvloop package — install it with "
                "the 'h2corn[uvloop]' extra"
            )
        tls_enabled = self.certfile is not None or self.keyfile is not None
        if tls_enabled and (self.certfile is None or self.keyfile is None):
            raise ValueError('certfile and keyfile must be configured together')
        if self.ca_certs is not None and self.cert_reqs == 'none':
            raise ValueError('ca_certs requires cert_reqs to be optional or required')
        if self.cert_reqs != 'none' and self.ca_certs is None:
            raise ValueError('cert_reqs optional/required requires ca_certs')
        if (self.ca_certs is not None or self.cert_reqs != 'none') and not tls_enabled:
            raise ValueError(
                'client certificate verification requires certfile and keyfile'
            )
        if tls_enabled:
            for bind in self.bind:
                match parse_bind_spec(bind):
                    case UnixBindSpec():
                        raise ValueError('TLS is supported only on TCP listeners')
                    case TcpBindSpec() | FdBindSpec():
                        pass
        if self.websocket_ping_timeout > 0 and self.websocket_ping_interval == 0:
            raise ValueError('websocket_ping_timeout requires websocket_ping_interval')

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Self:
        """
        Build a `Config` from `H2CORN_*` environment variables.

        Each option's environment variable name is its field name in upper
        case, prefixed with `H2CORN_` (for example, `H2CORN_BIND`,
        `H2CORN_WORKERS`, `H2CORN_PROXY_HEADERS`). Variables that are not set
        fall through to defaults; unrecognized variables are ignored.

        The convenience pair `H2CORN_HOST` / `H2CORN_PORT` is accepted only
        when `H2CORN_BIND` is unset.
        """
        return cls(**env_values(env))

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> Self:
        """
        Build a `Config` from a plain mapping (e.g. parsed JSON or TOML).

        Keys must match either a field name on `Config` or the convenience
        keys `host`/`port`. Unknown keys raise `ValueError`. String values
        are coerced through the same parsing rules as the matching
        environment variable; non-string values are passed through and
        validated by `__post_init__`.
        """
        option_map = {option.toml_key: option for option in config_options()}
        if unknown := sorted(data.keys() - option_map.keys() - CONVENIENCE_KEYS):
            raise ValueError(f'unknown config keys: {", ".join(unknown)}')

        values = {
            option.name: option.metadata.env_parse(value)
            if isinstance(value, str)
            else value
            for key, value in data.items()
            if (option := option_map.get(key)) is not None
        }
        values |= {key: data[key] for key in CONVENIENCE_KEYS & data.keys()}
        return cls(**values)

    @classmethod
    def from_toml(cls, path: str | os.PathLike[str]) -> Self:
        """
        Build a `Config` from a TOML file.

        The file must be a flat table whose keys correspond to `Config`
        field names. Example:

            bind = ["127.0.0.1:8000"]
            workers = 4
            proxy_headers = true
            forwarded_allow_ips = ["127.0.0.1", "::1", "unix"]
            http1 = false
        """
        import tomllib

        path = Path(path)
        with path.open('rb') as handle:
            data = tomllib.load(handle)
        return cls.from_mapping(data)
