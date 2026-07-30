from __future__ import annotations

import importlib.util
import ipaddress
import math
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
ServerHeaderMode = Literal['off', 'on', 'full']
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
_SERVER_HEADER_MODES = get_args(ServerHeaderMode)
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


def _parse_header_lines(value: str):
    """Split an environment value into `name: value` headers, one per line.

    Not comma-separated like the other list options: a comma is ordinary
    inside a header value, and splitting on it turned
    `cache-control: public, max-age=60` into two headers, the second of which
    is not a header at all.
    """
    return tuple(stripped for item in value.splitlines() if (stripped := item.strip()))


def _parse_bind_env(value: str):
    return tuple(
        stripped
        for line in value.splitlines()
        for item in line.split(',')
        if (stripped := item.strip())
    )


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


def _normalize_header_tuple(value: _StringTupleValue) -> tuple[str, ...]:
    """Headers from a value that may be one header or a collection of them.

    A lone string is one header, not a comma-separated list: commas belong to
    header values, so `'cache-control: public, max-age=60'` is a single
    header and splitting it produced a second entry that was not one.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        return _parse_header_lines(value)
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
        # Exact int only: bool is a subclass of int and must not become a uid/gid.
        if type(value) is int:
            if value < 0:
                raise ValueError(f'{name} must be non-negative')
            return value
        if type(value) is not str:
            raise TypeError(
                f'{name} must be a str or int, got {type(value).__name__}'
            )
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
    if port is not None:
        if type(port) is not int:
            raise TypeError(f'port must be an int, got {type(port).__name__}')
        if not 0 <= port <= 65_535:
            raise ValueError('port must be between 0 and 65535')
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


def _normalize_server_header(value: str) -> ServerHeaderMode:
    if value not in _SERVER_HEADER_MODES:
        raise ValueError(f'invalid server_header mode: {value!r}')
    return cast('ServerHeaderMode', value)


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


def _bounded_int(
    name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> Callable[[object], int]:
    def normalize(value: object) -> int:
        # Exact built-in int only: bool and int subclasses are not integers
        # for config purposes, and float/str must not coerce in.
        if type(value) is not int:
            raise TypeError(f'{name} must be an int, got {type(value).__name__}')
        if minimum is not None and value < minimum:
            if minimum == 0 and maximum is None:
                raise ValueError(f'{name} must be non-negative')
            raise ValueError(f'{name} must be at least {minimum}')
        if maximum is not None and value > maximum:
            raise ValueError(f'{name} must be at most {maximum}')
        return value

    return normalize


def _optional_bounded_int(
    name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> Callable[[object], int | None]:
    normalize_int = _bounded_int(name, minimum=minimum, maximum=maximum)

    def normalize(value: object) -> int | None:
        if value is None:
            return None
        return normalize_int(value)

    return normalize


def _non_negative_float(name: str) -> Callable[[object], float]:
    def normalize(value: object) -> float:
        # Exact int or float only; bool is a subclass of int and must not
        # become 1.0/0.0. Normalize ints to float so later code never sees
        # an integer-typed timeout.
        if type(value) is int:
            value = float(value)
        elif type(value) is not float:
            raise TypeError(
                f'{name} must be an int or float, got {type(value).__name__}'
            )
        # `nan < 0` is false, so a NaN timeout would pass a non-negative check
        # and then silently disable every `> 0` branch that consumes it;
        # infinity reaches the supervisor's selector and raises there instead.
        if not math.isfinite(value):
            raise ValueError(f'{name} must be a finite number of seconds')
        if value < 0:
            raise ValueError(f'{name} must be non-negative')
        return value

    return normalize


def _exact_bool(name: str) -> Callable[[object], bool]:
    def normalize(value: object) -> bool:
        if type(value) is not bool:
            raise TypeError(f'{name} must be a bool, got {type(value).__name__}')
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


@dataclass(frozen=True, slots=True)
class ConfigOption:
    name: str
    metadata: OptionMetadata
    default: Any

    @property
    def env_var(self) -> str:
        return f'H2CORN_{self.name.upper()}'

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
        (
            'Connection-level timeouts in seconds. For the optional timeouts, '
            '`0` disables the timer. For `timeout_graceful_shutdown`, `0` means '
            'no graceful wait — stop immediately rather than waiting for '
            'in-flight work.'
        ),
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
        converter=_optional_bounded_int('uds_permissions', minimum=0, maximum=0o777),
        cli_type=_parse_octal,
    )
    backlog: int = _option(
        default=1024,
        doc='Maximum number of queued connections on the listening socket.',
        env_parse=int,
        converter=_bounded_int('backlog', minimum=1),
        cli_type=int,
    )
    reuse_port: bool = _option(
        default=False,
        doc='Set SO_REUSEPORT on the TCP listeners so other server processes can bind the same port — for zero-downtime deploys or independent processes. Workers of one server always share the inherited listener. TCP listeners only.',
        env_parse=_parse_bool,
        converter=_exact_bool('reuse_port'),
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
        converter=_optional_bounded_int('umask', minimum=0, maximum=0o777),
        cli_flags=('-m', '--umask'),
        cli_type=_parse_octal,
    )
    workers: int = _option(
        default=1,
        doc='Number of worker processes to spawn.',
        env_parse=int,
        converter=_bounded_int('workers', minimum=1),
        cli_flags=('-w', '--workers'),
        cli_type=int,
    )
    runtime_threads: int = _option(
        default=2,
        doc='Number of Tokio runtime worker threads per worker process.',
        env_parse=int,
        converter=_bounded_int('runtime_threads', minimum=1),
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
        converter=_bounded_int('loop_threads', minimum=1),
        cli_type=int,
    )
    max_requests: int = _option(
        default=0,
        doc='Maximum number of requests or WebSocket sessions a worker should complete before retiring. Use 0 to disable.',
        env_parse=int,
        converter=_bounded_int('max_requests', minimum=0),
        cli_type=int,
    )
    max_requests_jitter: int = _option(
        default=0,
        doc='Maximum jitter added to max_requests to stagger worker retirements. Use 0 to disable.',
        env_parse=int,
        converter=_bounded_int('max_requests_jitter', minimum=0),
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
        converter=_exact_bool('http1'),
        cli_action='bool',
    )
    access_log: bool = _option(
        default=True,
        doc='Whether to log requests to stderr.',
        env_parse=_parse_bool,
        converter=_exact_bool('access_log'),
        cli_action='bool',
    )
    max_concurrent_streams: int = _option(
        default=256,
        doc='Maximum active HTTP/2 streams per connection. At least 1.',
        env_parse=int,
        converter=_bounded_int(
            'max_concurrent_streams', minimum=1, maximum=_U32_MAX
        ),
        cli_type=int,
    )
    limit_request_head_size: int = _option(
        default=1_048_576,
        doc='Maximum total HTTP/1.1 request head size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('limit_request_head_size', minimum=0),
        cli_type=int,
    )
    limit_request_line: int = _option(
        default=16_384,
        doc='Maximum HTTP/1.1 request line size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('limit_request_line', minimum=0),
        cli_type=int,
    )
    limit_request_fields: int = _option(
        default=100,
        doc='Maximum number of request header fields; HTTP/2 counts every decoded field, including duplicates. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('limit_request_fields', minimum=0),
        cli_type=int,
    )
    limit_request_field_size: int = _option(
        default=32_768,
        doc='Maximum individual HTTP/1.1 header field size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('limit_request_field_size', minimum=0),
        cli_type=int,
    )
    h2_max_header_list_size: int = _option(
        default=1_048_576,
        doc='Maximum decoded HTTP/2 header list size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int(
            'h2_max_header_list_size', minimum=0, maximum=_U32_MAX
        ),
        cli_type=int,
    )
    h2_max_header_block_size: int = _option(
        default=1_048_576,
        doc='Maximum compressed HTTP/2 header block size in bytes while collecting HEADERS and CONTINUATION frames. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('h2_max_header_block_size', minimum=0),
        cli_type=int,
    )
    h2_max_inbound_frame_size: int = _option(
        default=65_536,
        doc='Maximum inbound HTTP/2 frame payload size to accept and advertise via SETTINGS_MAX_FRAME_SIZE.',
        env_parse=int,
        converter=_bounded_int(
            'h2_max_inbound_frame_size',
            minimum=_H2_MIN_FRAME_SIZE,
            maximum=_H2_MAX_FRAME_SIZE,
        ),
        cli_type=int,
    )
    h2_initial_stream_window_size: int = _option(
        default=8_388_608,
        doc='HTTP/2 per-stream receive flow-control window in bytes (SETTINGS_INITIAL_WINDOW_SIZE). Controls inbound upload throughput; response throughput uses the peer-advertised send window. The default is 8 MiB.',
        env_parse=int,
        converter=_bounded_int(
            'h2_initial_stream_window_size',
            minimum=_H2_MIN_WINDOW_SIZE,
            maximum=_H2_MAX_WINDOW_SIZE,
        ),
        cli_type=int,
    )
    h2_initial_connection_window_size: int = _option(
        default=8_388_608,
        doc='HTTP/2 connection-wide receive flow-control window in bytes. Bounds total charged upload bytes per connection; the default matches the 8 MiB stream window so one upload can use the whole connection budget.',
        env_parse=int,
        converter=_bounded_int(
            'h2_initial_connection_window_size',
            minimum=_H2_MIN_WINDOW_SIZE,
            maximum=_H2_MAX_WINDOW_SIZE,
        ),
        cli_type=int,
    )
    max_request_body_size: int = _option(
        default=1_073_741_824,
        doc='Maximum request body size in bytes. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('max_request_body_size', minimum=0),
        cli_type=int,
    )
    limit_concurrency: int = _option(
        default=0,
        doc='Maximum number of concurrent ASGI request or session tasks per worker. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('limit_concurrency', minimum=0),
        cli_type=int,
    )
    limit_connections: int = _option(
        default=0,
        doc='Maximum number of live client connections per worker. Use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('limit_connections', minimum=0),
        cli_type=int,
    )
    timeout_handshake: float = _option(
        default=5.0,
        doc=(
            'Maximum time from accepting a connection to a finished PROXY '
            'preamble, TLS handshake and protocol preface, in seconds. Use 0 '
            'to disable.'
        ),
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
    websocket_max_message_size: int = _option(
        default=16_777_216,
        doc='Maximum WebSocket message size in bytes. Defaults to 16 MiB; use 0 for no limit.',
        env_parse=int,
        converter=_bounded_int('websocket_max_message_size', minimum=0),
        cli_type=int,
    )
    websocket_per_message_deflate: bool = _option(
        default=True,
        doc='Whether to negotiate permessage-deflate for WebSockets when the client offers it.',
        env_parse=_parse_bool,
        converter=_exact_bool('websocket_per_message_deflate'),
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
        converter=_exact_bool('proxy_headers'),
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
    server_header: ServerHeaderMode = _option(
        default='off',
        doc=(
            'Identify the server in a Server header: `off` sends none, `on` '
            'sends the name, `full` adds the version. Any other value goes '
            'through `--header`, which also wins over this.'
        ),
        env_parse=_normalize_server_header,
        converter=_normalize_server_header,
        cli_choices=_SERVER_HEADER_MODES,
    )
    date_header: bool = _option(
        default=True,
        doc='Whether to add a default Date header when the application sets none.',
        env_parse=_parse_bool,
        converter=_exact_bool('date_header'),
        cli_action='bool',
    )
    response_headers: tuple[str, ...] = _option(
        default_factory=tuple,
        doc=(
            'Default response headers in lowercase `name: value` form, added '
            'only when the application did not set that name itself. Repeat '
            'the flag to add more headers.'
        ),
        env_parse=_parse_header_lines,
        converter=_normalize_header_tuple,
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
        # Port 0 is no exception: listeners that ask for it share the one port
        # the kernel assigns, so a repeated `host:0` is the same listener twice
        # and the second bind fails with EADDRINUSE.
        seen: set[BindSpec] = set()
        for bind in self.bind:
            spec = parse_bind_spec(bind)
            if spec in seen:
                raise ValueError(f'duplicate bind entry: {bind!r}')
            seen.add(spec)
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
        if self.max_requests_jitter > 0 and self.max_requests == 0:
            raise ValueError('max_requests_jitter requires max_requests')
        if self.h2_initial_stream_window_size > self.h2_initial_connection_window_size:
            raise ValueError(
                'h2_initial_stream_window_size '
                f"'{self.h2_initial_stream_window_size}' exceeds "
                'h2_initial_connection_window_size '
                f"'{self.h2_initial_connection_window_size}': "
                'a stream window cannot exceed its connection window'
            )

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
        option_map = {option.name: option for option in config_options()}
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
