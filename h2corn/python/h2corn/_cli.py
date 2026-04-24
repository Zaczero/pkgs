from __future__ import annotations

import argparse
import importlib.metadata
import os
import sys
from dataclasses import MISSING, dataclass
from pathlib import Path
from typing import assert_never

from ._config import (
    _CONVENIENCE_KEYS,
    _DEFAULT_BIND,
    CONFIG_PATH_ENV_VAR,
    Config,
    _bind_from_convenience,
    _env_values,
    config_options,
)

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import Any

_DEFAULT_RELOAD_INCLUDE_PATTERNS = ('*.py',)
_DEFAULT_RELOAD_EXCLUDE_PATTERNS = ('.*', '.py[cod]', '.sw.*', '~*')
_TomlLiteralValue = None | bool | str | int | float | tuple[object, ...] | list[object]
_APPLICATION_CONFIG_OPTIONS = (
    'root_path',
    'lifespan',
    'timeout_lifespan_startup',
    'timeout_lifespan_shutdown',
)
_BINDING_CONFIG_OPTIONS = (
    'bind',
    'uds_permissions',
    'backlog',
)
_TLS_CONFIG_OPTIONS = (
    'certfile',
    'keyfile',
    'ca_certs',
    'cert_reqs',
)
_PROCESS_CONFIG_OPTIONS = (
    'pid',
    'user',
    'group',
    'umask',
    'workers',
    'runtime_threads',
    'max_requests',
    'max_requests_jitter',
    'timeout_worker_healthcheck',
)
_HTTP_LIMIT_CONFIG_OPTIONS = (
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
    'max_request_body_size',
    'limit_concurrency',
    'limit_connections',
)
_TIMEOUT_CONFIG_OPTIONS = (
    'timeout_handshake',
    'timeout_graceful_shutdown',
    'timeout_keep_alive',
    'timeout_request_header',
    'timeout_request_body_idle',
)
_WEBSOCKET_CONFIG_OPTIONS = (
    'websocket_max_message_size',
    'websocket_per_message_deflate',
    'websocket_ping_interval',
    'websocket_ping_timeout',
)
_PROXY_HEADER_CONFIG_OPTIONS = (
    'proxy_headers',
    'forwarded_allow_ips',
    'proxy_protocol',
    'server_header',
    'date_header',
    'response_headers',
)


@dataclass(frozen=True, slots=True)
class ImportSettings:
    target: str
    factory: bool = False
    app_dir: Path | None = None
    env_file: Path | None = None


@dataclass(frozen=True, slots=True)
class CliSettings:
    check_config: bool = False
    print_config: bool = False
    reload: bool = False
    reload_dirs: tuple[Path, ...] = ()
    reload_includes: tuple[str, ...] = _DEFAULT_RELOAD_INCLUDE_PATTERNS
    reload_excludes: tuple[str, ...] = _DEFAULT_RELOAD_EXCLUDE_PATTERNS


class _AppendConfigValue(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ):
        current = getattr(namespace, self.dest, ())
        items: list[str]
        if self.dest == 'bind' and isinstance(current, tuple):
            items = []
        elif isinstance(current, tuple | list):
            items = [str(item) for item in current]
        else:
            items = []
        if not isinstance(values, str):
            assert_never(values)
        items.append(values)
        setattr(namespace, self.dest, items)


def _add_config_argument(container, option, base: Config):
    kwargs: dict[str, Any] = {
        'default': getattr(base, option.name),
        'dest': option.name,
        'help': option.help_text(),
    }
    match option.metadata.cli_action:
        case 'bool':
            kwargs['action'] = argparse.BooleanOptionalAction
        case 'append':
            kwargs['action'] = _AppendConfigValue
            kwargs |= {
                key: value
                for key, value in (
                    ('type', option.metadata.cli_type),
                    ('metavar', option.metadata.cli_metavar),
                )
                if value is not None
            }
        case None:
            kwargs |= {
                key: value
                for key, value in (
                    ('type', option.metadata.cli_type),
                    ('choices', option.metadata.cli_choices),
                    ('metavar', option.metadata.cli_metavar),
                )
                if value is not None
            }
        case cli_action:
            assert_never(cli_action)
    container.add_argument(*option.cli_flags, **kwargs)


def _add_named_config_arguments(
    container,
    base: Config,
    option_map: dict[str, Any],
    names: tuple[str, ...],
):
    for name in names:
        _add_config_argument(container, option_map.pop(name), base)


def _toml_literal(value: _TomlLiteralValue):
    match value:
        case None:
            return 'null'
        case True:
            return 'true'
        case False:
            return 'false'
        case str():
            escaped = (
                value.replace('\\', '\\\\')
                .replace('"', '\\"')
                .replace('\b', '\\b')
                .replace('\f', '\\f')
                .replace('\n', '\\n')
                .replace('\r', '\\r')
                .replace('\t', '\\t')
            )
            return f'"{escaped}"'
        case int() | float():
            return str(value)
        case tuple() | list():
            return f"[{', '.join(_toml_literal(item) for item in value)}]"
        case unexpected_value:
            assert_never(unexpected_value)


def _print_config(config: Config):
    sys.stdout.write(
        '\n'.join(
            f'{option.toml_key} = {_toml_literal(getattr(config, option.name))}'
            for option in config_options()
        )
        + '\n'
    )


def build_parser(base: Config, config_path: Path | None) -> argparse.ArgumentParser:
    try:
        version = importlib.metadata.version('h2corn')
    except importlib.metadata.PackageNotFoundError:
        version = 'unknown'

    parser = argparse.ArgumentParser(
        prog='h2corn',
        description=f'High-performance HTTP/2 ASGI server (v{version})',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        'target',
        nargs='?',
        help='The ASGI application to run, e.g., module:app.',
    )
    parser.add_argument(
        '-c',
        '--config',
        type=Path,
        default=config_path,
        help=f'Path to a TOML configuration file. [env: {CONFIG_PATH_ENV_VAR}]',
    )
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {version}',
    )
    parser.add_argument(
        '--check-config',
        action='store_true',
        help='Validate configuration, then exit without importing the target or starting the server.',
    )
    parser.add_argument(
        '--print-config',
        action='store_true',
        help='Print the fully resolved configuration, then exit without importing the target or starting the server.',
    )
    application = parser.add_argument_group('Application')
    application.add_argument(
        '--factory',
        action='store_true',
        help='Treat the target as a zero-argument callable that returns an ASGI application.',
    )
    application.add_argument(
        '--app-dir',
        type=Path,
        default=None,
        help='Import the target module from this directory instead of the current working directory.',
    )
    application.add_argument(
        '--env-file',
        type=Path,
        default=None,
        help='Load application environment variables from this file before importing the target.',
    )
    option_map = {option.name: option for option in config_options()}
    _add_named_config_arguments(
        application,
        base,
        option_map,
        _APPLICATION_CONFIG_OPTIONS,
    )

    development = parser.add_argument_group('Development')
    development.add_argument(
        '--reload',
        action='store_true',
        help='Restart the server when watched Python files change. Development only.',
    )
    development.add_argument(
        '--reload-dir',
        action=_AppendConfigValue,
        default=(),
        help='Directory to watch for reload. Repeat the flag to add more directories. Overrides the default watch root.',
        metavar='DIR',
    )
    development.add_argument(
        '--reload-include',
        action=_AppendConfigValue,
        default=_DEFAULT_RELOAD_INCLUDE_PATTERNS,
        help='Glob pattern for files that should trigger reload. Repeat the flag to add more patterns.',
        metavar='PATTERN',
    )
    development.add_argument(
        '--reload-exclude',
        action=_AppendConfigValue,
        default=_DEFAULT_RELOAD_EXCLUDE_PATTERNS,
        help='Glob pattern for files or directories that should be ignored by reload. Repeat the flag to add more patterns.',
        metavar='PATTERN',
    )

    binding = parser.add_argument_group('Socket Binding')
    binding.add_argument(
        '--host',
        default=argparse.SUPPRESS,
        help='TCP host convenience override for a single listener. When --port is omitted, the base configuration port is reused.',
    )
    binding.add_argument(
        '-p',
        '--port',
        type=int,
        default=argparse.SUPPRESS,
        help='TCP port convenience override for a single listener. When --host is omitted, the base configuration host is reused.',
    )
    _add_named_config_arguments(binding, base, option_map, _BINDING_CONFIG_OPTIONS)

    tls = parser.add_argument_group('TLS')
    _add_named_config_arguments(tls, base, option_map, _TLS_CONFIG_OPTIONS)

    process = parser.add_argument_group('Process and Workers')
    _add_named_config_arguments(process, base, option_map, _PROCESS_CONFIG_OPTIONS)

    http_limits = parser.add_argument_group('HTTP and Resource Limits')
    _add_named_config_arguments(
        http_limits,
        base,
        option_map,
        _HTTP_LIMIT_CONFIG_OPTIONS,
    )

    timeouts = parser.add_argument_group('Timeouts')
    _add_named_config_arguments(timeouts, base, option_map, _TIMEOUT_CONFIG_OPTIONS)

    websocket = parser.add_argument_group('WebSocket')
    _add_named_config_arguments(
        websocket,
        base,
        option_map,
        _WEBSOCKET_CONFIG_OPTIONS,
    )

    proxy_headers = parser.add_argument_group('Proxy and Response Headers')
    _add_named_config_arguments(
        proxy_headers,
        base,
        option_map,
        _PROXY_HEADER_CONFIG_OPTIONS,
    )

    assert not option_map, f'unhandled config options: {", ".join(sorted(option_map))}'
    return parser


def _apply_tcp_bind_sugar(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    base: Config,
    values: dict[str, Any],
):
    cli_bind = getattr(args, 'bind', None)
    host = getattr(args, 'host', MISSING)
    port = getattr(args, 'port', MISSING)
    if isinstance(cli_bind, list):
        if host is not MISSING or port is not MISSING:
            parser.error('--bind cannot be combined with --host or --port')
        values['bind'] = tuple(cli_bind)
        return
    if host is MISSING and port is MISSING:
        return
    if base.host is None or base.port is None:
        parser.error('--host/--port require a single TCP listener base configuration')
    bind = _bind_from_convenience(
        base.host if host is MISSING else host,
        base.port if port is MISSING else port,
    )
    assert bind is not None
    values['bind'] = bind


def parse_cli(
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> tuple[CliSettings, ImportSettings, Config]:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument('-c', '--config', type=Path)
    pre_args, _ = pre_parser.parse_known_args(argv)

    if env is None:
        env = os.environ
    config_path = pre_args.config
    if config_path is None and (raw := env.get(CONFIG_PATH_ENV_VAR)):
        config_path = Path(raw)
    try:
        base = Config.from_toml(config_path) if config_path is not None else Config()
        env_values = _env_values(env)
        if env_values:
            values = {
                option.name: env_values.get(option.name, getattr(base, option.name))
                for option in config_options()
            }
            if _CONVENIENCE_KEYS & env_values.keys():
                if base.host is None or base.port is None:
                    raise ValueError(
                        'host/port environment overrides require a single configured TCP listener'
                    )
                values['bind'] = _DEFAULT_BIND
                values |= {
                    key: env_values.get(key, getattr(base, key))
                    for key in _CONVENIENCE_KEYS
                }
            base = Config(**values)
    except ValueError as exc:
        pre_parser.error(str(exc))
    parser = build_parser(base, config_path)
    args = parser.parse_args(argv)
    if args.target is None and not (args.check_config or args.print_config):
        parser.error('target is required')

    values = {option.name: getattr(args, option.name) for option in config_options()}
    _apply_tcp_bind_sugar(parser, args, base, values)
    config = Config(**values)
    if args.reload and (args.check_config or args.print_config):
        parser.error('--reload cannot be combined with --check-config or --print-config')
    if (
        args.reload_dir
        or args.reload_include != _DEFAULT_RELOAD_INCLUDE_PATTERNS
        or args.reload_exclude != _DEFAULT_RELOAD_EXCLUDE_PATTERNS
    ) and not args.reload:
        parser.error('--reload-dir, --reload-include, and --reload-exclude require --reload')
    if args.reload and config.workers != 1:
        parser.error('--reload requires workers=1')
    return (
        CliSettings(
            check_config=args.check_config,
            print_config=args.print_config,
            reload=args.reload,
            reload_dirs=tuple(Path(item).resolve() for item in args.reload_dir),
            reload_includes=tuple(args.reload_include),
            reload_excludes=tuple(args.reload_exclude),
        ),
        ImportSettings(
            target='' if args.target is None else args.target,
            factory=args.factory,
            app_dir=None if args.app_dir is None else args.app_dir.resolve(),
            env_file=None if args.env_file is None else args.env_file.resolve(),
        ),
        config,
    )


def run_cli(
    serve: Callable[[ImportSettings, Config], None],
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    cli_settings, import_settings, config = parse_cli(argv, env)
    if cli_settings.print_config:
        _print_config(config)
        return
    if cli_settings.check_config:
        from ._lib import validate_config

        validate_config(config)
        return
    if cli_settings.reload:
        from ._reload import _serve_with_reload

        _serve_with_reload(
            import_settings,
            config,
            reload_dirs=cli_settings.reload_dirs,
            reload_includes=cli_settings.reload_includes,
            reload_excludes=cli_settings.reload_excludes,
            argv=argv,
            env=env,
        )
        return
    serve(import_settings, config)
