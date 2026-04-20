from __future__ import annotations

import argparse
import importlib.metadata
import os
from dataclasses import MISSING, dataclass
from pathlib import Path

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

    from ._types import ASGIApp


@dataclass(frozen=True, slots=True)
class ImportSettings:
    target: str
    factory: bool = False
    app_dir: Path | None = None
    env_file: Path | None = None


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
            raise TypeError(f'{self.dest} values must be strings')
        items.append(values)
        setattr(namespace, self.dest, items)


def _add_config_arguments(parser: argparse.ArgumentParser, base: Config):
    for option in config_options():
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
            case _:
                kwargs |= {
                    key: value
                    for key, value in (
                        ('type', option.metadata.cli_type),
                        ('choices', option.metadata.cli_choices),
                        ('metavar', option.metadata.cli_metavar),
                    )
                    if value is not None
                }
        parser.add_argument(*option.cli_flags, **kwargs)


def build_parser(base: Config, config_path: Path | None) -> argparse.ArgumentParser:
    try:
        version = importlib.metadata.version('h2corn')
    except importlib.metadata.PackageNotFoundError:
        version = 'unknown'

    parser = argparse.ArgumentParser(
        description=f'High-performance HTTP/2 cleartext ASGI server (v{version})',
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
        '--factory',
        action='store_true',
        help='Treat the target as a zero-argument callable that returns an ASGI application.',
    )
    parser.add_argument(
        '--app-dir',
        type=Path,
        default=None,
        help='Import the target module from this directory instead of the current working directory.',
    )
    parser.add_argument(
        '--env-file',
        type=Path,
        default=None,
        help='Load application environment variables from this file before importing the target.',
    )
    parser.add_argument(
        '--host',
        default=argparse.SUPPRESS,
        help='TCP host convenience override for a single listener. When --port is omitted, the base configuration port is reused.',
    )
    parser.add_argument(
        '-p',
        '--port',
        type=int,
        default=argparse.SUPPRESS,
        help='TCP port convenience override for a single listener. When --host is omitted, the base configuration host is reused.',
    )
    _add_config_arguments(parser, base)
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
) -> tuple[ImportSettings, Config]:
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
    if args.target is None:
        parser.error('target is required')

    values = {option.name: getattr(args, option.name) for option in config_options()}
    _apply_tcp_bind_sugar(parser, args, base, values)
    return (
        ImportSettings(
            target=args.target,
            factory=args.factory,
            app_dir=None if args.app_dir is None else args.app_dir.resolve(),
            env_file=None if args.env_file is None else args.env_file.resolve(),
        ),
        Config(**values),
    )


def run_cli(
    serve: Callable[[ASGIApp, Config], None],
    import_target: Callable[[ImportSettings], ASGIApp],
    argv: Sequence[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    import_settings, config = parse_cli(argv, env)
    serve(import_target(import_settings), config)
