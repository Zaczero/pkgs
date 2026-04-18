from __future__ import annotations

import argparse
import importlib.metadata
import os
from dataclasses import replace
from pathlib import Path

from ._config import CONFIG_PATH_ENV_VAR, Config, _env_values, config_options

TYPE_CHECKING = False

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from ._types import ASGIApp


class _AppendConfigValue(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        current = getattr(namespace, self.dest, ())
        items = list(current)
        items.append(values)
        setattr(namespace, self.dest, items)


def _add_config_arguments(parser: argparse.ArgumentParser, base: Config) -> None:
    for option in config_options():
        kwargs: dict[str, object] = {
            'default': getattr(base, option.name),
            'dest': option.name,
            'help': option.help_text(),
        }
        if option.metadata.cli_action == 'bool':
            kwargs['action'] = argparse.BooleanOptionalAction
        elif option.metadata.cli_action == 'append':
            kwargs['action'] = _AppendConfigValue
            if option.metadata.cli_type is not None:
                kwargs['type'] = option.metadata.cli_type
            if option.metadata.cli_metavar is not None:
                kwargs['metavar'] = option.metadata.cli_metavar
        else:
            if option.metadata.cli_type is not None:
                kwargs['type'] = option.metadata.cli_type
            if option.metadata.cli_choices is not None:
                kwargs['choices'] = option.metadata.cli_choices
            if option.metadata.cli_metavar is not None:
                kwargs['metavar'] = option.metadata.cli_metavar
        parser.add_argument(*option.cli_flags, **kwargs)


def build_parser(base: Config, config_path: Path | None) -> argparse.ArgumentParser:
    try:
        version = importlib.metadata.version('h2corn')
    except Exception:
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
        '-b',
        '--bind',
        metavar='ADDRESS',
        help='The host and port to bind to, in HOST:PORT format. Overrides --host and --port. Also supports unix: socket paths.',
    )
    _add_config_arguments(parser, base)
    return parser


def _apply_bind_overrides(
    parser: argparse.ArgumentParser,
    values: dict[str, object],
    bind: str | None,
) -> None:
    if not bind:
        return
    if bind.startswith('fd://'):
        values['fd'] = int(bind[5:])
        values['uds'] = None
        return
    if bind.startswith('unix:'):
        values['uds'] = Path(bind[5:])
        values['fd'] = None
        return
    if ':' not in bind:
        values['host'] = bind
        values['fd'] = None
        values['uds'] = None
        return

    if bind.startswith('['):
        host, _, port = bind[1:].rpartition(']:')
    else:
        host, _, port = bind.rpartition(':')
    values['host'] = host
    values['fd'] = None
    values['uds'] = None
    try:
        values['port'] = int(port)
    except ValueError:
        parser.error(f'invalid port in --bind: {port}')


def parse_cli(
    argv: Sequence[str] | None = None,
    env: dict[str, str] | None = None,
) -> tuple[str, Config]:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument('-c', '--config', type=Path)
    pre_args, _ = pre_parser.parse_known_args(argv)

    env = env or os.environ
    config_path = pre_args.config
    if config_path is None and (raw := env.get(CONFIG_PATH_ENV_VAR)):
        config_path = Path(raw)
    base = Config.from_toml(config_path) if config_path is not None else Config()
    env_values = _env_values(env)
    if env_values:
        base = replace(base, **env_values)
    parser = build_parser(base, config_path)
    args = parser.parse_args(argv)
    if args.target is None:
        parser.error('target is required')

    values = {option.name: getattr(args, option.name) for option in config_options()}
    _apply_bind_overrides(parser, values, args.bind)
    return args.target, Config(**values)


def run_cli(
    serve: Callable[[ASGIApp, Config], None],
    import_target: Callable[[str], ASGIApp],
    argv: Sequence[str] | None = None,
    env: dict[str, str] | None = None,
) -> None:
    target, config = parse_cli(argv, env)
    serve(import_target(target), config)
