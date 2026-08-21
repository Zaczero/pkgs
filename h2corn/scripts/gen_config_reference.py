"""Generate the configuration reference and mirror checked-in benchmark plots."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import mkdocs_gen_files
from h2corn._cli import build_parser
from h2corn._config import (
    OPTION_GROUPS,
    Config,
    ConfigOption,
    OptionMetadata,
    config_options,
)


def _format_default(value: Any) -> str:
    if value is None:
        return '`None`'
    if value == ():
        return '`()`'
    if isinstance(value, tuple):
        return '`' + repr(list(value)) + '`'
    if isinstance(value, str) and not value:
        return '`""`'
    return f'`{value!r}`'


def _format_cli(option: ConfigOption) -> str:
    flags = ', '.join(f'`{flag}`' for flag in option.cli_flags)
    if option.metadata.cli_action == 'bool':
        # argparse_bool produces both --flag and --no-flag.
        primary = option.cli_flags[-1]
        flags = f'`{primary}` / `{primary[:2]}no-{primary[2:]}`'
    return flags


def _format_choices(meta: OptionMetadata) -> str | None:
    if meta.cli_choices:
        return ', '.join(f'`{c}`' for c in meta.cli_choices)
    if meta.cli_action == 'bool':
        return '`true`, `false`'
    return None


def _option_section(option: ConfigOption) -> str:
    meta = option.metadata
    rows = [
        f'### `{option.name}` {{ #option-{option.name} }}',
        '',
        meta.doc,
        '',
        '| | |',
        '| --- | --- |',
        f'| **Default** | {_format_default(option.default)} |',
        f'| **CLI** | {_format_cli(option)} |',
        f'| **Env** | `{option.env_var}` |',
        f'| **TOML key** | `{option.name}` |',
        '| **Precedence** | CLI > environment > TOML > default |',
    ]
    choices = _format_choices(meta)
    if choices is not None:
        rows.append(f'| **Choices** | {choices} |')
    rows.append('')
    # Trailing blank line so the next option heading is not absorbed into
    # this table (Markdown tables continue until a blank line).
    return '\n'.join(rows) + '\n'


def _intro_section() -> str:
    return (
        '---\n'
        'title: Configuration\n'
        'description: Every h2corn server option with its CLI flag, environment variable, TOML key, default, precedence, and operational conditions.\n'
        '---\n\n'
        '# Configuration\n\n'
        'Every server option is exposed in three equivalent ways:\n\n'
        '- a CLI flag on the `h2corn` command\n'
        '- an `H2CORN_*` environment variable\n'
        '- a key in a TOML config file (passed via `--config` or `H2CORN_CONFIG`)\n\n'
        'When the same option is provided in more than one place, the order of '
        'precedence is **CLI > environment > TOML > defaults**.\n\n'
        '`--host` / `--port` and `H2CORN_HOST` / `H2CORN_PORT` are shortcuts for '
        'one TCP listener and cannot be combined with `bind`. `--config` and '
        '`H2CORN_CONFIG` select the TOML file; command-only controls are listed '
        'below.\n\n'
        '## Option index\n\n'
        '<div class="option-index" markdown>\n\n'
    )


def _index_table() -> str:
    rows = ['| Option | Default | CLI |', '| --- | --- | --- |']
    rows.extend(
        f'| [`{option.name}`](#option-{option.name}) | '
        f'{_format_default(option.default)} | '
        f'{_format_cli(option)} |'
        for option in config_options()
    )
    return '\n'.join(rows) + '\n\n</div>\n\n'


def _factories_section() -> str:
    return (
        '## Building a `Config` programmatically\n\n'
        '`Config` is a frozen dataclass; instantiate it directly or use '
        '[`Config.from_env()`][h2corn.Config.from_env], '
        '[`Config.from_mapping()`][h2corn.Config.from_mapping], or '
        '[`Config.from_toml()`][h2corn.Config.from_toml]. See the '
        '[Config API reference](api/config.md) for their signatures.\n\n'
    )


def _command_only_section() -> str:
    """Render parser flags that are not Config fields."""
    option_names = {option.name for option in config_options()}
    parser = build_parser(Config(), None)
    entries: list[str] = []
    meanings = {
        'config': 'Select the TOML file; `H2CORN_CONFIG` selects the same file.',
        'factory': 'Call the target as a zero-argument app factory.',
        'app_dir': 'Import the target from this directory.',
        'env_file': 'Load application environment before import.',
        'reload': 'Watch application files and restart one development worker.',
        'reload_dir': 'Add a development reload directory; repeatable.',
        'reload_include': 'Add a development reload glob; repeatable.',
        'reload_exclude': 'Add a development reload exclusion; repeatable.',
        'check_config': 'Validate configuration and TLS, then exit.',
        'print_config': 'Print the fully resolved configuration, then exit.',
        'version': 'Print the installed h2corn version, then exit.',
        'help': 'Print command help, then exit.',
        'host': 'TCP host convenience input for `bind`; `H2CORN_HOST` is equivalent when `H2CORN_BIND` is unset.',
        'port': 'TCP port convenience input for `bind`; `H2CORN_PORT` is equivalent when `H2CORN_BIND` is unset.',
    }
    seen: set[str] = set()
    for action in parser._actions:
        if not action.option_strings or action.dest in option_names:
            continue
        if action.dest in seen:
            continue
        seen.add(action.dest)
        meaning = meanings.get(action.dest, 'Parser control; see `h2corn --help`.')
        flags = ', '.join(f'`{flag}`' for flag in action.option_strings)
        entries.extend((
            f'### Command-only flags: {flags} {{ #command-{action.dest} }}',
            '',
            meaning,
            '',
        ))
    return (
        '## Command-only CLI controls\n\n'
        'These flags control application loading, development reload, or process '
        'actions rather than a `Config` value.\n\n' + '\n'.join(entries) + '\n\n'
    )


def render() -> str:
    parts: list[str] = [_intro_section(), _index_table()]
    seen: set[str] = set()

    for group in OPTION_GROUPS:
        parts.append(f'## {group.title}\n\n{group.blurb}\n\n')
        for name in group.options:
            option = next((o for o in config_options() if o.name == name), None)
            if option is None:
                raise RuntimeError(
                    f'unknown option in config reference group: {name!r}'
                )
            parts.append(_option_section(option))
            seen.add(name)

    leftover = [o for o in config_options() if o.name not in seen]
    if leftover:
        parts.append('## Other\n\n')
        parts.extend(_option_section(option) for option in leftover)

    parts.append(_command_only_section())
    parts.append(_factories_section())
    return ''.join(parts)


with mkdocs_gen_files.open('configuration.md', 'w') as fh:
    fh.write(render())


def mirror_benchmark_plots() -> None:
    """Mirror bench/results/plots/*.svg into the build for docs/benchmarks.md."""
    plots = Path(__file__).resolve().parents[1] / 'bench' / 'results' / 'plots'
    for svg in sorted(plots.glob('*.svg')):
        with mkdocs_gen_files.open(f'assets/benchmarks/{svg.name}', 'wb') as fh:
            fh.write(svg.read_bytes())


mirror_benchmark_plots()
