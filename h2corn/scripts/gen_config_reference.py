"""Generate the configuration reference and mirror checked-in benchmark plots."""

from __future__ import annotations

import json
import sys
from html import escape
from pathlib import Path
from typing import Any

import mkdocs_gen_files

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from h2corn._cli import build_parser  # ruff: ignore[module-import-not-at-top-of-file]
from h2corn._config import (  # ruff: ignore[module-import-not-at-top-of-file]
    OPTION_GROUPS,
    Config,
    ConfigOption,
    OptionMetadata,
    config_options,
)

from bench.report import (  # ruff: ignore[module-import-not-at-top-of-file]
    alt_text,
    render_accessible_results,
)
from tools.docs.config_reference import (  # ruff: ignore[module-import-not-at-top-of-file]
    format_type,
    python_constructor_description,
    python_input_type,
    stored_type,
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


def _format_type_cell(annotation: Any) -> str:
    return '<code>' + escape(format_type(annotation)).replace('|', '&#124;') + '</code>'


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
        f'| **Python input** | {_format_type_cell(python_input_type(option))} |',
        '| **Python form** | Keyword-only `Config` argument |',
        f'| **Normalized value** | {_format_type_cell(stored_type(option.name))} |',
        f'| **CLI** | {_format_cli(option)} |',
        f'| **Env** | `{option.env_var}` |',
        f'| **TOML key** | `{option.name}` |',
        '| **Precedence** | CLI > environment > TOML > default |',
    ]
    if meta.normalize.__doc__:
        rows.append(
            '| **Python normalization** | '
            + ' '.join(meta.normalize.__doc__.split()).replace('|', '&#124;')
            + ' |'
        )
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
        'Every server option has CLI, environment, and TOML inputs:\n\n'
        '- a CLI flag on the `h2corn` command\n'
        '- an `H2CORN_*` environment variable\n'
        '- a key in a TOML config file (passed via `--config` or `H2CORN_CONFIG`)\n\n'
        'When the same option is provided in more than one place, the order of '
        'precedence is **CLI > environment > TOML > defaults**.\n\n'
        '`--host` / `--port` and `H2CORN_HOST` / `H2CORN_PORT` are shortcuts for '
        'one TCP listener and cannot be combined with `bind`. `--config` and '
        '`H2CORN_CONFIG` select the TOML file; command-only controls are listed '
        'below.\n\n'
        + python_constructor_description()
        + ' The direct constructor validates and normalizes its keyword arguments; '
        'it does not apply the CLI/environment/TOML precedence pipeline.\n\n'
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
        '[`Config.from_toml()`][h2corn.Config.from_toml]. Each option entry '
        'above gives its keyword-only Python input and normalized value types. '
        'See the [Config API reference](api/config.md) for helper signatures.\n\n'
    )


def _command_only_section() -> str:
    """Render parser flags that are not Config fields."""
    option_names = {option.name for option in config_options()}
    parser = build_parser(Config(), None)
    entries: list[str] = []
    meanings = {
        'config': 'Select the TOML file; `H2CORN_CONFIG` selects the same file.',
        'factory': 'Construct an application on each running request loop using the zero-argument target.',
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

    parts.extend((_command_only_section(), _factories_section()))
    return ''.join(parts)


def render_benchmark_results() -> str:
    raw_directory = ROOT / 'bench' / 'results' / 'raw'
    plot_directory = ROOT / 'bench' / 'results' / 'plots'
    records = {
        path: json.loads(path.read_text(encoding='utf-8'))
        for path in sorted(raw_directory.glob('*.json'))
    }
    if not records:
        raise RuntimeError('benchmark results are missing; populate bench/results/raw')

    for plot_path in sorted(plot_directory.glob('*.svg')):
        with mkdocs_gen_files.open(
            f'assets/benchmarks/{plot_path.name}', 'wb'
        ) as handle:
            handle.write(plot_path.read_bytes())

    servers = tuple(
        dict.fromkeys(
            name
            for record in records.values()
            if record.get('status') != 'skipped'
            for name in record['requested_servers']
        )
    )
    environment = next(
        record for record in records.values() if record.get('status') != 'skipped'
    )
    sections: list[str] = [
        '## Environment\n\n',
        environment['system_summary'].replace('\n', ' · ') + '\n\n',
        ', '.join(
            f'{name} {version}'
            for name, version in environment['package_versions'].items()
            if version is not None
        )
        + '.\n\n',
        '## Results\n\n',
        '| Workload | Network | ' + ' | '.join(servers) + ' |\n',
        '| --- | --- | ' + ' | '.join('---' for _ in servers) + ' |\n',
    ]
    for path, record in records.items():
        network = record['network_profile']
        if (measured_rtt := record.get('measured_rtt_ms')) is not None:
            network = f'{measured_rtt:.1f} ms RTT'
        states = [
            'timed'
            if name in record.get('aggregate', {})
            else 'failed'
            if name in record.get('failures', {})
            else 'unsupported'
            for name in servers
        ]
        sections.append(
            f'| [{record["scenario"]}](#result-{path.stem}) '
            f'| {network} | ' + ' | '.join(states) + ' |\n',
        )
    sections.append('\n')
    for raw_path, record in records.items():
        scenario = record.get('scenario', raw_path.stem)
        profile = record.get('network_profile')
        sections.append(f'<a id="result-{raw_path.stem}"></a>\n\n')
        if record.get('status') == 'skipped':
            excluded = record.get('excluded', {})
            reason = record.get('reason') or next(
                iter(excluded.values()), 'the measurement was skipped'
            )
            sections.extend((
                f'### {scenario} — {profile}\n\n',
                f'{profile} measurements were skipped: {reason}\n\n',
            ))
            continue
        if not record.get('aggregate'):
            raise RuntimeError(f'{raw_path.name} contains no benchmark measurements')

        plot_path = plot_directory / f'{raw_path.stem}.svg'
        table_path = plot_directory / f'{raw_path.stem}.md'
        if not plot_path.is_file() or not table_path.is_file():
            raise RuntimeError(
                f'{raw_path.name} has no matching plot and accessible table'
            )
        table = render_accessible_results(record)
        if table_path.read_text(encoding='utf-8') != table:
            raise RuntimeError(f'{table_path.name} does not match its aggregate record')

        relative_plot = f'assets/benchmarks/{plot_path.name}'
        dark_plot = plot_path.with_stem(plot_path.stem + '-dark')
        narrow_plot = plot_path.with_stem(plot_path.stem + '-narrow')
        narrow_dark_plot = plot_path.with_stem(plot_path.stem + '-narrow-dark')
        for variant in (dark_plot, narrow_plot, narrow_dark_plot):
            if not variant.is_file():
                raise RuntimeError(f'{raw_path.name} has no {variant.name}')
        results = record['aggregate']
        unit = 'sessions/s' if record['type'] == 'ws' else 'requests/s'
        description = escape(
            alt_text(scenario, results, unit, record.get('failures')), quote=True
        )
        sections.extend((
            f'### {scenario} — {profile}\n\n',
            '<picture>\n'
            f'  <source media="(max-width: 600px)" srcset="assets/benchmarks/{narrow_plot.name}">\n'
            f'  <img src="{relative_plot}#only-light" alt="{description}">\n'
            '</picture>\n'
            '<picture>\n'
            f'  <source media="(max-width: 600px)" srcset="assets/benchmarks/{narrow_dark_plot.name}">\n'
            f'  <img src="assets/benchmarks/{dark_plot.name}#only-dark" alt="{description}">\n'
            '</picture>\n\n',
            '<details markdown="1">\n<summary>Detailed results</summary>\n\n',
            table,
            '\n</details>\n\n',
        ))
    return ''.join(sections)


with mkdocs_gen_files.open('configuration.md', 'w') as fh:
    fh.write(render())


generated_results = ROOT / 'bench' / 'results' / '_generated' / 'benchmark-results.md'
generated_results.parent.mkdir(parents=True, exist_ok=True)
generated_results.write_text(render_benchmark_results(), encoding='utf-8')
