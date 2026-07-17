"""
Generate the docs-build artifacts that derive from sources outside docs/:

- `configuration.md`, rendered directly from the `Config` dataclass metadata.
  This is the single source of truth: the same `OptionMetadata.doc` strings
  drive the CLI `--help` output, the `H2CORN_*` environment variable list,
  and this docs page. No manual sync required.
- `assets/benchmarks/*.svg`, mirrored from the canonical benchmark plots in
  `bench/results/plots/` so `docs/benchmarks.md` can reference them without
  duplicating them in source control.

Wired as the single mkdocs `gen-files` script in `properdocs.yml`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import mkdocs_gen_files
from h2corn._config import (
    OPTION_GROUPS,
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
        f'| **TOML key** | `{option.toml_key}` |',
    ]
    choices = _format_choices(meta)
    if choices is not None:
        rows.append(f'| **Choices** | {choices} |')
    rows.append('')
    return '\n'.join(rows)


def _intro_section() -> str:
    return (
        '# Configuration\n\n'
        'Every server option is exposed in three equivalent ways:\n\n'
        '- a CLI flag on the `h2corn` command\n'
        '- an `H2CORN_*` environment variable\n'
        '- a key in a TOML config file (passed via `--config` or `H2CORN_CONFIG`)\n\n'
        'When the same option is provided in more than one place, the order of '
        'precedence is **CLI > environment > TOML > defaults**.\n\n'
        '!!! tip "Convenience pair"\n'
        '    `--host` / `--port` (and `H2CORN_HOST` / `H2CORN_PORT`) are accepted '
        'as a shortcut when you only need a single TCP listener and have not set '
        '`bind`. They cannot be combined with `bind`.\n\n'
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
        '`Config` is a frozen dataclass; instantiate it directly or use one '
        'of the alternative constructors:\n\n'
        '::: h2corn.Config\n'
        '    options:\n'
        '      show_root_heading: false\n'
        '      show_root_toc_entry: false\n'
        '      members:\n'
        '        - from_env\n'
        '        - from_mapping\n'
        '        - from_toml\n'
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
