"""
Render `docs/configuration.md` directly from the `Config` dataclass metadata.

This is the single source of truth: the same `OptionMetadata.doc` strings
drive the CLI `--help` output, the `H2CORN_*` environment variable list,
and this docs page. No manual sync required.
"""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any

import mkdocs_gen_files

from h2corn._config import (
    _CONVENIENCE_KEYS,
    _OPTION_METADATA_KEY,
    Config,
    ConfigOption,
    OptionMetadata,
    config_options,
)


# Logical grouping that mirrors the CLI parser sections, so the page is easy
# to scan by topic rather than by alphabetical field order.
GROUPS: list[tuple[str, str, tuple[str, ...]]] = [
    (
        'Application',
        'Where the ASGI app lives and how it is loaded.',
        ('root_path', 'lifespan', 'timeout_lifespan_startup', 'timeout_lifespan_shutdown'),
    ),
    (
        'Listeners',
        'How `h2corn` accepts connections.',
        ('bind', 'uds_permissions', 'backlog'),
    ),
    (
        'TLS',
        'Direct TLS for TCP listeners. Leave unset when terminating TLS at a proxy.',
        ('certfile', 'keyfile', 'ca_certs', 'cert_reqs'),
    ),
    (
        'Process and workers',
        'Process identity, worker pool, and lifecycle on Unix.',
        (
            'pid', 'user', 'group', 'umask',
            'workers', 'runtime_threads',
            'max_requests', 'max_requests_jitter',
            'timeout_worker_healthcheck',
        ),
    ),
    (
        'HTTP and resource limits',
        'Protocol behavior and per-connection bounds.',
        (
            'http1', 'access_log',
            'max_concurrent_streams',
            'limit_request_head_size', 'limit_request_line',
            'limit_request_fields', 'limit_request_field_size',
            'h2_max_header_list_size', 'h2_max_header_block_size',
            'h2_max_inbound_frame_size',
            'max_request_body_size',
            'limit_concurrency', 'limit_connections',
        ),
    ),
    (
        'Timeouts',
        'Connection-level timeouts. All values are in seconds; `0` disables.',
        (
            'timeout_handshake',
            'timeout_graceful_shutdown',
            'timeout_keep_alive',
            'timeout_request_header',
            'timeout_request_body_idle',
        ),
    ),
    (
        'WebSocket',
        'Limits and keep-alive for RFC 6455 and RFC 8441 WebSockets.',
        (
            'websocket_max_message_size',
            'websocket_per_message_deflate',
            'websocket_ping_interval',
            'websocket_ping_timeout',
        ),
    ),
    (
        'Proxy and response headers',
        'Trust boundaries with the upstream proxy and default response headers.',
        (
            'proxy_headers', 'forwarded_allow_ips', 'proxy_protocol',
            'server_header', 'date_header', 'response_headers',
        ),
    ),
]


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
    for option in config_options():
        rows.append(
            f'| [`{option.name}`](#option-{option.name}) | '
            f'{_format_default(option.default)} | '
            f'{_format_cli(option)} |'
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

    for title, blurb, names in GROUPS:
        parts.append(f'## {title}\n\n{blurb}\n\n')
        for name in names:
            option = next((o for o in config_options() if o.name == name), None)
            if option is None:
                raise RuntimeError(f'unknown option in config reference group: {name!r}')
            parts.append(_option_section(option))
            seen.add(name)

    leftover = [o for o in config_options() if o.name not in seen]
    if leftover:
        parts.append('## Other\n\n')
        for option in leftover:
            parts.append(_option_section(option))

    parts.append(_factories_section())
    return ''.join(parts)


_validate_known_fields = set(f.name for f in fields(Config) if _OPTION_METADATA_KEY in f.metadata)
_referenced = {name for _, _, names in GROUPS for name in names}
_orphans = _validate_known_fields - _referenced
if _orphans:
    # Soft-fail: leftover fields appear under the "Other" section so we never
    # silently drop options when new ones are added to the dataclass.
    pass

with mkdocs_gen_files.open('configuration.md', 'w') as fh:
    fh.write(render())


# Mirror benchmark plots into the build so docs/benchmarks.md can reference
# them as if they lived next to the page. The actual files stay in
# bench/results/plots/ — we never duplicate them in source control.
_BENCH_PLOTS = Path(__file__).resolve().parents[1] / 'bench' / 'results' / 'plots'
for svg in sorted(_BENCH_PLOTS.glob('*.svg')):
    with mkdocs_gen_files.open(f'assets/benchmarks/{svg.name}', 'wb') as fh:
        fh.write(svg.read_bytes())
