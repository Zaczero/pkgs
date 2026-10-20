import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import h2corn
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bench.report import (  # ruff: ignore[module-import-not-at-top-of-file]
    render_accessible_results,
)
from tools.docs.check import (  # ruff: ignore[module-import-not-at-top-of-file]
    _Anchors,
    check_api,
    check_benchmark_results,
    check_configuration,
)
from tools.docs.config_reference import (  # ruff: ignore[module-import-not-at-top-of-file]
    format_type,
    python_input_type,
    stored_type,
)


def _rendered_configuration() -> dict[str, _Anchors]:
    from h2corn._cli import build_parser
    from h2corn._config import Config, config_options

    options = config_options()
    option_names = {option.name for option in options}
    parts = ['<h1 id="configuration">Configuration</h1>']
    parts.extend(
        (
            f'<h3 id="option-{option.name}">{option.name}</h3>'
            f'{option.env_var} Python input {format_type(python_input_type(option))} '
            f'Python form Keyword-only Normalized value '
            f'{format_type(stored_type(option.name))} '
            f'{" ".join(option.cli_flags)} '
            'TOML key Precedence'
        )
        for option in options
    )
    for action in build_parser(Config(), None)._actions:
        if action.dest in option_names or not action.option_strings:
            continue
        parts.append(
            f'<h3 id="command-{action.dest}">{" ".join(action.option_strings)}</h3>'
        )
    parser = _Anchors()
    parser.feed(''.join(parts))
    parser.close()
    return {'configuration/index.html': parser}


def _rendered_api() -> dict[str, _Anchors]:
    import h2corn._types as public_types

    parts: list[str] = []
    for name in h2corn.__all__:
        parts.append(f'<h2 id="h2corn.{name}">{name}</h2>')
        value = getattr(public_types, name, None)
        annotations = getattr(value, '__annotations__', None)
        if annotations and hasattr(value, '__required_keys__'):
            parts.extend(
                f'<h3 id="h2corn.{name}.{field}">{field}</h3>' for field in annotations
            )
    parser = _Anchors()
    parser.feed(''.join(parts))
    parser.close()
    return {'api/index.html': parser}


def test_rendered_config_gate_rejects_removed_option_anchor() -> None:
    rendered = _rendered_configuration()
    page = rendered['configuration/index.html']
    identifier = next(
        identifier for identifier in page.ids if identifier.startswith('option-')
    )
    page.ids.remove(identifier)
    page.sections.pop(identifier)
    with pytest.raises(AssertionError, match='canonical heading'):
        check_configuration(rendered=rendered)


def test_rendered_config_gate_rejects_mutated_option_flag() -> None:
    rendered = _rendered_configuration()
    page = rendered['configuration/index.html']
    identifier = next(
        identifier for identifier in page.ids if identifier.startswith('option-')
    )
    page.sections[identifier][0] = page.sections[identifier][0].replace('--', '++')
    with pytest.raises(AssertionError, match='incomplete rendered entry'):
        check_configuration(rendered=rendered)


def test_rendered_config_gate_requires_python_constructor_types() -> None:
    from h2corn._config import config_options

    rendered = _rendered_configuration()
    page = rendered['configuration/index.html']
    identifier = next(
        identifier for identifier in page.ids if identifier.startswith('option-')
    )
    option = next(
        option for option in config_options() if f'option-{option.name}' == identifier
    )
    python_type = format_type(python_input_type(option))
    page.sections[identifier][0] = page.sections[identifier][0].replace(
        python_type, 'unsupported'
    )
    with pytest.raises(AssertionError, match='incomplete rendered entry'):
        check_configuration(rendered=rendered)


def test_rendered_config_gate_requires_help_entry() -> None:
    rendered = _rendered_configuration()
    page = rendered['configuration/index.html']
    page.ids.remove('command-help')
    page.sections.pop('command-help')
    with pytest.raises(AssertionError, match='command-only flag group'):
        check_configuration(rendered=rendered)


def test_rendered_api_gate_rejects_removed_export() -> None:
    rendered = _rendered_api()
    page = rendered['api/index.html']
    page.ids.remove('h2corn.Config')
    with pytest.raises(AssertionError, match='__all__ partition'):
        check_api(rendered=rendered)


def test_rendered_api_gate_rejects_removed_literal_typeddict_field() -> None:
    rendered = _rendered_api()
    page = rendered['api/index.html']
    identifier = 'h2corn.HTTPExtensions.http.response.pathsend'
    page.ids.remove(identifier)
    with pytest.raises(AssertionError, match='TypedDict fields'):
        check_api(rendered=rendered)


def test_rendered_benchmark_table_resolves_relative_figure_assets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools.docs import check as docs_check

    page = tmp_path / 'benchmarks' / 'index.html'
    page.parent.mkdir(parents=True)
    asset = tmp_path / 'assets' / 'benchmarks' / 'chart.svg'
    asset.parent.mkdir(parents=True)
    asset.write_text('<svg></svg>', encoding='utf-8')
    page.write_text(
        '<table><thead><tr><th>Comparison availability</th></tr></thead>'
        '<tbody><tr><td>Timed</td></tr></tbody></table>'
        '<img alt="HTTP/1 request rate" src="../assets/benchmarks/chart.svg">'
        '<table><thead><tr><th>Server</th></tr></thead>'
        '<tbody><tr><td>h2corn</td></tr></tbody></table>'
        '<table><thead><tr><th>Paired comparison</th></tr></thead>'
        '<tbody><tr><td>1.5 (1.4-1.6)</td></tr></tbody></table>',
        encoding='utf-8',
    )
    monkeypatch.setattr(docs_check, 'SITE', tmp_path)

    check_benchmark_results()


def test_benchmark_page_requires_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools.docs import check as docs_check

    page = tmp_path / 'benchmarks' / 'index.html'
    page.parent.mkdir()
    page.write_text('<h1>Benchmarks</h1><p>Measurement method only.</p>')
    monkeypatch.setattr(docs_check, 'SITE', tmp_path)
    with pytest.raises(AssertionError, match='contains no results'):
        check_benchmark_results()


def _benchmark_record(kind: str = 'h1') -> dict[str, Any]:
    return {
        'aggregate': {
            'h2corn': {
                'rps': 100.0,
                'rps_range': [98.0, 102.0],
                'rps_samples': [98.0, 100.0, 102.0],
                'completed': 1000,
                'peak_memory_bytes': 1_500_000,
                'latency_percentiles': {'p50': 0.0008, 'p99': 0.004},
            },
            'uvicorn': {
                'rps': 40.0,
                'rps_range': [38.0, 42.0],
                'rps_samples': [38.0, 40.0, 42.0],
                'completed': 400,
                'peak_memory_bytes': 2_500_000,
                'latency_percentiles': {'p50': 0.002, 'p99': 0.01},
            },
        },
        'type': kind,
        'paired_contrasts': {
            'uvicorn': {
                'h2corn_over_competitor': 2.5,
                'interval_95': [2.3, 2.7],
            },
        },
        'scenario': 'HTTP/1 GET (1 worker)',
        'planned_trials': 3,
        'requested_servers': ['h2corn', 'uvicorn'],
        'package_versions': {'h2corn': '1.6.0', 'uvicorn': '0.43.0'},
        'network_profile': 'loopback',
        'system_summary': 'Python 3.14; Linux; x86_64\nCPU: Example CPU',
        'duration': '10s',
        'warmup_duration': '2s',
        'excluded': {},
    }


@pytest.mark.parametrize('unsupported_server', [False, True])
def test_benchmark_publication_accepts_complete_supported_measurements(
    tmp_path: Path, unsupported_server: bool
) -> None:
    import json

    from bench.bench import assert_publishable

    raw = tmp_path / 'raw'
    raw.mkdir()
    record = _benchmark_record()
    if unsupported_server:
        record['requested_servers'].append('gunicorn')
        record['unsupported'] = {
            'gunicorn': 'The selected release has no prior-knowledge cleartext HTTP/2 mode.'
        }
    record['paired_contrasts']['uvicorn']['interval_95'] = [0.9, 1.1]
    record['stopped'] = 'max-trials'
    (raw / 'comparison.json').write_text(json.dumps(record))
    (tmp_path / 'identity.json').write_text(
        json.dumps({'build': {'command': ['maturin']}})
    )
    for block in range(1, 4):
        for name in record['aggregate']:
            path = tmp_path / 'evidence/comparison' / f'block-{block:02d}' / name
            path.mkdir(parents=True)
            (path / 'trial.json').write_text(json.dumps({'status': 'completed'}))
    assert_publishable([tmp_path])


def test_benchmark_publication_rejects_missing_results(tmp_path: Path) -> None:
    from bench.bench import BenchmarkError, assert_publishable

    with pytest.raises(BenchmarkError, match='no benchmark results'):
        assert_publishable([tmp_path])


def test_benchmark_table_reports_rates_latency_memory_and_comparison() -> None:
    markdown = render_accessible_results(_benchmark_record())

    assert '| h2corn | 100 | 0.800 | 4.000 | 1.5 | - |' in markdown
    assert '| uvicorn | 40.00 | 2.000 | 10.000 | 2.5 | slower |' in markdown


def test_websocket_benchmark_table_names_sessions_and_durations() -> None:
    markdown = render_accessible_results(_benchmark_record('ws'))

    assert 'Sessions/s' in markdown
    assert 'p99 session (ms)' in markdown


@pytest.mark.parametrize(
    ('interval', 'classification'),
    [
        ([0.7, 0.9], 'faster'),
        ([0.9, 1.1], 'inconclusive'),
        ([1.0, 1.2], 'inconclusive'),
    ],
)
def test_benchmark_table_classifies_paired_uncertainty(
    interval: list[float], classification: str
) -> None:
    record = _benchmark_record()
    record['paired_contrasts']['uvicorn']['interval_95'] = interval

    markdown = render_accessible_results(record)

    assert f'| uvicorn | 40.00 | 2.000 | 10.000 | 2.5 | {classification} |' in markdown


def test_benchmark_table_preserves_sub_request_per_second_rates() -> None:
    record = _benchmark_record()
    del record['aggregate']['uvicorn']
    record['paired_contrasts'] = {}
    result = record['aggregate']['h2corn']
    result.update(rps=0.25, rps_range=[0.24, 0.26], rps_samples=[0.24, 0.25, 0.26])
    markdown = render_accessible_results(record)
    assert '| h2corn | 0.25 |' in markdown


def test_benchmark_failed_response_has_no_numeric_rate_or_ratio() -> None:
    record = _benchmark_record()
    del record['aggregate']['uvicorn']
    del record['paired_contrasts']['uvicorn']
    record['failures'] = {
        'uvicorn': {
            'block': 1,
            'phase': 'correctness-before',
            'error': 'response body mismatch',
        }
    }
    markdown = render_accessible_results(record)
    assert '| uvicorn | failed | - | - | - | - |' in markdown
    assert 'response body mismatch' not in markdown


def test_benchmark_table_does_not_claim_a_winner_without_a_comparator() -> None:
    record = _benchmark_record()
    del record['aggregate']['uvicorn']

    markdown = render_accessible_results(record)

    assert '| uvicorn | unsupported | - | - | - | - |' in markdown


def test_api_docs_partition_every_top_level_export_once() -> None:
    text = '\n'.join(
        path.read_text(encoding='utf-8') for path in (ROOT / 'docs/api').glob('*.md')
    )
    directives = re.findall(
        r'^::: h2corn\.([A-Za-z_][A-Za-z0-9_]*)', text, re.MULTILINE
    )
    assert set(directives) == set(h2corn.__all__)
    assert len(directives) == len(h2corn.__all__)


# The accessible description restates the bars in words, which is the only form
# of them a screen-reader user receives, so it drifting is invisible to sighted
# review of the diagram.
_SPELLED_SECONDS = {10: 'ten', 30: 'thirty', 90: 'ninety', 100: 'one hundred'}


def _operations_page() -> str:
    return (ROOT / 'docs/deployment/operations.md').read_text(encoding='utf-8')


def _gantt(text: str) -> tuple[str, list[tuple[str, str, tuple[str, ...], int, int]]]:
    """The shutdown budget as its description and `(section, label, tags, start, end)` bars."""
    block = re.search(
        r'^```mermaid\n(gantt\n.*?)\n```$', text, re.MULTILINE | re.DOTALL
    )
    assert block is not None, 'operations.md no longer publishes a gantt diagram'
    described = re.search(r'^\s*accDescr:\s*(.+)$', block[1], re.MULTILINE)
    assert described is not None, 'the shutdown budget gantt has no accDescr'

    bars: list[tuple[str, str, tuple[str, ...], int, int]] = []
    section = ''
    for line in block[1].splitlines():
        stripped = line.strip()
        if stripped.startswith('section '):
            section = stripped.removeprefix('section ').strip()
            continue
        label, _, spec = stripped.partition(':')
        fields = [field.strip() for field in spec.split(',')]
        # A bar ends in a start and an end; every other directive does not.
        if len(fields) < 3 or not fields[-1].isdigit() or not fields[-2].isdigit():
            continue
        bars.append((
            section,
            label.strip(),
            tuple(fields[:-2]),
            int(fields[-2]),
            int(fields[-1]),
        ))
    return described[1].strip(), bars


def _check_shutdown_budget(text: str, config: h2corn.Config) -> None:
    described, bars = _gantt(text)
    timeouts = {
        name: getattr(config, name)
        for name in dir(config)
        if name.startswith('timeout_')
    }

    bounded = 0
    for _section, label, _tags, start, end in bars:
        for name, default in timeouts.items():
            if name in label:
                assert end - start == default, (
                    f'{label!r} spans {end - start}s, but {name} defaults to {default}s'
                )
                bounded += 1
    assert bounded == 3, f'expected three option-bounded phases, matched {bounded}'

    envelope = [bar for bar in bars if 'crit' in bar[2]]
    assert len(envelope) == 1, 'expected one service-manager envelope bar'
    sequential = [bar for bar in bars if 'crit' not in bar[2]]

    boundary = 0
    for _section, label, _tags, start, end in sequential:
        assert start == boundary, f'{label!r} starts at {start}s, not {boundary}s'
        boundary = end

    _, envelope_label, _, envelope_start, envelope_end = envelope[0]
    assert envelope_start == 0, 'the stop budget has to start with the first phase'
    assert envelope_end == boundary, (
        f'the stop budget covers {envelope_end}s against {boundary}s of phases'
    )
    assert 'TimeoutStopSec' in envelope_label, (
        f'the envelope bar no longer names the stop budget: {envelope_label!r}'
    )
    # The bar and the copyable systemd unit publish the same number, and a unit
    # shorter than the phases sends SIGKILL into whichever is still running.
    budgets = {int(match[1]) for match in re.finditer(r'TimeoutStopSec=(\d+)s', text)}
    assert budgets == {envelope_end}, (
        f'TimeoutStopSec values {sorted(budgets)} against a {envelope_end}s envelope'
    )

    spans = {end - start for _section, _label, _tags, start, end in sequential}
    spans.add(
        max(end for section, _l, _t, _s, end in sequential if section == 'worker')
    )
    spans.add(envelope_end)
    for value in sorted(spans):
        spelled = _SPELLED_SECONDS.get(value)
        assert spelled is not None, (
            f'accDescr cannot be checked for {value}s; add it to _SPELLED_SECONDS'
        )
        assert f'{spelled} seconds' in described.lower(), (
            f'accDescr no longer states {spelled} seconds for the {value}s span'
        )


def test_shutdown_budget_diagram_matches_the_configured_defaults() -> None:
    _check_shutdown_budget(_operations_page(), h2corn.Config())


def test_shutdown_budget_gate_rejects_a_default_the_diagram_never_followed() -> None:
    with pytest.raises(AssertionError, match='timeout_lifespan_shutdown'):
        _check_shutdown_budget(
            _operations_page(), h2corn.Config(timeout_lifespan_shutdown=45)
        )


def test_shutdown_budget_gate_rejects_a_phase_that_outlives_its_timeout() -> None:
    text = _operations_page().replace(':a3, 60, 90', ':a3, 60, 85')
    with pytest.raises(AssertionError, match='timeout_lifespan_shutdown'):
        _check_shutdown_budget(text, h2corn.Config())


def test_shutdown_budget_gate_rejects_a_stop_budget_that_undercuts_the_phases() -> None:
    text = _operations_page().replace('TimeoutStopSec=100s', 'TimeoutStopSec=90s')
    with pytest.raises(AssertionError, match='TimeoutStopSec'):
        _check_shutdown_budget(text, h2corn.Config())


def test_shutdown_budget_gate_rejects_alt_text_that_contradicts_the_bars() -> None:
    text = _operations_page().replace('one hundred seconds', 'two hundred seconds')
    with pytest.raises(AssertionError, match='accDescr'):
        _check_shutdown_budget(text, h2corn.Config())


def _check_start_budget(text: str, config: h2corn.Config) -> None:
    budgets = {int(match[1]) for match in re.finditer(r'TimeoutStartSec=(\d+)s', text)}
    assert budgets, 'the systemd unit no longer publishes a start budget'
    # The margin above the timeout is an editorial choice, so an equality would
    # pin incidental representation. A budget at or below the timeout is the
    # reachable failure: systemd kills the worker part-way through lifespan.
    for budget in budgets:
        assert budget > config.timeout_lifespan_startup, (
            f'TimeoutStartSec={budget}s does not cover '
            f'timeout_lifespan_startup={config.timeout_lifespan_startup}s'
        )


def test_start_budget_covers_the_lifespan_startup_timeout() -> None:
    _check_start_budget(_operations_page(), h2corn.Config())


def test_start_budget_gate_rejects_a_unit_that_kills_startup_mid_lifespan() -> None:
    with pytest.raises(AssertionError, match='TimeoutStartSec'):
        _check_start_budget(
            _operations_page(), h2corn.Config(timeout_lifespan_startup=120)
        )


def test_examples_compile_and_embedded_example_requests_then_stops() -> None:
    for path in sorted((ROOT / 'examples').rglob('*.py')):
        compile(path.read_text(encoding='utf-8'), str(path), 'exec')
    result = subprocess.run(
        [sys.executable, str(ROOT / 'examples/embedded.py')],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert 'embedded request: 200' in result.stdout


def test_public_docstrings_match_behavior() -> None:
    """This test exists so documented output cannot drift from behavior."""
    import doctest

    import h2corn._config as config_module
    import h2corn._server as server_module
    import h2corn._types as types_module

    for module in (h2corn, config_module, server_module, types_module):
        result = doctest.testmod(module, optionflags=doctest.ELLIPSIS)
        assert result.failed == 0, (
            f'{module.__name__}: {result.failed} doctest(s) failed'
        )
