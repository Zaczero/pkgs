import re
import subprocess
import sys
from pathlib import Path

import h2corn
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.docs.check import (  # noqa: E402
    _Anchors,
    check_api,
    check_configuration,
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
            f'{option.env_var} {" ".join(option.cli_flags)} '
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
    for path in sorted((ROOT / 'examples').glob('*.py')):
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
