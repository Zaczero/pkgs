"""Run public markdown Python examples.

Wraps ``tools/gates/_check_examples.py`` so runnable docs, README, and PyPI README
examples cannot drift from behavior without failing CI.
"""

from __future__ import annotations

from conftest import load_tool


def test_markdown_examples_execute() -> None:
    assert load_tool('_check_examples').main() == 0


def test_deprecated_source_display_gate_only_matches_fence_options() -> None:
    pattern = load_tool('_check_examples')._DEPRECATED_SOURCE_DISPLAY

    assert pattern.search('```python exec="on" source="material-block" result="text"\n')
    assert pattern.search("    ```python source='material-block'\n")
    assert not pattern.search('The old source="material-block" spelling is retired.')
    assert not pattern.search(
        '```python exec="on"\nprint(\'source="material-block"\')\n```\n'
    )
    assert not pattern.search('```python exec="on" source="block"\n')
