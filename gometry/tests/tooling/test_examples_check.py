"""Unit tests for documentation example extraction."""

from __future__ import annotations

from tests._support import load_tool


def test_deprecated_source_display_matches_only_fence_options() -> None:
    pattern = load_tool('_check_examples')._DEPRECATED_SOURCE_DISPLAY

    assert pattern.search('```python exec="on" source="material-block" result="text"\n')
    assert pattern.search("    ```python source='material-block'\n")
    assert not pattern.search('The old source="material-block" spelling is retired.')
    assert not pattern.search(
        '```python exec="on"\nprint(\'source="material-block"\')\n```\n'
    )
    assert not pattern.search('```python exec="on" source="block"\n')


def test_plain_python_fence_requires_execution_or_nonrunnable_label() -> None:
    gate = load_tool('_check_examples')
    assert gate._NONRUNNABLE_LABEL.search('title="partial: source example"')
    assert not gate._NONRUNNABLE_LABEL.search('title="copy this"')
    assert gate._PYTHON_FENCE.search('```python title="partial: source example"\n')
