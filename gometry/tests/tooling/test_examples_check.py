"""Behavioral tests for Markdown example extraction and execution."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests._support import load_tool


def _write_markdown(root: Path, relative: str, source: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source).lstrip('\n'), encoding='utf-8')
    return path


def _run_gate(root: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    gate = load_tool('_check_examples')
    examples_path = str(gate._EXAMPLES)
    had_examples_path = examples_path in sys.path
    monkeypatch.chdir(root)
    try:
        return gate.main()
    finally:
        if not had_examples_path and examples_path in sys.path:
            sys.path.remove(examples_path)


def _write_runnable_readme(root: Path) -> None:
    _write_markdown(
        root,
        'README.md',
        """\
        ```python
        readme_value = 19
        ```
        ```python
        assert readme_value == 19
        ```
        """,
    )


def _write_runnable_quickstart(root: Path) -> None:
    _write_markdown(
        root,
        'docs/get-started/quickstart.md',
        """\
        ```python exec="on" session="catchment"
        doc_value = 23
        ```
        ```python exec="on" session="catchment"
        assert doc_value == 23
        ```
        """,
    )


def test_superfences_extraction_keeps_containers_lines_and_retained_blocks() -> None:
    gate = load_tool('_check_examples')
    source = textwrap.dedent(
        """\
        # Fence owners
        ```python exec="on" session="catchment"
        seed = 17
        ```

        !!! note

            ```python exec="on" session="catchment"
            nested = seed
            ```

        === "Tabbed"

            ```python exec="on" session="catchment"
            tabbed_value = 19
            ```

        - List item

            ```python title="partial: external source"
            deliberately not Python!
            ```

        > ```python exec="on"
        > quoted = 23
        > ```

        ~~~~~~py exec="on" title="long tilde fence"
        alias_value = 29
        ~~~~~~

        ```{.python .configuration exec="on" title="configuration: inert"}
        not_python = 31
        ```

        ```python pseudocode
        not_python = 37
        ```

        ````python title="partial: literal nested fence"
        ```python exec="on"
        inner_literal = 41
        ```
        ````

            ```python exec="on"
            restored = 43
            ```
        """
    )

    fences, _markdown = gate.extract_markdown_fences(Path('docs/example.md'), source)

    assert [(fence.line, fence.language) for fence in fences] == [
        (2, 'python'),
        (8, 'python'),
        (14, 'python'),
        (20, 'python'),
        (24, 'python'),
        (28, 'py'),
        (32, 'python'),
        (36, 'python'),
        (40, 'python'),
    ]
    assert [fence.code for fence in fences[:6]] == [
        'seed = 17',
        'nested = seed',
        'tabbed_value = 19',
        'deliberately not Python!',
        'quoted = 23',
        'alias_value = 29',
    ]
    assert fences[6].classes == ('configuration',)
    assert fences[7].options['pseudocode'] == 'pseudocode'
    assert 'inner_literal = 41' in fences[8].code
    assert not any('restored = 43' in fence.code for fence in fences)
    assert not any(fence.line == 41 for fence in fences)
    assert not any(fence.line == 46 for fence in fences)


def test_main_preserves_readme_and_docs_namespaces_and_runs_session_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_markdown(
        tmp_path,
        'README.md',
        """\
        ```python
        readme_value = 19
        ```
        ```python
        assert readme_value == 19
        ```
        ```python title="display output for a reader"
        title_did_not_disable = True
        ```
        ```python
        assert title_did_not_disable is True
        ```
        ~~~py
        readme_alias_value = 29
        ~~~
        ```python
        assert readme_alias_value == 29
        ```
        ```python title="output: captured transcript"
        deliberately not Python!
        ```
        """,
    )
    _write_markdown(
        tmp_path,
        'docs/get-started/quickstart.md',
        """\
        ```python exec="on" session="catchment"
        doc_value = 23
        ```
        ```python exec="on" session="catchment"
        assert doc_value == 23
        ```
        ```python exec="on"
        assert 'doc_value' not in globals()
        ```
        ```python exec="on" session="output"
        output_value = 31
        ```
        ```python exec="on" session="output"
        assert output_value == 31
        ```
        ~~~py exec="on" session="alias"
        docs_alias_value = 37
        ~~~
        ```python exec="on" session="alias"
        assert docs_alias_value == 37
        ```
        """,
    )
    _write_markdown(
        tmp_path,
        'docs/guide.md',
        """\
        !!! note

            ```python title="partial: a fragment"
            deliberately not Python!
            ```
        ```python exec="on" session="other-page"
        assert 'doc_value' not in globals()
        ```
        """,
    )

    assert _run_gate(tmp_path, monkeypatch) == 0
    assert 'TOTAL EXAMPLE FAILURES: 0' in capsys.readouterr().out


def test_missing_source_line_uses_superfences_exception() -> None:
    gate = load_tool('_check_examples')
    capture = gate._FenceCapture(Path('README.md'))

    with pytest.raises(gate.SuperFencesException, match='without its source line'):
        capture.format('value = 1', 'python', '', {}, None)


def test_markdown_exec_auto_is_cleared_before_validator_import() -> None:
    tools = Path(__file__).resolve().parents[2] / 'tools'
    script = textwrap.dedent(
        f"""\
        import os
        import sys

        sys.path.insert(0, {str(tools)!r})
        sys.path.insert(0, {str(tools / 'gates')!r})
        import _check_examples
        from markdown_exec._internal.main import MARKDOWN_EXEC_AUTO

        assert os.environ['MARKDOWN_EXEC_AUTO'] == 'python'
        assert MARKDOWN_EXEC_AUTO == ['']
        """
    )
    environment = os.environ.copy()
    environment['MARKDOWN_EXEC_AUTO'] = 'python'

    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr


def test_wrong_published_answer_fails_at_its_readme_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_markdown(
        tmp_path,
        'README.md',
        """\
        ```python
        area_km2 = '8217.8 km²'
        assert area_km2 == '8217.7 km²'
        ```
        """,
    )
    _write_runnable_quickstart(tmp_path)

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert 'README.md:1' in output
    assert 'AssertionError' in output


def test_session_dependency_failure_names_the_dependent_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_runnable_readme(tmp_path)
    _write_markdown(
        tmp_path,
        'docs/get-started/quickstart.md',
        """\
        ```python exec="on" session="catchment"
        assert missing_setup_value == 23
        ```
        """,
    )

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert 'docs/get-started/quickstart.md:1' in output
    assert 'NameError' in output


def test_session_reuse_across_docs_pages_fails_at_both_owners(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_runnable_readme(tmp_path)
    _write_runnable_quickstart(tmp_path)
    _write_markdown(
        tmp_path,
        'docs/guide.md',
        '```python exec="on" session="catchment"\nguide_value = 31\n```\n',
    )

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert 'docs/get-started/quickstart.md:1' in output
    assert 'docs/guide.md:1' in output
    assert "session 'catchment' is reused across documentation pages" in output


@pytest.mark.parametrize(
    ('role', 'extra_option'),
    [
        (role, '')
        for role in ('partial', 'pseudocode', 'template', 'configuration', 'output')
    ]
    + [('partial', ' returncode="not-an-integer"')],
)
@pytest.mark.parametrize('owner', ['README.md', 'docs/get-started/quickstart.md'])
def test_explicit_nonrunnable_roles_skip_invalid_python_when_exec_is_off(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    role: str,
    extra_option: str,
    owner: str,
) -> None:
    _write_runnable_readme(tmp_path)
    _write_runnable_quickstart(tmp_path)
    with (tmp_path / owner).open('a', encoding='utf-8') as page:
        page.write(
            f'\n```python exec="off" title="{role}: source fragment"{extra_option}\n'
            'deliberately not Python!\n'
            '```\n'
        )

    assert _run_gate(tmp_path, monkeypatch) == 0
    output = capsys.readouterr().out
    assert 'TOTAL EXAMPLE FAILURES: 0' in output
    assert 'SyntaxError' not in output


@pytest.mark.parametrize('owner', ['README.md', 'docs/get-started/quickstart.md'])
def test_exec_enabled_nonrunnable_role_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    owner: str,
) -> None:
    _write_runnable_readme(tmp_path)
    _write_runnable_quickstart(tmp_path)
    with (tmp_path / owner).open('a', encoding='utf-8') as page:
        page.write(
            '\n```python exec="on" title="partial: source fragment"\n'
            'deliberately not Python!\n'
            '```\n'
        )

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert owner in output
    assert 'non-runnable role conflicts with enabled markdown-exec execution' in output
    assert 'SyntaxError' not in output


def test_bare_and_class_roles_skip_invalid_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_runnable_readme(tmp_path)
    _write_runnable_quickstart(tmp_path)
    with (tmp_path / 'README.md').open('a', encoding='utf-8') as readme:
        readme.write('\n```python pseudocode\ndeliberately not Python!\n```\n')
    with (tmp_path / 'docs/get-started/quickstart.md').open(
        'a', encoding='utf-8'
    ) as quickstart:
        quickstart.write(
            '\n```{.python .configuration exec="off"}\ndeliberately not Python!\n```\n'
        )

    assert _run_gate(tmp_path, monkeypatch) == 0
    assert 'TOTAL EXAMPLE FAILURES: 0' in capsys.readouterr().out


@pytest.mark.parametrize('owner', ['README.md', 'docs/get-started/quickstart.md'])
def test_missing_required_first_run_page_fails_even_if_other_pages_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    owner: str,
) -> None:
    if owner == 'README.md':
        _write_runnable_quickstart(tmp_path)
    else:
        _write_runnable_readme(tmp_path)
    _write_markdown(
        tmp_path,
        'docs/other.md',
        '```python exec="on"\nother_value = 1\n```\n',
    )

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert owner in output
    assert 'required first-run example page is missing' in output
    assert 'TOTAL EXAMPLE FAILURES:' in output


@pytest.mark.parametrize('owner', ['README.md', 'docs/get-started/quickstart.md'])
@pytest.mark.parametrize(
    'content',
    [
        '',
        '```python title="partial: source fragment"\nnot Python\n```\n',
    ],
    ids=['empty', 'only-labelled'],
)
def test_required_page_needs_a_nonempty_runnable_example(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    owner: str,
    content: str,
) -> None:
    if owner == 'README.md':
        _write_runnable_quickstart(tmp_path)
    else:
        _write_runnable_readme(tmp_path)
    _write_markdown(tmp_path, owner, content)
    _write_markdown(
        tmp_path,
        'docs/other.md',
        '```python exec="on"\nother_value = 1\n```\n',
    )

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert 'required first-run page has no nonempty runnable Python example' in output


def test_zero_overall_examples_and_unlabelled_docs_fence_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_markdown(
        tmp_path,
        'README.md',
        '```python title="output: transcript"\nnot Python\n```\n',
    )
    _write_markdown(
        tmp_path,
        'docs/get-started/quickstart.md',
        '```python\nvalue = 1\n```\n',
    )

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert 'docs/get-started/quickstart.md:1' in output
    assert 'plain Python fence is neither executable nor labelled' in output
    assert 'no nonempty runnable Python examples were discovered' in output


def test_malformed_exec_metadata_is_reported_at_the_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_runnable_readme(tmp_path)
    _write_markdown(
        tmp_path,
        'docs/get-started/quickstart.md',
        '```python exec="on" returncode="not-an-integer"\nvalue = 1\n```\n',
    )

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert 'docs/get-started/quickstart.md:1' in output
    assert 'invalid markdown-exec fence options' in output
    assert 'TOTAL EXAMPLE FAILURES:' in output


def test_deprecated_source_option_is_parsed_from_any_fence_header(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_markdown(
        tmp_path,
        'README.md',
        """\
        This prose mentions source="material-block".
        ```bash source="material-block"
        echo "not executed"
        ```
        ```python
        print('source="material-block"')
        ```
        """,
    )
    _write_runnable_quickstart(tmp_path)

    assert _run_gate(tmp_path, monkeypatch) == 1
    output = capsys.readouterr().out
    assert output.count('deprecated markdown-exec source="material-block"') == 1
    assert 'README.md:2' in output


def test_docs_type_corpus_uses_retained_parser_fences(
    tmp_path: Path,
) -> None:
    gate = load_tool('_check_doctest_types')
    docs = tmp_path / 'docs'
    _write_markdown(
        tmp_path,
        'docs/nested.md',
        """\
        !!! note

            ```python title="partial: setup fragment"
            fragment = gm.Point(1, 2)
            ```
        ```python exec="on" session="docs"
        executable = gm.Point(3, 4)
        ```
        ```python title="partial: external archive"
        from pmtiles.writer import write
        archived = gm.mvt_tiles(sources, tiles=addresses)
        ```
        ````text
        ```python
        literal = gm.Point(5, 6)
        ```
        ````
        """,
    )
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(gate, '_ROOT', tmp_path)
    monkeypatch.setattr(gate, '_DOCS', docs)
    try:
        corpus = gate.render_markdown_corpus()
    finally:
        monkeypatch.undo()

    assert '# from docs/nested.md:3' in corpus
    assert 'fragment = gm.Point(1, 2)' in corpus
    assert 'executable = gm.Point(3, 4)' not in corpus
    assert 'archived = gm.mvt_tiles' not in corpus
    assert 'literal = gm.Point(5, 6)' not in corpus
