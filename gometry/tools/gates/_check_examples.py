"""Execute runnable Python fences from docs/ and public READMEs.

The Markdown parser owns fence syntax, indentation, containers, and retained
blocks. This gate records code without executing it until parsing is complete,
then checks the examples against the public package.
"""

from __future__ import annotations

import io
import os
import sys
import traceback
from contextlib import redirect_stdout
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from markdown import Markdown

# Markdown Exec snapshots this setting at import; this gate requires explicit `exec`.
_automatic_exec_languages = os.environ.pop('MARKDOWN_EXEC_AUTO', None)
try:
    from markdown_exec import validator as _markdown_exec_validator
finally:
    if _automatic_exec_languages is not None:
        os.environ['MARKDOWN_EXEC_AUTO'] = _automatic_exec_languages
from pymdownx.superfences import (
    SuperFencesBlockPreprocessor,
    SuperFencesException,
)

_EXAMPLES = Path(__file__).resolve().parent.parent.parent / 'examples'
_ROOT = Path(__file__).resolve().parents[2]
_TOOLS = _ROOT / 'tools'
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))
from _gatelib import prepend_tools_import_paths, require_runtime

prepend_tools_import_paths()

_MARKER_ATTRIBUTE = 'data-gometry-gate-fence'
_MARKER_PREFIX = 'gometry-gate-fence-'
_MARKDOWN_EXTENSIONS = (
    'admonition',
    'attr_list',
    'md_in_html',
    'pymdownx.details',
    'pymdownx.superfences',
    'pymdownx.tabbed',
)
_NONRUNNABLE_ROLES = frozenset({
    'partial',
    'pseudocode',
    'template',
    'configuration',
    'output',
})
_FALSE_VALUES = frozenset({'', 'no', 'off', 'false', '0'})
_PYTHON_LANGUAGES = frozenset({'py', 'python'})
_REQUIRED_PAGES = (Path('README.md'), Path('docs/get-started/quickstart.md'))


@dataclass(frozen=True, slots=True)
class _Fence:
    path: Path
    line: int
    code: str
    language: str
    options: dict[str, str]
    classes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ParsedPage:
    path: Path
    fences: tuple[_Fence, ...]
    markdown: Markdown


class _FenceCapture:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.opening_line: int | None = None
        self.fences: list[_Fence] = []

    def validate(
        self,
        _language: str,
        inputs: dict[str, str],
        options: dict[str, Any],
        _attrs: dict[str, Any],
        _md: Markdown,
    ) -> bool:
        options.update(inputs)
        return True

    def format(
        self,
        source: str,
        language: str | None,
        _class_name: str,
        options: dict[str, str],
        _md: Markdown,
        **kwargs: Any,
    ) -> str:
        if self.opening_line is None:
            raise SuperFencesException(
                'SuperFences formatted a block without its source line'
            )
        marker = f'{_MARKER_PREFIX}{len(self.fences)}'
        self.fences.append(
            _Fence(
                path=self.path,
                line=self.opening_line,
                code=source,
                language=language or '',
                options=dict(options),
                classes=tuple(kwargs.get('classes') or ()),
            )
        )
        return f'<pre {_MARKER_ATTRIBUTE}="{marker}"></pre>'


class _LocatedSuperFencesBlockPreprocessor(SuperFencesBlockPreprocessor):
    def __init__(self, md: Markdown, capture: _FenceCapture) -> None:
        super().__init__(md)
        self.capture = capture

    def process_nested_block(self, ws: str, content: str, start: int, end: int) -> None:
        previous_line = self.capture.opening_line
        self.capture.opening_line = start + 1
        try:
            super().process_nested_block(ws, content, start, end)
        finally:
            self.capture.opening_line = previous_line


class _RetainedFenceMarkers(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.markers: list[int] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != 'pre':
            return
        value = dict(attrs).get(_MARKER_ATTRIBUTE)
        if value is None or not value.startswith(_MARKER_PREFIX):
            return
        suffix = value[len(_MARKER_PREFIX) :]
        if suffix.isdecimal():
            self.markers.append(int(suffix))


def extract_markdown_fences(
    path: Path, text: str
) -> tuple[tuple[_Fence, ...], Markdown]:
    """Parse fences with the docs grammar and return only retained blocks."""
    capture = _FenceCapture(path)
    markdown = Markdown(
        extensions=list(_MARKDOWN_EXTENSIONS),
        extension_configs={
            'pymdownx.superfences': {
                'custom_fences': [
                    {
                        'name': '*',
                        'class': 'gometry-gate-fence',
                        'validator': capture.validate,
                        'format': capture.format,
                    }
                ]
            }
        },
    )
    configured = markdown.preprocessors['fenced_code_block']
    located = _LocatedSuperFencesBlockPreprocessor(markdown, capture)
    located.config = configured.config
    located.extension = configured.extension
    markdown.preprocessors.register(located, 'fenced_code_block', 25)

    rendered = markdown.convert(text)
    retained = _RetainedFenceMarkers()
    retained.feed(rendered)
    seen: set[int] = set()
    fences: list[_Fence] = []
    for index in retained.markers:
        if index >= len(capture.fences):
            raise RuntimeError(f'Markdown retained an unknown fence marker: {index}')
        if index not in seen:
            seen.add(index)
            fences.append(capture.fences[index])
    return tuple(fences), markdown


def _parse_page(path: Path) -> _ParsedPage:
    text = path.read_text(encoding='utf-8')
    fences, markdown = extract_markdown_fences(path, text)
    return _ParsedPage(path=path, fences=fences, markdown=markdown)


def _ensure_examples_path() -> None:
    path = str(_EXAMPLES)
    if path not in sys.path:
        sys.path.insert(0, path)


def _is_nonrunnable(fence: _Fence) -> bool:
    if any(role.casefold() in _NONRUNNABLE_ROLES for role in fence.classes):
        return True
    for role in _NONRUNNABLE_ROLES:
        value = fence.options.get(role)
        if value is not None and value.casefold() not in _FALSE_VALUES:
            return True
    title = fence.options.get('title', '')
    label = title.split(':', 1)[0].strip().casefold()
    return label in _NONRUNNABLE_ROLES


def _markdown_exec_options(
    fence: _Fence, markdown: Markdown
) -> tuple[bool | None, dict[str, Any] | None, Exception | None]:
    parsed: dict[str, Any] = {}
    try:
        enabled = _markdown_exec_validator(
            fence.language,
            dict(fence.options),
            parsed,
            {},
            markdown,
        )
    except Exception as error:
        return None, None, error
    return enabled, parsed, None


def _report_failure(path: Path, line: int | None, message: str) -> None:
    location = f'{path}:{line}' if line is not None else str(path)
    print(f'\n### FAIL {location}  {message}')


def _check_file(page: _ParsedPage) -> tuple[int, int]:
    is_docs = page.path.is_relative_to(Path('docs'))
    sessions: dict[str, dict[str, object]] = {}
    readme_namespace: dict[str, object] = {'__name__': '__main__'}
    failures = 0
    executed = 0

    for index, fence in enumerate(page.fences):
        if fence.options.get('source') == 'material-block':
            failures += 1
            _report_failure(
                page.path,
                fence.line,
                'deprecated markdown-exec source="material-block"; use source="block"',
            )

        nonrunnable = _is_nonrunnable(fence)

        execution_options: dict[str, Any] | None = None
        execution_enabled: bool | None = None
        if 'exec' in fence.options:
            execution_enabled, execution_options, error = _markdown_exec_options(
                fence, page.markdown
            )
            if error is not None:
                failures += 1
                _report_failure(
                    page.path,
                    fence.line,
                    f'invalid markdown-exec fence options: {error}',
                )
                continue

        if nonrunnable:
            if execution_enabled:
                failures += 1
                _report_failure(
                    page.path,
                    fence.line,
                    'non-runnable role conflicts with enabled markdown-exec execution',
                )
            continue

        if fence.language.casefold() not in _PYTHON_LANGUAGES:
            continue

        if is_docs:
            if 'exec' not in fence.options:
                failures += 1
                _report_failure(
                    page.path,
                    fence.line,
                    'plain Python fence is neither executable nor labelled '
                    'partial/pseudocode/template/configuration/output',
                )
                continue
            if not execution_enabled:
                failures += 1
                _report_failure(
                    page.path,
                    fence.line,
                    'Python fence is not executable; use exec="on" or label it '
                    'partial/pseudocode/template/configuration/output',
                )
                continue

        if not fence.code.strip():
            failures += 1
            _report_failure(page.path, fence.line, 'runnable Python fence is empty')
            continue

        executed += 1
        if not is_docs:
            namespace = readme_namespace
        elif execution_options and (session := execution_options.get('session')):
            namespace = sessions.setdefault(session, {'__name__': '__main__'})
        else:
            namespace = {'__name__': '__main__'}

        output = io.StringIO()
        try:
            with redirect_stdout(output):
                exec(  # ruff: ignore[exec-builtin]
                    compile(fence.code, f'{page.path}:{fence.line}', 'exec'),
                    namespace,
                )
        except BaseException as error:
            # PyO3 deliberately derives PanicException from BaseException so
            # ordinary application handlers cannot swallow Rust panics.
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            failures += 1
            first = fence.code.strip().splitlines()[0]
            _report_failure(page.path, fence.line, f'(block {index}) first: {first!r}')
            if captured := output.getvalue().strip():
                print(captured[-2000:])
            print(traceback.format_exc().rstrip().split('\n')[-1])
    return failures, executed


def _check_cross_page_sessions(pages: list[_ParsedPage]) -> int:
    sessions: dict[str, list[tuple[Path, int]]] = {}
    for page in pages:
        if not page.path.is_relative_to(Path('docs')):
            continue
        for fence in page.fences:
            if (
                _is_nonrunnable(fence)
                or 'exec' not in fence.options
                or 'session' not in fence.options
            ):
                continue
            enabled, options, error = _markdown_exec_options(fence, page.markdown)
            if error is not None or not enabled or options is None:
                continue
            session = options.get('session')
            if session:
                sessions.setdefault(session, []).append((page.path, fence.line))

    failures = 0
    for session, locations in sorted(sessions.items()):
        page_lines: dict[Path, int] = {}
        for path, line in locations:
            page_lines.setdefault(path, line)
        if len(page_lines) < 2:
            continue
        pages_text = ', '.join(str(path) for path in page_lines)
        for path, line in page_lines.items():
            failures += 1
            _report_failure(
                path,
                line,
                f'markdown-exec session {session!r} is reused across documentation '
                f'pages: {pages_text}',
            )
    return failures


def main() -> int:
    require_runtime(_ROOT)
    _ensure_examples_path()
    docs = sorted(Path('docs').rglob('*.md'))
    readme = Path('README.md')
    files = [*docs, readme]
    failures = 0
    executed = 0
    required_counts = dict.fromkeys(_REQUIRED_PAGES, 0)
    pages: list[_ParsedPage] = []

    for owner in _REQUIRED_PAGES:
        if not owner.is_file():
            failures += 1
            _report_failure(owner, None, 'required first-run example page is missing')

    for path in files:
        if not path.is_file():
            continue
        try:
            pages.append(_parse_page(path))
        except Exception as error:
            failures += 1
            _report_failure(path, None, f'could not parse Markdown: {error}')

    failures += _check_cross_page_sessions(pages)

    for page in pages:
        path = page.path
        file_failures, file_executed = _check_file(page)
        failures += file_failures
        executed += file_executed
        if path in required_counts:
            required_counts[path] += file_executed

    for owner, count in required_counts.items():
        if owner.is_file() and count == 0:
            failures += 1
            _report_failure(
                owner,
                None,
                'required first-run page has no nonempty runnable Python example',
            )
    if executed == 0:
        failures += 1
        print('\n### FAIL no nonempty runnable Python examples were discovered')

    print(f'\nTOTAL EXAMPLE FAILURES: {failures}')
    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
