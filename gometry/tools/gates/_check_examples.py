"""Execute runnable Python fences from docs/ and public READMEs.

The Markdown parser owns fence syntax, indentation, containers, and retained
blocks. This gate records code without executing it until parsing is complete,
then checks the examples against the public package.
"""

from __future__ import annotations

import io
import sys
import traceback
from contextlib import redirect_stdout
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from markdown import Markdown
from markdown_exec import validator as _markdown_exec_validator
from pymdownx.superfences import SuperFencesBlockPreprocessor

_EXAMPLES = Path(__file__).resolve().parent.parent.parent / 'examples'
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
            raise RuntimeError('SuperFences formatted a block without its source line')
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


def _check_file(path: Path) -> tuple[int, int]:
    try:
        text = path.read_text(encoding='utf-8')
        fences, markdown = extract_markdown_fences(path, text)
    except Exception as error:
        _report_failure(path, None, f'could not parse Markdown: {error}')
        return 1, 0

    is_docs = path.is_relative_to(Path('docs'))
    sessions: dict[str, dict[str, object]] = {}
    readme_namespace: dict[str, object] = {'__name__': '__main__'}
    failures = 0
    executed = 0

    for index, fence in enumerate(fences):
        if fence.options.get('source') == 'material-block':
            failures += 1
            _report_failure(
                path,
                fence.line,
                'deprecated markdown-exec source="material-block"; use source="block"',
            )

        if _is_nonrunnable(fence):
            continue

        execution_options: dict[str, Any] | None = None
        execution_enabled: bool | None = None
        if 'exec' in fence.options:
            execution_enabled, execution_options, error = _markdown_exec_options(
                fence, markdown
            )
            if error is not None:
                failures += 1
                _report_failure(
                    path,
                    fence.line,
                    f'invalid markdown-exec fence options: {error}',
                )
                continue

        if fence.language.casefold() not in _PYTHON_LANGUAGES:
            continue

        if is_docs:
            if 'exec' not in fence.options:
                failures += 1
                _report_failure(
                    path,
                    fence.line,
                    'plain Python fence is neither executable nor labelled '
                    'partial/pseudocode/template/configuration/output',
                )
                continue
            if not execution_enabled:
                failures += 1
                _report_failure(
                    path,
                    fence.line,
                    'Python fence is not executable; use exec="on" or label it '
                    'partial/pseudocode/template/configuration/output',
                )
                continue

        if not fence.code.strip():
            failures += 1
            _report_failure(path, fence.line, 'runnable Python fence is empty')
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
                exec(  # noqa: S102
                    compile(fence.code, f'{path}:{fence.line}', 'exec'),
                    namespace,
                )
        except BaseException as error:
            # PyO3 deliberately derives PanicException from BaseException so
            # ordinary application handlers cannot swallow Rust panics.
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            failures += 1
            first = fence.code.strip().splitlines()[0]
            _report_failure(path, fence.line, f'(block {index}) first: {first!r}')
            if captured := output.getvalue().strip():
                print(captured[-2000:])
            print(traceback.format_exc().rstrip().split('\n')[-1])
    return failures, executed


def main() -> int:
    _ensure_examples_path()
    docs = sorted(Path('docs').rglob('*.md'))
    readme = Path('README.md')
    files = [*docs, readme]
    failures = 0
    executed = 0
    required_counts = dict.fromkeys(_REQUIRED_PAGES, 0)

    for owner in _REQUIRED_PAGES:
        if not owner.is_file():
            failures += 1
            _report_failure(owner, None, 'required first-run example page is missing')

    for path in files:
        if not path.is_file():
            continue
        file_failures, file_executed = _check_file(path)
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
