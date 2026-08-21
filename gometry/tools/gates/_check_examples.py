"""Extract runnable Python code blocks from docs/ and distribution READMEs.

markdown-exec runs ``exec="on"`` docs blocks at build time; a failure is only a
warning, so this script surfaces exactly which block fails and why. It also
rejects deprecated markdown-exec fence options before upstream turns their
warnings into removals. README examples are public contract, so plain
``python`` fences in that file run here too. Execution mirrors markdown-exec semantics: docs
blocks run in isolated namespaces except ``session="name"`` blocks, which share
one namespace per session, and ``examples/`` is importable (the figure helpers),
matching the sys.path entry ``tools/docs/gen_api_pages.py`` installs during the
real build.
"""

from __future__ import annotations

import io
import re
import sys
import textwrap
import traceback
from contextlib import redirect_stdout
from pathlib import Path

_BLOCK = re.compile(
    '```python([^\\n]*)\\bexec=[\\"\']on[\\"\']([^\\n]*)\\n(.*?)```', re.DOTALL
)
_PYTHON_BLOCK = re.compile('```python([^\\n]*)\\n(.*?)```', re.DOTALL)
_PYTHON_FENCE = re.compile(r'^[ \t]*```python(?P<options>[^\n]*)\n', re.MULTILINE)
_NONRUNNABLE_LABEL = re.compile(
    r'\b(?:partial|pseudocode|template|configuration|output)\b', re.IGNORECASE
)
_SESSION = re.compile('\\bsession=[\\"\']([^\\"\']+)[\\"\']')
_DEPRECATED_SOURCE_DISPLAY = re.compile(
    '^[ \\t]*```[^\\n]*\\bsource=([\\"\'])material-block\\1(?:\\s|$)',
    re.MULTILINE,
)
_EXAMPLES = Path(__file__).resolve().parent.parent.parent / 'examples'


def _ensure_examples_path() -> None:
    path = str(_EXAMPLES)
    if path not in sys.path:
        sys.path.insert(0, path)


_ensure_examples_path()


def main() -> int:
    _ensure_examples_path()
    failures = 0
    docs = sorted(Path('docs').rglob('*.md'))
    public_readmes = [Path('README.md')]
    for p in [*docs, *public_readmes]:
        text = p.read_text()
        for match in _DEPRECATED_SOURCE_DISPLAY.finditer(text):
            failures += 1
            line_no = text[: match.start()].count('\n') + 1
            print(
                f'\n### FAIL {p}:{line_no}  deprecated markdown-exec '
                'source="material-block"; use source="block"'
            )
        sessions: dict[str, dict[str, object]] = {}
        is_docs = p.is_relative_to(Path('docs'))
        block_re = _BLOCK if is_docs else _PYTHON_BLOCK
        if is_docs:
            for fence in _PYTHON_FENCE.finditer(text):
                options = fence.group('options')
                if 'exec=' in options or _NONRUNNABLE_LABEL.search(options):
                    continue
                failures += 1
                line_no = text[: fence.start()].count('\n') + 1
                print(
                    f'\n### FAIL {p}:{line_no}  plain Python fence is neither '
                    'executable nor labelled partial/pseudocode/template/configuration/output'
                )
        readme_ns: dict[str, object] = {'__name__': '__main__'}
        for i, m in enumerate(block_re.finditer(text)):
            code = textwrap.dedent(m.group(3 if is_docs else 2))
            line_no = text[: m.start()].count('\n') + 1
            session = _SESSION.search(m.group(1) + m.group(2)) if is_docs else None
            ns: dict[str, object]
            if session:
                ns = sessions.setdefault(session.group(1), {'__name__': '__main__'})
            elif not is_docs:
                ns = readme_ns
            else:
                ns = {'__name__': '__main__'}
            output = io.StringIO()
            try:
                with redirect_stdout(output):
                    exec(compile(code, f'{p}:{line_no}', 'exec'), ns)  # noqa: S102
            except BaseException as error:
                # PyO3 deliberately derives PanicException from BaseException so
                # ordinary application handlers cannot swallow Rust panics. This
                # is a validation boundary: report the failing public example and
                # continue checking the remaining documentation blocks.
                if isinstance(error, (KeyboardInterrupt, SystemExit)):
                    raise
                failures += 1
                first = code.strip().splitlines()[0] if code.strip() else '<empty>'
                print(f'\n### FAIL {p}:{line_no}  (block {i})  first: {first!r}')
                if captured := output.getvalue().strip():
                    print(captured[-2000:])
                print(traceback.format_exc().rstrip().split('\n')[-1])
    print(f'\nTOTAL EXAMPLE FAILURES: {failures}')
    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
