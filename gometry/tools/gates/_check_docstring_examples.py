"""Execute every ``Examples`` doctest embedded in gometry's docstrings.

Docstrings are authored in Rust ``///`` (see ```pyo3stubs gen-docs```) and become
the compiled extension's ``__doc__``. This harness walks the public API — the module,
its top-level functions, every public class, and each class's public methods — runs the
``>>>`` examples found in their docstrings, and reports mismatches. It keeps the
NumPy/Shapely-style ``Examples`` blocks honest: an example that drifts from real
behaviour fails here (and in ``tests/test_docstring_examples.py``).

Run directly: ``python tools/gates/_check_docstring_examples.py`` (exit 1 on any failure).
"""

from __future__ import annotations

import doctest

import gometry


def _public_objects() -> list[tuple[str, object]]:
    objects: list[tuple[str, object]] = [('gometry', gometry)]
    for name in dir(gometry):
        if name.startswith('_'):
            continue
        obj = getattr(gometry, name)
        objects.append((f'gometry.{name}', obj))
        if isinstance(obj, type):
            for member in dir(obj):
                if member.startswith('_'):
                    continue
                attr = getattr(obj, member, None)
                if getattr(attr, '__doc__', None):
                    objects.append((f'gometry.{name}.{member}', attr))
    return objects


def run() -> tuple[int, int, int]:
    globs = {'gm': gometry, 'gometry': gometry}
    parser = doctest.DocTestParser()
    runner = doctest.DocTestRunner(optionflags=doctest.ELLIPSIS)
    documented = examples = failures = 0
    for qualname, obj in _public_objects():
        doc = getattr(obj, '__doc__', None)
        if not doc or '>>>' not in doc:
            continue
        test = parser.get_doctest(doc, globs, qualname, None, None)
        if not test.examples:
            continue
        documented += 1
        examples += len(test.examples)
        result = runner.run(test)
        failures += result.failed
    return documented, examples, failures


def main() -> int:
    documented, examples, failures = run()
    print(f'docstrings with examples: {documented}')
    print(f'examples executed:        {examples}')
    print(f'TOTAL EXAMPLE FAILURES: {failures}')
    return 1 if failures else 0


if __name__ == '__main__':
    raise SystemExit(main())
