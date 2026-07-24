"""Runtime typing gate for precise public signatures and private type support.

Public annotations must resolve at runtime (numpy, geometry classes, and
``collections.abc`` are hard/compiled dependencies — they cannot live only
under ``if TYPE_CHECKING:``). Reusable implementation aliases live privately
in ``gometry._types``; genuine runtime protocols are exported at top level.

Also drives the static gates: pyright AND mypy over the ``assert_type``
conformance file and the negatives file (misuse whose ``# type: ignore`` must
stay *used* — a loosened stub makes one unused and fails both checkers), plus
a full-project pyright run over ``python/gometry`` locked at zero errors.

Usage::

    .venv/bin/python tools/gates/_check_typing_runtime.py

Exit 1 on any failure. The final line is ``TOTAL FAILURES: N``.
"""

from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
import typing
from pathlib import Path

import gometry
from gometry import _types

_ROOT = Path(__file__).resolve().parent.parent.parent
_PYRIGHT = _ROOT / '.venv/bin/pyright'
_PYTHON = _ROOT / '.venv/bin/python'
_CONFORMANCE = _ROOT / 'tests/test_typing_conformance.py'
_NEGATIVES = _ROOT / 'tests/test_typing_negatives.py'

_TOP_LEVEL_HINT_TARGETS = (
    'from_features',
    'to_feature',
    'to_feature_collection',
)

_INTEROP_MODULES = (
    'gometry._arrow',
    'gometry._geopandas',
    'gometry._geoparquet',
    'gometry._optional',
    'gometry._pandas',
    'gometry._polars',
    'gometry._viz',
)

# Highest-value free-function overload groups that must keep both a positive
# ``assert_type`` witness (tests/test_typing_conformance.py) and a negative
# misuse ``# type: ignore`` witness (tests/test_typing_negatives.py).
# Native PyO3 callables have no runtime ``@overload`` registry, so the
# fixtures — not ``typing.get_overloads`` — are the real narrowing gate.
_OVERLOAD_TARGETS: tuple[str, ...] = (
    'contains',
    'intersects',
    'distance',
    'area',
    'length',
    'from_wkt',
    'from_wkb',
    'Point',
    'bearing',
    'destination',
    'point_between',
)

_RESULT_CONTAINERS = frozenset({
    'Extremes',
    'Features',
    'PolygonizeResult',
})


def _check_type_checking_sentinel(errors: list[str]) -> None:
    offenders: list[str] = []
    missing: list[str] = []
    for path in sorted((_ROOT / 'python' / 'gometry').glob('*.py')):
        text = path.read_text(encoding='utf-8')
        rel = path.relative_to(_ROOT)
        if 'from typing import TYPE_CHECKING' in text:
            offenders.append(str(rel))
        if 'if TYPE_CHECKING:' in text and 'TYPE_CHECKING = False' not in text:
            missing.append(str(rel))
    if offenders:
        errors.append(
            'shipped python modules must use TYPE_CHECKING = False, not '
            f'from typing import TYPE_CHECKING: {offenders}'
        )
    if missing:
        errors.append(
            'shipped python modules with if TYPE_CHECKING: must define the '
            f'TYPE_CHECKING = False sentinel: {missing}'
        )


def _hintable_names(module: object, names: tuple[str, ...]) -> list[str]:
    out: list[str] = []
    for name in names:
        obj = getattr(module, name)
        if isinstance(obj, type):
            out.append(name)
    return out


def _check_get_type_hints(label: str, obj: object, errors: list[str]) -> None:
    try:
        typing.get_type_hints(obj)
    except Exception as error:
        errors.append(f'{label}: {error}')


def _check_annotated_members(label: str, obj: object, errors: list[str]) -> None:
    if not inspect.isclass(obj):
        _check_get_type_hints(label, obj, errors)
        return
    if obj.__dict__.get('__annotations__'):
        _check_get_type_hints(label, obj, errors)
    for member_name, member in vars(obj).items():
        target = member.fget if isinstance(member, property) else member
        if not inspect.isfunction(target) or not getattr(
            target, '__annotations__', None
        ):
            continue
        if member_name.startswith('_') and not (
            member_name.startswith('__') and member_name.endswith('__')
        ):
            continue
        _check_get_type_hints(f'{label}.{member_name}', target, errors)


def _check_public_interop_hints(errors: list[str]) -> None:
    seen: set[int] = set()
    for name in gometry._LAZY_EXPORTS:
        obj = getattr(gometry, name)
        seen.add(id(obj))
        _check_annotated_members(f'gometry.{name}', obj, errors)

    for module_name in _INTEROP_MODULES:
        module = importlib.import_module(module_name)
        for name, obj in vars(module).items():
            if name.startswith('_') or id(obj) in seen:
                continue
            if not (inspect.isfunction(obj) or inspect.isclass(obj)):
                continue
            if getattr(obj, '__module__', None) != module_name:
                continue
            seen.add(id(obj))
            _check_annotated_members(f'{module_name}.{name}', obj, errors)


def _check_overload_witnesses(errors: list[str]) -> None:
    """Require positive + negative static witnesses for each overload group.

    The stub's ``@overload`` sets are the source of truth; pyright/mypy
    over the fixtures prove narrowing. This hook only enforces that the
    curated high-value groups stay covered when the fixtures drift.
    """
    import re

    if not _OVERLOAD_TARGETS:
        return
    if not _CONFORMANCE.is_file():
        errors.append(f'conformance fixture missing: {_CONFORMANCE}')
        return
    if not _NEGATIVES.is_file():
        errors.append(f'negatives fixture missing: {_NEGATIVES}')
        return
    conf = _CONFORMANCE.read_text(encoding='utf-8')
    neg = _NEGATIVES.read_text(encoding='utf-8')
    for name in _OVERLOAD_TARGETS:
        # Positive: an assert_type call that exercises gm.<name>(...) narrowing.
        pos = re.search(
            rf'assert_type\(\s*(?:gm\.)?{re.escape(name)}\s*\(',
            conf,
        )
        if pos is None and name == 'Point':
            # Constructors also appear as ``gm.Point(...)`` outside assert_type
            # when the result is later assert_type'd — accept either form.
            pos = re.search(r'assert_type\(\s*gm\.Point\b', conf) or re.search(
                r'gm\.Point\s*\(', conf
            )
        if pos is None:
            errors.append(
                f'overload group {name!r}: missing positive assert_type witness '
                f'in {_CONFORMANCE.relative_to(_ROOT)}'
            )
        # Negative: a deliberate misuse of gm.<name> under TYPE_CHECKING with
        # a type: ignore that must stay used.
        neg_hit = re.search(
            rf'gm\.{re.escape(name)}\s*\([^;\n]*#\s*type:\s*ignore',
            neg,
        )
        if neg_hit is None:
            errors.append(
                f'overload group {name!r}: missing negative type:ignore witness '
                f'in {_NEGATIVES.relative_to(_ROOT)}'
            )


def _check_private_types_have_producers(exported: set[str], errors: list[str]) -> None:
    """Every typing export must be runtime-real or referenced by the shipped
    surface — a vocabulary alias nothing produces or consumes is a phantom
    (a private alias with no producer is still dead type vocabulary).
    A stub-private ``_Name`` alias counts as a reference to ``Name``.
    """
    import re

    corpus = (_ROOT / 'python' / 'gometry' / '_lib.pyi').read_text(encoding='utf-8')
    for path in (_ROOT / 'python' / 'gometry').glob('*.py'):
        corpus += path.read_text(encoding='utf-8')
    phantoms = sorted(
        name for name in exported if not re.search(rf'\b_?{re.escape(name)}\b', corpus)
    )
    if phantoms:
        errors.append(
            'gometry._types names with no producer or reference anywhere in '
            f'the shipped surface (phantom private types): {phantoms}'
        )


def _check_pyright(errors: list[str], label: str, *targets: str) -> None:
    """One pyright run; no targets means the whole configured project scope."""
    if not _PYRIGHT.is_file():
        errors.append(f'pyright not found at {_PYRIGHT}')
        return
    result = subprocess.run(
        [str(_PYRIGHT), *targets, '--pythonpath', str(_PYTHON)],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        output = (result.stdout + result.stderr).strip()
        errors.append(f'{label} pyright failed:\n' + output)


def _check_mypy(errors: list[str], label: str, *targets: str) -> None:
    """Run mypy over the same probes — its overload resolution genuinely differs
    from pyright's, and half of gometry's users run it.
    """
    from mypy import api

    stdout, stderr, status = api.run([
        *targets,
        '--warn-unused-ignores',
        '--no-error-summary',
        '--no-color-output',
    ])
    if status != 0:
        errors.append(f'{label} mypy failed:\n' + (stdout + stderr).strip())


def collect_errors() -> list[str]:
    errors: list[str] = []

    _check_type_checking_sentinel(errors)
    _check_private_types_have_producers(
        set(_types.__all__) - _RESULT_CONTAINERS,
        errors,
    )

    for name in _hintable_names(_types, tuple(_types.__all__)):
        _check_get_type_hints(f'gometry._types.{name}', getattr(_types, name), errors)

    for name in _TOP_LEVEL_HINT_TARGETS:
        _check_get_type_hints(f'gometry.{name}', getattr(gometry, name), errors)

    _check_public_interop_hints(errors)

    _check_overload_witnesses(errors)

    # Dual-checker conformance + negatives + the whole shipped package in ONE
    # run per checker (5 checker processes -> 2; the checkers dominate the
    # gate's runtime). Full-project pyright stays locked at 0 errors —
    # chasing it found a live pandas-repr crash.
    _check_pyright(
        errors,
        'pyright (conformance + negatives + python/gometry)',
        str(_CONFORMANCE),
        str(_NEGATIVES),
        str(_ROOT / 'python' / 'gometry'),
    )
    _check_mypy(
        errors,
        'mypy (conformance + negatives)',
        str(_CONFORMANCE),
        str(_NEGATIVES),
    )
    return errors


def main() -> int:
    errors = collect_errors()
    for error in errors:
        print(error, file=sys.stderr)
        print(file=sys.stderr)
    print(f'TOTAL FAILURES: {len(errors)}')
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
