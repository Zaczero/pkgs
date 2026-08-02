"""Docstring *structure*: a documented symbol documents its whole signature.

``doc-contract`` owns presence — every public symbol has prose. This owns what
that prose has to contain, which is the half that rots silently:

* every signature parameter appears in ``Parameters``;
* no documented parameter has left the signature (stale docs fail);
* a ``Returns`` section exists (documenting ``None`` explicitly is the point);
* a ``default X`` stated in prose matches the runtime default.

Sections are parsed format-agnostically — numpydoc (``Parameters`` over a
``----------`` underline) and Google (``Args:``) are both understood — so the
gate does not pin one house style.

Domain rules stay with their package: a ``Raises`` taxonomy names a project's
own exception classes and operation families, and does not travel.
"""

from __future__ import annotations

import ast
import inspect
import re
from typing import TYPE_CHECKING

from pyo3stubs.ast_util import function_groups
from pyo3stubs.context import CheckContext
from pyo3stubs.report import Findings, unused_allowlist_errors

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

_NUMPYDOC_SECTION = re.compile(r'^([A-Za-z ]+)\n-+\n', re.MULTILINE)
_GOOGLE_SECTION = re.compile(r'^(\w[\w ]*):\s*$', re.MULTILINE)
#: `x : int, default 3` / `x (int): ..., default 3`
_DOCUMENTED_DEFAULT = re.compile(r'\bdefaults? (?:to )?([^,;]+?)\.?$')
_NUMPYDOC_ENTRY = re.compile(r'[*\w]+(\s*,\s*[*\w]+)*')
_GOOGLE_ENTRY = re.compile(r'\s+([*\w]+(?:\s*,\s*[*\w]+)*)\s*(?:\(([^)]*)\))?:')


def _sections(doc: str) -> tuple[dict[str, str], bool]:
    """``(section body by name, is_google)`` for one cleaned docstring."""
    for pattern, google in ((_NUMPYDOC_SECTION, False), (_GOOGLE_SECTION, True)):
        matches = list(pattern.finditer(doc))
        if not matches:
            continue
        found: dict[str, str] = {}
        for index, match in enumerate(matches):
            end = matches[index + 1].start() if index + 1 < len(matches) else len(doc)
            name = match.group(1).strip()
            found[{'Args': 'Parameters', 'Arguments': 'Parameters'}.get(name, name)] = (
                doc[match.end() : end]
            )
        return found, google
    return {}, False


def _documented_parameters(body: str, *, google: bool) -> dict[str, str | None]:
    """``name -> documented default`` for each parameter a section names."""
    entries: dict[str, str | None] = {}
    for line in body.splitlines():
        if not line.strip():
            continue
        if google:
            match = _GOOGLE_ENTRY.match(line)
            if match is None:
                continue
            names, meta = match.group(1), match.group(2) or ''
        else:
            if line[:1].isspace():
                continue
            names, separator, meta = line.partition(':')
            if not separator or not meta.strip():
                continue
            if _NUMPYDOC_ENTRY.fullmatch(names.strip()) is None:
                continue
        default = _DOCUMENTED_DEFAULT.search(meta.strip())
        for name in names.split(','):
            entries[name.strip().lstrip('*')] = (
                default.group(1).strip() if default else None
            )
    return entries


def _defaults_match(documented: str, runtime: object) -> bool:
    text = documented.rstrip('.').strip('`')
    if text in (repr(runtime), str(runtime)):
        return True
    try:
        return bool(ast.literal_eval(text) == runtime)
    except (SyntaxError, ValueError):
        return False


def _parameters(obj: object) -> list[inspect.Parameter] | None:
    """Signature parameters worth documenting, or None when unreadable.

    An unreadable signature is the `structural` gate's finding, not this one's.
    """
    try:
        signature = inspect.signature(obj)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return [
        param
        for param in signature.parameters.values()
        if param.name not in ('self', 'cls')
    ]


def _check(qualname: str, obj: object, doc: str) -> list[str]:
    sections, google = _sections(inspect.cleandoc(doc))
    documented = _documented_parameters(sections.get('Parameters', ''), google=google)
    errors: list[str] = []
    if not ({'Returns', 'Yields'} & set(sections)):
        errors.append(f'{qualname}: no Returns section — document what it gives back')
    parameters = _parameters(obj)
    if parameters is None:
        return errors
    for param in parameters:
        if param.name not in documented:
            errors.append(f'{qualname}: parameter {param.name!r} is not documented')
            continue
        stated = documented[param.name]
        if (
            stated is not None
            and param.default is not inspect.Parameter.empty
            and param.default is not ...
            and not _defaults_match(stated, param.default)
        ):
            errors.append(
                f'{qualname}.{param.name}: documented default {stated} != '
                f'runtime {param.default!r}'
            )
    errors.extend(
        f'{qualname}: documented parameter {name!r} is not in the signature'
        for name in sorted(set(documented) - {param.name for param in parameters})
    )
    return errors


def collect_errors(cfg: StubConfig) -> Findings:
    """Public callables whose prose does not describe their whole signature."""
    ctx = CheckContext(cfg)
    runtime = ctx.runtime_module
    errors: list[str] = []
    examined = 0

    exempt: set[str] = set()

    def visit(qualname: str, obj: object) -> None:
        nonlocal examined
        doc = getattr(obj, '__doc__', None)
        if not callable(obj) or isinstance(obj, type) or not (doc or '').strip():
            return  # presence is the doc-contract gate's finding
        if qualname in cfg.doc_structure_allowlist:
            exempt.add(qualname)
            return
        examined += 1
        errors.extend(_check(qualname, obj, doc or ''))

    for name in function_groups(ctx.stub_ast.body):
        if not name.startswith('_'):
            visit(name, getattr(runtime, name, None))
    for class_name, node in ctx.stub_classes.items():
        cls = getattr(runtime, class_name, None)
        if class_name.startswith('_') or not isinstance(cls, type):
            continue
        for name in function_groups(node.body):
            if not name.startswith('_') and name in vars(cls):
                visit(f'{class_name}.{name}', getattr(cls, name, None))

    errors += unused_allowlist_errors(
        'doc structure', cfg.doc_structure_allowlist, exempt
    )
    return Findings(errors, examined=examined)
