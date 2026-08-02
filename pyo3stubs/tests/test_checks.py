"""Per-gate fixture suite: every gate proven clean AND proven to fire.

The fake runtime (``fixtures/fakepkg``) and fixture Rust source stand in for a
PyO3 package; mutated copies land in ``tmp_path`` so the pristine fixtures are
never written.

:data:`CASES` is the load-bearing structure. A gate cannot be registered without
a clean case and a seeded case, and the clean case asserts the gate reached
``OK`` — not merely "no violations" — so a gate that has quietly become
inactive, skipped, or permanently red fails here rather than reading as a pass.
Two gates were red against this very fixture with nothing noticing.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pytest
from conftest import make_config, seeded

from pyo3stubs.config import DualityConfig, MacroExport, StubConfig, SurfaceConfig
from pyo3stubs.doc_contract import collect_errors as doc_contract
from pyo3stubs.gates import GATES, Status, gate, gate_names, run_all
from pyo3stubs.gen import render_stub_with_docs
from pyo3stubs.leaked_types import collect_errors as leaked_types
from pyo3stubs.rust_nullability import collect_errors as rust_nullability
from pyo3stubs.rust_scan import pyclass_names, rust_class_map
from pyo3stubs.structural import collect_errors as structural
from pyo3stubs.text_signature import collect_errors as text_signature

if TYPE_CHECKING:
    from pathlib import Path

#: ``(tmp_path, pristine stub, pristine Rust) -> StubConfig``.
Build = Callable[['Path', str, str], StubConfig]

_DUALITY_STUB = '''\
from typing import Self, final

@final
class Leaf:
    def move(self) -> Self:
        """Kind-preserving."""
    def hull(self) -> Wide:
        """Kind-changing."""
    def pieces(self) -> LeafArray[Wide]:
        """Expansion: scalar returns an array."""

@final
class Wide: ...

@final
class LeafArray:
    def move(self) -> Self:
        """Kind-preserving twin."""
    def hull(self) -> LeafArray[Wide]:
        """Kind-changing twin."""
    def pieces(self) -> LeafArray[Wide]:
        """Expansion twin (flattened)."""
'''

_DOCUMENTED = "def documented(a: int, b: str = 'x') -> str:"
#: Unique to `Sealed` -- `method` now lives on both fixture classes, so it is no
#: longer an anchor that identifies one of them.
_SEALED = "    __match_args__: Final = ('x', 'y')"


@dataclass(frozen=True, slots=True)
class Case:
    """Both directions for one gate, and what its violation must say."""

    clean: Build
    seeded: Build
    expect: str


def _default(tmp_path: Path, stub: str, src: str) -> StubConfig:
    return make_config(tmp_path)


def _stub(anchor: str, replacement: str, *, count: int = 1) -> Build:
    """A config whose stub carries one seeded mutation."""

    def build(tmp_path: Path, stub: str, src: str) -> StubConfig:
        return make_config(
            tmp_path, stub_text=seeded(stub, anchor, replacement, count=count)
        )

    return build


def _src(anchor: str, replacement: str) -> Build:
    """A config whose Rust source carries one seeded mutation."""

    def build(tmp_path: Path, stub: str, src: str) -> StubConfig:
        return make_config(tmp_path, src_text=seeded(src, anchor, replacement))

    return build


def _duality(**overrides: object) -> Build:
    def build(tmp_path: Path, stub: str, src: str) -> StubConfig:
        text = overrides.pop('stub_text', _DUALITY_STUB)
        return make_config(
            tmp_path,
            stub_text=text,
            duality=DualityConfig(pairs=(('Leaf', 'LeafArray'),), **overrides),  # type: ignore[arg-type]
        )

    return build


def _surface_clean(tmp_path: Path, stub: str, src: str) -> StubConfig:
    import fakepkg._lib as lib

    return make_config(
        tmp_path,
        surface=SurfaceConfig(targets=(('Sealed', lib.Sealed), ('Open', lib.Open))),
    )


def _surface_seeded(tmp_path: Path, stub: str, src: str) -> StubConfig:
    class Left:
        def method(self, value: int, flag: bool = True) -> None: ...

    class Right:
        def method(self, value: int, flag: bool = False) -> None: ...

    return make_config(
        tmp_path,
        surface=SurfaceConfig(targets=(('Left', Left), ('Right', Right))),
    )


def _leaked_seeded(tmp_path: Path, stub: str, src: str) -> StubConfig:
    return make_config(tmp_path, leak_allowlist={})


def _doc_structure_seeded(tmp_path: Path, stub: str, src: str) -> StubConfig:
    return make_config(
        tmp_path, doc_structure_allowlist={'absent': 'stale: no such callable'}
    )


#: One clean case and one seeded case per registered gate.
CASES: dict[str, Case] = {
    'validity': Case(
        clean=_default,
        seeded=_stub(_DOCUMENTED, 'def documented(a: Undeclared) -> str:'),
        expect='Undeclared',
    ),
    'structural': Case(
        clean=_default,
        seeded=_stub(
            '@overload\ndef overloaded(value: str) -> str:',
            'def overloaded(value: str) -> str:',
        ),
        expect='without @overload',
    ),
    'text-signature': Case(
        clean=_default,
        seeded=_src('b = "x"', 'b = "y"'),
        expect='text_signature default',
    ),
    'surface': Case(
        clean=_surface_clean,
        seeded=_surface_seeded,
        expect='option parameters diverge',
    ),
    'duality': Case(
        clean=_duality(),
        seeded=_duality(
            stub_text=_DUALITY_STUB.replace(
                'def hull(self) -> LeafArray[Wide]:', 'def hull(self) -> Self:'
            )
        ),
        expect='breaks scalar<->array duality',
    ),
    'token-vocabulary': Case(
        clean=_default,
        seeded=_src('enum Mode(', 'enum Renamed('),
        expect='has no token_enum!',
    ),
    'leaked-types': Case(
        clean=_default,
        seeded=_leaked_seeded,
        expect="pyclass 'Orphan' is not registered",
    ),
    'rust-nullability': Case(
        clean=_default,
        seeded=_stub('def maybe(self) -> int | None:', 'def maybe(self) -> int:'),
        expect='does not admit None',
    ),
    'doc-contract': Case(
        clean=_default,
        seeded=_stub(_SEALED, f'{_SEALED}\n    def narrowed(self) -> int: ...'),
        expect='stub override needs its own docstring',
    ),
    'doc-structure': Case(
        clean=_default,
        # The gate reads the runtime `__doc__`, which a stub mutation cannot
        # reach; its allowlist-rot rule is seedable from config alone. The
        # prose rules themselves are covered directly in `test_doc_structure`.
        seeded=_doc_structure_seeded,
        expect="'absent' is unused",
    ),
    'gen-docs-sync': Case(
        clean=_default,
        seeded=_stub('"""A documented function.', '"""Stale prose.'),
        expect='stub docstrings are stale',
    ),
    'stubtest': Case(
        clean=_default,
        seeded=_stub(_DOCUMENTED, "def documented(a: int, b: str = 'y') -> str:"),
        expect='documented',
    ),
}


def test_every_gate_has_both_directions():
    """A gate with no case is a gate nothing proves anything about."""
    assert sorted(CASES) == sorted(gate_names())


@pytest.mark.parametrize('name', gate_names())
def test_gate_is_clean(name, tmp_path, pristine_stub, pristine_src):
    """The pristine fixture satisfies every gate, and every gate runs.

    `Status.OK` rather than "no violations": a gate that is inactive, skipped,
    or disabled reports no violations too, and reading that as a pass is how
    `duality` could print success on a project that never configured it.
    """
    cfg = CASES[name].clean(tmp_path, pristine_stub, pristine_src)
    result = gate(name).run(cfg)
    if result.status is Status.SKIPPED:
        pytest.skip(result.line)
    assert result.status is Status.OK, result.errors


@pytest.mark.parametrize('name', gate_names())
def test_gate_fires(name, tmp_path, pristine_stub, pristine_src):
    """Each gate reports the violation its own seeded mutation introduces."""
    case = CASES[name]
    cfg = case.seeded(tmp_path, pristine_stub, pristine_src)
    result = gate(name).run(cfg)
    if result.status is Status.SKIPPED:
        pytest.skip(result.line)
    assert result.status is Status.VIOLATIONS, result
    assert any(case.expect in error for error in result.errors), result.errors


def test_every_gate_record_is_populated():
    """No gate ships with a blank help line, success line, or remedy."""
    for entry in GATES:
        for field in ('name', 'summary', 'success', 'remedy'):
            assert getattr(entry, field), f'{entry.name}: empty {field}'
        assert entry.name == entry.name.lower().replace('_', '-')


def test_a_crashing_gate_is_a_violation_not_an_abort(tmp_path):
    """One broken gate must not take the other eleven down with it.

    A unit struct as the last `#[pyclass]` in a file used to raise `ValueError`
    out of a collector, and a `(` inside a Rust default an `IndexError` with no
    file or line -- both aborting `check-all` partway through.
    """

    def explode(cfg: StubConfig) -> list[str]:
        raise IndexError('string index out of range')

    broken = replace(gate('structural'), collect=explode)
    result = broken.run(make_config(tmp_path))

    assert result.status is Status.VIOLATIONS
    assert 'gate could not run: IndexError' in result.errors[0]
    assert 'test_checks.py:' in result.errors[0], 'the raising frame is the point'


def test_disabled_gates_are_reported_not_silently_passed(tmp_path):
    cfg = make_config(tmp_path, disabled_gates=frozenset({'stubtest'}))
    results = {result.gate.name: result for result in run_all(cfg)}

    assert results['stubtest'].status is Status.DISABLED
    assert 'disabled' in results['stubtest'].line
    assert results['structural'].status is Status.OK


def test_an_unconfigured_gate_reports_itself_inactive(tmp_path):
    cfg = make_config(tmp_path)
    assert gate('duality').run(cfg).status is Status.INACTIVE
    assert 'StubConfig.duality' in gate('duality').run(cfg).line


def test_an_empty_optional_config_deactivates_its_gate(tmp_path):
    """A config that checks nothing must not report that its rule held.

    `DualityConfig(pairs=())` and `SurfaceConfig(targets=())` compare no
    classes at all; the collectors return no violations, so the gate printed
    success -- the same lie as an unconfigured gate passing.
    """
    empty = make_config(
        tmp_path,
        duality=DualityConfig(pairs=()),
        surface=SurfaceConfig(targets=()),
    )
    assert gate('duality').run(empty).status is Status.INACTIVE
    assert gate('surface').run(empty).status is Status.INACTIVE


def test_config_rejects_paths_that_would_silently_pass(tmp_path):
    """A missing `src_root` must raise, not make every Rust gate return clean.

    `Path.rglob` on a nonexistent directory yields nothing, so before this the
    four source-scanning gates reported no violations and the run went green
    having read no files. Measured then: `text-signature` found one violation
    against the real fixture root and none against a typo.
    """
    real = make_config(tmp_path)

    with pytest.raises(ValueError, match='src_root'):
        replace(real, src_root=tmp_path / 'no-such-src')

    with pytest.raises(ValueError, match='stub_path'):
        replace(real, stub_path=tmp_path / 'no-such-stub.pyi')


def test_config_rejects_an_unknown_disabled_gate(tmp_path):
    with pytest.raises(ValueError, match='no such gate'):
        make_config(tmp_path, disabled_gates=frozenset({'structual'}))


def test_signature_coverage_requires_allowlist(tmp_path):
    errors = structural(make_config(tmp_path, uninspectable_allowlist={}))
    assert any('hidden_callable' in e and 'inspect.signature' in e for e in errors)


def test_signature_coverage_flags_stale_allowlist(tmp_path):
    cfg = make_config(
        tmp_path,
        uninspectable_allowlist={
            'hidden_callable': 'fixture: __signature__ raises',
            'documented': 'stale: perfectly inspectable',
        },
    )
    assert any('documented' in e and 'inspectable again' in e for e in structural(cfg))


def test_leak_allowlist_rot_is_reported(tmp_path):
    """An allowlist nobody prunes is how a real violation goes quiet."""
    cfg = make_config(
        tmp_path,
        leak_allowlist={
            'Orphan': 'fixture: deliberately unregistered',
            'Ghost': 'stale: no such pyclass',
        },
    )
    assert any("leak allowlist entry 'Ghost' is unused" in e for e in leaked_types(cfg))


def test_single_overload_flagged(tmp_path, pristine_stub):
    mutated = seeded(
        pristine_stub, '@overload\ndef overloaded(value: int) -> int: ...\n', ''
    )
    assert any(
        'single @overload' in e
        for e in structural(make_config(tmp_path, stub_text=mutated))
    )


def test_finality_flags_missing_final(tmp_path, pristine_stub):
    mutated = seeded(pristine_stub, '@final\nclass Sealed:', 'class Sealed:')
    errors = structural(make_config(tmp_path, stub_text=mutated))
    assert any('Sealed' in e and '@final' in e for e in errors)


def test_finality_flags_spurious_final(tmp_path, pristine_stub):
    mutated = seeded(pristine_stub, 'class Open:', '@final\nclass Open:')
    errors = structural(make_config(tmp_path, stub_text=mutated))
    assert any('Open' in e and 'subclassable' in e for e in errors)


def test_match_args_value_drift(tmp_path, pristine_stub):
    mutated = seeded(pristine_stub, "('x', 'y')", "('y', 'x')")
    assert any(
        '__match_args__' in e
        for e in structural(make_config(tmp_path, stub_text=mutated))
    )


def test_doc_contract_flags_missing_runtime_doc(tmp_path, monkeypatch):
    import fakepkg._lib as lib

    monkeypatch.setattr(lib.Sealed.method, '__doc__', None, raising=False)
    assert any('Sealed.method' in e for e in doc_contract(make_config(tmp_path)))


@pytest.mark.parametrize(
    'signature',
    [
        pytest.param('a: Orphan, /', id='positional-only'),
        pytest.param('*rest: Orphan', id='vararg'),
        pytest.param('**kw: Orphan', id='kwarg'),
        pytest.param('a: Orphan', id='positional-or-keyword'),
        pytest.param('*, a: Orphan', id='keyword-only'),
    ],
)
def test_leak_scan_sees_every_parameter_kind(tmp_path, pristine_stub, signature):
    """A leaked pyclass hides just as well behind `/` or `**` as anywhere else.

    Only `args` and `kwonlyargs` were walked, so `posonlyargs`, `*args` and
    `**kwargs` annotations were invisible -- and gometry's stub alone carries
    136 `, /` markers. Each kind is seeded alone: a signature leaking through
    several at once passes on the strength of whichever is already covered.
    """
    mutated = seeded(pristine_stub, _DOCUMENTED, f'def documented({signature}) -> str:')
    errors = leaked_types(make_config(tmp_path, stub_text=mutated, leak_allowlist={}))
    # Not `any('Orphan' in e)`: the fixture's unregistered-pyclass error also
    # names Orphan, so the loose form passed for every kind even when the
    # parameter itself was never walked.
    assert any(
        'documented: annotation references leaked pyclass' in e for e in errors
    ), errors


def test_doc_contract_rejects_an_ellipsis_body_as_prose(tmp_path, pristine_stub):
    """`...` is not a docstring.

    Every `.pyi` body is `...`, which parses to `Expr(Constant(Ellipsis))`.
    Treating any leading constant as prose made `_has_docstring` return True
    for every stub def, so this branch -- a stub-only override must carry its
    own hand-written prose -- could never fire for anyone.
    """
    # `narrowed` exists only in the stub, so it takes the stub-override branch.
    mutated = seeded(
        pristine_stub, _SEALED, f'{_SEALED}\n    def narrowed(self) -> int: ...'
    )
    assert any(
        'Sealed.narrowed: stub override needs its own docstring' in e
        for e in doc_contract(make_config(tmp_path, stub_text=mutated))
    )

    documented = seeded(
        mutated,
        '    def narrowed(self) -> int: ...',
        '    def narrowed(self) -> int:\n        """Narrows the inherited return."""',
    )
    assert doc_contract(make_config(tmp_path, stub_text=documented)) == []


def test_text_signature_flags_param_drift(tmp_path, pristine_src):
    mutated = seeded(
        pristine_src,
        'text_signature = "(a, b=\'x\')"',
        'text_signature = "(a, c=\'x\')"',
    )
    errors = text_signature(make_config(tmp_path, src_text=mutated))
    assert any('text_signature params' in e for e in errors), errors


def test_text_signature_flags_literal_default_drift(tmp_path, pristine_src):
    mutated = seeded(pristine_src, 'b = "x"', 'b = "y"')
    errors = text_signature(make_config(tmp_path, src_text=mutated))
    # Not `any('default' in e)`: both text_signature messages contain that word,
    # so the loose form passed while the gate was emitting the wrong one.
    assert len(errors) == 1, errors
    assert 'text_signature default \'x\' != signature default "y"' in errors[0]


def test_text_signature_ignores_spelling_of_the_same_value(tmp_path, pristine_src):
    """`1_000` and `1000` are one value; only a different value is drift."""
    same = seeded(seeded(pristine_src, 'b = "x"', 'b = 1_000'), "b='x'", 'b=1000')
    assert text_signature(make_config(tmp_path, src_text=same)) == []

    drifted = seeded(seeded(pristine_src, 'b = "x"', 'b = 1_000'), "b='x'", 'b=999')
    assert len(text_signature(make_config(tmp_path, src_text=drifted))) == 1


@pytest.mark.parametrize('literal', [',', '(', ')('])
def test_text_signature_survives_a_delimiter_inside_a_default(
    tmp_path, pristine_src, literal
):
    """A separator inside a Rust string default is not a separator.

    Splitting raw text on commas turned `b = ","` into two parameters, and
    counting parentheses over an unbalanced `"("` ran the scan off the end of
    the attribute and raised `IndexError` with no file or line. The grammar
    knows all three are one string literal.
    """
    src = seeded(
        seeded(pristine_src, 'b = "x"', f'b = "{literal}"'),
        "b='x'",
        f"b='{literal}'",
    )
    assert text_signature(make_config(tmp_path, src_text=src)) == []


def test_text_signature_rejects_a_body_that_is_not_python(tmp_path, pristine_src):
    """CPython's `inspect` parses `__text_signature__` as Python; so do we."""
    src = seeded(
        pristine_src, 'text_signature = "(a, b=\'x\')"', 'text_signature = "(a, b=)"'
    )
    errors = text_signature(make_config(tmp_path, src_text=src))
    assert any('not a valid Python signature' in e for e in errors), errors


@pytest.mark.parametrize(
    'invocation',
    [
        pytest.param('geometry_leaf!(PyPoint, "Point", "doc");', id='bare'),
        pytest.param(
            'crate::leaves::geometry_leaf!(PyPoint, "Point", "doc");', id='scoped'
        ),
        pytest.param(
            'geometry_leaf!(\n    PyPoint,\n    "Point",\n    "doc, with a comma",\n);',
            id='multiline-with-a-comma-in-a-literal',
        ),
    ],
)
def test_a_macro_declared_pyclass_is_found_by_argument_position(tmp_path, invocation):
    """Positions read off the parsed token tree, not a regex over raw text.

    The regex form this replaced decided its semantics by capture-group
    *count*, needed a `(?P<py>...)` convention to be remembered, and matched
    text rather than syntax -- so a comma inside a doc-comment argument could
    shift what it captured.
    """
    cfg = make_config(
        tmp_path,
        src_text=invocation + '\n',
        macro_exports=(MacroExport(macro='geometry_leaf', rust=0, python=1),),
    )
    assert rust_class_map(cfg)['PyPoint'] == 'Point'
    assert pyclass_names(cfg)['Point'].endswith('lib.rs')


def test_a_single_name_macro_export_defaults_rust_to_the_python_name(tmp_path):
    cfg = make_config(
        tmp_path,
        src_text='simple_leaf!("Widget");\n',
        macro_exports=(MacroExport(macro='simple_leaf', python=0),),
    )
    assert rust_class_map(cfg) == {'Widget': 'Widget'}


def test_rust_scan_skips_cfg_test_modules(tmp_path):
    """A pyclass inside a ``#[cfg(test)]`` module never ships; the scan skips it."""
    src = (
        '#[pyclass]\n'
        'struct Shipped;\n'
        '\n'
        '#[cfg(test)]\n'
        'mod tests {\n'
        '    // a "{" inside comments or strings must not break brace matching\n'
        '    const BRACE: &str = "{";\n'
        '\n'
        '    #[pyclass(frozen)]\n'
        '    struct TestOnlyBenchmark;\n'
        '}\n'
    )
    cfg = make_config(tmp_path, src_text=src)
    assert set(rust_class_map(cfg)) == {'Shipped'}
    assert pyclass_names(cfg)['Shipped'].endswith('lib.rs')


def test_rust_scan_ignores_pyclasses_that_are_not_code(tmp_path):
    """A `#[pyclass]` in a comment, a string, or a macro *definition* is not one.

    Regex scanning over raw text saw all three as declarations. The grammar
    classifies them as `line_comment`, `block_comment`, `string_literal` and
    `macro_definition` respectively, so the phantom-export class is gone by
    construction rather than by another exclusion pattern. Measured against this
    exact source, the regex reported all five names; the grammar reports one.
    """
    src = (
        '// #[pyclass] pub struct Commented;\n'
        '/* #[pyclass] pub struct Blocked; */\n'
        'const DOC: &str = "#[pyclass] pub struct Quoted;";\n'
        'macro_rules! leaf {\n'
        '    ($name:ident) => {\n'
        '        #[pyclass]\n'
        '        pub struct Templated;\n'
        '    };\n'
        '}\n'
        '#[pyclass]\n'
        'pub struct Real;\n'
    )
    cfg = make_config(tmp_path, src_text=src)
    assert set(rust_class_map(cfg)) == {'Real'}


def test_rust_scan_does_not_leak_fields_across_a_unit_struct(tmp_path):
    """A unit struct has no body, so it cannot own the next struct's fields.

    Brace scanning started at the unit struct's `;` and found the *following*
    struct's `{`, so `Marker` inherited `Holder`'s `#[pyo3(get)]` field and the
    nullability gate demanded `Marker.value: | None`.
    """
    src = (
        '#[pyclass]\n'
        'pub struct Marker;\n'
        '\n'
        '#[pyclass]\n'
        'pub struct Holder {\n'
        '    #[pyo3(get)]\n'
        '    value: Option<i64>,\n'
        '}\n'
    )
    # Both stub classes declare `value: int`, so the gate has something to
    # report for either one -- an absent member would make the Marker half of
    # this assertion pass whatever the scan attributed to it.
    stub = 'class Marker:\n    value: int\n\nclass Holder:\n    value: int\n'
    errors = rust_nullability(make_config(tmp_path, src_text=src, stub_text=stub))
    assert any('Holder.value' in e for e in errors), errors
    assert [e for e in errors if 'Marker' in e] == []


def test_token_scan_is_bounded_by_its_own_macro(tmp_path, pristine_src):
    """The enum a token macro declares is the one inside its token tree.

    An unbounded forward search adopted the next `enum` anywhere later in the
    file, so an empty macro silently borrowed an unrelated name.
    """
    src = pristine_src + '\ntoken_enum! {}\n\npub enum Unrelated { A }\n'
    errors = gate('token-vocabulary').run(make_config(tmp_path, src_text=src)).errors
    assert errors == []


@pytest.mark.parametrize(
    ('descriptor', 'injected'),
    [
        pytest.param(object.__lt__, False, id='wrapper_descriptor'),
        pytest.param(str.join, True, id='method_descriptor'),
        pytest.param(int.__dict__['real'], True, id='getset_descriptor'),
        pytest.param(dict.__dict__['fromkeys'], True, id='classmethod_descriptor'),
    ],
)
def test_only_cpython_slot_wrappers_keep_their_own_docs(descriptor, injected):
    """The descriptor kind decides, and all four kinds are real C-level ones.

    PyO3 emits methods as `method_descriptor`, getters as `getset_descriptor`
    and class constructors as `classmethod_descriptor` -- those carry the Rust
    `///` prose and must be injected. Only CPython's synthesised protocol slots
    are `wrapper_descriptor`, and their docstrings belong to CPython.

    The fake runtime is pure Python, where every member is a plain function, so
    no fixture member can tell the kinds apart. The stdlib supplies genuine
    instances of each, which is what makes this a test of the rule rather than
    of the fixture.
    """
    from pyo3stubs.gen import _runtime_doc_for_stub

    assert (_runtime_doc_for_stub('member', descriptor) is not None) is injected


def test_gen_docs_leaves_cpython_slot_wrappers_alone(
    tmp_path, pristine_stub, monkeypatch
):
    """A slot's doc belongs to CPython, not to the class that registered it.

    PyO3 exposes protocol methods as type slots; CPython synthesises the wrapper
    and supplies its own docstring, so ``__len__`` reads "Return len(self)."
    whatever the Rust ``///`` said. Injecting that overwrites the stub's own
    prose with boilerplate.

    ``list.__len__`` stands in for the PyO3 slot: a pure-Python ``def __len__``
    is a plain function, and only a C-level slot reproduces the descriptor kind
    that tells the two apart.
    """
    import fakepkg._lib as lib

    monkeypatch.setattr(lib.Sealed, '__len__', list.__len__, raising=False)
    assert lib.Sealed.__len__.__doc__ == 'Return len(self).'

    mutated = seeded(
        pristine_stub,
        _SEALED,
        f'{_SEALED}\n'
        '    def __len__(self) -> int:\n'
        '        """Number of things, as the stub author described it."""',
    )
    rendered = render_stub_with_docs(make_config(tmp_path, stub_text=mutated))

    assert 'Number of things, as the stub author described it.' in rendered
    assert 'Return len(self).' not in rendered


def test_gen_docs_places_docstring_on_last_overload(tmp_path, pristine_stub):
    # Move the docstring onto the FIRST variant; the renderer must put it back
    # on the last (the canonical carrier) and strip earlier variants.
    mutated = seeded(
        seeded(
            pristine_stub,
            'def overloaded(value: int) -> int: ...',
            'def overloaded(value: int) -> int:\n    """Misplaced docstring."""',
        ),
        '    """Scalar-or-batch fixture for overload checks.',
        '    """Prose the renderer must replace.',
    )
    rendered = render_stub_with_docs(make_config(tmp_path, stub_text=mutated))
    assert 'Misplaced docstring' not in rendered, 'stripped from the first variant'
    assert 'Prose the renderer must replace.' not in rendered
    assert (
        'def overloaded(value: str) -> str:\n'
        '    """Scalar-or-batch fixture for overload checks.'
    ) in rendered


def test_gen_docs_is_idempotent(tmp_path):
    """Rendering an already-rendered stub must be a fixed point."""
    once = render_stub_with_docs(make_config(tmp_path))
    twice = render_stub_with_docs(make_config(tmp_path, stub_text=once))
    assert once == twice


def test_return_parity_exempt_silences(tmp_path, pristine_stub, pristine_src):
    build = _duality(
        stub_text=_DUALITY_STUB.replace(
            'def hull(self) -> LeafArray[Wide]:', 'def hull(self) -> Self:'
        ),
        exempt={'hull': 'fixture: deliberate divergence'},
    )
    assert (
        gate('duality').run(build(tmp_path, pristine_stub, pristine_src)).errors == []
    )


def test_return_parity_flags_non_self_on_kind_preserving(
    tmp_path, pristine_stub, pristine_src
):
    build = _duality(
        stub_text=seeded(
            _DUALITY_STUB,
            '    def move(self) -> Self:\n        """Kind-preserving twin."""',
            '    def move(self) -> LeafArray[Wide]:\n        """Kind-preserving twin."""',
        )
    )
    errors = gate('duality').run(build(tmp_path, pristine_stub, pristine_src)).errors
    assert len(errors) == 1
    assert 'LeafArray.move' in errors[0]
    assert 'must return `Self`' in errors[0]


def test_duality_exempt_rot_is_reported(tmp_path, pristine_stub, pristine_src):
    build = _duality(exempt={'absent': 'stale: no such method'})
    errors = gate('duality').run(build(tmp_path, pristine_stub, pristine_src)).errors
    assert any("'absent' is unused" in e for e in errors), errors


def test_gate_test_shape(tmp_path):
    """`gate_test` yields one parametrized test per gate, addressable by name."""
    from pyo3stubs.testing import gate_test

    marks = list(getattr(gate_test(make_config(tmp_path)), 'pytestmark', []))
    assert marks and marks[0].name == 'parametrize'
    assert [param.id for param in marks[0].args[1]] == list(gate_names())


def test_config_path_resolves_from_the_caller_not_the_cwd(tmp_path, monkeypatch):
    """`gate_test('tools/shim.py')` must find the shim from a nested test file.

    The path used to resolve against the working directory, so the same suite
    passed from the repository root and failed from inside `tests/`.
    """
    from pathlib import Path

    from pyo3stubs.testing import _find_upward

    shim = tmp_path / 'tools' / 'shim.py'
    shim.parent.mkdir()
    shim.write_text('')
    deep = tmp_path / 'tests' / 'contracts'
    deep.mkdir(parents=True)
    monkeypatch.chdir(tmp_path.parent)

    assert _find_upward(Path('tools/shim.py'), deep) == shim
    with pytest.raises(FileNotFoundError, match='no such config shim'):
        _find_upward(Path('tools/absent.py'), deep)


def test_a_gate_that_examines_nothing_says_so(tmp_path):
    """Finding no violations and looking at nothing are different outcomes.

    Measured on the real consumers: `text-signature` compares 0 attributes on
    h2corn and lrucache-rs, `leaked-types` sees 0 public pyclasses on h2corn,
    and `rust-nullability` finds 0 Option surfaces on both -- while all three
    printed the same confident success a project with 99 attributes and 32
    surfaces gets.
    """
    cfg = make_config(tmp_path, src_text='#[pyclass]\nstruct Shipped;\n')

    result = gate('text-signature').run(cfg)
    assert result.status is Status.NOTHING_TO_CHECK
    assert 'no text_signature attributes' in result.line

    # ...and a gate that did examine something reports how much.
    populated = gate('text-signature').run(make_config(tmp_path))
    assert populated.status is Status.OK
    assert populated.examined == 1
    assert '(1 text_signature attribute)' in populated.line


def test_every_countable_gate_names_its_subject():
    """A count with no noun is not a report."""
    named = {entry.name for entry in GATES if entry.subject}
    assert named == {
        'doc-structure',
        'structural',
        'text-signature',
        'surface',
        'duality',
        'token-vocabulary',
        'leaked-types',
        'rust-nullability',
        'doc-contract',
    }
