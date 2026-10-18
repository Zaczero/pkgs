# Configuration and gate reference

This reference describes the `pyo3stubs` configuration fields, CLI gates, and result statuses.

Your configuration file exposes `config() -> StubConfig` or a `CONFIG` value. It runs as ordinary Python code, so load only trusted files. `stub_path` must name an existing file and `src_root` an existing directory.

The `reference` command requires LibCST.

Build and install your extension in the environment running the checks. See the [quickstart](README.md#first-check) for setup.

## Configuration records

### `DualityConfig`

Scalar↔array return duality (`duality` gate).

#### `DualityConfig.pairs`

`tuple[tuple[str, str], ...]` · Required

`(scalar_class, array_class)` stub-class pairs whose same-name methods form a scalar↔array duality.

#### `DualityConfig.exempt`

`dict[str, str]` · Default: `dict()`

Method names exempt from the duality return rule (reason per name).

#### `DualityConfig.self_atoms`

`frozenset[str]` · Default: `frozenset()`

Scalar return atoms treated as kind-preserving besides `Self` (e.g. an element TypeVar free functions thread through).

### `MacroExport`

A project macro that declares a `#[pyclass]` the attribute scan cannot see.

#### `MacroExport.macro`

`str` · Required

Macro name without the `!`; a scoped invocation (`crate::x::geometry_leaf!`) matches on the bare name.

#### `MacroExport.python`

`int` · Required

Zero-based argument position holding the Python export name.

#### `MacroExport.rust`

`int | None` · Default: `None`

Position holding the Rust identifier; defaults to the Python one for a macro that takes a single name.

### `StubConfig`

Everything a PyO3 project must supply to run the gates.

#### `StubConfig.module`

`str` · Required

Import path of the compiled extension (e.g. `"gometry._lib"`).

#### `StubConfig.stub_path`

`Path` · Required

Path to the hand-authored `.pyi` stub.

#### `StubConfig.src_root`

`Path` · Required

Rust `src/` directory scanned for `#[pyclass]` and macro exports.

#### `StubConfig.leak_allowlist`

`dict[str, str]` · Default: `dict()`

Public `pyclass` names exempt from registration (reason per name).

#### `StubConfig.stubtest_allowlist`

`Path | None` · Default: `None`

Optional `mypy.stubtest` allowlist file; unused entries fail the run.

#### `StubConfig.mypy_config`

`Path | None` · Default: `None`

Optional mypy config file applied to validity and stubtest.

#### `StubConfig.mypy_args`

`tuple[str, ...]` · Default: `()`

Extra mypy flags for the stub validity gate.

#### `StubConfig.typecheck_targets`

`tuple[Path, ...]` · Default: `()`

Explicit paths checked by validity and pyright. Defaults to the stub.

#### `StubConfig.uninspectable_allowlist`

`dict[str, str]` · Default: `dict()`

Public runtime callables or constructible class bindings where `inspect.signature` legitimately fails (reason per name).

#### `StubConfig.doc_structure_allowlist`

`dict[str, str]` · Default: `dict()`

Public callables exempt from the `doc-structure` prose rules (reason per qualified name). Unused entries fail the run.

#### `StubConfig.docstring_style`

`Literal[numpy, google] | None` · Default: `None`

Required NumPy or Google section style, or `None` to accept either. Checked on runtime documentation and Python source and stub docstrings beneath `stub_path.parent`, including short docstrings with sections.

#### `StubConfig.extra_ignored_type_names`

`frozenset[str]` · Default: `frozenset()`

Annotation names skipped by the leaked-types scan, *in addition to* `pyo3stubs.config.DEFAULT_IGNORED_TYPE_NAMES`.

#### `StubConfig.macro_exports`

`tuple[MacroExport, ...]` · Default: `()`

Project macros that declare `#[pyclass]` types, by argument position.

#### `StubConfig.constructor_doc_macros`

`frozenset[str]` · Default: `frozenset()`

Macros that carry a constructor's Rust doc comment on the class `impl` passed to the invocation rather than on a visible `#[new]` item.

#### `StubConfig.text_signature_default_aliases`

`dict[str, TextSignatureDefault]` · Default: `dict()`

Exact nonliteral Rust default expressions and their deliberate public text-signature values, with a reason for each presentation. Unused entries fail the text-signature gate.

#### `StubConfig.disabled_gates`

`frozenset[str]` · Default: `frozenset()`

Gate names to skip entirely. Reported as disabled, never as passing.

#### `StubConfig.surface`

`SurfaceConfig | None` · Default: `None`

Optional cross-surface parity config (activates `surface`).

#### `StubConfig.duality`

`DualityConfig | None` · Default: `None`

Optional scalar↔array duality config (activates `duality`).

#### `StubConfig.tokens`

`TokenConfig | None` · Default: `None`

Optional token-vocabulary config (activates `token-vocabulary`).

### `SurfaceConfig`

Cross-surface option-block parity (`surface` gate).

#### `SurfaceConfig.targets`

`tuple[tuple[str, object], ...]` · Required

`(label, owner)` pairs — a class or module whose public callables spell the same operations.

#### `SurfaceConfig.known_divergences`

`dict[str, str]` · Default: `dict()`

Operations whose option blocks deliberately differ (reason per name).

### `TextSignatureDefault`

One nonliteral Rust default's explicit public text-signature value.

#### `TextSignatureDefault.value`

`object` · Required

Effective Python value shown by the binding's `text_signature`.

#### `TextSignatureDefault.reason`

`str` · Required

Why that public value differs from the nonliteral Rust expression.

### `TokenConfig`

`Literal` token aliases vs runtime `token_enum!` (`token-vocabulary`).

#### `TokenConfig.types_module`

`str` · Required

Module carrying `Literal` token aliases (e.g. `"gometry._types"`).

#### `TokenConfig.vocabulary_export`

`str` · Required

Callable on the runtime module returning token vocabulary tuples.

#### `TokenConfig.enum_macro`

`str` · Default: `'token_enum'`

Rust macro declaring token enums, written without the `!`.

#### `TokenConfig.extra_vocabulary`

`frozenset[str]` · Default: `frozenset()`

Additional runtime vocabulary names owned outside the configured macro.

## Other public exports

### `DEFAULT_IGNORED_TYPE_NAMES`

Annotation names omitted by the leaked-types scan. Add project-specific names with `StubConfig.extra_ignored_type_names`.

Current names: `Any`, `BaseException`, `Buffer`, `BufferError`, `Callable`, `ClassVar`, `Exception`, `Final`, `Generic`, `IndexError`, `Iterable`, `Iterator`, `Literal`, `Mapping`, `MutableMapping`, `NDArray`, `Protocol`, `RuntimeError`, `Self`, `Sequence`, `StopIteration`, `TypeError`, `TypeVar`, `TypedDict`, `Union`, `ValueError`, `bool`, `bytes`, `dict`, `float`, `frozenset`, `int`, `list`, `np`, `npt`, `numpy`, `object`, `override`, `set`, `str`, `tuple`, `type`.

### `Reasons`

A `dict[str, str]` mapping each allowlisted or deliberately divergent name to its reason.

### `render_stub_with_docs`

`render_stub_with_docs(cfg: 'StubConfig') -> 'str'`

Return the stub source with runtime docstrings injected; does not write.

- Runtime dependency: LibCST.

## Gates

Gates run by default unless they require an optional configuration below.

### `validity`

The stub type-checks on its own (mypy).

- Requires: `mypy`.
- On failure: fix the stub until mypy is clean.

### `pyright`

The stub type-checks under pyright.

- Requires: `pyright and its Node.js-based checker`.
- On failure: fix the stub until pyright is clean, under the project's own [tool.pyright] settings.

### `structural`

Overload hygiene, finality, signature coverage, __match_args__.

- Requires: `compiled extension`.
- On failure: see each violation; they name the exact stub line.

### `text-signature`

A manual text_signature agrees with the Rust signature attribute.

- Requires: `Rust source`, `tree-sitter-rust`.
- On failure: correct the Rust attribute that documents the wrong shape.

### `surface`

One operation keeps one option block on every surface.

- Enable with `StubConfig.surface`.
- Requires: `configured Python classes or modules`.
- On failure: add the missing option, or register the divergence with a reason.

### `duality`

Array method returns are derived from the scalar contract.

- Enable with `StubConfig.duality`.
- Requires: `configured class pairs in the stub`.
- On failure: fix the array return, or exempt it with a reason.

### `token-vocabulary`

Stub Literal unions match the runtime token enums.

- Enable with `StubConfig.tokens`.
- Requires: `Rust source and tree-sitter-rust`, `compiled extension vocabulary export`, `configured Literal module`.
- On failure: regenerate the Literal alias from the runtime vocabulary.

### `leaked-types`

Every reachable pyclass is registered and publicly stubbed.

- Requires: `Rust source and tree-sitter-rust`, `compiled extension`.
- On failure: register the class, add a public stub class, or allowlist it.

### `rust-nullability`

Every Option<..> Rust surface admits None in the stub.

- Requires: `Rust source and tree-sitter-rust`.
- On failure: widen the stub annotation with `| None`.

### `doc-contract`

Every public symbol is documented, exactly once.

- Requires: `Rust source and tree-sitter-rust`, `compiled extension`.
- On failure: document it in the Rust `///` comment, or in the stub override.

### `doc-structure`

Documented prose covers callable signatures and descriptor results.

- Requires: `Rust source and tree-sitter-rust`, `compiled extension`.
- On failure: document the parameter, or the return, in the Rust `///`.

### `gen-docs-sync`

Stub docstrings match the runtime ones.

- Requires: `Rust source and tree-sitter-rust`, `compiled extension`, `LibCST`.
- On failure: run `pyo3stubs gen-docs --config <shim>`.

### `stubtest`

The stub matches the compiled runtime (mypy.stubtest).

- Requires: `mypy.stubtest`, `compiled extension`.
- On failure: fix the stub, or add a stubtest allowlist entry.

## Result statuses

### `ok`

The gate ran and found no violations.

### `violations`

The gate found one or more contract violations.

### `disabled`

StubConfig.disabled_gates explicitly disabled this gate.

### `inactive`

The optional gate has no non-empty activating configuration.

### `nothing to check`

The gate ran, but its configured input contained no subjects to inspect.

## Command and pytest outcomes

`pyo3stubs check-all` exits with status 1 when any gate reports a violation and 0 otherwise. A zero exit status does not mean every gate examined a subject: disabled, inactive, and `nothing to check` outcomes are reported separately and do not change the CLI exit status. The pytest adapter skips those same non-violation outcomes with their reason; only `ok` is a pytest pass.

`pyo3stubs gen-docs` writes the rendered stub even when the `doc-contract` gate reports violations. Its nonzero status reports the contract failure; the generated file remains available for editing.
