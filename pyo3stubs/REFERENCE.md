# Configuration and gate reference

This reference describes the `pyo3stubs` configuration fields, CLI gates, and result statuses.

A configuration shim is imported as ordinary Python code and must be trusted. It can expose `config() -> StubConfig` or a `CONFIG` value. Dataclass annotations describe expected values but do not validate or coerce them at runtime. `StubConfig` checks that `stub_path` names a file, `src_root` names a directory, and every disabled gate is known.

`StubConfig` validates its required paths for every gate. Each gate lists the additional tools and runtime/source inputs it reads. Package metadata supplies the installed dependency versions.

Generating this reference also imports the doc renderer to record its public signature, so the `reference` command requires LibCST.

Install and build the consumer extension in the interpreter used for the checks. Gates marked CPython-only report themselves as skipped on other implementations; the gate-specific reason appears below.

## Configuration records

### `DualityConfig`

Scalar↔array return duality (`duality` gate).

#### `DualityConfig.pairs`

- Type: `tuple[tuple[str, str], ...]`
- Default: `required`
- Purpose: `(scalar_class, array_class)` stub-class pairs whose same-name methods form a scalar↔array duality.

#### `DualityConfig.exempt`

- Type: `dict[str, str]`
- Default: `dict()`
- Purpose: Method names exempt from the duality return rule (reason per name).

#### `DualityConfig.self_atoms`

- Type: `frozenset[str]`
- Default: `frozenset()`
- Purpose: Scalar return atoms treated as kind-preserving besides `Self` (e.g. an element TypeVar free functions thread through).

### `MacroExport`

A project macro that declares a `#[pyclass]` the attribute scan cannot see.

#### `MacroExport.macro`

- Type: `str`
- Default: `required`
- Purpose: Macro name without the `!`; a scoped invocation (`crate::x::geometry_leaf!`) matches on the bare name.

#### `MacroExport.python`

- Type: `int`
- Default: `required`
- Purpose: Zero-based argument position holding the Python export name.

#### `MacroExport.rust`

- Type: `int | None`
- Default: `None`
- Purpose: Position holding the Rust identifier; defaults to the Python one for a macro that takes a single name.

### `StubConfig`

Everything a PyO3 project must supply to run the gates.

#### `StubConfig.module`

- Type: `str`
- Default: `required`
- Purpose: Import path of the compiled extension (e.g. `"gometry._lib"`).

#### `StubConfig.stub_path`

- Type: `Path`
- Default: `required`
- Purpose: Path to the hand-authored `.pyi` stub.

#### `StubConfig.src_root`

- Type: `Path`
- Default: `required`
- Purpose: Rust `src/` directory scanned for `#[pyclass]` and macro exports.

#### `StubConfig.leak_allowlist`

- Type: `dict[str, str]`
- Default: `dict()`
- Purpose: Public `pyclass` names exempt from registration (reason per name).

#### `StubConfig.stubtest_allowlist`

- Type: `Path | None`
- Default: `None`
- Purpose: Optional `mypy.stubtest` allowlist file; unused entries fail the run.

#### `StubConfig.mypy_config`

- Type: `Path | None`
- Default: `None`
- Purpose: Optional mypy config file applied to validity and stubtest.

#### `StubConfig.mypy_args`

- Type: `tuple[str, ...]`
- Default: `()`
- Purpose: Extra mypy flags for the stub validity gate.

#### `StubConfig.typecheck_targets`

- Type: `tuple[Path, ...]`
- Default: `()`
- Purpose: Explicit paths checked by validity and pyright. Defaults to the stub.

#### `StubConfig.uninspectable_allowlist`

- Type: `dict[str, str]`
- Default: `dict()`
- Purpose: Public runtime callables or constructible class bindings where `inspect.signature` legitimately fails (reason per name).

#### `StubConfig.doc_structure_allowlist`

- Type: `dict[str, str]`
- Default: `dict()`
- Purpose: Public callables exempt from the `doc-structure` prose rules (reason per qualified name). Unused entries fail the run.

#### `StubConfig.extra_ignored_type_names`

- Type: `frozenset[str]`
- Default: `frozenset()`
- Purpose: Annotation names skipped by the leaked-types scan, *in addition to* `pyo3stubs.config.DEFAULT_IGNORED_TYPE_NAMES`.

#### `StubConfig.macro_exports`

- Type: `tuple[MacroExport, ...]`
- Default: `()`
- Purpose: Project macros that declare `#[pyclass]` types, by argument position.

#### `StubConfig.constructor_doc_macros`

- Type: `frozenset[str]`
- Default: `frozenset()`
- Purpose: Macros that carry a constructor's Rust doc comment on the class `impl` passed to the invocation rather than on a visible `#[new]` item.

#### `StubConfig.text_signature_default_aliases`

- Type: `dict[str, TextSignatureDefault]`
- Default: `dict()`
- Purpose: Exact nonliteral Rust default expressions and their deliberate public text-signature values, with a reason for each presentation. Unused entries fail the text-signature gate.

#### `StubConfig.disabled_gates`

- Type: `frozenset[str]`
- Default: `frozenset()`
- Purpose: Gate names to skip entirely. Reported as disabled, never as passing.

#### `StubConfig.surface`

- Type: `SurfaceConfig | None`
- Default: `None`
- Purpose: Optional cross-surface parity config (activates `surface`).

#### `StubConfig.duality`

- Type: `DualityConfig | None`
- Default: `None`
- Purpose: Optional scalar↔array duality config (activates `duality`).

#### `StubConfig.tokens`

- Type: `TokenConfig | None`
- Default: `None`
- Purpose: Optional token-vocabulary config (activates `token-vocabulary`).

### `SurfaceConfig`

Cross-surface option-block parity (`surface` gate).

#### `SurfaceConfig.targets`

- Type: `tuple[tuple[str, object], ...]`
- Default: `required`
- Purpose: `(label, owner)` pairs — a class or module whose public callables spell the same operations.

#### `SurfaceConfig.known_divergences`

- Type: `dict[str, str]`
- Default: `dict()`
- Purpose: Operations whose option blocks deliberately differ (reason per name).

### `TextSignatureDefault`

One nonliteral Rust default's explicit public text-signature value.

#### `TextSignatureDefault.value`

- Type: `object`
- Default: `required`
- Purpose: Effective Python value shown by the binding's `text_signature`.

#### `TextSignatureDefault.reason`

- Type: `str`
- Default: `required`
- Purpose: Why that public value differs from the nonliteral Rust expression.

### `TokenConfig`

`Literal` token aliases vs runtime `token_enum!` (`token-vocabulary`).

#### `TokenConfig.types_module`

- Type: `str`
- Default: `required`
- Purpose: Module carrying `Literal` token aliases (e.g. `"gometry._types"`).

#### `TokenConfig.vocabulary_export`

- Type: `str`
- Default: `required`
- Purpose: Callable on the runtime module returning token vocabulary tuples.

#### `TokenConfig.enum_macro`

- Type: `str`
- Default: `'token_enum'`
- Purpose: Rust macro declaring token enums, written without the `!`.

#### `TokenConfig.extra_vocabulary`

- Type: `frozenset[str]`
- Default: `frozenset()`
- Purpose: Additional runtime vocabulary names owned outside the configured macro.

## Other public exports

### `DEFAULT_IGNORED_TYPE_NAMES`

Annotation names omitted by the leaked-types scan. Add project-specific names with `StubConfig.extra_ignored_type_names`.

Current names: `Any`, `BaseException`, `Buffer`, `BufferError`, `Callable`, `ClassVar`, `Exception`, `Final`, `Generic`, `IndexError`, `Iterable`, `Iterator`, `Literal`, `Mapping`, `MutableMapping`, `NDArray`, `Protocol`, `RuntimeError`, `Self`, `Sequence`, `StopIteration`, `TypeError`, `TypeVar`, `TypedDict`, `Union`, `ValueError`, `bool`, `bytes`, `dict`, `float`, `frozenset`, `int`, `list`, `np`, `npt`, `numpy`, `object`, `override`, `set`, `str`, `tuple`, `type`.

### `Reasons`

A `dict[str, str]` mapping each allowlisted or deliberately divergent name to its reason.

### `render_stub_with_docs`

`(cfg: 'StubConfig') -> 'str'`

Return the stub source with runtime docstrings injected; does not write.

- Runtime dependency: LibCST (declared for CPython).

## Gates

### `validity`

The stub type-checks on its own (mypy).

- Activation: Always active.
- Additional inputs and tools: `mypy`.
- Runtime: CPython only: mypy does not run on this interpreter.
- Clean result: stub validity OK: mypy-clean.
- Violation remedy: fix the stub until mypy is clean.

### `pyright`

The stub type-checks under pyright.

- Activation: Always active.
- Additional inputs and tools: `pyright and its Node.js-based checker`.
- Runtime: No CPython-only restriction is declared for this gate. Counts `checked file` subjects when applicable.
- Clean result: pyright OK: stub is clean.
- Violation remedy: fix the stub until pyright is clean, under the project's own [tool.pyright] settings.

### `structural`

Overload hygiene, finality, signature coverage, __match_args__.

- Activation: Always active.
- Additional inputs and tools: `compiled extension`.
- Runtime: No CPython-only restriction is declared for this gate. Counts `stub definition` subjects when applicable.
- Clean result: structural checks OK.
- Violation remedy: see each violation; they name the exact stub line.

### `text-signature`

A manual text_signature agrees with the Rust signature attribute.

- Activation: Always active.
- Additional inputs and tools: `Rust source`, `tree-sitter-rust`.
- Runtime: CPython only: the Rust source parser has no PyPy wheel. Counts `text_signature attribute` subjects when applicable.
- Clean result: text_signature OK: matches signature attributes.
- Violation remedy: correct the Rust attribute that documents the wrong shape.

### `surface`

One operation keeps one option block on every surface.

- Activation: Activated by a truthy `StubConfig.surface` value.
- Additional inputs and tools: `configured Python classes or modules`.
- Runtime: No CPython-only restriction is declared for this gate. Counts `shared operation` subjects when applicable.
- Clean result: surface parity OK.
- Violation remedy: add the missing option, or register the divergence with a reason.

### `duality`

Array method returns are derived from the scalar contract.

- Activation: Activated by a truthy `StubConfig.duality` value.
- Additional inputs and tools: `configured class pairs in the stub`.
- Runtime: No CPython-only restriction is declared for this gate. Counts `method pair` subjects when applicable.
- Clean result: scalar↔array duality OK.
- Violation remedy: fix the array return, or exempt it with a reason.

### `token-vocabulary`

Stub Literal unions match the runtime token enums.

- Activation: Activated by a truthy `StubConfig.tokens` value.
- Additional inputs and tools: `Rust source and tree-sitter-rust`, `compiled extension vocabulary export`, `configured Literal module`.
- Runtime: CPython only: the Rust source parser has no PyPy wheel. Counts `token enum` subjects when applicable.
- Clean result: token vocabulary OK.
- Violation remedy: regenerate the Literal alias from the runtime vocabulary.

### `leaked-types`

Every reachable pyclass is registered and publicly stubbed.

- Activation: Always active.
- Additional inputs and tools: `Rust source and tree-sitter-rust`, `compiled extension`.
- Runtime: CPython only: the Rust source parser has no PyPy wheel. Counts `pyclass export` subjects when applicable.
- Clean result: no leaked types.
- Violation remedy: register the class, add a public stub class, or allowlist it.

### `rust-nullability`

Every Option<..> Rust surface admits None in the stub.

- Activation: Always active.
- Additional inputs and tools: `Rust source and tree-sitter-rust`.
- Runtime: CPython only: the Rust source parser has no PyPy wheel. Counts `Option surface` subjects when applicable.
- Clean result: rust nullability OK: every Option surface admits None.
- Violation remedy: widen the stub annotation with `| None`.

### `doc-contract`

Every public symbol is documented, exactly once.

- Activation: Always active.
- Additional inputs and tools: `Rust source and tree-sitter-rust`, `compiled extension`.
- Runtime: CPython only: the Rust source parser has no PyPy wheel. Counts `public symbol` subjects when applicable.
- Clean result: doc contract OK.
- Violation remedy: document it in the Rust `///` comment, or in the stub override.

### `doc-structure`

Documented prose covers callable signatures and descriptor results.

- Activation: Always active.
- Additional inputs and tools: `Rust source and tree-sitter-rust`, `compiled extension`.
- Runtime: CPython only: the Rust source parser has no PyPy wheel. Counts `documented member` subjects when applicable.
- Clean result: doc structure OK.
- Violation remedy: document the parameter, or the return, in the Rust `///`.

### `gen-docs-sync`

Stub docstrings match the runtime ones.

- Activation: Always active.
- Additional inputs and tools: `Rust source and tree-sitter-rust`, `compiled extension`, `LibCST`.
- Runtime: CPython only: the doc generator has no PyPy wheel.
- Clean result: stub docstrings in sync.
- Violation remedy: run `pyo3stubs gen-docs --config <shim>`.

### `stubtest`

The stub matches the compiled runtime (mypy.stubtest).

- Activation: Always active.
- Additional inputs and tools: `mypy.stubtest`, `compiled extension`.
- Runtime: CPython only: mypy does not run on this interpreter.
- Clean result: stubtest OK: stub matches the runtime.
- Violation remedy: fix the stub, or add a stubtest allowlist entry.

## Result statuses

### `ok`

The gate ran and found no violations.

### `violations`

The gate found one or more contract violations.

### `skipped`

The current Python implementation cannot run this gate.

### `disabled`

StubConfig.disabled_gates explicitly disabled this gate.

### `inactive`

The optional gate has no non-empty activating configuration.

### `nothing to check`

The gate ran, but its configured input contained no subjects to inspect.

## Command and pytest outcomes

`pyo3stubs check-all` exits with status 1 when any gate reports a violation and 0 otherwise. A zero exit status does not mean every gate examined a subject: disabled, skipped, inactive, and `nothing to check` outcomes are reported separately and do not change the CLI exit status. The pytest adapter skips those same non-violation outcomes with their reason; only `ok` is a pytest pass.

`pyo3stubs gen-docs` writes the rendered stub even when the `doc-contract` gate reports violations. Its nonzero status reports the contract failure; the generated file remains available for editing.
