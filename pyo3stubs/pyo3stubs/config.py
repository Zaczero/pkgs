"""Configuration for PyO3 stub/runtime parity gates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from pyo3stubs.protocols import Check


@dataclass(frozen=True)
class StubConfig:
    """Everything a PyO3 project must supply to run the universal gates.

    Parameters
    ----------
    module:
        Import path of the compiled extension (e.g. ``"shaper._lib"``).
    stub_path:
        Path to the hand-authored ``.pyi`` stub.
    src_root:
        Rust ``src/`` directory scanned for ``#[pyclass]`` and macro exports.
    surfaces:
        Cross-surface parity targets as ``(label, owner)`` pairs where
        *owner* is the runtime module or class object to inspect.
    known_divergences:
        Deliberate cross-surface option-block divergences keyed by op name.
    leak_allowlist:
        Public ``pyclass`` names exempt from registration (reason per name).
    stubtest_allowlist:
        Optional ``mypy.stubtest`` allowlist file (one error-summary regex per
        line); entries that stop matching fail the run, so the list cannot rot.
    mypy_config:
        Optional mypy config file (e.g. the project ``pyproject.toml``) applied
        to both the validity gate and stubtest, so error-code policy lives in
        one place.
    mypy_args:
        Extra mypy flags for the stub validity gate (e.g. a python-version pin).
    uninspectable_allowlist:
        Public runtime callables where ``inspect.signature`` legitimately fails
        (reason per qualname); anything else uninspectable is a hard error.
    plugins:
        Optional project-specific checks (token vocab, namespace facades, …).
    package_module:
        Top-level package import path for namespace-facade checks
        (e.g. ``"shaper"``).
    types_module:
        Module carrying ``Literal`` token aliases (e.g. ``"shaper._types"``).
    namespace_modules:
        Namespace submodule names re-exporting ``_lib`` entry points.
    namespace_prefix_template:
        Format string mapping a namespace to its ``_lib`` name prefix.
        Default ``"{namespace}_"`` matches shaper's convention.
    token_vocabulary_export:
        Callable on the runtime module returning token vocabulary tuples.
    token_enum_macro:
        Rust macro name declaring token enums (default ``"token_enum!"``).
    ignored_type_names:
        Annotation names skipped by the leaked-types reachability scan.
    pyclass_patterns:
        Extra ``re.Pattern`` objects scanning ``src_root`` for exported
        Python class names beyond ``#[pyclass]`` attributes.
    duality_pairs:
        ``(scalar_class, array_class)`` stub-class pairs whose same-name
        methods form a scalar<->array duality: the array method's return must
        be ``Self`` exactly when the scalar's is kind-preserving
        (``Self``/element TypeVar), else ``ArrayClass[<scalar return>]``.
    duality_exempt:
        Method names exempt from the duality return rule (reason per name).
    duality_self_atoms:
        Scalar return atoms treated as kind-preserving besides ``Self``
        (e.g. the element TypeVar free functions thread through).
    """

    module: str
    stub_path: Path
    src_root: Path
    surfaces: tuple[tuple[str, Any], ...] = ()
    known_divergences: dict[str, str] = field(default_factory=dict)
    leak_allowlist: dict[str, str] = field(default_factory=dict)
    stubtest_allowlist: Path | None = None
    mypy_config: Path | None = None
    mypy_args: tuple[str, ...] = ()
    uninspectable_allowlist: dict[str, str] = field(default_factory=dict)
    plugins: tuple[Check, ...] = ()
    package_module: str | None = None
    types_module: str | None = None
    namespace_modules: tuple[str, ...] = ()
    namespace_prefix_template: str = '{namespace}_'
    token_vocabulary_export: str | None = None
    token_enum_macro: str = 'token_enum!'
    ignored_type_names: frozenset[str] = field(default_factory=frozenset)
    pyclass_patterns: tuple[Any, ...] = ()
    duality_pairs: tuple[tuple[str, str], ...] = ()
    duality_exempt: dict[str, str] = field(default_factory=dict)
    duality_self_atoms: frozenset[str] = field(default_factory=frozenset)
