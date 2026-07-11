"""Configuration for PyO3 stub/runtime parity gates.

Tier A gates need only ``module`` / ``stub_path`` / ``src_root``.
Tier B gates activate when their optional nested config is set (empty = no-op),
same pattern as ``surfaces``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class DualityConfig:
    """Scalar↔array return duality (config-activated ``duality`` gate).

    Parameters
    ----------
    pairs:
        ``(scalar_class, array_class)`` stub-class pairs whose same-name
        methods form a scalar↔array duality.
    exempt:
        Method names exempt from the duality return rule (reason per name).
    self_atoms:
        Scalar return atoms treated as kind-preserving besides ``Self``
        (e.g. an element TypeVar free functions thread through).
    """

    pairs: tuple[tuple[str, str], ...]
    exempt: dict[str, str] = field(default_factory=dict)
    self_atoms: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class NamespaceConfig:
    """Namespace facade aliases of ``_lib`` entry points (``namespace-facade`` gate).

    Parameters
    ----------
    package_module:
        Top-level package import path (e.g. ``"gometry"``).
    modules:
        Namespace submodule names re-exporting ``_lib`` entry points.
    prefix_template:
        Format string mapping a namespace to its ``_lib`` name prefix.
        Default ``"{namespace}_"`` matches gometry's convention.
    """

    package_module: str
    modules: tuple[str, ...]
    prefix_template: str = '{namespace}_'


@dataclass(frozen=True)
class TokenConfig:
    """``Literal`` token aliases vs runtime ``token_enum!`` (``token-vocabulary`` gate).

    Parameters
    ----------
    types_module:
        Module carrying ``Literal`` token aliases (e.g. ``"gometry._types"``).
    vocabulary_export:
        Callable on the runtime module returning token vocabulary tuples.
    enum_macro:
        Rust macro name declaring token enums (default ``"token_enum!"``).
    """

    types_module: str
    vocabulary_export: str
    enum_macro: str = 'token_enum!'


@dataclass(frozen=True)
class StubConfig:
    """Everything a PyO3 project must supply to run the gates.

    Parameters
    ----------
    module:
        Import path of the compiled extension (e.g. ``"gometry._lib"``).
    stub_path:
        Path to the hand-authored ``.pyi`` stub.
    src_root:
        Rust ``src/`` directory scanned for ``#[pyclass]`` and macro exports.
    surfaces:
        Cross-surface parity targets as ``(label, owner)`` pairs; empty = no-op.
    known_divergences:
        Deliberate cross-surface option-block divergences keyed by op name.
    leak_allowlist:
        Public ``pyclass`` names exempt from registration (reason per name).
    stubtest_allowlist:
        Optional ``mypy.stubtest`` allowlist file; unused entries fail the run.
    mypy_config:
        Optional mypy config file applied to validity and stubtest.
    mypy_args:
        Extra mypy flags for the stub validity gate.
    uninspectable_allowlist:
        Public runtime callables where ``inspect.signature`` legitimately fails.
    ignored_type_names:
        Annotation names skipped by the leaked-types reachability scan.
    pyclass_patterns:
        Extra ``re.Pattern`` objects for macro-exported class names.
        Group 1 = Python name, or groups 1+2 = ``(rust_ident, py_name)``.
    duality:
        Optional scalar↔array duality config (activates the ``duality`` gate).
    namespace:
        Optional namespace-facade config (activates ``namespace-facade``).
    tokens:
        Optional token-vocabulary config (activates ``token-vocabulary``).
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
    ignored_type_names: frozenset[str] = field(default_factory=frozenset)
    pyclass_patterns: tuple[Any, ...] = ()
    duality: DualityConfig | None = None
    namespace: NamespaceConfig | None = None
    tokens: TokenConfig | None = None
