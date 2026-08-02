"""Configuration for PyO3 stub/runtime parity gates.

Always-on gates need only ``module`` / ``stub_path`` / ``src_root``. Every
other gate is activated by one optional nested config and is a reported no-op
until it is set — see :mod:`pyo3stubs.gates`, which names the activating field
for each gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

#: Name -> why it is exempt. Every allowlist and every registered divergence
#: carries a reason, so a stale entry can be read and dropped rather than
#: guessed at.
Reasons = dict[str, str]


@dataclass(frozen=True)
class MacroExport:
    """A project macro that declares a ``#[pyclass]`` the attribute scan cannot see.

    gometry writes ``geometry_leaf!(PyPoint, "Point", "docstring")``, so the
    Rust identifier and the Python export name are the invocation's first two
    arguments. Positions, not a regex: the arguments come from the parsed token
    tree, so there is no pattern to get subtly wrong and no capture-group
    convention to remember.

    Parameters
    ----------
    macro:
        Macro name without the ``!``; a scoped invocation
        (``crate::x::geometry_leaf!``) matches on the bare name.
    python:
        Zero-based argument position holding the Python export name.
    rust:
        Position holding the Rust identifier; defaults to the Python one for a
        macro that takes a single name.
    """

    macro: str
    python: int
    rust: int | None = None


@dataclass(frozen=True)
class SurfaceConfig:
    """Cross-surface option-block parity (``surface`` gate).

    Parameters
    ----------
    targets:
        ``(label, owner)`` pairs — a class or module whose public callables
        spell the same operations.
    known_divergences:
        Operations whose option blocks deliberately differ (reason per name).
    """

    targets: tuple[tuple[str, object], ...]
    known_divergences: Reasons = field(default_factory=dict)

    def __bool__(self) -> bool:
        """Whether this config gives its gate anything to check.

        An empty one deactivates the gate rather than passing it: `surface=
        SurfaceConfig(targets=())` compares nothing, and reporting that as
        parity held is the same lie as an unconfigured gate printing success.
        """
        return bool(self.targets)


@dataclass(frozen=True)
class DualityConfig:
    """Scalar↔array return duality (``duality`` gate).

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
    exempt: Reasons = field(default_factory=dict)
    self_atoms: frozenset[str] = field(default_factory=frozenset)

    def __bool__(self) -> bool:
        """A pairless config activates nothing — see `SurfaceConfig.__bool__`."""
        return bool(self.pairs)


@dataclass(frozen=True)
class TokenConfig:
    """``Literal`` token aliases vs runtime ``token_enum!`` (``token-vocabulary``).

    Parameters
    ----------
    types_module:
        Module carrying ``Literal`` token aliases (e.g. ``"gometry._types"``).
    vocabulary_export:
        Callable on the runtime module returning token vocabulary tuples.
    enum_macro:
        Rust macro declaring token enums, written without the ``!``.
    """

    types_module: str
    vocabulary_export: str
    enum_macro: str = 'token_enum'


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
    doc_structure_allowlist:
        Public callables exempt from the ``doc-structure`` prose rules
        (reason per qualified name). Unused entries fail the run.
    extra_ignored_type_names:
        Annotation names skipped by the leaked-types scan, *in addition to*
        :data:`~pyo3stubs.leaked_types.DEFAULT_IGNORED_TYPE_NAMES`.
    macro_exports:
        Project macros that declare ``#[pyclass]`` types, by argument position.
    disabled_gates:
        Gate names to skip entirely. Reported as disabled, never as passing.
    surface:
        Optional cross-surface parity config (activates ``surface``).
    duality:
        Optional scalar↔array duality config (activates ``duality``).
    tokens:
        Optional token-vocabulary config (activates ``token-vocabulary``).
    """

    module: str
    stub_path: Path
    src_root: Path
    leak_allowlist: Reasons = field(default_factory=dict)
    stubtest_allowlist: Path | None = None
    mypy_config: Path | None = None
    mypy_args: tuple[str, ...] = ()
    uninspectable_allowlist: Reasons = field(default_factory=dict)
    doc_structure_allowlist: Reasons = field(default_factory=dict)
    extra_ignored_type_names: frozenset[str] = field(default_factory=frozenset)
    macro_exports: tuple[MacroExport, ...] = ()
    disabled_gates: frozenset[str] = field(default_factory=frozenset)
    surface: SurfaceConfig | None = None
    duality: DualityConfig | None = None
    tokens: TokenConfig | None = None

    def __post_init__(self) -> None:
        """Reject a config that would make gates pass by finding nothing.

        A `src_root` that does not exist is the dangerous one: `Path.rglob`
        yields nothing for a missing directory, so every Rust-scanning gate
        returns no violations and the run goes green having read no files. A
        typo in a config should not be indistinguishable from a clean tree.
        """
        from pyo3stubs.gates import gate_names

        problems = [
            f'{name}={value}: {expected}'
            for name, value, ok, expected in (
                ('stub_path', self.stub_path, self.stub_path.is_file(), 'not a file'),
                ('src_root', self.src_root, self.src_root.is_dir(), 'not a directory'),
            )
            if not ok
        ]
        unknown = sorted(self.disabled_gates - set(gate_names()))
        if unknown:
            problems.append(f'disabled_gates: no such gate(s): {", ".join(unknown)}')
        if problems:
            raise ValueError('StubConfig: ' + '; '.join(problems))
