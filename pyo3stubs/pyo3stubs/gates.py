"""Single gate registry: CLI, pytest, and check-all share this list.

Every gate is one :class:`Gate` record. Adding a monorepo standard means adding
one entry here — not a plugin bag, and not three parallel tables keyed by the
same names, which is how a gate could once be registered with no success
message and greet its user with a ``KeyError``.

Two properties this file is responsible for:

**A gate that cannot run says so.** Skipped (wrong interpreter), disabled, and
inactive (its config is unset) are distinct outcomes, and none of them prints
success. ``check-all`` on PyPy used to drop the two strongest gates in silence.

**A gate that crashes is a violation, not an abort.** Collectors are wrapped at
this boundary, so one broken gate reports itself with its raising frame and the
other eleven still run — and the exit status is still non-zero.
"""

from __future__ import annotations

import platform
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from pyo3stubs.doc_contract import collect_errors as collect_doc_contract_errors
from pyo3stubs.doc_structure import collect_errors as collect_doc_structure_errors
from pyo3stubs.duality import collect_errors as collect_duality_errors
from pyo3stubs.leaked_types import collect_errors as collect_leaked_types_errors
from pyo3stubs.oracle import collect_stubtest_errors, collect_validity_errors
from pyo3stubs.report import Findings
from pyo3stubs.rust_nullability import collect_errors as collect_rust_nullability_errors
from pyo3stubs.structural import collect_errors as collect_structural_errors
from pyo3stubs.surface import collect_errors as collect_surface_errors
from pyo3stubs.text_signature import collect_errors as collect_text_signature_errors
from pyo3stubs.token_vocabulary import collect_errors as collect_token_vocabulary_errors

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

Collector = Callable[['StubConfig'], list[str]]


class Status(Enum):
    """Why a gate produced the result it did."""

    OK = 'ok'
    VIOLATIONS = 'violations'
    #: The interpreter cannot run it (mypy does not run on PyPy).
    SKIPPED = 'skipped'
    #: ``StubConfig.disabled_gates`` turned it off.
    DISABLED = 'disabled'
    #: Its activating config field is unset, so there is nothing to check.
    INACTIVE = 'inactive'
    #: Active and configured, but this project has none of what it inspects.
    NOTHING_TO_CHECK = 'nothing to check'


def _plural(noun: str, count: int) -> str:
    return noun if count == 1 else f'{noun}s'


@dataclass(frozen=True, slots=True)
class Gate:
    """One check: what it is, what turns it on, and what to do when it fails."""

    name: str
    collect: Collector
    #: One line for ``--help``: what the gate proves.
    summary: str
    #: Printed when the gate is clean.
    success: str
    #: What the user should do about a violation.
    remedy: str
    #: Singular noun for what the gate inspects, for the "n examined" report.
    #: Every one of these pluralises with a plain `s`.
    subject: str = ''
    #: ``StubConfig`` field that activates it; empty means always-on. The
    #: field activates the gate when it is set *and* non-empty.
    activated_by: str = ''
    #: Backed by mypy, which does not run outside CPython.
    requires_cpython: bool = False

    @property
    def tier(self) -> str:
        return 'always-on' if not self.activated_by else f'needs {self.activated_by}'

    def blocked_by(self, cfg: StubConfig) -> Status | None:
        """Why this gate will not run against ``cfg``, or None if it will."""
        if self.name in cfg.disabled_gates:
            return Status.DISABLED
        if self.requires_cpython and platform.python_implementation() != 'CPython':
            return Status.SKIPPED
        if self.activated_by and not getattr(cfg, self.activated_by):
            return Status.INACTIVE
        return None

    def run(self, cfg: StubConfig) -> Result:
        """Run the gate, turning any crash into a violation of its own."""
        blocked = self.blocked_by(cfg)
        if blocked is not None:
            return Result(self, blocked, [], None)
        try:
            errors = self.collect(cfg)
        except Exception as exc:
            frame = traceback.extract_tb(exc.__traceback__)[-1]
            return Result(
                self,
                Status.VIOLATIONS,
                [
                    f'{self.name}: gate could not run: {type(exc).__name__}: {exc} '
                    f'(at {frame.filename}:{frame.lineno})'
                ],
                None,
            )
        examined = errors.examined if isinstance(errors, Findings) else None
        if errors:
            return Result(self, Status.VIOLATIONS, errors, examined)
        status = Status.NOTHING_TO_CHECK if examined == 0 else Status.OK
        return Result(self, status, errors, examined)


@dataclass(frozen=True, slots=True)
class Result:
    """One gate's outcome against one config."""

    gate: Gate
    status: Status
    errors: list[str]
    #: Subjects the gate inspected; None where counting does not apply.
    examined: int | None = None

    @property
    def line(self) -> str:
        """One-line summary, whatever the outcome."""
        if self.status is Status.OK:
            counted = (
                f' ({self.examined} {_plural(self.gate.subject, self.examined)})'
                if self.examined is not None
                else ''
            )
            return f'{self.gate.name}: {self.gate.success}{counted}'
        if self.status is Status.NOTHING_TO_CHECK:
            return (
                f'{self.gate.name}: nothing to check — this project has no '
                f'{_plural(self.gate.subject, 0)}'
            )
        if self.status is Status.SKIPPED:
            return (
                f'{self.gate.name}: skipped — needs CPython, and mypy does not '
                f'run on {platform.python_implementation()}'
            )
        if self.status is Status.DISABLED:
            return f'{self.gate.name}: disabled by StubConfig.disabled_gates'
        if self.status is Status.INACTIVE:
            return (
                f'{self.gate.name}: inactive — set a non-empty '
                f'StubConfig.{self.gate.activated_by} to run it'
            )
        return f'{self.gate.name}: {len(self.errors)} problem(s)'


def _gen_docs_sync(cfg: StubConfig) -> list[str]:
    from pyo3stubs.context import CheckContext
    from pyo3stubs.gen import render_stub_with_docs

    if CheckContext(cfg).stub_text == render_stub_with_docs(cfg):
        return []
    return [f'{cfg.stub_path}: stub docstrings are stale']


_GEN_DOCS = 'run `pyo3stubs gen-docs --config <shim>`'

#: Every gate, in the order they run.
GATES: tuple[Gate, ...] = (
    Gate(
        name='validity',
        collect=collect_validity_errors,
        summary='the stub type-checks on its own (mypy)',
        success='stub validity OK: mypy-clean',
        remedy='fix the stub until mypy is clean',
        requires_cpython=True,
    ),
    Gate(
        name='structural',
        collect=collect_structural_errors,
        summary='overload hygiene, finality, signature coverage, __match_args__',
        success='structural checks OK',
        remedy='see each violation; they name the exact stub line',
        subject='stub definition',
    ),
    Gate(
        name='text-signature',
        collect=collect_text_signature_errors,
        summary='a manual text_signature agrees with the real signature',
        success='text_signature OK: matches signature attributes',
        remedy='correct the Rust attribute that documents the wrong shape',
        subject='text_signature attribute',
    ),
    Gate(
        name='surface',
        collect=collect_surface_errors,
        summary='one operation keeps one option block on every surface',
        success='surface parity OK',
        remedy='add the missing option, or register the divergence with a reason',
        activated_by='surface',
        subject='shared operation',
    ),
    Gate(
        name='duality',
        collect=collect_duality_errors,
        summary='array method returns are derived from the scalar contract',
        success='scalar↔array duality OK',
        remedy='fix the array return, or exempt it with a reason',
        activated_by='duality',
        subject='method pair',
    ),
    Gate(
        name='token-vocabulary',
        collect=collect_token_vocabulary_errors,
        summary='stub Literal unions match the runtime token enums',
        success='token vocabulary OK',
        remedy='regenerate the Literal alias from the runtime vocabulary',
        activated_by='tokens',
        subject='token enum',
    ),
    Gate(
        name='leaked-types',
        collect=collect_leaked_types_errors,
        summary='every reachable pyclass is registered and publicly stubbed',
        success='no leaked types',
        remedy='register the class, add a public stub class, or allowlist it',
        subject='pyclass export',
    ),
    Gate(
        name='rust-nullability',
        collect=collect_rust_nullability_errors,
        summary='every Option<..> Rust surface admits None in the stub',
        success='rust nullability OK: every Option surface admits None',
        remedy='widen the stub annotation with `| None`',
        subject='Option surface',
    ),
    Gate(
        name='doc-contract',
        collect=collect_doc_contract_errors,
        summary='every public symbol is documented, exactly once',
        success='doc contract OK',
        remedy='document it in the Rust `///` comment, or in the stub override',
        subject='public symbol',
    ),
    Gate(
        name='doc-structure',
        collect=collect_doc_structure_errors,
        summary='documented prose describes the whole signature',
        success='doc structure OK',
        remedy='document the parameter, or the return, in the Rust `///`',
        subject='documented callable',
    ),
    Gate(
        name='gen-docs-sync',
        collect=_gen_docs_sync,
        summary='stub docstrings match the runtime ones',
        success='stub docstrings in sync',
        remedy=_GEN_DOCS,
    ),
    Gate(
        name='stubtest',
        collect=collect_stubtest_errors,
        summary='the stub matches the compiled runtime (mypy.stubtest)',
        success='stubtest OK: stub matches the runtime',
        remedy='fix the stub, or add a stubtest allowlist entry',
        requires_cpython=True,
    ),
)


def gate_names() -> tuple[str, ...]:
    """Every registered gate name, in registry order."""
    return tuple(gate.name for gate in GATES)


def gate(name: str) -> Gate:
    """One gate by name; raises ``KeyError`` for an unknown one."""
    for entry in GATES:
        if entry.name == name:
            return entry
    raise KeyError(name)


def run_all(cfg: StubConfig) -> tuple[Result, ...]:
    """Run every gate in registry order."""
    return tuple(entry.run(cfg) for entry in GATES)
